//! M2tsStream — BD transport stream with embedded metadata header.
//!
//! Write: prepends FMKV metadata header, then passes through BD-TS bytes.
//! Read: extracts metadata header (or scans PMT), then yields BD-TS bytes.

use super::{meta, ts, IOStream};
use crate::disc::{DiscTitle, Stream as DiscStream};
use std::io::{self, Read, Write};

type PesSetup = (Vec<u16>, Vec<(u16, Box<dyn super::codec::CodecParser>)>, Vec<(u16, usize)>);

/// Size of initial scan buffer for PMT/stream detection.
const SCAN_SIZE: usize = 1024 * 1024;

enum Mode {
    Write {
        writer: Box<dyn Write>,
        header_written: bool,
    },
    Read {
        reader: Box<dyn Read>,
    },
}

/// Read as many bytes as possible into buf (multiple read calls if needed).
/// Bounded by buf.len() — caller controls max bytes read.
fn read_fill(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match r.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

/// BD transport stream with embedded metadata.
pub struct M2tsStream {
    disc_title: DiscTitle,
    mode: Mode,
    finished: bool,
    /// Content size in bytes (file size minus header), set for read mode.
    content_size: Option<u64>,
    // PES support
    demuxer: Option<ts::TsDemuxer>,
    parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    pending_frames: std::collections::VecDeque<crate::pes::PesFrame>,
    pid_to_track: Vec<(u16, usize)>,
    pes_eof: bool,
    /// Codec private data per stream (from FMKV header).
    stored_codec_privates: Vec<Option<Vec<u8>>>,
}

impl M2tsStream {
    /// Create for writing. Metadata header is written on first write().
    pub fn new(writer: impl Write + 'static) -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            mode: Mode::Write {
                writer: Box::new(writer),
                header_written: false,
            },
            finished: false,
            content_size: None,
            demuxer: None,
            parsers: Vec::new(),
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track: Vec::new(),
            pes_eof: false,
            stored_codec_privates: Vec::new(),
        }
    }

    fn setup_pes(streams: &[DiscStream]) -> PesSetup {
        let mut pids = Vec::new();
        let mut parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)> = Vec::new();
        let mut pid_to_track = Vec::new();
        for (i, s) in streams.iter().enumerate() {
            let (pid, codec) = match s {
                DiscStream::Video(v) => (v.pid, v.codec),
                DiscStream::Audio(a) => (a.pid, a.codec),
                DiscStream::Subtitle(s) => (s.pid, s.codec),
            };
            pids.push(pid);
            pid_to_track.push((pid, i));
            parsers.push((pid, super::codec::parser_for_codec(codec)));
        }
        (pids, parsers, pid_to_track)
    }

    /// Set stream metadata. Returns self for chaining.
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    /// Open an M2TS stream for reading. Takes any Read source — file, pipe, socket.
    ///
    /// Tries FMKV metadata header first. Falls back to PMT scan of first 1 MB.
    pub fn open(mut reader: impl Read + 'static) -> io::Result<Self> {
        // Read first chunk — enough for FMKV header or PMT scan
        let mut head = vec![0u8; SCAN_SIZE];
        let head_len = read_fill(&mut reader, &mut head)?;
        head.truncate(head_len);

        // Try FMKV metadata header from the buffered head
        let mut cursor = io::Cursor::new(&head);
        if let Ok(Some(m)) = meta::read_header(&mut cursor) {
            let header_end = cursor.position() as usize;
            let title = m.to_title();
            let (pids, parsers, pid_to_track) = Self::setup_pes(&title.streams);
            // Chain: remaining head bytes + rest of reader
            let remaining_head = &head[header_end..];
            let chain: Box<dyn Read> = Box::new(io::Cursor::new(remaining_head.to_vec()).chain(reader));
            return Ok(Self {
                disc_title: title.clone(),
                mode: Mode::Read { reader: chain },
                finished: false,
                content_size: None,
                demuxer: if pids.is_empty() { None } else { Some(ts::TsDemuxer::new(&pids)) },
                parsers,
                pending_frames: std::collections::VecDeque::new(),
                pid_to_track,
                pes_eof: false,
                stored_codec_privates: title.codec_privates,
            });
        }

        // No FMKV header — scan head for PMT
        let streams = ts::scan_streams(&head)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no streams found"))?;

        let (pids, parsers, pid_to_track) = Self::setup_pes(&streams);

        // Chain: full head (it's all TS data) + rest of reader
        let chain: Box<dyn Read> = Box::new(io::Cursor::new(head).chain(reader));

        Ok(Self {
            disc_title: DiscTitle {
                duration_secs: 0.0, // unknown without seeking
                streams,
                ..DiscTitle::empty()
            },
            mode: Mode::Read { reader: chain },
            finished: false,
            content_size: None,
            demuxer: if pids.is_empty() { None } else { Some(ts::TsDemuxer::new(&pids)) },
            parsers,
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track,
            pes_eof: false,
            stored_codec_privates: Vec::new(),
        })
    }
}

impl crate::pes::Stream for M2tsStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(Some(frame));
        }
        if self.pes_eof { return Ok(None); }

        loop {
            let reader = match &mut self.mode {
                Mode::Read { reader } => reader,
                _ => return Err(io::Error::new(io::ErrorKind::Unsupported, "not in read mode")),
            };
            let mut buf = vec![0u8; 192 * 1024];
            let n = reader.read(&mut buf)?;
            if n == 0 {
                self.pes_eof = true;
                return Ok(None);
            }

            if let Some(ref mut demuxer) = self.demuxer {
                let packets = demuxer.feed(&buf[..n]);
                for pes in &packets {
                    if let Some((_, track)) = self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid) {
                        if let Some((_, parser)) = self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid) {
                            for frame in parser.parse(pes) {
                                self.pending_frames.push_back(
                                    crate::pes::PesFrame::from_codec_frame(*track, frame)
                                );
                            }
                        }
                    }
                }
            }

            if let Some(frame) = self.pending_frames.pop_front() {
                return Ok(Some(frame));
            }
        }
    }

    fn write(&mut self, _frame: &crate::pes::PesFrame) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "use M2tsOutputStream for writing"))
    }

    fn finish(&mut self) -> io::Result<()> { Ok(()) }

    fn info(&self) -> &crate::disc::DiscTitle { &self.disc_title }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        // First check stored codec_privates from FMKV header
        if let Some(Some(cp)) = self.stored_codec_privates.get(track) {
            return Some(cp.clone());
        }
        // Fall back to parser-extracted codec_private
        let pid = self.pid_to_track.iter()
            .find(|(_, idx)| *idx == track)
            .map(|(pid, _)| *pid)?;
        self.parsers.iter()
            .find(|(p, _)| *p == pid)
            .and_then(|(_, parser)| parser.codec_private())
    }

    fn headers_ready(&self) -> bool {
        for (idx, s) in self.disc_title.streams.iter().enumerate() {
            if let crate::disc::Stream::Video(v) = s {
                if !v.secondary && self.codec_private(idx).is_none() {
                    return false;
                }
            }
        }
        true
    }
}

impl IOStream for M2tsStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()
        } else {
            Ok(())
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        self.content_size
    }
}

impl Write for M2tsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Write {
                ref mut writer,
                ref mut header_written,
            } => {
                if !*header_written {
                    if !self.disc_title.streams.is_empty() {
                        let m = meta::M2tsMeta::from_title(&self.disc_title);
                        meta::write_header(&mut *writer, &m)?;
                    }
                    *header_written = true;
                }
                writer.write(buf)
            }
            Mode::Read { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for reading",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

impl Read for M2tsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Read { ref mut reader } => reader.read(buf),
            Mode::Write { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for writing",
            )),
        }
    }
}
