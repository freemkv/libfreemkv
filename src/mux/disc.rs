//! DiscStream — read any disc (physical drive or ISO file) → PES frames.
//!
//! One stream type for all disc sources. The source is a SectorReader —
//! Drive (hardware) or IsoSectorReader (file). DiscStream doesn't care.
//!
//! Read-only. For disc→ISO (raw sector copy), use `Disc::copy()`.

use crate::disc::{
    detect_max_batch_sectors, Disc, DiscTitle, Extent, ScanOptions,
};
use crate::sector::SectorReader;
use std::io;

/// Disc stream. Reads sectors from any source → PES frames.
///
/// Sources: physical drive, ISO file, or any SectorReader.
/// Decrypt, demux, and codec parsing happen internally.
pub struct DiscStream {
    reader: Box<dyn SectorReader>,
    title: DiscTitle,
    disc: Option<Disc>,
    decrypt_keys: crate::decrypt::DecryptKeys,

    // Extents to read
    extents: Vec<Extent>,

    // Position
    current_extent: usize,
    current_offset: u32,

    // Buffer
    read_buf: Vec<u8>,
    buf_valid: usize,

    // Batch size for reads
    batch_sectors: u16,
    pub errors: u64,
    eof: bool,

    // PES output
    ts_demuxer: Option<super::ts::TsDemuxer>,
    ps_demuxer: Option<super::ps::PsDemuxer>,
    parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    pending_frames: std::collections::VecDeque<crate::pes::PesFrame>,
    pid_to_track: Vec<(u16, usize)>,
}

impl DiscStream {
    /// Open from a physical drive. Caller must have already called
    /// drive.wait_ready(), drive.init(), drive.probe_disc().
    /// Drive is moved into the stream — caller manages lock/unlock before/after.
    pub fn open_drive(
        drive: crate::drive::Drive,
        keydb_path: Option<&str>,
        title_index: usize,
    ) -> crate::error::Result<(Self, Disc)> {
        let scan_opts = match keydb_path {
            Some(kp) => ScanOptions::with_keydb(kp),
            None => ScanOptions::default(),
        };
        let mut drive = drive;
        let disc = Disc::scan(&mut drive, &scan_opts)?;

        if title_index >= disc.titles.len() {
            return Err(crate::error::Error::DiscTitleRange {
                index: title_index,
                count: disc.titles.len(),
            });
        }

        let title = disc.titles[title_index].clone();
        let keys = disc.decrypt_keys();
        let max_batch = detect_max_batch_sectors(drive.device_path());
        let content_format = disc.content_format;

        let mut stream = Self::from_reader(Box::new(drive), title, keys, max_batch);

        if content_format == crate::disc::ContentFormat::MpegPs {
            stream.ts_demuxer = None;
            stream.ps_demuxer = Some(super::ps::PsDemuxer::new());
        }
        Ok((stream, disc))
    }

    /// Open from an ISO file.
    pub fn open_iso(
        path: &str,
        title_index: Option<usize>,
        opts: &ScanOptions,
    ) -> io::Result<Self> {
        let mut reader = super::iso::IsoSectorReader::open(path)?;
        let capacity = reader.capacity();

        let disc = Disc::scan_image(&mut reader, capacity, opts)
            .map_err(|e| -> io::Error { e.into() })?;

        if disc.titles.is_empty() {
            return Err(crate::error::Error::NoStreams.into());
        }
        let idx = title_index.unwrap_or(0);
        if idx >= disc.titles.len() {
            return Err(crate::error::Error::DiscTitleRange {
                index: idx,
                count: disc.titles.len(),
            }.into());
        }

        let title = disc.titles[idx].clone();
        let keys = disc.decrypt_keys();
        let batch: u16 = 64;

        let mut stream = Self::from_reader(Box::new(reader), title, keys, batch);
        stream.disc = Some(disc);
        Ok(stream)
    }

    /// Create from any SectorReader + title + keys.
    pub fn from_reader(
        reader: Box<dyn SectorReader>,
        title: DiscTitle,
        decrypt_keys: crate::decrypt::DecryptKeys,
        batch_sectors: u16,
    ) -> Self {
        let extents = title.extents.clone();

        let mut pids = Vec::new();
        let mut parsers = Vec::new();
        let mut pid_to_track = Vec::new();
        for (idx, s) in title.streams.iter().enumerate() {
            let (pid, codec) = match s {
                crate::disc::Stream::Video(v) => (v.pid, v.codec),
                crate::disc::Stream::Audio(a) => (a.pid, a.codec),
                crate::disc::Stream::Subtitle(s) => (s.pid, s.codec),
            };
            pids.push(pid);
            pid_to_track.push((pid, idx));
            parsers.push((pid, super::codec::parser_for_codec(codec)));
        }

        Self {
            reader,
            title,
            disc: None,
            decrypt_keys,
            extents,
            current_extent: 0,
            current_offset: 0,
            read_buf: Vec::with_capacity(batch_sectors as usize * 2048),
            buf_valid: 0,
            batch_sectors,
            errors: 0,
            eof: false,
            ts_demuxer: if pids.is_empty() { None } else { Some(super::ts::TsDemuxer::new(&pids)) },
            ps_demuxer: None,
            parsers,
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track,
        }
    }

    /// Skip decryption — return raw encrypted bytes.
    pub fn set_raw(&mut self) {
        self.decrypt_keys = crate::decrypt::DecryptKeys::None;
    }

    /// Get the scanned Disc (for listing all titles).
    pub fn disc(&self) -> Option<&Disc> {
        self.disc.as_ref()
    }

    fn fill_extents(&mut self) -> bool {
        if self.current_extent >= self.extents.len() {
            return false;
        }
        let ext_start = self.extents[self.current_extent].start_lba;
        let ext_sectors = self.extents[self.current_extent].sector_count;

        let remaining = ext_sectors.saturating_sub(self.current_offset);
        let sectors = remaining.min(self.batch_sectors as u32) as u16;
        let sectors = sectors - (sectors % 3);
        if sectors == 0 {
            self.current_extent += 1;
            self.current_offset = 0;
            return self.fill_extents();
        }

        let lba = ext_start + self.current_offset;
        let bytes = sectors as usize * 2048;
        self.read_buf.resize(bytes, 0);

        match self.reader.read_sectors(lba, sectors, &mut self.read_buf[..bytes]) {
            Ok(_) => {
                self.buf_valid = bytes;
                self.current_offset += sectors as u32;
                if self.current_offset >= ext_sectors {
                    self.current_extent += 1;
                    self.current_offset = 0;
                }
                true
            }
            Err(_) => false,
        }
    }
}

impl crate::pes::Stream for DiscStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(Some(frame));
        }

        if self.eof {
            return Ok(None);
        }

        loop {
            if !self.fill_extents() {
                self.eof = true;
                return Ok(None);
            }

            let bytes = self.buf_valid;
            if let Err(e) = crate::decrypt::decrypt_sectors(
                &mut self.read_buf[..bytes],
                &self.decrypt_keys,
                0,
            ) {
                return Err(e.into());
            }

            if let Some(ref mut demuxer) = self.ts_demuxer {
                let packets = demuxer.feed(&self.read_buf[..bytes]);
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
            } else if let Some(ref mut demuxer) = self.ps_demuxer {
                let packets = demuxer.feed(&self.read_buf[..bytes]);
                for ps in &packets {
                    let track = match ps.stream_id {
                        0xE0..=0xEF => 0,
                        0xC0..=0xDF => 1,
                        0xBD => ps.sub_stream_id.map(|s| (s & 0x1F) as usize + 1).unwrap_or(1),
                        _ => continue,
                    };
                    if track < self.title.streams.len() {
                        let pts_ns = ps.pts.map(|p| (p as i64) * 1_000_000_000 / 90_000).unwrap_or(0);
                        self.pending_frames.push_back(crate::pes::PesFrame {
                            track,
                            pts: pts_ns,
                            keyframe: true,
                            data: ps.data.clone(),
                        });
                    }
                }
            }

            self.buf_valid = 0;

            if let Some(frame) = self.pending_frames.pop_front() {
                return Ok(Some(frame));
            }
        }
    }

    fn write(&mut self, _frame: &crate::pes::PesFrame) -> io::Result<()> {
        Err(crate::error::Error::StreamReadOnly.into())
    }

    fn finish(&mut self) -> io::Result<()> { Ok(()) }

    fn info(&self) -> &DiscTitle { &self.title }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        let pid = self.pid_to_track.iter()
            .find(|(_, idx)| *idx == track)
            .map(|(pid, _)| *pid)?;
        self.parsers.iter()
            .find(|(p, _)| *p == pid)
            .and_then(|(_, parser)| parser.codec_private())
    }

    fn headers_ready(&self) -> bool {
        for (idx, s) in self.title.streams.iter().enumerate() {
            if let crate::disc::Stream::Video(v) = s {
                if !v.secondary && self.codec_private(idx).is_none() {
                    return false;
                }
            }
        }
        true
    }
}
