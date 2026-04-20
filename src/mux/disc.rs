//! DiscStream — read any disc (physical drive or ISO file) → PES frames.
//!
//! One stream type for all disc sources. The source is a SectorReader —
//! Drive (hardware) or IsoSectorReader (file). DiscStream doesn't care.
//!
//! Read-only. For disc→ISO (raw sector copy), use `Disc::copy()`.

use crate::disc::{detect_max_batch_sectors, Disc, DiscTitle, Extent, ScanOptions};
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
    pub skip_errors: bool,
    eof: bool,

    // PES output
    ts_demuxer: Option<super::ts::TsDemuxer>,
    ps_demuxer: Option<super::ps::PsDemuxer>,
    parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    pending_frames: std::collections::VecDeque<crate::pes::PesFrame>,
    pid_to_track: Vec<(u16, usize)>,
}

impl DiscStream {
    /// Create a disc stream from any sector reader.
    ///
    /// Works with physical drives and ISO files — both implement SectorReader.
    /// The caller opens the source, scans for titles/keys, and passes them in.
    /// The stream handles demuxing, decryption, and codec parsing internally.
    pub fn new(
        reader: Box<dyn SectorReader>,
        title: DiscTitle,
        decrypt_keys: crate::decrypt::DecryptKeys,
        batch_sectors: u16,
        content_format: crate::disc::ContentFormat,
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

        let mut ts_demuxer = None;
        let mut ps_demuxer = None;
        match content_format {
            crate::disc::ContentFormat::MpegPs => {
                ps_demuxer = Some(super::ps::PsDemuxer::new());
            }
            crate::disc::ContentFormat::BdTs => {
                let ts_pids: Vec<u16> = pids.clone();
                if !ts_pids.is_empty() {
                    ts_demuxer = Some(super::ts::TsDemuxer::new(&ts_pids));
                }
            }
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
            skip_errors: false,
            eof: false,
            ts_demuxer,
            ps_demuxer,
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

    fn fill_extents(&mut self) -> io::Result<bool> {
        if self.current_extent >= self.extents.len() {
            return Ok(false);
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
            }
            Err(e) if self.skip_errors => {
                // Skip mode: try each sector individually, zero-fill failures
                self.buf_valid = 0;
                for i in 0..sectors {
                    let offset = i as usize * 2048;
                    let sector_lba = lba + i as u32;
                    if self
                        .reader
                        .read_sectors(sector_lba, 1, &mut self.read_buf[offset..offset + 2048])
                        .is_err()
                    {
                        self.read_buf[offset..offset + 2048].fill(0);
                        self.errors += 1;
                    }
                    self.buf_valid += 2048;
                }
            }
            Err(e) => return Err(e.into()),
        }
        self.current_offset += sectors as u32;
        if self.current_offset >= ext_sectors {
            self.current_extent += 1;
            self.current_offset = 0;
        }
        Ok(true)
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
            if !self.fill_extents()? {
                self.eof = true;
                // Flush demuxer — last PES packet may still be in the assembler
                if let Some(ref mut demuxer) = self.ts_demuxer {
                    for pes in &demuxer.flush() {
                        if let Some((_, track)) =
                            self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid)
                        {
                            if let Some((_, parser)) =
                                self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid)
                            {
                                for frame in parser.parse(pes) {
                                    self.pending_frames.push_back(
                                        crate::pes::PesFrame::from_codec_frame(*track, frame),
                                    );
                                }
                            }
                        }
                    }
                }
                // PS demuxer flush (DVD)
                if let Some(ref mut demuxer) = self.ps_demuxer {
                    for ps in &demuxer.flush() {
                        let track = match ps.stream_id {
                            0xE0..=0xEF => 0,
                            0xC0..=0xDF => 1,
                            0xBD => ps
                                .sub_stream_id
                                .map(|s| (s & 0x1F) as usize + 1)
                                .unwrap_or(1),
                            _ => continue,
                        };
                        if track >= self.title.streams.len() {
                            continue;
                        }
                        let pid = self
                            .pid_to_track
                            .iter()
                            .find(|(_, idx)| *idx == track)
                            .map(|(p, _)| *p)
                            .unwrap_or(0);
                        let pes = super::ts::PesPacket {
                            pid,
                            pts: ps.pts.map(|p| p as i64),
                            dts: ps.dts.map(|d| d as i64),
                            data: ps.data.clone(),
                        };
                        if let Some((_, parser)) = self.parsers.iter_mut().find(|(p, _)| *p == pid)
                        {
                            for frame in parser.parse(&pes) {
                                self.pending_frames.push_back(
                                    crate::pes::PesFrame::from_codec_frame(track, frame),
                                );
                            }
                        }
                    }
                }
                return Ok(self.pending_frames.pop_front());
            }

            let bytes = self.buf_valid;
            if let Err(e) =
                crate::decrypt::decrypt_sectors(&mut self.read_buf[..bytes], &self.decrypt_keys, 0)
            {
                return Err(e.into());
            }

            if let Some(ref mut demuxer) = self.ts_demuxer {
                let packets = demuxer.feed(&self.read_buf[..bytes]);
                for pes in &packets {
                    if let Some((_, track)) =
                        self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid)
                    {
                        if let Some((_, parser)) =
                            self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid)
                        {
                            for frame in parser.parse(pes) {
                                self.pending_frames.push_back(
                                    crate::pes::PesFrame::from_codec_frame(*track, frame),
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
                        0xBD => ps
                            .sub_stream_id
                            .map(|s| (s & 0x1F) as usize + 1)
                            .unwrap_or(1),
                        _ => continue,
                    };
                    if track >= self.title.streams.len() {
                        continue;
                    }

                    // Convert PsPacket to PesPacket for codec parser (same as BD-TS path)
                    let pid = self
                        .pid_to_track
                        .iter()
                        .find(|(_, idx)| *idx == track)
                        .map(|(p, _)| *p)
                        .unwrap_or(0);

                    let pes = super::ts::PesPacket {
                        pid,
                        pts: ps.pts.map(|p| p as i64),
                        dts: ps.dts.map(|d| d as i64),
                        data: ps.data.clone(),
                    };

                    if let Some((_, parser)) = self.parsers.iter_mut().find(|(p, _)| *p == pid) {
                        for frame in parser.parse(&pes) {
                            self.pending_frames
                                .push_back(crate::pes::PesFrame::from_codec_frame(track, frame));
                        }
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

    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        let pid = self
            .pid_to_track
            .iter()
            .find(|(_, idx)| *idx == track)
            .map(|(pid, _)| *pid)?;
        self.parsers
            .iter()
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
