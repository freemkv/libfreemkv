//! MkvStream — Matroska container stream.
//!
//! Write: BD-TS bytes in → demux → codec parse → MKV container out.
//! Read: MKV container in → extract frames → wrap as BD-TS → bytes out.

use super::codec::{self, CodecParser};
use super::lookahead::{LookaheadBuffer, LookaheadState, DEFAULT_LOOKAHEAD_SIZE};
use super::mkv::{MkvMuxer, MkvTrack};
use super::ts::TsDemuxer;
use super::{ebml, IOStream, ReadSeek, WriteSeek};

type MkvHeaderResult = io::Result<(crate::disc::DiscTitle, Vec<(u16, Vec<u8>)>)>;
use crate::disc::*;
use std::io::{self, Read, Seek, SeekFrom, Write};

/// Lookahead buffer for codec header detection (5 MB default).
const DEFAULT_MAX_BUFFER: usize = DEFAULT_LOOKAHEAD_SIZE;

#[derive(Debug, Clone, Copy, PartialEq)]
enum WritePhase {
    Scanning,
    Streaming,
}

struct WriteState {
    demuxer: TsDemuxer,
    muxer: Option<MkvMuxer<Box<dyn WriteSeek>>>,
    writer: Option<Box<dyn WriteSeek>>,
    parsers: Vec<(u16, Box<dyn CodecParser>)>,
    pid_to_track: Vec<(u16, usize)>,
    tracks: Vec<MkvTrack>,
    lookahead: LookaheadBuffer,
    phase: WritePhase,
    video_pending: usize,
}

struct ReadState {
    reader: Box<dyn ReadSeek>,
    buf: Vec<u8>,
    pos: usize,
    len: usize,
    cluster_ts_ms: i64,
    /// Codec private data per track (track_number, hvcC/avcC bytes).
    /// Emitted as Annex B NALs before first frame of each video track.
    codec_privates: Vec<(u16, Vec<u8>)>,
    /// Tracks that have already had their codec_private emitted.
    initialized_tracks: Vec<u16>,
}

enum Mode {
    Write(Box<WriteState>),
    Read(ReadState),
}

/// Matroska container stream.
pub struct MkvStream {
    disc_title: DiscTitle,
    mode: Mode,
    max_buffer: usize,
    finished: bool,
    /// File size in bytes, set for read mode.
    file_size: Option<u64>,
}

impl MkvStream {
    /// Create for writing. BD-TS bytes written to this stream produce MKV output.
    pub fn new(writer: impl Write + Seek + 'static) -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            mode: Mode::Write(Box::new(WriteState {
                demuxer: TsDemuxer::new(&[]),
                muxer: None,
                writer: Some(Box::new(writer)),
                parsers: Vec::new(),
                pid_to_track: Vec::new(),
                tracks: Vec::new(),
                lookahead: LookaheadBuffer::new(DEFAULT_MAX_BUFFER),
                phase: WritePhase::Scanning,
                video_pending: 0,
            })),
            max_buffer: DEFAULT_MAX_BUFFER,
            finished: false,
            file_size: None,
        }
    }

    /// Set stream metadata. Returns self for chaining.
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        if let Mode::Write(ref mut ws) = self.mode {
            let mut pids = Vec::new();
            for s in &dt.streams {
                let (pid, track, parser) = match s {
                    crate::disc::Stream::Video(v) => {
                        // Only count primary video as pending — secondary streams
                        // (Dolby Vision EL, PiP) may never produce codec headers
                        if !v.secondary {
                            ws.video_pending += 1;
                        }
                        (v.pid, MkvTrack::video(v), codec::parser_for_codec(v.codec))
                    }
                    crate::disc::Stream::Audio(a) => {
                        (a.pid, MkvTrack::audio(a), codec::parser_for_codec(a.codec))
                    }
                    crate::disc::Stream::Subtitle(s) => (
                        s.pid,
                        MkvTrack::subtitle(s),
                        codec::parser_for_codec_with_data(s.codec, s.codec_data.clone()),
                    ),
                };
                let idx = ws.tracks.len();
                pids.push(pid);
                ws.pid_to_track.push((pid, idx));
                ws.parsers.push((pid, parser));
                ws.tracks.push(track);
            }
            ws.demuxer = TsDemuxer::new(&pids);
        }
        self.disc_title = dt.clone();
        self
    }

    /// Set lookahead buffer size. Returns self.
    pub fn max_buffer(mut self, size: usize) -> Self {
        self.max_buffer = size;
        if let Mode::Write(ref mut ws) = self.mode {
            ws.lookahead = LookaheadBuffer::new(size);
        }
        self
    }

    /// Open an MKV file for reading.
    pub fn open(mut reader: impl Read + Seek + 'static) -> io::Result<Self> {
        let file_size = reader.seek(SeekFrom::End(0))?;
        reader.seek(SeekFrom::Start(0))?;
        let (disc_title, codec_privates) = parse_mkv_header(&mut reader)?;
        Ok(Self {
            disc_title,
            mode: Mode::Read(ReadState {
                reader: Box::new(reader),
                buf: Vec::new(),
                pos: 0,
                len: 0,
                cluster_ts_ms: 0,
                codec_privates,
                initialized_tracks: Vec::new(),
            }),
            max_buffer: 0,
            finished: false,
            file_size: Some(file_size),
        })
    }
}

impl crate::pes::Stream for MkvStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        let rs = match self.mode {
            Mode::Read(ref mut rs) => rs,
            Mode::Write(_) => return Err(io::Error::new(io::ErrorKind::Unsupported, "write-only")),
        };

        loop {
            let (id, size, _) = match ebml::read_element_header(&mut rs.reader) {
                Ok(h) => h,
                Err(_) => return Ok(None),
            };

            match id {
                ebml::CLUSTER => continue,
                ebml::CLUSTER_TIMESTAMP => {
                    rs.cluster_ts_ms = ebml::read_uint_val(&mut rs.reader, size as usize)? as i64;
                    continue;
                }
                ebml::SIMPLE_BLOCK => {
                    let block = ebml::read_binary_val(&mut rs.reader, size as usize)?;
                    if block.len() < 4 { continue; }

                    let (track, vl) = block_vint(&block);
                    if vl + 3 > block.len() { continue; }

                    let rel_ts = i16::from_be_bytes([block[vl], block[vl + 1]]);
                    let keyframe = block[vl + 2] & 0x80 != 0;
                    let data = block[vl + 3..].to_vec();
                    let pts_ms = rs.cluster_ts_ms + rel_ts as i64;
                    let track_idx = (track as usize).saturating_sub(1); // MKV tracks are 1-based

                    return Ok(Some(crate::pes::PesFrame {
                        track: track_idx,
                        pts: pts_ms * 1_000_000, // ms → ns
                        keyframe,
                        data,
                    }));
                }
                _ => {
                    // Skip unknown element
                    let mut skip = vec![0u8; size as usize];
                    let _ = rs.reader.read_exact(&mut skip);
                    continue;
                }
            }
        }
    }

    fn write(&mut self, _frame: &crate::pes::PesFrame) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "use MkvOutputStream for writing"))
    }

    fn finish(&mut self) -> io::Result<()> { Ok(()) }

    fn info(&self) -> &crate::disc::DiscTitle { &self.disc_title }
}

impl IOStream for MkvStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if let Mode::Write(ref mut ws) = self.mode {
            // Flush remaining PES packets
            if let Some(ref mut muxer) = ws.muxer {
                for pes in &ws.demuxer.flush() {
                    write_pes(&ws.pid_to_track, &mut ws.parsers, muxer, pes)?;
                }
            }
            // Write cues and finalize
            if let Some(muxer) = ws.muxer.take() {
                muxer.finish()?;
            }
        }
        Ok(())
    }

    fn total_bytes(&self) -> Option<u64> {
        self.file_size
    }
}

// ── Write ──────────────────────────────────────────────────────

impl Write for MkvStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let dt = &self.disc_title;
        let ws = match self.mode {
            Mode::Write(ref mut ws) => ws,
            Mode::Read(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "stream opened for reading",
                ))
            }
        };

        match ws.phase {
            WritePhase::Scanning => {
                // Feed demuxer for codec detection
                let packets = ws.demuxer.feed(buf);
                for pes in &packets {
                    if let Some((_, p)) = ws.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid) {
                        let _ = p.parse(pes);
                    }
                }

                let state = ws.lookahead.push(buf);

                // Check if all video codec headers found
                if check_codec_private(ws) {
                    ws.lookahead.mark_ready();
                    begin_streaming(ws, dt)?;
                    return Ok(buf.len());
                }

                match state {
                    LookaheadState::Collecting | LookaheadState::Ready => Ok(buf.len()),
                    LookaheadState::Overflow => Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "no codec headers found within lookahead buffer",
                    )),
                }
            }
            WritePhase::Streaming => {
                let packets = ws.demuxer.feed(buf);
                if let Some(ref mut muxer) = ws.muxer {
                    for pes in &packets {
                        write_pes(&ws.pid_to_track, &mut ws.parsers, muxer, pes)?;
                    }
                }
                Ok(buf.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ── Read ───────────────────────────────────────────────────────

impl Read for MkvStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let rs = match self.mode {
            Mode::Read(ref mut rs) => rs,
            Mode::Write(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "stream opened for writing",
                ))
            }
        };

        // Drain internal buffer first
        if rs.pos < rs.len {
            let n = (rs.len - rs.pos).min(buf.len());
            buf[..n].copy_from_slice(&rs.buf[rs.pos..rs.pos + n]);
            rs.pos += n;
            return Ok(n);
        }

        // Read next element from MKV
        loop {
            let (id, size, _) = match ebml::read_element_header(&mut rs.reader) {
                Ok(h) => h,
                Err(_) => return Ok(0),
            };

            match id {
                ebml::CLUSTER => continue,
                ebml::CLUSTER_TIMESTAMP => {
                    rs.cluster_ts_ms = ebml::read_uint_val(&mut rs.reader, size as usize)? as i64;
                    continue;
                }
                ebml::SIMPLE_BLOCK => {
                    let block = ebml::read_binary_val(&mut rs.reader, size as usize)?;
                    if block.len() < 4 {
                        continue;
                    }

                    let (track, vl) = block_vint(&block);
                    if vl + 3 > block.len() {
                        continue;
                    }

                    let rel_ts = i16::from_be_bytes([block[vl], block[vl + 1]]);
                    let frame = &block[vl + 3..];
                    let pts_ms = rs.cluster_ts_ms + rel_ts as i64;
                    let tnum = track as u16;

                    rs.buf.clear();

                    // First frame of a track: emit codec_private as Annex B NALs
                    if !rs.initialized_tracks.contains(&tnum) {
                        rs.initialized_tracks.push(tnum);
                        if let Some((_, cp)) = rs.codec_privates.iter().find(|(t, _)| *t == tnum) {
                            let annex_b = hvcc_to_annex_b(cp);
                            if !annex_b.is_empty() {
                                frame_to_ts(&mut rs.buf, tnum, pts_ms, &annex_b);
                            }
                        }
                    }

                    frame_to_ts(&mut rs.buf, tnum, pts_ms, frame);
                    rs.pos = 0;
                    rs.len = rs.buf.len();

                    if rs.len > 0 {
                        let n = rs.len.min(buf.len());
                        buf[..n].copy_from_slice(&rs.buf[..n]);
                        rs.pos = n;
                        return Ok(n);
                    }
                }
                _ => {
                    if size != u64::MAX && size > 0 {
                        rs.reader.seek(SeekFrom::Current(size as i64))?;
                    }
                }
            }
        }
    }
}

// ── Write internals ────────────────────────────────────────────

fn check_codec_private(ws: &mut WriteState) -> bool {
    if ws.video_pending == 0 {
        return true;
    }
    for (pid, parser) in &ws.parsers {
        if let Some(cp) = parser.codec_private() {
            if let Some((_, idx)) = ws.pid_to_track.iter().find(|(p, _)| p == pid) {
                if ws.tracks[*idx].codec_private.is_none() {
                    ws.tracks[*idx].codec_private = Some(cp);
                    ws.video_pending -= 1;
                }
            }
        }
    }
    ws.video_pending == 0
}

fn begin_streaming(ws: &mut WriteState, dt: &DiscTitle) -> io::Result<()> {
    let writer = ws
        .writer
        .take()
        .ok_or_else(|| io::Error::other("writer already consumed"))?;

    ws.muxer = Some(MkvMuxer::new_with_chapters(
        writer,
        &ws.tracks,
        Some(&dt.playlist),
        dt.duration_secs,
        &dt.chapters,
    )?);
    ws.phase = WritePhase::Streaming;

    // Re-parse buffered data through a fresh demuxer, then reset the main
    // demuxer so stale PES assembler state from scanning doesn't cause
    // duplicate or incomplete packets during streaming.
    let pids: Vec<u16> = ws.pid_to_track.iter().map(|(pid, _)| *pid).collect();
    let buffered = ws.lookahead.drain();
    if !buffered.is_empty() {
        let mut temp = TsDemuxer::new(&pids);
        let packets = temp.feed(&buffered);
        if let Some(ref mut muxer) = ws.muxer {
            for pes in &packets {
                write_pes(&ws.pid_to_track, &mut ws.parsers, muxer, pes)?;
            }
        }
    }
    // Transfer remainder bytes from old demuxer to new one.
    // Preserves 192-byte packet alignment across the reset.
    let remainder = ws.demuxer.take_remainder();
    ws.demuxer = TsDemuxer::new(&pids);
    ws.demuxer.set_remainder(remainder);
    Ok(())
}

fn write_pes(
    pid_to_track: &[(u16, usize)],
    parsers: &mut [(u16, Box<dyn CodecParser>)],
    muxer: &mut MkvMuxer<Box<dyn WriteSeek>>,
    pes: &super::ts::PesPacket,
) -> io::Result<()> {
    let idx = match pid_to_track.iter().find(|(pid, _)| *pid == pes.pid) {
        Some((_, idx)) => *idx,
        None => return Ok(()),
    };
    let parser = match parsers.iter_mut().find(|(pid, _)| *pid == pes.pid) {
        Some((_, p)) => p,
        None => return Ok(()),
    };
    for frame in parser.parse(pes) {
        muxer.write_frame(idx, frame.pts_ns, frame.keyframe, &frame.data)?;
    }
    Ok(())
}

// ── MKV header parsing (read side) ────────────────────────────

/// Returns (DiscTitle, codec_privates: Vec<(track_number, codec_private_bytes)>)
fn parse_mkv_header(
    r: &mut (impl Read + Seek),
) -> MkvHeaderResult {
    let mut title = String::new();
    let mut duration_ms = 0.0f64;
    let mut ts_scale: u64 = 1_000_000;
    let mut streams: Vec<crate::disc::Stream> = Vec::new();
    let mut codec_privates: Vec<(u16, Vec<u8>)> = Vec::new();

    let (id, size, _) = ebml::read_element_header(r)?;
    if id != ebml::EBML {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "not EBML"));
    }
    if size > i64::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "EBML header too large",
        ));
    }
    r.seek(SeekFrom::Current(size as i64))?;

    let (id, _, _) = ebml::read_element_header(r)?;
    if id != ebml::SEGMENT {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "no Segment"));
    }

    let (mut got_info, mut got_tracks) = (false, false);

    loop {
        if got_info && got_tracks {
            break;
        }
        let (id, size, _) = match ebml::read_element_header(r) {
            Ok(h) => h,
            Err(_) => break,
        };

        match id {
            ebml::INFO => {
                let end = r.stream_position()? + size;
                while r.stream_position()? < end {
                    let (cid, cs, _) = ebml::read_element_header(r)?;
                    match cid {
                        ebml::TIMESTAMP_SCALE => ts_scale = ebml::read_uint_val(r, cs as usize)?,
                        ebml::DURATION => duration_ms = ebml::read_float_val(r, cs as usize)?,
                        ebml::TITLE => title = ebml::read_string_val(r, cs as usize)?,
                        _ => {
                            r.seek(SeekFrom::Current(cs as i64))?;
                        }
                    }
                }
                got_info = true;
            }
            ebml::TRACKS => {
                let end = r.stream_position()? + size;
                while r.stream_position()? < end {
                    let (cid, cs, _) = ebml::read_element_header(r)?;
                    if cid == ebml::TRACK_ENTRY {
                        let (stream, tnum, cp) = parse_track(r, cs)?;
                        if let Some(s) = stream {
                            streams.push(s);
                        }
                        if let Some(cp) = cp {
                            codec_privates.push((tnum, cp));
                        }
                    } else {
                        r.seek(SeekFrom::Current(cs as i64))?;
                    }
                }
                got_tracks = true;
            }
            ebml::CLUSTER => break,
            _ if size != u64::MAX => {
                r.seek(SeekFrom::Current(size as i64))?;
            }
            _ => break,
        }
    }

    let disc_title = DiscTitle {
        playlist: title,
        duration_secs: duration_ms * (ts_scale as f64) / 1_000_000_000.0,
        streams,
        ..DiscTitle::empty()
    };
    Ok((disc_title, codec_privates))
}

/// Returns (stream, track_number, codec_private_bytes)
fn parse_track(
    r: &mut (impl Read + Seek),
    size: u64,
) -> io::Result<(Option<crate::disc::Stream>, u16, Option<Vec<u8>>)> {
    let end = r.stream_position()? + size;
    let (mut ttype, mut tnum) = (0u64, 0u16);
    let (mut codec_id, mut lang, mut name) = (String::new(), String::from("und"), String::new());
    let (mut ph, mut sr, mut ch, mut forced) = (0u32, 0.0f64, 0u8, false);
    let mut codec_priv: Option<Vec<u8>> = None;

    while r.stream_position()? < end {
        let (cid, cs, _) = ebml::read_element_header(r)?;
        match cid {
            ebml::TRACK_NUMBER => tnum = ebml::read_uint_val(r, cs as usize)? as u16,
            ebml::TRACK_TYPE => ttype = ebml::read_uint_val(r, cs as usize)?,
            ebml::CODEC_ID => codec_id = ebml::read_string_val(r, cs as usize)?,
            ebml::CODEC_PRIVATE => codec_priv = Some(ebml::read_binary_val(r, cs as usize)?),
            ebml::LANGUAGE => lang = ebml::read_string_val(r, cs as usize)?,
            ebml::TRACK_NAME => name = ebml::read_string_val(r, cs as usize)?,
            ebml::FLAG_FORCED => forced = ebml::read_uint_val(r, cs as usize)? != 0,
            ebml::VIDEO => {
                let ve = r.stream_position()? + cs;
                while r.stream_position()? < ve {
                    let (vid, vs, _) = ebml::read_element_header(r)?;
                    if vid == ebml::PIXEL_HEIGHT {
                        ph = ebml::read_uint_val(r, vs as usize)? as u32;
                    } else {
                        r.seek(SeekFrom::Current(vs as i64))?;
                    }
                }
            }
            ebml::AUDIO => {
                let ae = r.stream_position()? + cs;
                while r.stream_position()? < ae {
                    let (aid, as_, _) = ebml::read_element_header(r)?;
                    match aid {
                        ebml::SAMPLING_FREQUENCY => sr = ebml::read_float_val(r, as_ as usize)?,
                        ebml::CHANNELS => ch = ebml::read_uint_val(r, as_ as usize)? as u8,
                        _ => {
                            r.seek(SeekFrom::Current(as_ as i64))?;
                        }
                    }
                }
            }
            _ => {
                r.seek(SeekFrom::Current(cs as i64))?;
            }
        }
    }

    let codec = match codec_id.as_str() {
        "V_MPEGH/ISO/HEVC" => Codec::Hevc,
        "V_MPEG4/ISO/AVC" => Codec::H264,
        "V_MS/VFW/FOURCC" => Codec::Vc1,
        "V_MPEG2" => Codec::Mpeg2,
        "A_AC3" => Codec::Ac3,
        "A_EAC3" => Codec::Ac3Plus,
        "A_TRUEHD" => Codec::TrueHd,
        "A_DTS" => Codec::Dts,
        "A_PCM/INT/BIG" => Codec::Lpcm,
        "S_HDMV/PGS" => Codec::Pgs,
        "S_VOBSUB" => Codec::DvdSub,
        _ => Codec::Unknown(0),
    };
    let res = Resolution::from_height(ph);
    let chs = AudioChannels::from_count(ch);
    let srs = if sr >= 96000.0 {
        SampleRate::S96
    } else {
        SampleRate::S48
    };

    // Map MKV track numbers to BD-TS PIDs (same mapping as frame_to_ts)
    let ts_pid = if tnum == 1 { 0x1011 } else { 0x1100 + (tnum - 2) };

    let stream = match ttype {
        1 => {
            let is_secondary = name.contains("Dolby Vision EL") || name.contains("DV EL");
            Some(crate::disc::Stream::Video(VideoStream {
                pid: ts_pid,
                codec,
                resolution: res,
                frame_rate: FrameRate::Unknown,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                secondary: is_secondary,
                label: name,
            }))
        }
        2 => Some(crate::disc::Stream::Audio(AudioStream {
            pid: ts_pid,
            codec,
            channels: chs,
            language: lang,
            sample_rate: srs,
            secondary: false,
            label: name,
        })),
        17 => Some(crate::disc::Stream::Subtitle(SubtitleStream {
            pid: ts_pid,
            codec,
            language: lang,
            forced,
            codec_data: None,
        })),
        _ => None,
    };
    Ok((stream, tnum, codec_priv))
}

// ── BD-TS frame wrapping (read side) ──────────────────────────

fn block_vint(d: &[u8]) -> (u64, usize) {
    if d.is_empty() {
        return (0, 0);
    }
    if d[0] & 0x80 != 0 {
        return ((d[0] & 0x7F) as u64, 1);
    }
    if d[0] & 0x40 != 0 && d.len() >= 2 {
        return ((((d[0] & 0x3F) as u64) << 8) | d[1] as u64, 2);
    }
    (0, 1)
}

/// Convert HEVCDecoderConfigurationRecord (hvcC) to Annex B NAL units.
/// Extracts VPS, SPS, PPS arrays and prefixes each with 0x00000001.
fn hvcc_to_annex_b(hvcc: &[u8]) -> Vec<u8> {
    // hvcC format (ISO 14496-15):
    //   byte 0: configurationVersion (1)
    //   bytes 1-21: profile/level info
    //   byte 22: numOfArrays
    //   For each array:
    //     byte 0: array_completeness(1) + reserved(1) + NAL_unit_type(6)
    //     bytes 1-2: numNalus (big-endian u16)
    //     For each NAL:
    //       bytes 0-1: nalUnitLength (big-endian u16)
    //       bytes 2..: NAL data
    if hvcc.len() < 23 {
        return Vec::new();
    }
    let num_arrays = hvcc[22] as usize;
    let mut pos = 23;
    let mut out = Vec::new();

    for _ in 0..num_arrays {
        if pos + 3 > hvcc.len() {
            break;
        }
        pos += 1; // skip array_completeness + NAL type byte
        let num_nalus = u16::from_be_bytes([hvcc[pos], hvcc[pos + 1]]) as usize;
        pos += 2;
        for _ in 0..num_nalus {
            if pos + 2 > hvcc.len() {
                break;
            }
            let nal_len = u16::from_be_bytes([hvcc[pos], hvcc[pos + 1]]) as usize;
            pos += 2;
            if pos + nal_len > hvcc.len() {
                break;
            }
            out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
            out.extend_from_slice(&hvcc[pos..pos + nal_len]);
            pos += nal_len;
        }
    }
    out
}

fn frame_to_ts(out: &mut Vec<u8>, track: u16, pts_ms: i64, data: &[u8]) {
    let pid = if track == 1 {
        0x1011
    } else {
        0x1100 + (track - 2)
    };
    let is_video = track <= 1 || pid == 0x1011;
    let stream_id: u8 = if is_video { 0xE0 } else { 0xBD };
    let pts = encode_pts(pts_ms * 90);
    let hdr = [0x00, 0x00, 0x01, stream_id, 0x00, 0x00, 0x80, 0x80, 0x05];

    let mut pes = Vec::with_capacity(hdr.len() + pts.len() + data.len());
    pes.extend_from_slice(&hdr);
    pes.extend_from_slice(&pts);

    // Video: convert MKV length-prefixed NALs to Annex B start codes
    if is_video && data.len() > 4 {
        let mut pos = 0;
        while pos + 4 <= data.len() {
            let nal_len = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
            pos += 4;
            if nal_len == 0 || pos + nal_len > data.len() {
                break;
            }
            pes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
            pes.extend_from_slice(&data[pos..pos + nal_len]);
            pos += nal_len;
        }
    } else {
        pes.extend_from_slice(data);
    }

    let mut off = 0;
    let mut pusi = true;
    while off < pes.len() {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47;
        pkt[5] = (pid >> 8) as u8 & 0x1F;
        if pusi {
            pkt[5] |= 0x40;
            pusi = false;
        }
        pkt[6] = pid as u8;

        let space = 184;
        let rem = pes.len() - off;
        let n = rem.min(space);

        if n < space {
            let pad = space - n;
            pkt[7] = 0x30; // AF + payload
            pkt[8] = pad as u8;
            if pad > 1 {
                pkt[9] = 0x00;
            }
            for byte in pkt.iter_mut().take((8 + pad).min(192)).skip(10) {
                *byte = 0xFF;
            }
            pkt[8 + pad..8 + pad + n].copy_from_slice(&pes[off..off + n]);
        } else {
            pkt[7] = 0x10; // payload only
            pkt[8..8 + n].copy_from_slice(&pes[off..off + n]);
        }

        out.extend_from_slice(&pkt);
        off += n;
    }
}

fn encode_pts(pts: i64) -> [u8; 5] {
    let p = pts as u64;
    [
        0x21 | ((p >> 29) & 0x0E) as u8,
        ((p >> 22) & 0xFF) as u8,
        0x01 | ((p >> 14) & 0xFE) as u8,
        ((p >> 7) & 0xFF) as u8,
        0x01 | ((p << 1) & 0xFE) as u8,
    ]
}
