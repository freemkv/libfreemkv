//! MkvStream — Matroska container stream.
//!
//! Read: MKV container → demux EBML → PES frames out.
//! Write: PES frames in → MKV mux → Matroska container.

use super::mkv::{MkvMuxer, MkvTrack};
use super::{WriteSeek, ebml};

/// (title, codec_privates, ts_scale_ns) — `ts_scale_ns` is the
/// TimestampScale in nanoseconds per tick, threaded into the frame read path.
type MkvHeaderResult = io::Result<(crate::disc::DiscTitle, Vec<(u16, Vec<u8>)>, i64)>;

/// Skip `n` bytes on a forward-only reader (no Seek required).
fn skip_bytes(r: &mut impl Read, n: u64) -> io::Result<()> {
    io::copy(&mut r.take(n), &mut io::sink())?;
    Ok(())
}

// ── Sanity caps for untrusted EBML element sizes ──────────────
//
// Sizes come straight from the EBML stream (file or network) and are
// otherwise cast to `usize` and used to allocate/read. An adversarial
// or corrupt container can claim a multi-GB element and trigger an OOM
// allocation, or claim an integer element wider than 8 bytes and panic
// the fixed 8-byte reader. Every untrusted size is validated against
// one of these caps before allocation.

/// Largest accepted SIMPLE_BLOCK payload. A block is a small vint track
/// header + 2-byte rel-ts + 1-byte flags + one frame of elementary data.
/// UHD HEVC keyframes run a few MB; 64 MiB is generously above any real
/// single-frame block while still bounding a hostile allocation.
const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
/// Largest accepted CODEC_PRIVATE payload. hvcC/avcC/setup blobs are a
/// few KB in practice; 16 MiB is far above any legitimate value.
const MAX_CODEC_PRIVATE: u64 = 16 * 1024 * 1024;
/// Largest accepted string element (TITLE/CODEC_ID/LANGUAGE/TRACK_NAME).
const MAX_STRING_LEN: u64 = 64 * 1024;
/// EBML unsigned-int elements are at most 8 bytes wide.
const MAX_UINT_LEN: u64 = 8;

/// Reject an untrusted element size that exceeds `cap` before it is used
/// to allocate or read. Returns the size as `usize` when within bounds.
fn checked_size(size: u64, cap: u64) -> io::Result<usize> {
    if size > cap {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    Ok(size as usize)
}

/// Read a bounded unsigned int. Guards against `size > 8` (which would
/// otherwise index out of the fixed 8-byte buffer in `read_uint_val`)
/// before delegating.
fn read_uint_bounded(r: &mut impl Read, size: u64) -> io::Result<u64> {
    ebml::read_uint_val(r, checked_size(size, MAX_UINT_LEN)?)
}

/// Read a bounded UTF-8 string element.
fn read_string_bounded(r: &mut impl Read, size: u64) -> io::Result<String> {
    ebml::read_string_val(r, checked_size(size, MAX_STRING_LEN)?)
}

use crate::disc::*;
use std::io::{self, Read};

struct ReadState {
    reader: Box<dyn Read + Send>,
    /// Current cluster timestamp in TimestampScale *ticks* (not ms). Combined
    /// with each block's relative tick offset and scaled to nanoseconds via
    /// `ts_scale_ns`.
    cluster_ts_ticks: i64,
    /// TimestampScale in nanoseconds per tick (Matroska INFO/TimestampScale,
    /// default 1_000_000 = 1 ms). Foreign MKVs may use a different scale; the
    /// frame PTS must honour it, not assume milliseconds.
    ts_scale_ns: i64,
    /// Codec private data per track (track_number, hvcC/avcC bytes).
    codec_privates: Vec<(u16, Vec<u8>)>,
}

enum Mode {
    Write {
        muxer: Option<MkvMuxer<Box<dyn WriteSeek + Send>>>,
    },
    Read(ReadState),
}

/// Matroska container stream.
pub struct MkvStream {
    disc_title: DiscTitle,
    mode: Mode,
}

impl MkvStream {
    /// Create for writing PES frames → MKV container.
    /// Codec privates come from title.codec_privates (populated by input stream).
    pub fn create(writer: Box<dyn WriteSeek + Send>, title: &DiscTitle) -> io::Result<Self> {
        let mut tracks = Vec::new();
        let mut has_default_video = false;
        let mut has_default_audio = false;
        for (idx, s) in title.streams.iter().enumerate() {
            let mut track = match s {
                crate::disc::Stream::Video(v) => MkvTrack::video(v),
                crate::disc::Stream::Audio(a) => MkvTrack::audio(a),
                crate::disc::Stream::Subtitle(s) => MkvTrack::subtitle(s),
            };
            // Only first video and first audio are default
            if track.is_default {
                match track.track_type {
                    1 if !has_default_video => has_default_video = true,
                    2 if !has_default_audio => has_default_audio = true,
                    _ => track.is_default = false,
                }
            }
            if let Some(cp) = title.codec_privates.get(idx).and_then(|c| c.as_ref()) {
                track.codec_private = Some(cp.clone());
            }
            tracks.push(track);
        }

        let muxer = MkvMuxer::new(
            writer,
            &tracks,
            Some(&title.playlist),
            title.duration_secs,
            &title.chapters,
        )?;

        Ok(Self {
            disc_title: title.clone(),
            mode: Mode::Write { muxer: Some(muxer) },
        })
    }

    /// Open an MKV file for reading → PES frames.
    pub fn open(mut reader: impl Read + Send + 'static) -> io::Result<Self> {
        let (disc_title, codec_privates, ts_scale_ns) = parse_mkv_header(&mut reader)?;
        Ok(Self {
            disc_title,
            mode: Mode::Read(ReadState {
                reader: Box::new(reader),
                cluster_ts_ticks: 0,
                ts_scale_ns,
                codec_privates,
            }),
        })
    }
}

impl crate::pes::Stream for MkvStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        let streams_len = self.disc_title.streams.len();
        let rs = match self.mode {
            Mode::Read(ref mut rs) => rs,
            Mode::Write { .. } => return Err(crate::error::Error::StreamWriteOnly.into()),
        };

        loop {
            let (id, size, _) = match ebml::read_element_header(&mut rs.reader) {
                Ok(h) => h,
                // Only a genuine premature/clean EOF ends the stream. Any other
                // error (disc read failure, corrupt sector, network drop) must
                // propagate, or a mid-mux I/O failure would silently truncate
                // the output with no error signal.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            };

            match id {
                ebml::CLUSTER => continue,
                ebml::CLUSTER_TIMESTAMP => {
                    let raw = read_uint_bounded(&mut rs.reader, size)?;
                    // The cluster timestamp is an untrusted u64; a value above
                    // i64::MAX would cast to a large negative i64 and poison
                    // every block PTS in the cluster. Reject it, mirroring the
                    // EBML-size guard in parse_mkv_header.
                    if raw > i64::MAX as u64 {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    rs.cluster_ts_ticks = raw as i64;
                    continue;
                }
                ebml::SIMPLE_BLOCK => {
                    let block =
                        ebml::read_binary_val(&mut rs.reader, checked_size(size, MAX_BLOCK_SIZE)?)?;
                    if let Some(frame) = parse_block(
                        &block,
                        rs.cluster_ts_ticks,
                        rs.ts_scale_ns,
                        streams_len,
                        None,
                    ) {
                        return Ok(Some(frame));
                    }
                    continue;
                }
                ebml::BLOCK_GROUP => {
                    // MkvMuxer emits a BlockGroup (BLOCK + BLOCK_DURATION) for
                    // every frame carrying a duration — i.e. all AC3 audio and
                    // PGS subtitle frames. Descend into the group, read the
                    // inner BLOCK (0xA1) and BLOCK_DURATION (0x9B), and yield a
                    // frame so a round-trip through this muxer does not silently
                    // drop those tracks. A non-u64::MAX size bounds the children.
                    if size == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    let mut remaining = size;
                    let mut block: Option<Vec<u8>> = None;
                    let mut duration_ms: Option<u64> = None;
                    while remaining > 0 {
                        let (cid, cs, hlen) = ebml::read_element_header(&mut rs.reader)?;
                        if cs == u64::MAX {
                            return Err(crate::error::Error::MkvInvalid.into());
                        }
                        remaining = remaining.saturating_sub(hlen as u64 + cs);
                        match cid {
                            ebml::BLOCK => {
                                block = Some(ebml::read_binary_val(
                                    &mut rs.reader,
                                    checked_size(cs, MAX_BLOCK_SIZE)?,
                                )?);
                            }
                            ebml::BLOCK_DURATION => {
                                duration_ms = Some(read_uint_bounded(&mut rs.reader, cs)?);
                            }
                            _ => skip_bytes(&mut rs.reader, cs)?,
                        }
                    }
                    if let Some(block) = block {
                        let dur_ns = duration_ms.map(|ms| ms.saturating_mul(1_000_000));
                        if let Some(frame) = parse_block(
                            &block,
                            rs.cluster_ts_ticks,
                            rs.ts_scale_ns,
                            streams_len,
                            dur_ns,
                        ) {
                            return Ok(Some(frame));
                        }
                    }
                    continue;
                }
                _ => {
                    // An unknown-size element here would drain the whole stream
                    // (take(u64::MAX)) and silently drop all later frames;
                    // reject it like the rest of the parser.
                    if size == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    skip_bytes(&mut rs.reader, size)?;
                    continue;
                }
            }
        }
    }

    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match &mut self.mode {
            Mode::Write { muxer: Some(m) } => m.write_frame(
                frame.track,
                frame.pts,
                frame.keyframe,
                &frame.data,
                frame.duration_ns,
            ),
            Mode::Write { muxer: None } => Ok(()),
            Mode::Read(_) => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        if let Mode::Write { ref mut muxer } = self.mode {
            if let Some(m) = muxer.take() {
                m.finish()?;
            }
        }
        Ok(())
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        let track_num = (track + 1) as u16; // MKV tracks are 1-based
        if let Mode::Read(ref rs) = self.mode {
            rs.codec_privates
                .iter()
                .find(|(tn, _)| *tn == track_num)
                .map(|(_, data)| data.clone())
        } else {
            None
        }
    }

    fn headers_ready(&self) -> bool {
        true // MKV has all headers upfront in the EBML header
    }
}

// ── MKV header parsing (read side) ────────────────────────────

/// Returns (DiscTitle, codec_privates: Vec<(track_number, codec_private_bytes)>)
fn parse_mkv_header(r: &mut impl Read) -> MkvHeaderResult {
    let mut title = String::new();
    // EBML `DURATION` is a float expressed in TimestampScale ticks, not
    // milliseconds (Matroska spec). Named accordingly; converted to
    // seconds below as ticks * ts_scale_ns / 1e9.
    let mut duration_ticks = 0.0f64;
    let mut ts_scale: u64 = 1_000_000;
    let mut streams: Vec<crate::disc::Stream> = Vec::new();
    let mut codec_privates: Vec<(u16, Vec<u8>)> = Vec::new();

    let (id, size, _) = ebml::read_element_header(r)?;
    if id != ebml::EBML {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    if size > i64::MAX as u64 {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    skip_bytes(r, size)?;

    let (id, _, _) = ebml::read_element_header(r)?;
    if id != ebml::SEGMENT {
        return Err(crate::error::Error::MkvInvalid.into());
    }

    let (mut got_info, mut got_tracks) = (false, false);

    loop {
        if got_info && got_tracks {
            break;
        }
        let (id, size, _) = match ebml::read_element_header(r) {
            Ok(h) => h,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        match id {
            ebml::INFO => {
                // An unknown-size (u64::MAX) parent would drain children until
                // an EOF read error instead of a clean MkvInvalid; reject it for
                // parity with the segment loop guard below.
                if size == u64::MAX {
                    return Err(crate::error::Error::MkvInvalid.into());
                }
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
                    // An inner child declaring EBML unknown size (cs == u64::MAX)
                    // would overflow `hlen + cs` (debug panic) and is meaningless
                    // for a sized parent — reject it.
                    if cs == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    remaining = remaining.saturating_sub(hlen as u64 + cs);
                    match cid {
                        ebml::TIMESTAMP_SCALE => ts_scale = read_uint_bounded(r, cs)?,
                        ebml::DURATION => duration_ticks = ebml::read_float_val(r, cs as usize)?,
                        ebml::TITLE => title = read_string_bounded(r, cs)?,
                        _ => {
                            skip_bytes(r, cs)?;
                        }
                    }
                }
                got_info = true;
            }
            ebml::TRACKS => {
                if size == u64::MAX {
                    return Err(crate::error::Error::MkvInvalid.into());
                }
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
                    if cs == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    remaining = remaining.saturating_sub(hlen as u64 + cs);
                    if cid == ebml::TRACK_ENTRY {
                        let (stream, tnum, cp) = parse_track(r, cs)?;
                        if let Some(s) = stream {
                            streams.push(s);
                        }
                        if let Some(cp) = cp {
                            codec_privates.push((tnum, cp));
                        }
                    } else {
                        skip_bytes(r, cs)?;
                    }
                }
                got_tracks = true;
            }
            ebml::CLUSTER => break,
            _ if size != u64::MAX => {
                skip_bytes(r, size)?;
            }
            _ => break,
        }
    }

    let disc_title = DiscTitle {
        playlist: title,
        duration_secs: duration_ticks * (ts_scale as f64) / 1_000_000_000.0,
        streams,
        ..DiscTitle::empty()
    };
    // Clamp the (untrusted) scale to a positive i64 for the tick→ns multiply on
    // the read path; default to 1 ms if absent or absurd.
    let ts_scale_ns = if ts_scale == 0 || ts_scale > i64::MAX as u64 {
        1_000_000
    } else {
        ts_scale as i64
    };
    Ok((disc_title, codec_privates, ts_scale_ns))
}

/// Largest valid 13-bit MPEG-TS PID.
const MAX_TS_PID: u32 = 0x1FFF;

/// Map an MKV track number to a synthetic BD-TS PID, rejecting any value that
/// would overflow the 13-bit PID space. Track 1 is the video PID (0x1011);
/// every other track maps to `0x1100 + (tnum - 2)`. Computed in `u32` so the
/// addition can never wrap, unlike the prior `u16` arithmetic.
fn ts_pid_for_track(tnum: u16) -> io::Result<u16> {
    // MKV track numbers are 1-based; 0 is invalid (and would underflow the
    // `tnum - 2` below).
    if tnum == 0 {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    let pid: u32 = if tnum == 1 {
        0x1011
    } else {
        0x1100u32 + (tnum as u32 - 2)
    };
    if pid > MAX_TS_PID {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    Ok(pid as u16)
}

/// Returns (stream, track_number, codec_private_bytes)
fn parse_track(
    r: &mut impl Read,
    size: u64,
) -> io::Result<(Option<crate::disc::Stream>, u16, Option<Vec<u8>>)> {
    let (mut ttype, mut tnum) = (0u64, 0u16);
    let (mut codec_id, mut lang, mut name) = (String::new(), String::from("und"), String::new());
    let (mut ph, mut sr, mut ch, mut forced) = (0u32, 0.0f64, 0u8, false);
    let mut codec_priv: Option<Vec<u8>> = None;

    let mut remaining = size;
    while remaining > 0 {
        let (cid, cs, hlen) = ebml::read_element_header(r)?;
        if cs == u64::MAX {
            return Err(crate::error::Error::MkvInvalid.into());
        }
        remaining = remaining.saturating_sub(hlen as u64 + cs);
        match cid {
            ebml::TRACK_NUMBER => {
                // Reject a TRACK_NUMBER above u16::MAX rather than truncating
                // with `as u16` (which would alias 65536→0, 65537→1, … onto
                // existing small track numbers and corrupt PID/codec lookup).
                let n = read_uint_bounded(r, cs)?;
                if n > u16::MAX as u64 {
                    return Err(crate::error::Error::MkvInvalid.into());
                }
                tnum = n as u16;
            }
            ebml::TRACK_TYPE => ttype = read_uint_bounded(r, cs)?,
            ebml::CODEC_ID => codec_id = read_string_bounded(r, cs)?,
            ebml::CODEC_PRIVATE => {
                codec_priv = Some(ebml::read_binary_val(
                    r,
                    checked_size(cs, MAX_CODEC_PRIVATE)?,
                )?)
            }
            ebml::LANGUAGE => lang = read_string_bounded(r, cs)?,
            ebml::TRACK_NAME => name = read_string_bounded(r, cs)?,
            ebml::FLAG_FORCED => forced = read_uint_bounded(r, cs)? != 0,
            ebml::VIDEO => {
                let mut vrem = cs;
                while vrem > 0 {
                    let (vid, vs, vhlen) = ebml::read_element_header(r)?;
                    if vs == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    vrem = vrem.saturating_sub(vhlen as u64 + vs);
                    if vid == ebml::PIXEL_HEIGHT {
                        ph = read_uint_bounded(r, vs)? as u32;
                    } else {
                        skip_bytes(r, vs)?;
                    }
                }
            }
            ebml::AUDIO => {
                let mut arem = cs;
                while arem > 0 {
                    let (aid, as_, ahlen) = ebml::read_element_header(r)?;
                    if as_ == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    arem = arem.saturating_sub(ahlen as u64 + as_);
                    match aid {
                        ebml::SAMPLING_FREQUENCY => sr = ebml::read_float_val(r, as_ as usize)?,
                        ebml::CHANNELS => ch = read_uint_bounded(r, as_)? as u8,
                        _ => {
                            skip_bytes(r, as_)?;
                        }
                    }
                }
            }
            _ => {
                skip_bytes(r, cs)?;
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

    // Map MKV track numbers to BD-TS PIDs. A 13-bit TS PID tops out at
    // 0x1FFF; compute in u32 so the `0x1100 + (tnum - 2)` arithmetic can't
    // wrap u16 for large track numbers, and reject anything that would land
    // outside the valid PID space.
    let ts_pid = ts_pid_for_track(tnum)?;

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
            purpose: crate::disc::LabelPurpose::Normal,
            label: name,
        })),
        17 => Some(crate::disc::Stream::Subtitle(SubtitleStream {
            pid: ts_pid,
            codec,
            language: lang,
            forced,
            qualifier: crate::disc::LabelQualifier::None,
            codec_data: None,
        })),
        _ => None,
    };
    Ok((stream, tnum, codec_priv))
}

/// Parse a (Simple)Block payload into a PesFrame, or `None` if it should be
/// skipped (too short, track 0, or a track index out of range).
///
/// `cluster_ts_ticks` is the open cluster's timestamp in TimestampScale ticks
/// and `ts_scale_ns` is that scale (ns per tick); the block PTS is computed as
/// `(cluster_ts_ticks + rel_ts) * ts_scale_ns` so foreign MKVs whose scale
/// isn't 1 ms are honoured (freemkv's own output uses 1_000_000 and round-trips
/// unchanged). `streams_len` bounds the resolved track index; `duration_ns` is
/// propagated for BlockGroup blocks (None for SimpleBlock).
fn parse_block(
    block: &[u8],
    cluster_ts_ticks: i64,
    ts_scale_ns: i64,
    streams_len: usize,
    duration_ns: Option<u64>,
) -> Option<crate::pes::PesFrame> {
    if block.len() < 4 {
        return None;
    }
    let (track, vl) = block_vint(block);
    if vl + 3 > block.len() {
        return None;
    }
    // Track 0 is invalid (MKV track numbers are 1-based). block_vint also
    // returns 0 for an unsupported 5+ byte VINT, so a corrupt/zero-track block
    // must be skipped rather than attributed to the first stream.
    if track == 0 {
        return None;
    }

    let rel_ts = i16::from_be_bytes([block[vl], block[vl + 1]]);
    let keyframe = block[vl + 2] & 0x80 != 0;
    let data = block[vl + 3..].to_vec();
    let pts_ticks = cluster_ts_ticks + rel_ts as i64;
    let track_idx = (track as usize) - 1; // track >= 1 checked above

    // Skip blocks for non-existent tracks.
    if track_idx >= streams_len {
        return None;
    }

    Some(crate::pes::PesFrame {
        track: track_idx,
        // saturating_mul: a hostile CLUSTER_TIMESTAMP could push pts_ticks near
        // i64::MAX, where ticks→ns would overflow and panic in debug builds.
        pts: pts_ticks.saturating_mul(ts_scale_ns),
        keyframe,
        data,
        duration_ns,
    })
}

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
    if d[0] & 0x20 != 0 && d.len() >= 3 {
        return (
            (((d[0] & 0x1F) as u64) << 16) | ((d[1] as u64) << 8) | d[2] as u64,
            3,
        );
    }
    if d[0] & 0x10 != 0 && d.len() >= 4 {
        return (
            (((d[0] & 0x0F) as u64) << 24)
                | ((d[1] as u64) << 16)
                | ((d[2] as u64) << 8)
                | d[3] as u64,
            4,
        );
    }
    (0, 1) // Unsupported 5+ byte VINT — treat as track 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pes::Stream as _;
    use std::io::Cursor;

    // `From<Error> for io::Error` encodes the numeric code into the
    // Display string as "E{code}: ...". Check the prefix.
    /// Extract the error from a `MkvStream::open` result without requiring
    /// `MkvStream: Debug` (which `unwrap_err` would).
    fn open_err(r: io::Result<MkvStream>) -> io::Error {
        match r {
            Ok(_) => panic!("expected MkvStream::open to fail"),
            Err(e) => e,
        }
    }

    fn is_mkv_invalid(e: &io::Error) -> bool {
        e.kind() == io::ErrorKind::InvalidData
            && e.to_string()
                .starts_with(&format!("E{}", crate::error::E_MKV_INVALID))
    }

    #[test]
    fn ts_pid_for_track_maps_and_rejects_overflow() {
        // Track 1 → video PID; track 2 → first audio PID base.
        assert_eq!(ts_pid_for_track(1).unwrap(), 0x1011);
        assert_eq!(ts_pid_for_track(2).unwrap(), 0x1100);
        assert_eq!(ts_pid_for_track(3).unwrap(), 0x1101);
        // Highest track that still lands inside the 13-bit PID space.
        // 0x1100 + (tnum-2) <= 0x1FFF  ⇒  tnum <= 0xF01.
        assert_eq!(ts_pid_for_track(0xF01).unwrap(), 0x1FFF);
        // One past the edge must be rejected, not wrap u16.
        assert!(is_mkv_invalid(&ts_pid_for_track(0xF02).unwrap_err()));
        // Former overflow case (debug panic / release garbage PID) is rejected.
        assert!(is_mkv_invalid(&ts_pid_for_track(u16::MAX).unwrap_err()));
        // Track 0 is invalid (1-based) and would underflow tnum-2.
        assert!(is_mkv_invalid(&ts_pid_for_track(0).unwrap_err()));
    }

    #[test]
    fn checked_size_rejects_over_cap() {
        // Within cap → Ok with usize value.
        assert_eq!(checked_size(100, 256).unwrap(), 100);
        assert_eq!(checked_size(256, 256).unwrap(), 256);
        // Over cap → MkvInvalid, never a giant allocation.
        let e = checked_size(257, 256).unwrap_err();
        assert!(is_mkv_invalid(&e));
        // A hostile multi-GB block size is rejected as MkvInvalid.
        let e = checked_size(4 * 1024 * 1024 * 1024, MAX_BLOCK_SIZE).unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn read_uint_bounded_rejects_oversized_int() {
        // size > 8 would index out of the fixed 8-byte buffer in
        // read_uint_val (panic / OOB). The guard turns it into a clean
        // MkvInvalid error instead.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = read_uint_bounded(&mut data, 9).unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn read_uint_bounded_accepts_valid_width() {
        // 8 bytes is the max legal EBML uint width and must still work.
        let mut data = Cursor::new(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02]);
        assert_eq!(read_uint_bounded(&mut data, 8).unwrap(), 0x0102);
    }

    #[test]
    fn read_string_bounded_rejects_huge_string() {
        // Claimed string length far above the cap must not allocate.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = read_string_bounded(&mut data, MAX_STRING_LEN + 1).unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    /// Build a minimal MKV (EBML header + Segment + Info + Tracks) so the
    /// reader is positioned in the cluster body, then append the given
    /// cluster bytes. Returns the full byte stream.
    fn minimal_mkv_with_cluster(cluster_body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        // EBML header (empty body).
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        // Segment (unknown size so the reader streams children).
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        // Empty Info.
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        // Empty Tracks.
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        out.extend_from_slice(cluster_body);
        out
    }

    #[test]
    fn simple_block_oversized_size_is_rejected() {
        // Cluster containing a SIMPLE_BLOCK that claims a 2 GiB payload.
        // The reader must reject it (MkvInvalid) rather than attempt a
        // multi-GB allocation. Header parse stops at CLUSTER, so the
        // SIMPLE_BLOCK is hit on the first read().
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, 2 * 1024 * 1024 * 1024).unwrap();
        // No payload follows — but we must fail on the size check, before
        // any read of the body.
        let bytes = minimal_mkv_with_cluster(&cluster);

        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn well_formed_simple_block_round_trips() {
        // A small, well-formed SIMPLE_BLOCK must still parse into a frame.
        // We need at least one stream so the track index is in range, so
        // give Tracks one video TRACK_ENTRY (track number 1).
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();

        // Tracks → one TRACK_ENTRY (track number 1, type 1 = video).
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        // Cluster with a SIMPLE_BLOCK: track vint=0x81 (track 1),
        // rel_ts=0x0000, flags=0x80 (keyframe), then 4 bytes of data.
        ebml::write_id(&mut out, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA, 0xBB, 0xCC, 0xDD];
        ebml::write_id(&mut out, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut out, block.len() as u64).unwrap();
        out.extend_from_slice(&block);

        let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
        let frame = stream.read().unwrap().expect("expected a frame");
        assert_eq!(frame.track, 0);
        assert!(frame.keyframe);
        assert_eq!(frame.data, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    }
    #[test]
    fn truncated_simple_block_body_errors_not_panics() {
        // A SIMPLE_BLOCK that declares a 64-byte payload but supplies none.
        // read_exact_bounded must surface a clean typed MkvInvalid error
        // (a truncated declared element is malformed input), never panic,
        // and never allocate the full declared size up front.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, 64).unwrap();
        // No body bytes follow → short read.
        let bytes = minimal_mkv_with_cluster(&cluster);

        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    /// Build a minimal MKV header + Segment + Info, then a Tracks element with a
    /// single TRACK_ENTRY of the given track number/type, then the cluster bytes.
    fn mkv_with_track_and_cluster(tnum: u64, ttype: u64, cluster_body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();

        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, tnum).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, ttype).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        out.extend_from_slice(cluster_body);
        out
    }

    #[test]
    fn oversized_codec_private_is_rejected() {
        // A TRACK_ENTRY whose CODEC_PRIVATE declares a payload above
        // MAX_CODEC_PRIVATE must be rejected (MkvInvalid) before any
        // multi-MB allocation, while parsing the header.
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        // CODEC_PRIVATE header claiming a huge size (no body needed — the
        // size check fires first).
        ebml::write_id(&mut entry, ebml::CODEC_PRIVATE).unwrap();
        ebml::write_size(&mut entry, MAX_CODEC_PRIVATE + 1).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        let e = match MkvStream::open(Cursor::new(out)) {
            Ok(_) => panic!("expected MkvInvalid, got Ok"),
            Err(e) => e,
        };
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn block_group_frame_round_trips_with_duration() {
        // MkvMuxer emits AC3/PGS frames as a BlockGroup (BLOCK + BLOCK_DURATION).
        // The reader must descend into the group and yield the frame (with its
        // duration) rather than skipping it — otherwise every AC3/PGS frame this
        // muxer writes is lost on read-back.
        let block = [0x82u8, 0x00, 0x05, 0x00, 0x11, 0x22, 0x33]; // track 2, rel 5, not-kf, 3 data
        let mut bg_body = Vec::new();
        ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
        ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
        bg_body.extend_from_slice(&block);
        ebml::write_uint(&mut bg_body, ebml::BLOCK_DURATION, 40).unwrap(); // 40 ms

        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        // CLUSTER_TIMESTAMP = 100 ms so pts = (100 + 5) ms.
        ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 100).unwrap();
        ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
        ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
        cluster.extend_from_slice(&bg_body);

        // Track 2 (audio) so track_idx 1 needs two streams; give two TRACK_ENTRYs.
        // Reuse the helper for track 1, then a manual second entry would be
        // simpler — instead build directly with two entries.
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        let mut tracks = Vec::new();
        for (n, t) in [(1u64, 1u64), (2u64, 2u64)] {
            let mut entry = Vec::new();
            ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, n).unwrap();
            ebml::write_uint(&mut entry, ebml::TRACK_TYPE, t).unwrap();
            ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
            ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
            tracks.extend_from_slice(&entry);
        }
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);
        out.extend_from_slice(&cluster);

        let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
        let frame = stream
            .read()
            .unwrap()
            .expect("BlockGroup frame must be read");
        assert_eq!(frame.track, 1, "track 2 → index 1");
        assert!(!frame.keyframe);
        assert_eq!(frame.data, vec![0x11, 0x22, 0x33]);
        assert_eq!(frame.pts, 105 * 1_000_000, "pts = (cluster 100 + rel 5) ms");
        assert_eq!(frame.duration_ns, Some(40 * 1_000_000));
    }

    #[test]
    fn track_number_zero_is_rejected() {
        // A TRACK_ENTRY with TRACK_NUMBER 0 must be rejected (the ts_pid
        // computation would underflow `tnum - 2`).
        let bytes = mkv_with_track_and_cluster(0, 1, &[]);
        let e = open_err(MkvStream::open(Cursor::new(bytes)));
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn track_number_above_u16_is_rejected() {
        // 65536 would truncate to 0 via `as u16` and then underflow.
        let bytes = mkv_with_track_and_cluster(65536, 1, &[]);
        let e = open_err(MkvStream::open(Cursor::new(bytes)));
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn unknown_size_inner_child_in_tracks_is_rejected() {
        // A TRACK_ENTRY child declaring EBML unknown size (cs == u64::MAX) must
        // be rejected, not used in `hlen + cs` (which would overflow → debug
        // panic). Hand-build a TRACK_ENTRY whose first child carries the
        // unknown-size marker.
        let mut entry = Vec::new();
        ebml::write_id(&mut entry, ebml::TRACK_NUMBER).unwrap();
        ebml::write_unknown_size(&mut entry).unwrap(); // child size = unknown

        let mut tracks = Vec::new();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);

        let e = open_err(MkvStream::open(Cursor::new(out)));
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn oversized_title_string_is_rejected() {
        // INFO/TITLE declaring a string above MAX_STRING_LEN must be
        // rejected during header parse, not allocated.
        let mut info = Vec::new();
        ebml::write_id(&mut info, ebml::TITLE).unwrap();
        ebml::write_size(&mut info, MAX_STRING_LEN + 1).unwrap();

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, info.len() as u64).unwrap();
        out.extend_from_slice(&info);

        let e = match MkvStream::open(Cursor::new(out)) {
            Ok(_) => panic!("expected MkvInvalid, got Ok"),
            Err(e) => e,
        };
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn read_uint_val_len_nine_errors_not_panics() {
        // Direct helper test: an EBML uint cannot exceed 8 bytes. len=9
        // would index past the fixed 8-byte stack buffer and panic on
        // untrusted input; it must return MkvInvalid instead.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = ebml::read_uint_val(&mut data, 9).unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn read_float_val_bad_width_errors() {
        // EBML floats are exactly 0, 4, or 8 bytes. Any other width is
        // malformed and must error rather than over- or under-read.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = ebml::read_float_val(&mut data, 5).unwrap_err();
        assert!(is_mkv_invalid(&e));
        // 0/4/8 remain valid widths.
        let mut z = Cursor::new(vec![0u8; 16]);
        assert_eq!(ebml::read_float_val(&mut z, 0).unwrap(), 0.0);
        let mut f4 = Cursor::new(vec![0u8; 16]);
        assert!(ebml::read_float_val(&mut f4, 4).is_ok());
        let mut f8 = Cursor::new(vec![0u8; 16]);
        assert!(ebml::read_float_val(&mut f8, 8).is_ok());
    }

    #[test]
    fn non_utf8_string_element_is_rejected() {
        // A string element with invalid UTF-8 bytes must surface a numeric
        // MkvInvalid error, not an io::Error wrapping the FromUtf8Error
        // English message (library no-English rule).
        let mut data = Cursor::new(vec![0xFF, 0xFE, 0xFD, 0xFC]);
        let e = ebml::read_string_val(&mut data, 4).unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn simple_block_track_zero_is_skipped() {
        // A SimpleBlock with track vint 0 must be skipped, not attributed to
        // track 0. Build one track, then a cluster whose only block is track 0
        // followed by a valid track-1 block; read() must return the track-1 one.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        // track vint 0 is not directly encodable (0x80 is track 0 → block_vint
        // returns (0,1)); use 0x80 as the track byte.
        let bad = [0x80u8, 0x00, 0x00, 0x80, 0xEE];
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, bad.len() as u64).unwrap();
        cluster.extend_from_slice(&bad);
        let good = [0x81u8, 0x00, 0x00, 0x80, 0xAB, 0xCD];
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, good.len() as u64).unwrap();
        cluster.extend_from_slice(&good);

        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frame = stream.read().unwrap().expect("track-1 frame expected");
        assert_eq!(frame.track, 0);
        assert_eq!(frame.data, vec![0xAB, 0xCD]);
    }
}
