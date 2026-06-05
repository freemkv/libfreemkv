//! MkvStream — Matroska container stream.
//!
//! Read: MKV container → demux EBML → PES frames out.
//! Write: PES frames in → MKV mux → Matroska container.

use super::mkv::{MkvMuxer, MkvTrack};
use super::{WriteSeek, ebml};

type MkvHeaderResult = io::Result<(crate::disc::DiscTitle, Vec<(u16, Vec<u8>)>)>;

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
    cluster_ts_ms: i64,
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
        let (disc_title, codec_privates) = parse_mkv_header(&mut reader)?;
        Ok(Self {
            disc_title,
            mode: Mode::Read(ReadState {
                reader: Box::new(reader),
                cluster_ts_ms: 0,
                codec_privates,
            }),
        })
    }
}

impl crate::pes::Stream for MkvStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        let rs = match self.mode {
            Mode::Read(ref mut rs) => rs,
            Mode::Write { .. } => return Err(crate::error::Error::StreamWriteOnly.into()),
        };

        loop {
            let (id, size, _) = match ebml::read_element_header(&mut rs.reader) {
                Ok(h) => h,
                Err(_) => return Ok(None),
            };

            match id {
                ebml::CLUSTER => continue,
                ebml::CLUSTER_TIMESTAMP => {
                    rs.cluster_ts_ms = read_uint_bounded(&mut rs.reader, size)? as i64;
                    continue;
                }
                ebml::SIMPLE_BLOCK => {
                    let block =
                        ebml::read_binary_val(&mut rs.reader, checked_size(size, MAX_BLOCK_SIZE)?)?;
                    if block.len() < 4 {
                        continue;
                    }

                    let (track, vl) = block_vint(&block);
                    if vl + 3 > block.len() {
                        continue;
                    }

                    let rel_ts = i16::from_be_bytes([block[vl], block[vl + 1]]);
                    let keyframe = block[vl + 2] & 0x80 != 0;
                    let data = block[vl + 3..].to_vec();
                    let pts_ms = rs.cluster_ts_ms + rel_ts as i64;
                    let track_idx = (track as usize).saturating_sub(1); // MKV tracks are 1-based

                    // Skip blocks for non-existent tracks
                    if track_idx >= self.disc_title.streams.len() {
                        continue;
                    }

                    return Ok(Some(crate::pes::PesFrame {
                        track: track_idx,
                        pts: pts_ms * 1_000_000, // ms → ns
                        keyframe,
                        data,
                        duration_ns: None,
                    }));
                }
                _ => {
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
            Err(_) => break,
        };

        match id {
            ebml::INFO => {
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
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
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
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
    Ok((disc_title, codec_privates))
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
        remaining = remaining.saturating_sub(hlen as u64 + cs);
        match cid {
            ebml::TRACK_NUMBER => tnum = read_uint_bounded(r, cs)? as u16,
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

    // Map MKV track numbers to BD-TS PIDs
    let ts_pid = if tnum == 1 {
        0x1011
    } else {
        0x1100 + (tnum - 2)
    };

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
    fn is_mkv_invalid(e: &io::Error) -> bool {
        e.kind() == io::ErrorKind::InvalidData
            && e.to_string()
                .starts_with(&format!("E{}", crate::error::E_MKV_INVALID))
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
}
