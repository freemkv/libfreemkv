//! MkvStream — Matroska container stream.
//!
//! Read: MKV container → demux EBML → PES frames out.
//! Write: PES frames in → MKV mux → Matroska container.

use super::mkv::{MkvMuxer, MkvTrack};
use super::{ebml, WriteSeek};

type MkvHeaderResult = io::Result<(crate::disc::DiscTitle, Vec<(u16, Vec<u8>)>)>;

/// Skip `n` bytes on a forward-only reader (no Seek required).
fn skip_bytes(r: &mut impl Read, n: u64) -> io::Result<()> {
    io::copy(&mut r.take(n), &mut io::sink())?;
    Ok(())
}

use crate::disc::*;
use std::io::{self, Read};

struct ReadState {
    reader: Box<dyn Read>,
    cluster_ts_ms: i64,
    /// Codec private data per track (track_number, hvcC/avcC bytes).
    codec_privates: Vec<(u16, Vec<u8>)>,
}

enum Mode {
    Write {
        muxer: Option<MkvMuxer<Box<dyn WriteSeek>>>,
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
    pub fn create(writer: Box<dyn WriteSeek>, title: &DiscTitle) -> io::Result<Self> {
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
    pub fn open(mut reader: impl Read + 'static) -> io::Result<Self> {
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
            Mode::Write {
                muxer: Some(ref mut m),
            } => m.write_frame(frame.track, frame.pts, frame.keyframe, &frame.data),
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
    let mut duration_ms = 0.0f64;
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
                        ebml::TIMESTAMP_SCALE => ts_scale = ebml::read_uint_val(r, cs as usize)?,
                        ebml::DURATION => duration_ms = ebml::read_float_val(r, cs as usize)?,
                        ebml::TITLE => title = ebml::read_string_val(r, cs as usize)?,
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
        duration_secs: duration_ms * (ts_scale as f64) / 1_000_000_000.0,
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
            ebml::TRACK_NUMBER => tnum = ebml::read_uint_val(r, cs as usize)? as u16,
            ebml::TRACK_TYPE => ttype = ebml::read_uint_val(r, cs as usize)?,
            ebml::CODEC_ID => codec_id = ebml::read_string_val(r, cs as usize)?,
            ebml::CODEC_PRIVATE => codec_priv = Some(ebml::read_binary_val(r, cs as usize)?),
            ebml::LANGUAGE => lang = ebml::read_string_val(r, cs as usize)?,
            ebml::TRACK_NAME => name = ebml::read_string_val(r, cs as usize)?,
            ebml::FLAG_FORCED => forced = ebml::read_uint_val(r, cs as usize)? != 0,
            ebml::VIDEO => {
                let mut vrem = cs;
                while vrem > 0 {
                    let (vid, vs, vhlen) = ebml::read_element_header(r)?;
                    vrem = vrem.saturating_sub(vhlen as u64 + vs);
                    if vid == ebml::PIXEL_HEIGHT {
                        ph = ebml::read_uint_val(r, vs as usize)? as u32;
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
                        ebml::CHANNELS => ch = ebml::read_uint_val(r, as_ as usize)? as u8,
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
