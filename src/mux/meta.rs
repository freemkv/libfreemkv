//! M2TS metadata header — embeds title/stream info in raw m2ts files.
//!
//! Format: [8B magic] [4B json_len] [JSON] [padding to 192B boundary] [BD-TS data...]
//! Other tools skip the header during TS sync recovery (scan for 0x47).

use crate::disc::{
    AudioStream, Codec, ColorSpace, DiscTitle, HdrFormat, Stream, SubtitleStream, VideoStream,
};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// Magic bytes: "FMKV" + version 1 + 2 reserved bytes.
const MAGIC: [u8; 8] = [b'F', b'M', b'K', b'V', 0x00, 0x01, 0x00, 0x00];

/// BD-TS packet size (header must be padded to this boundary).
const PACKET_SIZE: usize = 192;

/// Metadata embedded in an m2ts file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct M2tsMeta {
    /// Format version.
    pub v: u8,
    /// Title name (e.g. filename stem or disc title).
    #[serde(default)]
    pub title: String,
    /// Duration in seconds.
    #[serde(default)]
    pub duration: f64,
    /// Stream descriptors.
    pub streams: Vec<MetaStream>,
}

/// A single stream descriptor in the metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MetaStream {
    #[serde(rename = "video")]
    Video {
        pid: u16,
        codec: String,
        #[serde(default)]
        resolution: String,
        #[serde(default)]
        frame_rate: String,
        #[serde(default)]
        hdr: String,
        #[serde(default)]
        label: String,
        #[serde(default)]
        secondary: bool,
    },
    #[serde(rename = "audio")]
    Audio {
        pid: u16,
        codec: String,
        #[serde(default)]
        channels: String,
        #[serde(default)]
        language: String,
        #[serde(default)]
        sample_rate: String,
        #[serde(default)]
        label: String,
        #[serde(default)]
        secondary: bool,
    },
    #[serde(rename = "subtitle")]
    Subtitle {
        pid: u16,
        codec: String,
        #[serde(default)]
        language: String,
        #[serde(default)]
        forced: bool,
    },
}

impl M2tsMeta {
    /// Build metadata from a disc Title.
    pub fn from_title(title: &DiscTitle) -> Self {
        let streams = title
            .streams
            .iter()
            .map(|s| match s {
                Stream::Video(v) => MetaStream::Video {
                    pid: v.pid,
                    codec: codec_to_str(v.codec),
                    resolution: v.resolution.clone(),
                    frame_rate: v.frame_rate.clone(),
                    hdr: hdr_to_str(v.hdr),
                    label: v.label.clone(),
                    secondary: v.secondary,
                },
                Stream::Audio(a) => MetaStream::Audio {
                    pid: a.pid,
                    codec: codec_to_str(a.codec),
                    channels: a.channels.clone(),
                    language: a.language.clone(),
                    sample_rate: a.sample_rate.clone(),
                    label: a.label.clone(),
                    secondary: a.secondary,
                },
                Stream::Subtitle(s) => MetaStream::Subtitle {
                    pid: s.pid,
                    codec: codec_to_str(s.codec),
                    language: s.language.clone(),
                    forced: s.forced,
                },
            })
            .collect();

        Self {
            v: 1,
            title: title.playlist.clone(),
            duration: title.duration_secs,
            streams,
        }
    }

    /// Convert back to a library Title (for remux).
    pub fn to_title(&self) -> DiscTitle {
        let streams = self
            .streams
            .iter()
            .map(|s| match s {
                MetaStream::Video {
                    pid,
                    codec,
                    resolution,
                    frame_rate,
                    hdr,
                    label,
                    secondary,
                } => Stream::Video(VideoStream {
                    pid: *pid,
                    codec: str_to_codec(codec),
                    resolution: resolution.clone(),
                    frame_rate: frame_rate.clone(),
                    hdr: str_to_hdr(hdr),
                    color_space: ColorSpace::Bt709,
                    secondary: *secondary,
                    label: label.clone(),
                }),
                MetaStream::Audio {
                    pid,
                    codec,
                    channels,
                    language,
                    sample_rate,
                    label,
                    secondary,
                } => Stream::Audio(AudioStream {
                    pid: *pid,
                    codec: str_to_codec(codec),
                    channels: channels.clone(),
                    language: language.clone(),
                    sample_rate: sample_rate.clone(),
                    secondary: *secondary,
                    label: label.clone(),
                }),
                MetaStream::Subtitle {
                    pid,
                    codec,
                    language,
                    forced,
                } => Stream::Subtitle(SubtitleStream {
                    pid: *pid,
                    codec: str_to_codec(codec),
                    language: language.clone(),
                    forced: *forced,
                    codec_data: None,
                }),
            })
            .collect();

        DiscTitle {
            playlist: self.title.clone(),
            playlist_id: 0,
            duration_secs: self.duration,
            size_bytes: 0,
            clips: Vec::new(),
            streams,
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: crate::disc::ContentFormat::BdTs,
        }
    }
}

/// Write the metadata header to a writer. Padded to 192-byte boundary.
pub fn write_header(w: &mut impl Write, meta: &M2tsMeta) -> io::Result<()> {
    let json = serde_json::to_vec(meta).map_err(|e| io::Error::other(e))?;

    let json_len = json.len() as u32;
    let raw_len = 8 + 4 + json.len(); // magic + len + json
    let padded_len = raw_len.div_ceil(PACKET_SIZE) * PACKET_SIZE;
    let padding = padded_len - raw_len;

    w.write_all(&MAGIC)?;
    w.write_all(&json_len.to_be_bytes())?;
    w.write_all(&json)?;
    if padding > 0 {
        w.write_all(&vec![0u8; padding])?;
    }
    Ok(())
}

/// Try to read a metadata header from the start of an m2ts file.
/// Returns None for bare m2ts files (no header).
/// On success, leaves reader positioned at the first TS packet.
/// On failure, seeks back to the start.
pub fn read_header<R: Read + Seek>(r: &mut R) -> io::Result<Option<M2tsMeta>> {
    let start = r.stream_position()?;

    let mut magic = [0u8; 8];
    if r.read_exact(&mut magic).is_err() {
        r.seek(SeekFrom::Start(start))?;
        return Ok(None);
    }

    if magic[..4] != MAGIC[..4] {
        // Not a freemkv m2ts — seek back
        r.seek(SeekFrom::Start(start))?;
        return Ok(None);
    }

    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let json_len = u32::from_be_bytes(len_buf) as usize;

    let mut json_buf = vec![0u8; json_len];
    r.read_exact(&mut json_buf)?;

    let meta: M2tsMeta = serde_json::from_slice(&json_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Skip padding to next 192-byte boundary
    let raw_len = 8 + 4 + json_len;
    let padded_len = raw_len.div_ceil(PACKET_SIZE) * PACKET_SIZE;
    let padding = padded_len - raw_len;
    if padding > 0 {
        r.seek(SeekFrom::Current(padding as i64))?;
    }

    Ok(Some(meta))
}

/// Read a metadata header from a forward-only stream (no Seek required).
/// Returns None if the magic bytes don't match. Consumes the header bytes.
pub fn read_header_from_stream(r: &mut impl Read) -> io::Result<Option<M2tsMeta>> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;

    if magic[..4] != MAGIC[..4] {
        return Ok(None);
    }

    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let json_len = u32::from_be_bytes(len_buf) as usize;

    let mut json_buf = vec![0u8; json_len];
    r.read_exact(&mut json_buf)?;

    let meta: M2tsMeta = serde_json::from_slice(&json_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Skip padding
    let raw_len = 8 + 4 + json_len;
    let padded_len = raw_len.div_ceil(PACKET_SIZE) * PACKET_SIZE;
    let padding = padded_len - raw_len;
    if padding > 0 {
        let mut skip = vec![0u8; padding];
        r.read_exact(&mut skip)?;
    }

    Ok(Some(meta))
}

// Codec string conversion (compact, no English — just codec identifiers)
fn codec_to_str(c: Codec) -> String {
    match c {
        Codec::Hevc => "hevc",
        Codec::H264 => "h264",
        Codec::Vc1 => "vc1",
        Codec::Mpeg2 => "mpeg2",
        Codec::TrueHd => "truehd",
        Codec::DtsHdMa => "dtshd_ma",
        Codec::DtsHdHr => "dtshd_hr",
        Codec::Dts => "dts",
        Codec::Ac3 => "ac3",
        Codec::Ac3Plus => "eac3",
        Codec::Lpcm => "lpcm",
        Codec::Pgs => "pgs",
        Codec::DvdSub => "dvdsub",
        Codec::Unknown(_) => "unknown",
    }
    .into()
}

fn str_to_codec(s: &str) -> Codec {
    match s {
        "hevc" => Codec::Hevc,
        "h264" => Codec::H264,
        "vc1" => Codec::Vc1,
        "mpeg2" => Codec::Mpeg2,
        "truehd" => Codec::TrueHd,
        "dtshd_ma" => Codec::DtsHdMa,
        "dtshd_hr" => Codec::DtsHdHr,
        "dts" => Codec::Dts,
        "ac3" => Codec::Ac3,
        "eac3" => Codec::Ac3Plus,
        "lpcm" => Codec::Lpcm,
        "pgs" => Codec::Pgs,
        _ => Codec::Unknown(0),
    }
}

fn hdr_to_str(h: HdrFormat) -> String {
    match h {
        HdrFormat::Sdr => "sdr",
        HdrFormat::Hdr10 => "hdr10",
        HdrFormat::DolbyVision => "dv",
    }
    .into()
}

fn str_to_hdr(s: &str) -> HdrFormat {
    match s {
        "hdr10" => HdrFormat::Hdr10,
        "dv" => HdrFormat::DolbyVision,
        _ => HdrFormat::Sdr,
    }
}
