//! M2TS metadata header — embeds title/stream info in raw m2ts files.
//!
//! Format: [8B magic] [4B json_len] [JSON] [padding to 192B boundary] [BD-TS data...]
//! Other tools skip the header during TS sync recovery (scan for 0x47).

use crate::disc::{AudioStream, ColorSpace, DiscTitle, Stream, SubtitleStream, VideoStream};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

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
        /// Base64-encoded codec initialization data (HEVCDecoderConfigurationRecord, etc.)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        codec_private: Option<String>,
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
    /// Build metadata from a DiscTitle. Codec privates come from title.codec_privates.
    pub fn from_title(title: &DiscTitle) -> Self {
        use base64::Engine;
        let streams = title
            .streams
            .iter()
            .enumerate()
            .map(|(i, s)| match s {
                Stream::Video(v) => MetaStream::Video {
                    pid: v.pid,
                    codec: v.codec.id().into(),
                    resolution: v.resolution.to_string(),
                    frame_rate: v.frame_rate.to_string(),
                    hdr: v.hdr.id().into(),
                    label: v.label.clone(),
                    secondary: v.secondary,
                    codec_private: title
                        .codec_privates
                        .get(i)
                        .and_then(|cp| cp.as_ref())
                        .map(|cp| base64::engine::general_purpose::STANDARD.encode(cp)),
                },
                Stream::Audio(a) => MetaStream::Audio {
                    pid: a.pid,
                    codec: a.codec.id().into(),
                    channels: a.channels.to_string(),
                    language: a.language.clone(),
                    sample_rate: a.sample_rate.to_string(),
                    label: a.label.clone(),
                    secondary: a.secondary,
                },
                Stream::Subtitle(s) => MetaStream::Subtitle {
                    pid: s.pid,
                    codec: s.codec.id().into(),
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
                    codec_private: _,
                } => Stream::Video(VideoStream {
                    pid: *pid,
                    codec: codec.parse().unwrap_or(crate::disc::Codec::Unknown(0)),
                    resolution: resolution
                        .parse()
                        .unwrap_or(crate::disc::Resolution::Unknown),
                    frame_rate: frame_rate
                        .parse()
                        .unwrap_or(crate::disc::FrameRate::Unknown),
                    hdr: hdr.parse().unwrap_or(crate::disc::HdrFormat::Sdr),
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
                    codec: codec.parse().unwrap_or(crate::disc::Codec::Unknown(0)),
                    channels: channels
                        .parse()
                        .unwrap_or(crate::disc::AudioChannels::Unknown),
                    language: language.clone(),
                    sample_rate: sample_rate
                        .parse()
                        .unwrap_or(crate::disc::SampleRate::Unknown),
                    secondary: *secondary,
                    purpose: crate::disc::LabelPurpose::Normal,
                    label: label.clone(),
                }),
                MetaStream::Subtitle {
                    pid,
                    codec,
                    language,
                    forced,
                } => Stream::Subtitle(SubtitleStream {
                    pid: *pid,
                    codec: codec.parse().unwrap_or(crate::disc::Codec::Unknown(0)),
                    language: language.clone(),
                    forced: *forced,
                    qualifier: crate::disc::LabelQualifier::None,
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
            codec_privates: self.codec_privates(),
        }
    }

    /// Extract codec_private data per stream (from FMKV header).
    /// Returns a Vec matching stream order — None for streams without codec_private.
    pub fn codec_privates(&self) -> Vec<Option<Vec<u8>>> {
        self.streams
            .iter()
            .map(|s| {
                if let MetaStream::Video {
                    codec_private: Some(b64),
                    ..
                } = s
                {
                    {
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD.decode(b64).ok()
                    }
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Write the metadata header to a writer. Padded to 192-byte boundary.
pub fn write_header(w: &mut impl Write, meta: &M2tsMeta) -> io::Result<()> {
    let json = serde_json::to_vec(meta).map_err(io::Error::other)?;

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

/// Try to read an FMKV metadata header.
/// Returns None if magic bytes don't match. Consumes header bytes on success.
/// Caller handles seek-back on failure if needed (e.g. for fallback PMT scan).
pub fn read_header(r: &mut impl Read) -> io::Result<Option<M2tsMeta>> {
    const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10 MB

    let mut magic = [0u8; 8];
    if r.read_exact(&mut magic).is_err() {
        return Ok(None);
    }

    if magic[..4] != MAGIC[..4] {
        return Ok(None);
    }

    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let json_len = u32::from_be_bytes(len_buf) as usize;
    if json_len > MAX_JSON_SIZE {
        return Err(crate::error::Error::NoMetadata.into());
    }

    let mut json_buf = vec![0u8; json_len];
    r.read_exact(&mut json_buf)?;

    let meta: M2tsMeta = serde_json::from_slice(&json_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Skip padding to next 192-byte boundary
    let raw_len = 8 + 4 + json_len;
    let padded_len = raw_len.div_ceil(PACKET_SIZE) * PACKET_SIZE;
    let padding = padded_len - raw_len;
    if padding > 0 {
        let mut skip = vec![0u8; padding];
        r.read_exact(&mut skip)?;
    }

    Ok(Some(meta))
}

// Serialization uses Codec::id() / HdrFormat::id() and Display impls.
// Deserialization uses FromStr impls (.parse()) on each enum.
