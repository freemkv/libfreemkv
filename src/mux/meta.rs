//! M2TS metadata header — embeds title/stream info in raw m2ts files.
//!
//! Format: `[8B magic] [4B json_len] [JSON] [padding to 192B boundary] [BD-TS data...]`
//! Other tools skip the header during TS sync recovery (scan for 0x47).

use crate::disc::{
    AudioStream, ColorSpace, DiscTitle, HdrFormat, Stream, SubtitleStream, VideoStream,
};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

/// Derive the color space from the HDR format, for metadata written before the
/// color space was persisted (pre-0.30.7). All HDR formats use BT.2020 wide
/// gamut; SDR uses BT.709.
fn color_space_from_hdr(hdr: HdrFormat) -> ColorSpace {
    match hdr {
        HdrFormat::Hdr10 | HdrFormat::Hdr10Plus | HdrFormat::Hlg | HdrFormat::DolbyVision => {
            ColorSpace::Bt2020
        }
        HdrFormat::Sdr => ColorSpace::Bt709,
    }
}

/// Magic bytes: "FMKV" + 1 reserved byte + version (=1) + 2 reserved bytes.
const MAGIC: [u8; 8] = [b'F', b'M', b'K', b'V', 0x00, 0x01, 0x00, 0x00];

/// Highest header format version this build understands. A header tagged with
/// a newer version is rejected so older readers cleanly refuse incompatible
/// formats instead of silently mis-parsing them as v1. Version 2 = every PES
/// frame after the header carries an 8-byte DiscardPadding extension; it is
/// written only when a track has decoder timing, so other streams stay v1.
const SUPPORTED_VERSION: u8 = 2;

/// Index of the version byte within [`MAGIC`].
const VERSION_BYTE: usize = 5;

use crate::consts::BD_SOURCE_PACKET_BYTES;

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
    /// Per-track decoder timing (Opus CodecDelay/SeekPreRoll); non-default only.
    /// Readers that predate it ignore the field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timings: Vec<MetaTiming>,
    /// Chapter marks; absent in older headers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chapters: Vec<MetaChapter>,
    /// Frame format of the source ("mpeg_ps"); empty/absent = BD-TS.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content_format: String,
    /// Frames on the wire carry the DiscardPadding extension (header v2). Taken
    /// from the version byte, not the JSON.
    #[serde(skip)]
    pub frame_padding: bool,
}

/// Decoder timing for one stream index, carried in the FMKV header.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MetaTiming {
    pub track: usize,
    #[serde(default)]
    pub codec_delay_ns: u64,
    #[serde(default)]
    pub seek_preroll_ns: u64,
}

/// One chapter mark carried in the FMKV header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaChapter {
    pub time_secs: f64,
    #[serde(default)]
    pub name: String,
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
        /// Color space id (e.g. "bt709", "bt2020"). Empty/absent in pre-0.30.7
        /// metadata — `to_title` then derives it from `hdr` so HDR color
        /// primaries/transfer/matrix still round-trip.
        #[serde(default)]
        color_space: String,
        #[serde(default)]
        label: String,
        #[serde(default)]
        secondary: bool,
        /// Base64-encoded codec initialization data (HEVCDecoderConfigurationRecord, etc.)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        codec_private: Option<String>,
        /// Display aspect `[w, h]` when it differs from the pixel grid.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_aspect: Option<[u32; 2]>,
        /// Measured CICP `[matrix, transfer, primaries, range]`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cicp: Option<[u8; 4]>,
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
        /// Base64-encoded codec initialization data. Absent for codecs that
        /// carry none. Without this, a remux driven from an FMKV header would
        /// emit audio tracks missing their init data versus a direct rip.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        codec_private: Option<String>,
        /// Stream purpose ("commentary", "descriptive", "score", "ime"); empty = normal.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        purpose: String,
    },
    #[serde(rename = "subtitle")]
    Subtitle {
        pid: u16,
        codec: String,
        #[serde(default)]
        language: String,
        #[serde(default)]
        forced: bool,
        /// Base64-encoded codec initialization data (e.g. VobSub idx palette).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        codec_private: Option<String>,
        /// Qualifier ("sdh", "descriptive_service", "forced"); empty = none.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        qualifier: String,
    },
}

impl M2tsMeta {
    /// Build metadata from a DiscTitle. Codec privates come from title.codec_privates.
    pub fn from_title(title: &DiscTitle) -> Self {
        use base64::Engine;
        // Per-stream codec init data, base64-encoded. Preserved for ALL stream
        // kinds (video/audio/subtitle) so an FMKV-header-driven remux matches a
        // direct disc rip — previously only video round-tripped.
        let codec_private_b64 = |i: usize| -> Option<String> {
            title
                .codec_privates
                .get(i)
                .and_then(|cp| cp.as_ref())
                .map(|cp| base64::engine::general_purpose::STANDARD.encode(cp))
        };
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
                    color_space: v.color_space.id().into(),
                    label: v.label.clone(),
                    secondary: v.secondary,
                    codec_private: codec_private_b64(i),
                    display_aspect: v.display_aspect.map(|(w, h)| [w, h]),
                    cicp: v
                        .measured_cicp
                        .map(|c| [c.matrix, c.transfer, c.primaries, c.range]),
                },
                Stream::Audio(a) => MetaStream::Audio {
                    pid: a.pid,
                    codec: a.codec.id().into(),
                    channels: a.channels.to_string(),
                    language: a.language.clone(),
                    sample_rate: a.sample_rate.to_string(),
                    label: a.label.clone(),
                    secondary: a.secondary,
                    codec_private: codec_private_b64(i),
                    purpose: purpose_id(a.purpose).into(),
                },
                Stream::Subtitle(s) => MetaStream::Subtitle {
                    pid: s.pid,
                    codec: s.codec.id().into(),
                    language: s.language.clone(),
                    forced: s.forced,
                    codec_private: codec_private_b64(i),
                    qualifier: qualifier_id(s.qualifier).into(),
                },
            })
            .collect();

        Self {
            v: 1,
            title: title.playlist.clone(),
            duration: title.duration_secs,
            streams,
            timings: Vec::new(),
            // A non-finite time would serialize as null and fail the whole header on read.
            chapters: title
                .chapters
                .iter()
                .filter(|c| c.time_secs.is_finite())
                .map(|c| MetaChapter {
                    time_secs: c.time_secs,
                    name: c.name.clone(),
                })
                .collect(),
            content_format: match title.content_format {
                crate::disc::ContentFormat::BdTs => String::new(),
                // The DVD navigation does not survive into a transport stream: the
                // container class is all a reader of this header can use.
                crate::disc::ContentFormat::MpegPs | crate::disc::ContentFormat::DvdPs => {
                    "mpeg_ps".into()
                }
            },
            frame_padding: false,
        }
    }

    /// Attach per-track decoder timing; any non-default timing switches the
    /// stream to header v2 so DiscardPadding travels with each frame.
    pub fn with_timings(mut self, timings: &[crate::pes::TrackTiming]) -> Self {
        self.timings = timings
            .iter()
            .enumerate()
            .filter(|(_, t)| **t != crate::pes::TrackTiming::default())
            .map(|(track, t)| MetaTiming {
                track,
                codec_delay_ns: t.codec_delay_ns,
                seek_preroll_ns: t.seek_preroll_ns,
            })
            .collect();
        self.frame_padding = !self.timings.is_empty();
        self
    }

    /// Decoder timing of stream `track` (default when the header has none).
    pub fn timing(&self, track: usize) -> crate::pes::TrackTiming {
        self.timings
            .iter()
            .find(|t| t.track == track)
            .map(|t| crate::pes::TrackTiming {
                codec_delay_ns: t.codec_delay_ns,
                seek_preroll_ns: t.seek_preroll_ns,
            })
            .unwrap_or_default()
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
                    color_space,
                    label,
                    secondary,
                    codec_private: _,
                    display_aspect,
                    cicp,
                } => {
                    let hdr_fmt = hdr.parse().unwrap_or(crate::disc::HdrFormat::Sdr);
                    // Prefer the stored color space; pre-0.30.7 metadata has none,
                    // so derive from HDR format: every HDR variant (HDR10/HDR10+/
                    // HLG/Dolby Vision) is BT.2020, SDR is BT.709.
                    let cs = if color_space.is_empty() {
                        color_space_from_hdr(hdr_fmt)
                    } else {
                        color_space
                            .parse::<ColorSpace>()
                            .unwrap_or(ColorSpace::Unknown)
                    };
                    Stream::Video(VideoStream {
                        pid: *pid,
                        codec: codec.parse().unwrap_or(crate::disc::Codec::Unknown(0)),
                        resolution: resolution
                            .parse()
                            .unwrap_or(crate::disc::Resolution::Unknown),
                        frame_rate: frame_rate
                            .parse()
                            .unwrap_or(crate::disc::FrameRate::Unknown),
                        hdr: hdr_fmt,
                        color_space: cs,
                        display_aspect: display_aspect.map(|[w, h]| (w, h)),
                        secondary: *secondary,
                        label: label.clone(),
                        measured_cicp: cicp.map(|[matrix, transfer, primaries, range]| {
                            crate::disc::MeasuredCicp {
                                matrix,
                                transfer,
                                primaries,
                                range,
                            }
                        }),
                    })
                }
                MetaStream::Audio {
                    pid,
                    codec,
                    channels,
                    language,
                    sample_rate,
                    label,
                    secondary,
                    codec_private: _,
                    purpose,
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
                    purpose: purpose_from_id(purpose),
                    label: label.clone(),
                }),
                MetaStream::Subtitle {
                    pid,
                    codec,
                    language,
                    forced,
                    codec_private,
                    qualifier,
                } => Stream::Subtitle(SubtitleStream {
                    pid: *pid,
                    codec: codec.parse().unwrap_or(crate::disc::Codec::Unknown(0)),
                    language: language.clone(),
                    forced: *forced,
                    qualifier: qualifier_from_id(qualifier),
                    codec_data: decode_codec_private(codec_private),
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
            chapters: self
                .chapters
                .iter()
                .map(|c| crate::disc::Chapter {
                    time_secs: c.time_secs,
                    name: c.name.clone(),
                })
                .collect(),
            extents: Vec::new(),
            content_format: match self.content_format.as_str() {
                "mpeg_ps" => crate::disc::ContentFormat::MpegPs,
                _ => crate::disc::ContentFormat::BdTs,
            },
            codec_privates: self.codec_privates(),
        }
    }

    /// Extract codec_private data per stream (from FMKV header).
    /// Returns a Vec matching stream order — None for streams without codec_private.
    /// Covers all three stream kinds so audio/subtitle init data round-trips,
    /// not just video.
    pub fn codec_privates(&self) -> Vec<Option<Vec<u8>>> {
        self.streams
            .iter()
            .map(|s| {
                let b64 = match s {
                    MetaStream::Video { codec_private, .. }
                    | MetaStream::Audio { codec_private, .. }
                    | MetaStream::Subtitle { codec_private, .. } => codec_private,
                };
                decode_codec_private(b64)
            })
            .collect()
    }
}

// Wire ids for the label enums (purpose shares json://'s ids); the neutral value is
// omitted, and unknown ids read back as it.
fn purpose_id(p: crate::disc::LabelPurpose) -> &'static str {
    match p {
        crate::disc::LabelPurpose::Normal => "",
        p => super::meta_sink::purpose_id(p),
    }
}

fn purpose_from_id(id: &str) -> crate::disc::LabelPurpose {
    use crate::disc::LabelPurpose::*;
    [Commentary, Descriptive, Score, Ime]
        .into_iter()
        .find(|p| purpose_id(*p) == id)
        .unwrap_or(Normal)
}

fn qualifier_id(q: crate::disc::LabelQualifier) -> &'static str {
    use crate::disc::LabelQualifier::*;
    match q {
        None => "",
        Sdh => "sdh",
        DescriptiveService => "descriptive_service",
        Forced => "forced",
    }
}

fn qualifier_from_id(id: &str) -> crate::disc::LabelQualifier {
    use crate::disc::LabelQualifier::*;
    [Sdh, DescriptiveService, Forced]
        .into_iter()
        .find(|q| qualifier_id(*q) == id)
        .unwrap_or(None)
}

/// Decode an optional base64 codec_private string into raw bytes. Invalid
/// base64 decodes to `None` (treated as absent) rather than erroring — a
/// corrupt init blob shouldn't fail the whole metadata parse.
fn decode_codec_private(b64: &Option<String>) -> Option<Vec<u8>> {
    use base64::Engine;
    b64.as_ref()
        .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
}

/// Write the metadata header to a writer. Padded to 192-byte boundary.
pub fn write_header(w: &mut impl Write, meta: &M2tsMeta) -> io::Result<()> {
    // Serializing our own struct effectively cannot fail, but map the
    // error to a numeric crate variant rather than embedding serde's
    // English string into an io::Error (no-English rule).
    let json = serde_json::to_vec(meta).map_err(|_| crate::error::Error::NoMetadata)?;

    // Guard the length field against truncation: `as u32` would silently wrap a
    // >=4 GiB JSON into a wrong, smaller length. Near-impossible for real
    // metadata, but a v1.0 primitive shouldn't truncate.
    let json_len = u32::try_from(json.len()).map_err(|_| crate::error::Error::NoMetadata)?;
    let raw_len = 8 + 4 + json.len(); // magic + len + json
    let padded_len = raw_len.div_ceil(BD_SOURCE_PACKET_BYTES) * BD_SOURCE_PACKET_BYTES;
    let padding = padded_len - raw_len;

    let mut magic = MAGIC;
    if meta.frame_padding {
        magic[VERSION_BYTE] = 2;
    }
    w.write_all(&magic)?;
    w.write_all(&json_len.to_be_bytes())?;
    w.write_all(&json)?;
    if padding > 0 {
        // Padding is at most BD_SOURCE_PACKET_BYTES-1 bytes — stack buffer, no heap alloc.
        let pad = [0u8; BD_SOURCE_PACKET_BYTES];
        w.write_all(&pad[..padding])?;
    }
    Ok(())
}

/// Try to read an FMKV metadata header.
/// Returns None if magic bytes don't match. Consumes header bytes on success.
/// Caller handles seek-back on failure if needed (e.g. for fallback PMT scan).
pub fn read_header(r: &mut impl Read) -> io::Result<Option<M2tsMeta>> {
    const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10 MB

    // Read the first byte alone so a zero-byte stream (legitimate headerless
    // file) stays Ok(None), while a stream that truncates mid-magic surfaces as
    // an error rather than being masked as "no header".
    let mut first = [0u8; 1];
    if let Err(e) = r.read_exact(&mut first) {
        // A clean EOF means "no FMKV header" — caller falls back to a PMT scan.
        // Any OTHER I/O failure (broken pipe, permission denied, disc error) is
        // real and must propagate, not masquerade as a headerless stream.
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e);
    }
    if first[0] != MAGIC[0] {
        return Ok(None); // not an FMKV stream
    }
    let mut rest = [0u8; 7];
    r.read_exact(&mut rest)?; // started with 'F' but truncated → error
    let magic = [
        first[0], rest[0], rest[1], rest[2], rest[3], rest[4], rest[5], rest[6],
    ];

    if magic[..4] != MAGIC[..4] {
        return Ok(None);
    }
    if magic[VERSION_BYTE] > SUPPORTED_VERSION {
        // Newer, incompatible format — refuse rather than mis-parse as v1.
        return Err(crate::error::Error::NoMetadata.into());
    }

    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let json_len = u32::from_be_bytes(len_buf) as usize;
    if json_len > MAX_JSON_SIZE {
        return Err(crate::error::Error::NoMetadata.into());
    }

    let mut json_buf = vec![0u8; json_len];
    r.read_exact(&mut json_buf)?;

    let mut meta: M2tsMeta =
        serde_json::from_slice(&json_buf).map_err(|_| crate::error::Error::NoMetadata)?;
    meta.frame_padding = magic[VERSION_BYTE] >= 2;
    // The wire track is a u8: a frame can address at most 256 streams.
    if meta.streams.len() > 256 {
        return Err(crate::error::Error::NoMetadata.into());
    }
    // The wire track is a u8: cap untrusted timings so timing() stays O(256).
    meta.timings.retain(|t| t.track < 256);
    meta.timings.truncate(256);

    // Skip padding to next 192-byte boundary (at most BD_SOURCE_PACKET_BYTES-1 bytes →
    // a stack buffer, no heap allocation).
    let raw_len = 8 + 4 + json_len;
    let padded_len = raw_len.div_ceil(BD_SOURCE_PACKET_BYTES) * BD_SOURCE_PACKET_BYTES;
    let padding = padded_len - raw_len;
    if padding > 0 {
        let mut skip = [0u8; BD_SOURCE_PACKET_BYTES];
        r.read_exact(&mut skip[..padding])?;
    }

    Ok(Some(meta))
}

// Serialization uses Codec::id() / HdrFormat::id() and Display impls.
// Deserialization uses FromStr impls (.parse()) on each enum.

#[cfg(test)]
#[path = "meta_tests.rs"]
mod tests;
