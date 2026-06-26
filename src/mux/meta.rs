//! M2TS metadata header — embeds title/stream info in raw m2ts files.
//!
//! Format: [8B magic] [4B json_len] [JSON] [padding to 192B boundary] [BD-TS data...]
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
/// formats instead of silently mis-parsing them as v1.
const SUPPORTED_VERSION: u8 = 1;

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
                },
                Stream::Subtitle(s) => MetaStream::Subtitle {
                    pid: s.pid,
                    codec: s.codec.id().into(),
                    language: s.language.clone(),
                    forced: s.forced,
                    codec_private: codec_private_b64(i),
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
                    color_space,
                    label,
                    secondary,
                    codec_private: _,
                } => {
                    let hdr_fmt = hdr.parse().unwrap_or(crate::disc::HdrFormat::Sdr);
                    // Prefer the explicitly stored color space. Pre-0.30.7
                    // metadata has none, so derive it from the HDR format:
                    // every HDR variant (HDR10/HDR10+/HLG/Dolby Vision) is
                    // BT.2020; SDR is BT.709.
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
                        display_aspect: None,
                        secondary: *secondary,
                        label: label.clone(),
                        measured_cicp: None,
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
                    codec_private,
                } => Stream::Subtitle(SubtitleStream {
                    pid: *pid,
                    codec: codec.parse().unwrap_or(crate::disc::Codec::Unknown(0)),
                    language: language.clone(),
                    forced: *forced,
                    qualifier: crate::disc::LabelQualifier::None,
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
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: crate::disc::ContentFormat::BdTs,
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

    // Guard the length field against truncation: the read side rejects
    // anything over MAX_JSON_SIZE, and `as u32` would silently wrap a
    // >=4 GiB JSON into a wrong, smaller length. Near-impossible for
    // real stream metadata, but a v1.0 primitive shouldn't truncate.
    let json_len = u32::try_from(json.len()).map_err(|_| crate::error::Error::NoMetadata)?;
    let raw_len = 8 + 4 + json.len(); // magic + len + json
    let padded_len = raw_len.div_ceil(BD_SOURCE_PACKET_BYTES) * BD_SOURCE_PACKET_BYTES;
    let padding = padded_len - raw_len;

    w.write_all(&MAGIC)?;
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

    // Read the first byte alone so a zero-byte stream (a legitimate
    // headerless file) stays Ok(None), while a stream that begins with some
    // magic bytes then truncates mid-magic surfaces as an error rather than
    // being masked as "no header".
    let mut first = [0u8; 1];
    if let Err(e) = r.read_exact(&mut first) {
        // A clean EOF (no header at all) means "no FMKV header" — the caller
        // falls back to a PMT scan. Any OTHER I/O failure (broken pipe,
        // permission denied, mid-read disc error) is a real error and must
        // propagate, not masquerade as a headerless stream.
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

    let meta: M2tsMeta =
        serde_json::from_slice(&json_buf).map_err(|_| crate::error::Error::NoMetadata)?;

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
mod tests {
    use super::*;
    use crate::disc::{Codec, FrameRate, Resolution};

    fn video_title(hdr: HdrFormat, cs: ColorSpace) -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams.push(Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: FrameRate::F23_976,
            hdr,
            color_space: cs,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }));
        t
    }

    fn round_trip_color_space(title: &DiscTitle) -> ColorSpace {
        let meta = M2tsMeta::from_title(title);
        let back = meta.to_title();
        match &back.streams[0] {
            Stream::Video(v) => v.color_space,
            _ => panic!("expected video stream"),
        }
    }

    #[test]
    fn color_space_round_trips_bt2020() {
        // The regression: HDR10 / BT.2020 must survive from_title → to_title,
        // not collapse to the hardcoded BT.709.
        let cs = round_trip_color_space(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
        assert_eq!(cs, ColorSpace::Bt2020, "BT.2020 must round-trip");
    }

    #[test]
    fn color_space_round_trips_bt709() {
        let cs = round_trip_color_space(&video_title(HdrFormat::Sdr, ColorSpace::Bt709));
        assert_eq!(cs, ColorSpace::Bt709);
    }

    #[test]
    fn color_space_serialized_in_json() {
        let meta = M2tsMeta::from_title(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
        let json = serde_json::to_string(&meta).unwrap();
        assert!(
            json.contains("bt2020"),
            "color_space must be serialized: {json}"
        );
    }

    #[test]
    fn legacy_metadata_without_color_space_derives_from_hdr() {
        // Pre-0.30.7 JSON has no color_space field. to_title must derive the
        // color space from the HDR format so HDR color metadata is preserved.
        let json = r#"{
            "v": 1,
            "title": "x",
            "duration": 0.0,
            "streams": [
                {"type":"video","pid":4113,"codec":"hevc","resolution":"2160p",
                 "frame_rate":"23.976","hdr":"hdr10","label":"","secondary":false}
            ]
        }"#;
        let meta: M2tsMeta = serde_json::from_str(json).unwrap();
        let back = meta.to_title();
        match &back.streams[0] {
            Stream::Video(v) => {
                assert_eq!(v.hdr, HdrFormat::Hdr10);
                assert_eq!(
                    v.color_space,
                    ColorSpace::Bt2020,
                    "HDR10 must derive BT.2020 when color_space absent"
                );
            }
            _ => panic!("expected video stream"),
        }
    }

    #[test]
    fn read_header_empty_is_none_not_error() {
        // No bytes at all → clean EOF on the magic read → Ok(None), the
        // "no FMKV header, fall back" signal.
        let empty: &[u8] = &[];
        let mut cursor = io::Cursor::new(empty);
        let got = read_header(&mut cursor).expect("clean EOF must be Ok(None)");
        assert!(got.is_none());
    }

    #[test]
    fn read_header_propagates_non_eof_error() {
        // A reader that fails with a non-EOF error must surface that
        // error, not be swallowed as Ok(None).
        struct BrokenReader;
        impl Read for BrokenReader {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::BrokenPipe))
            }
        }
        let mut r = BrokenReader;
        let err = read_header(&mut r).expect_err("broken pipe must propagate");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn write_header_then_read_header_round_trips() {
        let title = video_title(HdrFormat::Hdr10, ColorSpace::Bt2020);
        let meta = M2tsMeta::from_title(&title);
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).expect("write");
        let mut cursor = io::Cursor::new(&buf);
        let back = read_header(&mut cursor)
            .expect("read")
            .expect("header present");
        assert_eq!(back.streams.len(), 1);
        // Header is padded to a 192-byte boundary; the cursor must land
        // exactly there so the following BD-TS data stays aligned.
        assert_eq!(cursor.position() as usize % BD_SOURCE_PACKET_BYTES, 0);
    }

    #[test]
    fn audio_and_subtitle_codec_private_round_trip() {
        use crate::disc::{AudioChannels, AudioStream, LabelPurpose, SampleRate, SubtitleStream};
        let mut t = DiscTitle::empty();
        t.streams.push(Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Dts,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }));
        t.streams.push(Stream::Subtitle(SubtitleStream {
            pid: 0x1200,
            codec: Codec::DvdSub,
            language: "eng".into(),
            forced: false,
            qualifier: crate::disc::LabelQualifier::None,
            codec_data: None,
        }));
        // codec_privates: index 0 = audio init data, index 1 = subtitle init data.
        t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC]), Some(vec![0x01, 0x02])];

        let meta = M2tsMeta::from_title(&t);
        // Must serialize for both audio and subtitle (not just video).
        let cps = meta.codec_privates();
        assert_eq!(cps[0].as_deref(), Some(&[0xAA, 0xBB, 0xCC][..]));
        assert_eq!(cps[1].as_deref(), Some(&[0x01, 0x02][..]));

        // And to_title restores the subtitle codec_data from the header.
        let back = meta.to_title();
        match &back.streams[1] {
            Stream::Subtitle(s) => {
                assert_eq!(s.codec_data.as_deref(), Some(&[0x01, 0x02][..]))
            }
            _ => panic!("expected subtitle stream"),
        }
        // The round-tripped title also carries all codec_privates.
        assert_eq!(
            back.codec_privates[0].as_deref(),
            Some(&[0xAA, 0xBB, 0xCC][..])
        );
        assert_eq!(back.codec_privates[1].as_deref(), Some(&[0x01, 0x02][..]));
    }

    #[test]
    fn newer_version_header_rejected() {
        // A header tagged with a version above SUPPORTED_VERSION must be
        // refused, not silently parsed as v1.
        let meta = M2tsMeta::from_title(&video_title(HdrFormat::Sdr, ColorSpace::Bt709));
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).unwrap();
        buf[VERSION_BYTE] = SUPPORTED_VERSION + 1; // bump version byte
        let mut cur = io::Cursor::new(buf);
        let err = read_header(&mut cur).unwrap_err();
        // NoMetadata (E9008) maps to InvalidInput.
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn empty_stream_is_clean_none_but_partial_magic_errors() {
        // Zero bytes → no header (Ok(None)).
        let mut empty = io::Cursor::new(Vec::<u8>::new());
        assert!(read_header(&mut empty).unwrap().is_none());

        // Begins with 'F' (MAGIC[0]) then truncates → error, not None.
        let mut partial = io::Cursor::new(vec![b'F', b'M', b'K']);
        assert!(read_header(&mut partial).is_err());

        // Does not begin with the FMKV magic at all → Ok(None) (headerless).
        let mut other = io::Cursor::new(vec![0x47u8; 16]);
        assert!(read_header(&mut other).unwrap().is_none());
    }

    #[test]
    fn header_round_trips_through_write_read() {
        let meta = M2tsMeta::from_title(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).unwrap();
        let mut cur = io::Cursor::new(buf);
        let back = read_header(&mut cur).unwrap().expect("header present");
        assert_eq!(back.streams.len(), 1);
    }

    #[test]
    fn legacy_sdr_without_color_space_derives_bt709() {
        let json = r#"{
            "v": 1,
            "title": "x",
            "duration": 0.0,
            "streams": [
                {"type":"video","pid":4113,"codec":"h264","resolution":"1080p",
                 "frame_rate":"24","hdr":"sdr","label":"","secondary":false}
            ]
        }"#;
        let meta: M2tsMeta = serde_json::from_str(json).unwrap();
        let back = meta.to_title();
        match &back.streams[0] {
            Stream::Video(v) => assert_eq!(v.color_space, ColorSpace::Bt709),
            _ => panic!("expected video stream"),
        }
    }

    // ============================================================
    // Header byte-layout invariants
    //
    // Format: [8B magic][4B json_len BE][JSON][padding to 192B].
    // The header MUST end on a 192-byte (BD-TS packet) boundary so the
    // following TS data stays packet-aligned and other tools can resync
    // by scanning for 0x47. A wrong padding calc silently misaligns the
    // entire m2ts payload.
    // ============================================================

    #[test]
    fn magic_bytes_exact_layout() {
        // The magic is "FMKV" + reserved 0x00 + version 0x01 + 2 reserved.
        // The version byte lives at index 5. A regression that shifted the
        // version byte would make every header read the wrong version.
        assert_eq!(&MAGIC[0..4], b"FMKV");
        assert_eq!(MAGIC[VERSION_BYTE], SUPPORTED_VERSION);
        assert_eq!(VERSION_BYTE, 5);
        assert_eq!(MAGIC.len(), 8);
    }

    #[test]
    fn write_header_pads_to_192_byte_boundary() {
        // The total written length must always be a multiple of BD_SOURCE_PACKET_BYTES
        // (192). Test a range of JSON sizes by varying stream count.
        for n_streams in 0..6 {
            let mut t = DiscTitle::empty();
            for _ in 0..n_streams {
                t.streams.push(Stream::Video(VideoStream {
                    pid: 0x1011,
                    codec: Codec::Hevc,
                    resolution: Resolution::R2160p,
                    frame_rate: FrameRate::F23_976,
                    hdr: HdrFormat::Hdr10,
                    color_space: ColorSpace::Bt2020,
                    display_aspect: None,
                    secondary: false,
                    label: "x".into(),
                    measured_cicp: None,
                }));
            }
            let meta = M2tsMeta::from_title(&t);
            let mut buf = Vec::new();
            write_header(&mut buf, &meta).unwrap();
            assert_eq!(
                buf.len() % BD_SOURCE_PACKET_BYTES,
                0,
                "header for {n_streams} streams (len {}) not 192-aligned",
                buf.len()
            );
            // The declared json_len (bytes 8..12, big-endian) must equal the
            // actual JSON byte length embedded.
            let json_len = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
            let json_bytes = &buf[12..12 + json_len];
            // Round-trips as valid JSON for M2tsMeta.
            let parsed: M2tsMeta = serde_json::from_slice(json_bytes).unwrap();
            assert_eq!(parsed.streams.len(), n_streams);
        }
    }

    #[test]
    fn json_length_field_is_big_endian() {
        // The 4-byte length is stored big-endian (most-significant byte first).
        // read_header decodes it the same way; a little-endian regression would
        // request a wildly wrong JSON length.
        let meta = M2tsMeta::from_title(&video_title(HdrFormat::Sdr, ColorSpace::Bt709));
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).unwrap();
        let json_len_be = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
        // Reconstruct the JSON object directly and confirm the length matches.
        let json = serde_json::to_vec(&meta).unwrap();
        assert_eq!(json_len_be, json.len());
    }

    #[test]
    fn oversized_json_len_field_rejected_not_allocated() {
        // A header whose json_len field claims > 10 MiB must be rejected
        // (NoMetadata → InvalidInput) BEFORE the reader allocates a 10 MiB+
        // buffer for untrusted input.
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        let huge = (10 * 1024 * 1024 + 1) as u32;
        buf.extend_from_slice(&huge.to_be_bytes());
        // No JSON body needed — the size check fires first.
        let mut cur = io::Cursor::new(buf);
        let err = read_header(&mut cur).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn truncated_json_body_errors_not_panics() {
        // magic + a json_len of 100 but no body → read_exact must surface a
        // UnexpectedEof error, never panic or return a half-filled meta.
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&100u32.to_be_bytes());
        // supply only 10 of the promised 100 JSON bytes.
        buf.extend_from_slice(&[b'{'; 10]);
        let mut cur = io::Cursor::new(buf);
        let err = read_header(&mut cur).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn malformed_json_body_is_no_metadata() {
        // Valid magic + valid length but the JSON itself is garbage → the
        // parse must fail with the numeric NoMetadata code, not panic and not
        // leak serde's English error into the io::Error.
        let bad = b"not json at all!"; // 16 bytes
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&(bad.len() as u32).to_be_bytes());
        buf.extend_from_slice(bad);
        let mut cur = io::Cursor::new(buf);
        let err = read_header(&mut cur).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput); // NoMetadata
    }

    #[test]
    fn second_magic_byte_mismatch_is_none_not_error() {
        // First byte matches MAGIC[0] ('F') so we commit to reading 7 more,
        // but the resulting 4-byte magic differs from "FMKV". Per the reader
        // contract this is "not an FMKV stream" → Ok(None), letting the caller
        // fall back to a PMT scan. (Only a truncated read after 'F' errors.)
        let mut buf = vec![b'F', b'X', b'X', b'X', 0, 0, 0, 0];
        // pad so the 8-byte magic read succeeds.
        buf.extend_from_slice(&[0u8; 8]);
        let mut cur = io::Cursor::new(buf);
        let got = read_header(&mut cur).unwrap();
        assert!(got.is_none(), "non-FMKV 4-byte magic must be Ok(None)");
    }

    #[test]
    fn read_header_consumes_exactly_one_packet_boundary() {
        // After a successful read_header, the reader must be positioned exactly
        // at a 192-byte boundary AND nothing of the following data consumed.
        // Append a sentinel TS sync byte (0x47) right after the header and
        // confirm it is the very next byte available.
        let meta = M2tsMeta::from_title(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).unwrap();
        let header_len = buf.len();
        buf.push(0x47); // TS sync byte follows the header
        let mut cur = io::Cursor::new(buf);
        read_header(&mut cur).unwrap().expect("header present");
        assert_eq!(cur.position() as usize, header_len);
        assert_eq!(header_len % BD_SOURCE_PACKET_BYTES, 0);
        let mut next = [0u8; 1];
        use std::io::Read as _;
        cur.read_exact(&mut next).unwrap();
        assert_eq!(next[0], 0x47, "byte after header must be the TS sync byte");
    }

    #[test]
    fn invalid_base64_codec_private_decodes_to_none() {
        // decode_codec_private treats invalid base64 as absent (None) rather
        // than failing the whole metadata parse — a corrupt init blob must not
        // sink an otherwise-good header.
        assert_eq!(decode_codec_private(&None), None);
        assert_eq!(
            decode_codec_private(&Some("!!!not base64!!!".to_string())),
            None
        );
        // Valid base64 round-trips to the raw bytes.
        use base64::Engine;
        let enc = base64::engine::general_purpose::STANDARD.encode([0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(
            decode_codec_private(&Some(enc)),
            Some(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn video_codec_private_round_trips_through_header() {
        // A video stream's HEVCDecoderConfigurationRecord must survive
        // from_title → write_header → read_header → codec_privates(). Without
        // this, an FMKV-driven remux loses the hvcC and the MKV video track is
        // undecodable.
        let mut t = video_title(HdrFormat::Hdr10, ColorSpace::Bt2020);
        t.codec_privates = vec![Some(vec![0x01, 0x02, 0x20, 0x00])]; // fake hvcC
        let meta = M2tsMeta::from_title(&t);
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).unwrap();
        let mut cur = io::Cursor::new(buf);
        let back = read_header(&mut cur).unwrap().expect("header present");
        assert_eq!(
            back.codec_privates()[0].as_deref(),
            Some(&[0x01, 0x02, 0x20, 0x00][..]),
            "video codec_private (hvcC) must round-trip through the header"
        );
    }

    #[test]
    fn duration_and_title_round_trip() {
        // Title string and duration must survive the JSON round-trip — these
        // populate the MKV Info element on remux.
        let mut t = video_title(HdrFormat::Sdr, ColorSpace::Bt709);
        t.playlist = "The Movie".into();
        t.duration_secs = 7384.5;
        let meta = M2tsMeta::from_title(&t);
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).unwrap();
        let mut cur = io::Cursor::new(buf);
        let back = read_header(&mut cur).unwrap().unwrap().to_title();
        assert_eq!(back.playlist, "The Movie");
        assert_eq!(back.duration_secs, 7384.5);
    }
}
