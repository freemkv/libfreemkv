//! Physical media constants — the single source of truth.
//!
//! Naming convention: a constant is prefixed by the **narrowest scope where it
//! is valid**. A value common to all optical media carries no prefix; a value
//! specific to a container/format/disc-type is prefixed by it
//! (`TS_`, `BD_`, …). Define each physical quantity here exactly once and import
//! it — never re-declare a bare literal or a local copy.

/// Bytes per logical sector on every optical medium freemkv reads
/// (Blu-ray, DVD-Video, CD-ROM Mode 1). Universal — hence unprefixed.
///
/// `usize` because its dominant use is buffer sizing and slice indexing, where
/// Rust *requires* `usize` (`vec![0u8; SECTOR_BYTES]`, `buf.len() < SECTOR_BYTES`).
/// For byte-offset / capacity arithmetic — which is `u64` because a disc can
/// exceed 4 GiB — use [`SECTOR_BYTES_U64`] instead of casting at each site.
pub const SECTOR_BYTES: usize = 2048;

/// [`SECTOR_BYTES`] as `u64`, for byte-offset and capacity arithmetic. The
/// single `usize → u64` boundary cast lives here, once, so offset math across
/// the workspace reads as `sectors * SECTOR_BYTES_U64` with no per-site cast.
pub const SECTOR_BYTES_U64: u64 = SECTOR_BYTES as u64;

/// Bytes per MPEG-2 transport-stream packet. Common to all MPEG-TS, not just
/// Blu-ray — prefixed by the format, not a disc type.
pub const TS_PACKET_BYTES: usize = 188;

/// Bytes in an MPEG-2 transport-stream packet header: sync byte, the
/// flags/PID word, and the adaptation/continuity byte.
pub const TS_HEADER_BYTES: usize = 4;

/// Bytes in the arrival-timestamp prefix a Blu-ray M2TS prepends to each TS
/// packet to form a source packet. Same width as a TS header but a distinct
/// quantity ([`TS_HEADER_BYTES`]) — do not conflate.
pub const BD_TIMESTAMP_PREFIX_BYTES: usize = 4;

/// Bytes of payload in an MPEG-2 transport-stream packet:
/// [`TS_PACKET_BYTES`] minus the [`TS_HEADER_BYTES`] header.
pub const TS_PAYLOAD_BYTES: usize = TS_PACKET_BYTES - TS_HEADER_BYTES;

/// Bytes per Blu-ray M2TS *source packet*: a TS packet ([`TS_PACKET_BYTES`])
/// prefixed with the [`BD_TIMESTAMP_PREFIX_BYTES`] arrival-timestamp header.
/// A BDAV/M2TS construct only — DVD VOBs have no source packets — hence `BD_`.
pub const BD_SOURCE_PACKET_BYTES: usize = TS_PACKET_BYTES + BD_TIMESTAMP_PREFIX_BYTES;

/// Elementary-stream coding-type codes — the single source of truth for the
/// byte that identifies a stream's codec.
///
/// This is one registry used in two places that share the same value space:
/// the MPEG-TS PMT `stream_type` (ISO/IEC 13818-1 Table 2-34) and the Blu-ray
/// STN/CLPI `stream_coding_type` (BD-ROM Part 3). The standardized video codes
/// (`0x02`, `0x1B`, `0x24`, `0xEA`) are ISO assignments; the `0x80..=0xA2`
/// audio/graphics codes sit in the ISO user-private range and follow the
/// Blu-ray Disc Association / ATSC A/52 convention. Because every consumer
/// reads or writes this single byte, the family is unprefixed — the scope is
/// "any elementary stream freemkv parses or muxes".
///
/// Each constant is `u8`: the spec defines an 8-bit field and the code compares
/// it directly against a byte read from the buffer, so no casts are needed.
pub mod coding_type {
    /// MPEG-2 video (ISO/IEC 13818-1 Table 2-34).
    pub const MPEG2_VIDEO: u8 = 0x02;
    /// H.264 / AVC video (ISO/IEC 13818-1 Table 2-34).
    pub const H264: u8 = 0x1B;
    /// HEVC / H.265 video (ISO/IEC 13818-1 Table 2-34, 2015 amendment).
    pub const HEVC: u8 = 0x24;
    /// SMPTE VC-1 video (BD-ROM convention, ISO user-private range).
    pub const VC1: u8 = 0xEA;

    /// LPCM audio (BD-ROM convention).
    pub const LPCM: u8 = 0x80;
    /// Dolby Digital (AC-3) audio (BD-ROM / ATSC A/52 convention).
    pub const AC3: u8 = 0x81;
    /// DTS audio (BD-ROM convention).
    pub const DTS: u8 = 0x82;
    /// Dolby TrueHD audio (BD-ROM convention).
    pub const TRUEHD: u8 = 0x83;
    /// Dolby Digital Plus (E-AC-3 / AC-3+) audio (BD-ROM convention).
    pub const AC3_PLUS: u8 = 0x84;
    /// DTS-HD High Resolution audio (BD-ROM Part 3-1).
    pub const DTS_HD_HR: u8 = 0x85;
    /// DTS-HD Master Audio (lossless) (BD-ROM Part 3-1).
    pub const DTS_HD_MA: u8 = 0x86;

    /// Presentation Graphics — PG subtitle stream (BD-ROM HDMV).
    pub const PG: u8 = 0x90;
    /// Interactive Graphics — IG / BD-J menu overlay, NOT a subtitle (BD-ROM HDMV).
    pub const IG: u8 = 0x91;
    /// Text subtitle stream (BD-ROM HDMV).
    pub const TEXT_SUBTITLE: u8 = 0x92;

    /// Secondary Dolby Digital Plus audio (BD-ROM convention).
    pub const AC3_PLUS_SECONDARY: u8 = 0xA1;
    /// Secondary DTS-HD audio (lossless MA, not lossy HR) (BD-ROM convention).
    pub const DTS_HD_SECONDARY: u8 = 0xA2;
}

/// MPEG PES `stream_id` codes — the byte after the `00 00 01` start-code prefix
/// that identifies an elementary stream's role in a PES packet (ISO/IEC
/// 13818-1 Table 2-22). Shared by the program-stream demuxer and the TS/M2TS
/// muxers, so defined here once. Each is `u8` (matches the byte on the wire).
pub mod pes_stream_id {
    /// Video stream (`110x xxxx`; freemkv emits the base id `0xE0`).
    pub const VIDEO: u8 = 0xE0;
    /// private_stream_1 — AC-3 / DTS / LPCM / PGS subtitle payloads.
    pub const PRIVATE_STREAM_1: u8 = 0xBD;
    /// padding_stream — stuffing bytes only, no payload to demux.
    pub const PADDING_STREAM: u8 = 0xBE;
    /// private_stream_2 — DVD navigation (PCI/DSI); carries no muxable ES.
    pub const PRIVATE_STREAM_2: u8 = 0xBF;

    /// Highest video stream_id — the `110x xxxx` video range tops out at 0xEF.
    pub const VIDEO_MAX: u8 = 0xEF;

    /// Inclusive range of every PES `stream_id` that carries demuxable payload:
    /// [`PRIVATE_STREAM_1`] (0xBD) through [`VIDEO_MAX`] (0xEF) — i.e. private
    /// stream 1/2, padding, MPEG audio (0xC0-0xDF) and video (0xE0-0xEF). The
    /// pack (0xBA), system-header (0xBB) and program-end (0xB9) codes sit below
    /// this range and are deliberately excluded: they're structural, not ES.
    pub const PAYLOAD_RANGE: core::ops::RangeInclusive<u8> = PRIVATE_STREAM_1..=VIDEO_MAX;
}
