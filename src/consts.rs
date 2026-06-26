//! Physical media constants — the single source of truth.
//!
//! Naming convention: a constant is prefixed by the **narrowest scope where it
//! is valid**. A value common to all optical media carries no prefix; a value
//! specific to a container/format/disc-type is prefixed by it
//! (`TS_`, `BD_`, …). Define each physical quantity here exactly once and import
//! it — never re-declare a bare literal or a local copy.

/// Bytes per logical sector on every optical medium freemkv reads
/// (Blu-ray, DVD-Video, CD-ROM Mode 1). Universal — hence unprefixed.
pub const SECTOR_BYTES: usize = 2048;

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
