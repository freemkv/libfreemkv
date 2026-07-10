//! AACS Media Key Block — [C] Chapter 3.
//!
//! The MKB record format (framing walker, the `MkbRecord` view, record-body
//! finders), the MKBType / AACS-generation classification, and MKB-file
//! utilities (content length, trimming, version). Consolidated here from the
//! former `keys.rs` / `variant.rs` so the one place that understands MKB bytes
//! is `mkb`. A follow-up collapses the remaining duplicate finders (see the
//! private refactor notes) — for now both dialects live here side by side.

// ── MKB record types ([C] Chapter 3) ──────────────────────────────────────
// The ONE canonical set. Every record-type comparison in the `aacs` module
// references these, so a type byte is never a bare literal scattered across
// files (the `0x0c` variant-data record in particular used to appear in several
// hand-rolled forms).

/// Type-and-Version — carries the 32-bit MKBType / AACS generation.
pub(crate) const REC_TYPE_AND_VERSION: u8 = 0x10;
/// Subset-Difference index — the per-slot `(u_mask_shift, uv)` table.
pub(crate) const REC_SUBSET_DIFFERENCE: u8 = 0x04;
/// Media Key Data — the classical (1.0 / 2.0) per-subset cvalue table.
pub(crate) const REC_MEDIA_KEY_DATA: u8 = 0x05;
/// Explicit Subset-Difference — the smaller cvalue table some MKBs use.
pub(crate) const REC_EXPLICIT_SUBSET_DIFF: u8 = 0x07;
/// Media Key Variant Data (AACS 2.1) — the per-subset-difference `C` table
/// (one 16-byte C per slot); the `Kmp` step reads C from HERE, not `0x2d`.
pub(crate) const REC_MEDIA_KEY_VARIANT_DATA: u8 = 0x0c;
/// Variant Data + Nonce (AACS 2.1) — the `VARIANTS[uv]` table (leading bytes)
/// with the 16-byte `Kvn` Nonce at the tail.
pub(crate) const REC_VARIANT_DATA_AND_NONCE: u8 = 0x2d;
/// Variant Key Data table (AACS 2.1) — 65,535×16, indexed by the resolved VKD index.
pub(crate) const REC_VKD_TABLE: u8 = 0x2f;
/// Verify-Media-Key — AACS 1.0.
pub(crate) const REC_VERIFY_MEDIA_KEY_V1: u8 = 0x81;
/// Verify-Media-Key — AACS 2.x.
pub(crate) const REC_VERIFY_MEDIA_KEY_V2: u8 = 0x86;

/// A single MKB record produced by [`walk_mkb`].
#[derive(Debug, Clone)]
pub struct MkbRecord {
    /// Byte offset of the record within the MKB.
    pub offset: usize,
    /// Record type byte.
    pub rec_type: u8,
    /// Record length in bytes (includes the 4-byte header).
    pub rec_len: usize,
    /// Record body (the bytes after the 4-byte header).
    pub body: Vec<u8>,
}

/// Walk an MKB into a flat list of records.
///
/// MKB record framing per AACS: 1 byte type, 3 bytes BE length
/// INCLUDING the 4-byte header, followed by payload. The walker stops
/// at the first `(type=0, len=0)` end marker or at end of buffer.
pub fn walk_mkb(mkb: &[u8]) -> Vec<MkbRecord> {
    mkb_records(mkb)
        .map(|(offset, rec_type, rec_len)| MkbRecord {
            offset,
            rec_type,
            rec_len,
            body: mkb[offset + 4..offset + rec_len].to_vec(),
        })
        .collect()
}

/// THE single MKB record-framing walker: yields `(offset, rec_type, rec_len)`
/// for each record — a 4-byte header (type byte + big-endian 24-bit length)
/// then the body — stopping at the `00 000000` end marker or a
/// malformed/out-of-bounds length. Lazy (no body clone), so a find-one-record
/// caller never materialises the multi-MB cvalue table. [`walk_mkb`] and every
/// MKB record walk in `aacs::resolve`/`aacs::derive` are built on this, so the framing rules — and
/// any future fix to them — live in exactly one place (they had drifted across
/// six hand-rolled copies).
pub(crate) fn mkb_records(mkb: &[u8]) -> impl Iterator<Item = (usize, u8, usize)> + '_ {
    let mut pos = 0usize;
    std::iter::from_fn(move || {
        if pos + 4 > mkb.len() {
            return None;
        }
        let rec_type = mkb[pos];
        let rec_len = ((mkb[pos + 1] as usize) << 16)
            | ((mkb[pos + 2] as usize) << 8)
            | (mkb[pos + 3] as usize);
        if rec_type == 0 && rec_len == 0 {
            return None;
        }
        if rec_len < 4 || pos + rec_len > mkb.len() {
            return None;
        }
        let here = pos;
        pos += rec_len;
        Some((here, rec_type, rec_len))
    })
}

pub(crate) fn mkb_find_body(records: &[MkbRecord], rec_type: u8) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == rec_type && !r.body.is_empty())
        .map(|r| r.body.as_slice())
}

/// AACS protection generation a disc carries.
///
/// The content cert byte distinguishes V10 (`0x00`) from V20 (`0x01`). V21
/// cannot be detected from the cert alone — a V21 disc carries a V20 cert
/// and is upgraded to `V21` only after the MKB walk turns up the real Variant
/// records `0x2d` / `0x2f` (Encrypted Media Key Variant Data and the Variant
/// Key Data table).
///
/// Key-storage stride in `Unit_Key_RO.inf` is 48 bytes for V10 and 64
/// bytes for V20 / V21.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AacsVersion {
    /// AACS 1.0 — original BD-ROM.
    V10,
    /// AACS 2.0 — UHD-BD, classical Media Key derivation.
    V20,
    /// AACS 2.1 — UHD-BD with Media Key Variant chain on top of V20.
    V21,
}

/// AACS major version as the small integer threaded through the scan / key
/// paths (`AacsState.version`, `DiscInputs.version`, `DiscInputsCtx::new`):
/// 1 = AACS 1.0 (BD), 2 = AACS 2.x (UHD). Centralised so the bare `1`/`2` — and
/// the V10-vs-else stride choice it drives — lives in exactly one place.
pub const AACS_MAJOR_BD: u8 = 1;

pub const AACS_MAJOR_UHD: u8 = 2;

impl AacsVersion {
    /// Stride (in bytes) between successive encrypted unit keys in
    /// `Unit_Key_RO.inf`.
    pub(crate) fn unit_key_stride(self) -> usize {
        match self {
            AacsVersion::V10 => 48,
            AacsVersion::V20 | AacsVersion::V21 => 64,
        }
    }

    /// This version as the major integer ([`AACS_MAJOR_BD`] / [`AACS_MAJOR_UHD`]).
    pub fn major(self) -> u8 {
        match self {
            AacsVersion::V10 => AACS_MAJOR_BD,
            AacsVersion::V20 | AacsVersion::V21 => AACS_MAJOR_UHD,
        }
    }

    /// The version a bare major integer selects for stride purposes: only the
    /// BD major is V10; every other value takes the V20/V21 64-byte stride.
    pub fn from_major(major: u8) -> Self {
        if major == AACS_MAJOR_BD {
            AacsVersion::V10
        } else {
            AacsVersion::V20
        }
    }
}

/// Find Verify Media Key Record (type 0x81 for AACS 1.0, 0x86 for AACS 2.0/2.1) in MKB.
/// 0x81: [C] §3.2.5.1.4. 0x86 (AACS 2.x): [libaacs] `mkb.c` — not in the public spec.
pub(crate) fn mkb_find_mk_dv(mkb: &[u8]) -> Option<[u8; 16]> {
    // Verify-Media-Key record (0x81 for AACS 1.0, 0x86 for AACS 2.x): mk_dv is
    // the 16 bytes at record offset 4 (body offset 0). Needs rec_len >= 20.
    let found = mkb_records(mkb).find(|&(_, rt, len)| {
        (rt == REC_VERIFY_MEDIA_KEY_V1 || rt == REC_VERIFY_MEDIA_KEY_V2) && len >= 20
    });
    match found {
        Some((o, rec_type, rec_len)) => {
            let mut dv = [0u8; 16];
            dv.copy_from_slice(&mkb[o + 4..o + 20]);
            tracing::debug!(
                target: "freemkv::disc",
                phase = "mkb_mk_dv_found",
                rec_type,
                pos = o,
                rec_len,
                "mk_dv extracted from MKB"
            );
            Some(dv)
        }
        None => {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "mkb_mk_dv_not_found",
                "no 0x81/0x86 record with rec_len>=20 found"
            );
            None
        }
    }
}

/// Find Subset-Difference records (type 0x04) in MKB. [C] §3.2.5.1.5.
pub(crate) fn mkb_find_subdiff_records(mkb: &[u8]) -> Option<Vec<u8>> {
    find_record_body(mkb, 0x04)
}

/// Find the Media Key Data Record (cvalues table) in an MKB. [C] §3.2.4 / §3.2.5.1.7.
///
/// The cvalue table is record type `0x05` (Media Key Data) on BOTH AACS
/// 1.0 and AACS 2.x MKBs — its 16-byte cvalue entries are 1:1 with the
/// 5-byte Subset-Difference index entries in record `0x04`. This matches
/// libaacs, whose `mkb_cvalues()` reads `0x05` and `mkb_subdiff_records()`
/// reads `0x04`.
///
/// On AACS 2.x in-drive UHD MKBs the `0x05` table is large (the full
/// subset-difference cvalue set: ~181k entries on a retail MKB, 1:1 with
/// the giant `0x04` index), while record `0x07` (Explicit
/// Subset-Difference Record) is a much smaller structure (~96 entries) and
/// is NOT the cvalue table. An earlier version of this function preferred
/// `0x07`, which under-tested the Subset-Difference walk on UHD discs and
/// prevented the DK→walk path from ever finding the matching uv. The
/// selection MUST therefore be `0x05`-first; `0x07` is only a fallback for
/// malformed/legacy MKBs that somehow lack a `0x05` record.
pub(crate) fn mkb_find_cvalues(mkb: &[u8]) -> Option<Vec<u8>> {
    if let Some(body) = find_record_body(mkb, 0x05) {
        return Some(body);
    }
    find_record_body(mkb, 0x07)
}

/// Walk an MKB and return the payload (header stripped) of the first
/// record matching `rec_type`. Returns `None` if no such record exists or
/// the record is empty.
pub(crate) fn find_record_body(mkb: &[u8], rec_type_wanted: u8) -> Option<Vec<u8>> {
    mkb_records(mkb)
        .find(|&(_, rt, len)| rt == rec_type_wanted && len > 4)
        .map(|(o, _, len)| mkb[o + 4..o + len].to_vec())
}

/// Real content length of an MKB: the byte offset where the record stream
/// ends. MKB files (especially `MKB_RW.inf`, but `MKB_RO.inf` too on some
/// discs) are allocated to a fixed size — often ~128 MiB — with the records at
/// the front and the rest zero padding. Walking records (type+len) and stopping
/// at the first padding byte (`type == 0` / zero-length / overrun) gives the
/// actual size so callers can trim off megabytes of zeros before sending or
/// archiving. Returns `mkb.len()` only if the whole buffer parsed as records.
pub fn mkb_content_len(mkb: &[u8]) -> usize {
    // End of the last framed record = where the fixed-region zero padding begins.
    // (The `00 000000` terminator / overrun stops the walk; real MKBs pad with
    // zeros, so this matches the prior "stop at the first padding byte".)
    mkb_records(mkb)
        .last()
        .map(|(o, _, len)| o + len)
        .unwrap_or(0)
}

/// Trim an MKB's trailing fixed-region padding to its real content length —
/// but ONLY when [`mkb_content_len`] actually found one. It returns 0 for an
/// MKB whose first record cannot be parsed; truncating to 0 in that case would
/// hand downstream consumers (and the online key service) an EMPTY MKB that can
/// never resolve. So a 0 (or a length that isn't strictly inside the buffer)
/// leaves the MKB untouched. A 0.31.0 regression dropped this guard and
/// `truncate`-d unconditionally, zeroing unrecognised MKBs.
pub fn trim_mkb(mut mkb: Vec<u8>) -> Vec<u8> {
    let n = mkb_content_len(&mkb);
    if n > 0 && n < mkb.len() {
        mkb.truncate(n);
    }
    mkb
}

/// Get MKB version from Type and Version Record (type 0x10).
/// Layout: 4-byte record header at `pos` (type + BE24 length), then the
/// record body starts at `pos + 4`. The body holds the BE u32 Type field at
/// body offset 0 (`pos + 4`), then the BE u32 version at body offset 4
/// (`pos + 8`).
pub fn mkb_version(mkb: &[u8]) -> Option<u32> {
    // Type-and-Version record (0x10): version is the BE u32 at body offset 4
    // (record offset 8). Needs rec_len >= 12 (4 header + 4 type + 4 version).
    mkb_records(mkb)
        .find(|&(_, rt, len)| rt == REC_TYPE_AND_VERSION && len >= 12)
        .map(|(o, _, _)| u32::from_be_bytes([mkb[o + 8], mkb[o + 9], mkb[o + 10], mkb[o + 11]]))
}

/// `0x00031003` — recordable media MKB (Class I & II compute Km directly).
pub const MKB_TYPE_3_RECORDABLE: u32 = 0x0003_1003;

/// `0x00041003` — AACS 1.0 pre-recorded content MKB (KCD-based). Standard BD.
pub const MKB_TYPE_4_PRERECORDED: u32 = 0x0004_1003;

/// `0x000A1003` — Class II / Unified MKB (Sequence-Key-Block functionality).
pub const MKB_TYPE_10_CLASS_II: u32 = 0x000A_1003;

/// `0x48141003` — AACS 2.0 Category C (UHD content). libaacs `MKB_20_CATEGORY_C`.
pub const MKB_20_CATEGORY_C: u32 = 0x4814_1003;

/// `0x48151003` — AACS 2.1 Category C (UHD content). libaacs `MKB_21_CATEGORY_C`.
pub const MKB_21_CATEGORY_C: u32 = 0x4815_1003;

/// The AACS MKB Type field, decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MkbType {
    /// Type 3 — recordable media.
    Recordable,
    /// Type 4 — AACS 1.0 pre-recorded content (KCD). Standard Blu-ray.
    Prerecorded,
    /// Type 10 — Class II / Unified (SKB).
    ClassII,
    /// AACS 2.0 Category C — UHD content.
    CategoryC20,
    /// AACS 2.1 Category C — UHD content.
    CategoryC21,
    /// Unrecognized MKBType value (raw field preserved).
    Other(u32),
}

impl MkbType {
    pub(crate) fn from_raw(raw: u32) -> Self {
        match raw {
            MKB_TYPE_3_RECORDABLE => MkbType::Recordable,
            MKB_TYPE_4_PRERECORDED => MkbType::Prerecorded,
            MKB_TYPE_10_CLASS_II => MkbType::ClassII,
            MKB_20_CATEGORY_C => MkbType::CategoryC20,
            MKB_21_CATEGORY_C => MkbType::CategoryC21,
            other => MkbType::Other(other),
        }
    }

    /// AACS generation this MKB belongs to (Category C → 2.0/2.1, else 1.0).
    pub fn generation(self) -> AacsVersion {
        match self {
            MkbType::CategoryC21 => AacsVersion::V21,
            MkbType::CategoryC20 => AacsVersion::V20,
            _ => AacsVersion::V10,
        }
    }

    /// `true` for UHD (AACS 2.x Category C); `false` for Blu-ray (AACS 1.x).
    pub fn is_uhd(self) -> bool {
        matches!(self, MkbType::CategoryC20 | MkbType::CategoryC21)
    }
}

/// The raw 32-bit MKBType field from the Type-and-Version record (0x10), bytes
/// 4-7. `None` if no 0x10 record is present. [C] §3.2.5.1.1 Table 3-2.
pub fn mkb_type_raw(mkb: &[u8]) -> Option<u32> {
    // Type-and-Version record (0x10): the 32-bit MKBType is bytes 4-7 (body
    // offset 0). Needs rec_len >= 8 (4 header + 4 type).
    mkb_records(mkb)
        .find(|&(_, rt, len)| rt == REC_TYPE_AND_VERSION && len >= 8)
        .map(|(o, _, _)| u32::from_be_bytes([mkb[o + 4], mkb[o + 5], mkb[o + 6], mkb[o + 7]]))
}

/// Decode an MKB's Type field. `None` if no Type-and-Version record is present.
pub fn mkb_type(mkb: &[u8]) -> Option<MkbType> {
    mkb_type_raw(mkb).map(MkbType::from_raw)
}

/// `Some(true)` if this MKB is a UHD (AACS 2.x Category C) block, `Some(false)`
/// for Blu-ray (AACS 1.x), `None` if the Type record is absent.
pub fn mkb_is_uhd(mkb: &[u8]) -> Option<bool> {
    mkb_type(mkb).map(MkbType::is_uhd)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One MKB record: 1 type byte + big-endian 24-bit total length + body.
    fn rec(rec_type: u8, body: &[u8]) -> Vec<u8> {
        let len = 4 + body.len();
        let mut v = vec![rec_type, (len >> 16) as u8, (len >> 8) as u8, len as u8];
        v.extend_from_slice(body);
        v
    }

    /// Type-and-Version record (0x10): body = 4-byte MKBType + 4-byte version.
    fn type_and_version(mkb_type: u32, version: u32) -> Vec<u8> {
        let mut body = mkb_type.to_be_bytes().to_vec();
        body.extend_from_slice(&version.to_be_bytes());
        rec(REC_TYPE_AND_VERSION, &body)
    }

    #[test]
    fn walker_frames_records_and_stops_at_end_marker() {
        let mut mkb = type_and_version(MKB_20_CATEGORY_C, 77);
        mkb.extend(rec(REC_VKD_TABLE, &[0xAA; 16]));
        mkb.extend([0x00, 0x00, 0x00, 0x00]); // end marker
        mkb.extend(rec(0x99, &[0xFF; 8])); // must NOT be walked (past the marker)

        let recs = walk_mkb(&mkb);
        assert_eq!(recs.len(), 2, "walk stops at the 00 000000 end marker");
        assert_eq!(recs[0].rec_type, REC_TYPE_AND_VERSION);
        assert_eq!(recs[1].rec_type, REC_VKD_TABLE);
        assert_eq!(recs[1].body, vec![0xAA; 16]);
    }

    #[test]
    fn walker_stops_on_malformed_or_out_of_bounds_length() {
        // A record whose declared length runs past the buffer end must terminate
        // the walk rather than panic or read OOB.
        let mkb = vec![REC_VKD_TABLE, 0x00, 0xFF, 0xFF, 0x01, 0x02]; // len=0xFFFF, only 6 bytes
        assert!(
            walk_mkb(&mkb).is_empty(),
            "over-long record yields no records"
        );
        // A sub-4 length (shorter than the header itself) is also rejected.
        let short = vec![REC_VKD_TABLE, 0x00, 0x00, 0x02];
        assert!(walk_mkb(&short).is_empty(), "sub-4 length is rejected");
        // A truncated header (< 4 bytes) yields nothing.
        assert!(walk_mkb(&[0x10, 0x00]).is_empty());
    }

    #[test]
    fn mkb_type_and_version_decode_from_the_type_record() {
        let mut mkb = type_and_version(MKB_21_CATEGORY_C, 100);
        mkb.extend([0x00, 0x00, 0x00, 0x00]);
        assert_eq!(mkb_type_raw(&mkb), Some(MKB_21_CATEGORY_C));
        assert_eq!(mkb_version(&mkb), Some(100));
        assert_eq!(mkb_is_uhd(&mkb), Some(true), "2.1 Category C is UHD");

        let bd = type_and_version(MKB_TYPE_4_PRERECORDED, 68);
        assert_eq!(
            mkb_is_uhd(&bd),
            Some(false),
            "AACS 1.0 prerecorded is not UHD"
        );
        // No Type record → None (not a panic, not a fabricated value).
        assert_eq!(mkb_version(&rec(REC_VKD_TABLE, &[0; 16])), None);
        assert_eq!(mkb_type_raw(&[]), None);
    }

    #[test]
    fn trim_mkb_keeps_only_the_framed_records() {
        let mut mkb = type_and_version(MKB_20_CATEGORY_C, 1);
        let content_len = mkb.len(); // the single framed record, no end marker
        mkb.extend([0x00, 0x00, 0x00, 0x00]); // end marker
        mkb.extend([0xDE; 4096]); // trailing padding past the end marker
        let trimmed = trim_mkb(mkb);
        assert_eq!(
            trimmed.len(),
            content_len,
            "trim keeps the framed records, dropping the end marker and padding"
        );
    }
}
