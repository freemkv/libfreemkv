//! AACS key resolution — VUK derivation, MKB processing, disc hash, unit key parsing.

use super::decrypt::aes_ecb_decrypt;
use super::keydb::DeviceKey;

// ── AACS version ────────────────────────────────────────────────────────────

/// AACS protection generation a disc carries.
///
/// The content cert byte distinguishes V10 (`0x00`) from V20 (`0x01`). V21
/// cannot be detected from the cert alone — a V21 disc carries a V20 cert
/// and is upgraded to `V21` only after the MKB walk turns up record types
/// `0x82` / `0x83` (Media Key Variant Data and Variant Number).
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

impl AacsVersion {
    /// Stride (in bytes) between successive encrypted unit keys in
    /// `Unit_Key_RO.inf`.
    fn unit_key_stride(self) -> usize {
        match self {
            AacsVersion::V10 => 48,
            AacsVersion::V20 | AacsVersion::V21 => 64,
        }
    }
}

// ── VUK derivation ──────────────────────────────────────────────────────────

/// Derive VUK from Media Key and Volume ID.
/// VUK = AES-128-ECB-DECRYPT(media_key, volume_id) XOR volume_id
pub fn derive_vuk(media_key: &[u8; 16], volume_id: &[u8; 16]) -> [u8; 16] {
    let mut vuk = aes_ecb_decrypt(media_key, volume_id);
    for i in 0..16 {
        vuk[i] ^= volume_id[i];
    }
    vuk
}

/// Decrypt an encrypted unit key using the VUK (AES-128-ECB).
pub fn decrypt_unit_key(vuk: &[u8; 16], encrypted_uk: &[u8; 16]) -> [u8; 16] {
    aes_ecb_decrypt(vuk, encrypted_uk)
}

// ── Unit_Key_RO.inf parsing ─────────────────────────────────────────────────

/// Parsed Unit_Key_RO.inf file.
#[derive(Debug)]
pub struct UnitKeyFile {
    /// Disc hash (SHA1 of the entire file) — used as KEYDB lookup key
    pub disc_hash: [u8; 20],
    /// Application type (1 = BD-ROM)
    pub app_type: u8,
    /// Number of BDMV directories
    pub num_bdmv_dir: u8,
    /// Whether SKB MKB is used
    pub use_skb_mkb: bool,
    /// AACS generation this file's stride matches
    pub version: AacsVersion,
    /// Encrypted unit keys (CPS unit number, encrypted key)
    pub encrypted_keys: Vec<(u32, [u8; 16])>,
    /// Title → CPS unit index mapping (title_idx → unit_key_idx)
    pub title_cps_unit: Vec<u16>,
}

/// Compute disc hash (SHA1 of Unit_Key_RO.inf content).
pub fn disc_hash(data: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let hash = Sha1::digest(data);
    let mut out = [0u8; 20];
    out.copy_from_slice(&hash);
    out
}

/// Format disc hash as hex string with 0x prefix (for KEYDB lookup).
pub fn disc_hash_hex(hash: &[u8; 20]) -> String {
    let mut s = String::with_capacity(42);
    s.push_str("0x");
    for b in hash {
        s.push_str(&format!("{b:02X}"));
    }
    s
}

/// Parse Unit_Key_RO.inf from raw bytes.
///
/// Format (from AACS spec):
///   [0..4]   BE32: offset to key storage area (uk_pos)
///   [16]     app_type (1 = BD-ROM)
///   [17]     num_bdmv_dir
///   [18]     bit 7: use_skb_mkb
///   [20..22] BE16: first_play CPS unit
///   [22..24] BE16: top_menu CPS unit
///   [24..26] BE16: num_titles
///   [26..]   title entries: 2 bytes padding + 2 bytes CPS unit, × num_titles
///
///   Key storage at uk_pos:
///   [uk_pos..uk_pos+2]   BE16: num_unit_keys
///   [uk_pos+48..]        encrypted keys, 16 bytes each
///                         AACS 1.0: 48-byte stride
///                         AACS 2.0 / 2.1: 64-byte stride (48 + 16 extra)
pub fn parse_unit_key_ro(data: &[u8], version: AacsVersion) -> Option<UnitKeyFile> {
    if data.len() < 20 {
        return None;
    }

    let hash = disc_hash(data);

    // Header
    let app_type = data[16];
    let num_bdmv_dir = data[17];
    let use_skb_mkb = (data[18] >> 7) & 1 == 1;

    // Key storage offset
    let uk_pos = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if uk_pos + 2 > data.len() {
        return None;
    }

    // Number of unit keys
    let num_uk = u16::from_be_bytes([data[uk_pos], data[uk_pos + 1]]) as usize;
    if num_uk == 0 {
        return Some(UnitKeyFile {
            disc_hash: hash,
            app_type,
            num_bdmv_dir,
            use_skb_mkb,
            version,
            encrypted_keys: Vec::new(),
            title_cps_unit: Vec::new(),
        });
    }

    // Stride between keys
    let stride = version.unit_key_stride();

    // Validate size
    let keys_start = uk_pos + 48; // first key at uk_pos + 48
    if keys_start + 16 > data.len() {
        return None;
    }

    // Extract encrypted keys
    let mut encrypted_keys = Vec::with_capacity(num_uk);
    let mut pos = keys_start;
    for i in 0..num_uk {
        if pos + 16 > data.len() {
            break;
        }
        let mut key = [0u8; 16];
        key.copy_from_slice(&data[pos..pos + 16]);
        encrypted_keys.push(((i + 1) as u32, key));
        pos += stride;
    }

    // Title → CPS unit mapping
    let mut title_cps_unit = Vec::new();
    if data.len() >= 26 {
        let first_play = u16::from_be_bytes([data[20], data[21]]);
        let top_menu = u16::from_be_bytes([data[22], data[23]]);
        let num_titles = u16::from_be_bytes([data[24], data[25]]) as usize;

        title_cps_unit.push(first_play);
        title_cps_unit.push(top_menu);

        for i in 0..num_titles {
            let off = 26 + i * 4 + 2; // 2 bytes padding + 2 bytes CPS unit
            if off + 2 <= data.len() {
                let cps = u16::from_be_bytes([data[off], data[off + 1]]);
                title_cps_unit.push(cps);
            }
        }
    }

    Some(UnitKeyFile {
        disc_hash: hash,
        app_type,
        num_bdmv_dir,
        use_skb_mkb,
        version,
        encrypted_keys,
        title_cps_unit,
    })
}

// ── MKB processing ──────────────────────────────────────────────────────────

/// Derive Media Key from MKB data using processing keys.
///
/// Processing keys are pre-computed keys that work for specific MKB versions.
/// This is the fast path — no subset-difference tree traversal needed.
///
/// MKB format:
///   Record type 0x10 = Type and Version Record (has MKB version)
///   Record type 0x81 = Verify Media Key Record, AACS 1.0 (has mk_dv)
///   Record type 0x86 = Verify Media Key Record, AACS 2.0/2.1 (has mk_dv)
///   Record type 0x04 = Subset-Difference Index (has UVS entries)
///   Record type 0x05 = Media Key Data Record (cvalues, 1:1 with 0x04)
///   Record type 0x07 = Explicit Subset-Difference Record (NOT cvalues)
pub fn derive_media_key_from_pk(mkb: &[u8], processing_keys: &[[u8; 16]]) -> Option<[u8; 16]> {
    derive_media_key_from_pk_walked(mkb, processing_keys, PK_WALK_MAX_DEPTH)
}

/// SD-tree walk depth applied to every entry in `processing_keys`.
///
/// Each entry is treated as a node-key (label) at unknown depth. The
/// resolver applies `AES-G3(K, 1)` to derive the PK at this node, then
/// descends via `AES-G3(K, 0)` (left child) and `AES-G3(K, 2)` (right
/// child) up to this many additional levels — try-everything since we
/// have no path bits per entry.
///
/// Each level doubles the candidate count. Cost per entry per MKB
/// cvalue ≈ `2 × (2^(D+1) - 1)` AES decrypts. For a ~100-cvalue MKB
/// (typical UHD) at depth 2: ~14 × 100 = 1400 ops per entry; for 1.5k
/// entries that's ~2 M validate calls, sub-second with AES-NI.
///
/// Set to 0 to disable walking (entries tried only as terminal PKs).
const PK_WALK_MAX_DEPTH: u8 = 3;

/// Hard ceiling on the requested walk depth. The BFS frontier holds `2^depth`
/// 16-byte node keys, so an uncapped `max_depth` (e.g. 26+) would exhaust
/// memory; the walk silently clamps to this. 5 (32-wide frontier) covers every
/// realistic leaked-label case with margin.
const PK_WALK_MAX_DEPTH_CAP: u8 = 5;

/// Same as [`derive_media_key_from_pk`] but with explicit walk depth.
/// Each entry is tried as a terminal PK at depth 0, then as a node-key
/// whose PK and children are derived via `AES-G3(K, 0|1|2)` for up to
/// `max_depth` additional levels.
///
/// The BFS frontier grows as `2^max_depth`; `max_depth` is clamped to
/// [`PK_WALK_MAX_DEPTH_CAP`] so a large value cannot exhaust memory.
pub fn derive_media_key_from_pk_walked(
    mkb: &[u8],
    processing_keys: &[[u8; 16]],
    max_depth: u8,
) -> Option<[u8; 16]> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;
    walk_pk_against_tables_impl(processing_keys, &uvs, &cvalues, &mk_dv, max_depth)
}

/// Core Subset-Difference PK walk over explicit record bodies. Shared by
/// [`derive_media_key_from_pk_walked`] (production, records auto-selected) and
/// [`probe::walk_pk_against_tables`] (harness, records caller-pinned).
fn walk_pk_against_tables_impl(
    processing_keys: &[[u8; 16]],
    uvs: &[u8],
    cvalues: &[u8],
    mk_dv: &[u8; 16],
    max_depth: u8,
) -> Option<[u8; 16]> {
    // Clamp the frontier depth (2^depth node keys) so a caller-supplied value
    // cannot OOM the process.
    let max_depth = max_depth.min(PK_WALK_MAX_DEPTH_CAP);
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    let try_against_mkb = |pk: &[u8; 16]| -> Option<[u8; 16]> {
        for i in 0..num_uvs {
            if (i + 1) * 16 > cvalues.len() {
                continue;
            }
            let record_start = i * 5;
            if record_start + 5 > uvs.len() {
                continue;
            }
            let uv = &uvs[record_start + 1..record_start + 5];
            let cv = &cvalues[i * 16..(i + 1) * 16];
            if let Some(mk) = validate_processing_key(pk, cv, uv, mk_dv) {
                return Some(mk);
            }
        }
        None
    };

    // Two interpretations per entry:
    //   (a) entry IS already a terminal PK → validate directly
    //   (b) entry is a node key (label) → derive PK via aesg3(K, 1) and validate
    // Then descend to children's node keys via aesg3(K, 0) / aesg3(K, 2) and
    // repeat up to max_depth levels deep.
    for entry in processing_keys {
        // Depth-0 attempts on the raw entry
        if let Some(mk) = try_against_mkb(entry) {
            return Some(mk);
        }
        let pk_at_node = aesg3(entry, 1);
        if let Some(mk) = try_against_mkb(&pk_at_node) {
            return Some(mk);
        }
        if max_depth == 0 {
            continue;
        }
        // Walk: BFS through child node keys
        let mut frontier: Vec<[u8; 16]> = vec![aesg3(entry, 0), aesg3(entry, 2)];
        for depth in 1..=max_depth {
            let mut next = Vec::with_capacity(frontier.len() * 2);
            for nk in &frontier {
                // Try this node's PK (label → PK at this level)
                let pk_here = aesg3(nk, 1);
                if let Some(mk) = try_against_mkb(&pk_here) {
                    return Some(mk);
                }
                // Some leaked materials are themselves PKs at this depth, so
                // also try the node-key bytes directly.
                if let Some(mk) = try_against_mkb(nk) {
                    return Some(mk);
                }
                if depth < max_depth {
                    next.push(aesg3(nk, 0));
                    next.push(aesg3(nk, 2));
                }
            }
            frontier = next;
        }
    }
    None
}

/// Validate a processing key against a cvalue/UV pair.
/// Returns the Media Key if valid.
///
/// Steps:
///   1. `mk = AES-128D(pk, cvalue)`
///   2. `mk[12..16] ^= uv` (4 bytes XOR into the last 4 bytes only)
///   3. `dec_vd = AES-128D(mk, mk_dv)`
///   4. If `dec_vd[0..8] == 01 23 45 67 89 AB CD EF` → valid.
fn validate_processing_key(
    pk: &[u8; 16],
    cvalue: &[u8],
    uv: &[u8],
    mk_dv: &[u8; 16],
) -> Option<[u8; 16]> {
    if cvalue.len() < 16 || uv.len() < 4 {
        return None;
    }

    // Step 1: mk = AES-128D(pk, cvalue)
    let mut cv = [0u8; 16];
    cv.copy_from_slice(&cvalue[..16]);
    let mut mk = aes_ecb_decrypt(pk, &cv);

    // Step 2: XOR uv into the last 4 bytes of mk (mk[12..16]).
    for a in 0..4 {
        mk[12 + a] ^= uv[a];
    }

    // Step 3 + 4: dec_vd = AES-128D(mk, mk_dv); verify magic.
    let dec_vd = aes_ecb_decrypt(&mk, mk_dv);
    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
    if dec_vd[..8] == VERIFY_MAGIC {
        return Some(mk);
    }
    None
}

/// Public, side-effect-free accessors over the MKB record helpers, exposed so
/// independent reproduction harnesses (e.g. `examples/prove_hkd_aacs.rs`) can
/// exercise the exact same parser + verify primitives the production walk uses.
/// These are thin wrappers — no new logic.
pub mod probe {
    use super::aes_ecb_decrypt;

    /// `mk_dv` from the MKB's Verify-Media-Key record (type 0x81 / 0x86).
    pub fn mkb_mk_dv(mkb: &[u8]) -> Option<[u8; 16]> {
        super::mkb_find_mk_dv(mkb)
    }

    /// Body of the MKB's Subset-Difference Index record (type 0x04).
    pub fn mkb_subdiff(mkb: &[u8]) -> Option<Vec<u8>> {
        super::mkb_find_subdiff_records(mkb)
    }

    /// Body of the MKB's Media-Key-Data (cvalues) record. Selects record
    /// `0x05` (the large cvalue table, 1:1 with the `0x04` Subset-Difference
    /// index on AACS 2.x UHD MKBs), falling back to `0x07` only when `0x05`
    /// is absent.
    pub fn mkb_cvalues(mkb: &[u8]) -> Option<Vec<u8>> {
        super::mkb_find_cvalues(mkb)
    }

    /// Body (header stripped) of the first MKB record of `rec_type`. Lets a
    /// harness pin an exact record type for cross-checking the production
    /// cvalue selection (e.g. compare record `0x05` vs `0x07` sizes).
    pub fn mkb_record_body(mkb: &[u8], rec_type: u8) -> Option<Vec<u8>> {
        super::find_record_body(mkb, rec_type)
    }

    /// AES-128-ECB single-block decrypt (the AACS verify primitive).
    pub fn aes_dec(key: &[u8; 16], block: &[u8; 16]) -> [u8; 16] {
        aes_ecb_decrypt(key, block)
    }

    /// Does `km` satisfy the MKB's Verify-Media-Key relation?
    /// `AES-D(km, mk_dv)[0..8] == 01 23 45 67 89 AB CD EF`.
    pub fn km_verifies(mkb: &[u8], km: &[u8; 16]) -> bool {
        match super::mkb_find_mk_dv(mkb) {
            Some(mk_dv) => {
                aes_ecb_decrypt(km, &mk_dv)[..8] == [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
            }
            None => false,
        }
    }

    /// Run the exact production Subset-Difference PK walk
    /// ([`super::derive_media_key_from_pk_walked`]) but against
    /// CALLER-SUPPLIED record bodies — so a harness can pin a specific
    /// Media-Key-Data table (record `0x05` on AACS 2.x UHD MKBs, which the
    /// production `mkb_find_cvalues` now selects) and the matching `0x04`
    /// Subset-Difference Index, across ALL entries.
    ///
    /// `subdiff` is the type-0x04 body (5-byte entries
    /// `[u_mask_shift][uv:be32]`); `cvalues` is the chosen cvalue table
    /// (16-byte entries); `mk_dv` is from the verify record. Each entry in
    /// `keys` is tried as a terminal PK and as an SD node-key descending via
    /// `AES-G3(K, 0|1|2)` for `max_depth` levels — identical logic to the
    /// production walk (`max_depth` is clamped to the same internal cap to
    /// bound the `2^depth` frontier). Returns the verified Media Key, if any.
    pub fn walk_pk_against_tables(
        keys: &[[u8; 16]],
        subdiff: &[u8],
        cvalues: &[u8],
        mk_dv: &[u8; 16],
        max_depth: u8,
    ) -> Option<[u8; 16]> {
        super::walk_pk_against_tables_impl(keys, subdiff, cvalues, mk_dv, max_depth)
    }
}

/// Find Verify Media Key Record (type 0x81 for AACS 1.0, 0x86 for AACS 2.0/2.1) in MKB.
fn mkb_find_mk_dv(mkb: &[u8]) -> Option<[u8; 16]> {
    let mut pos = 0;
    let mut verify_rec_seen: Vec<(u8, usize, usize)> = Vec::new();
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }

        if rec_type == 0x81 || rec_type == 0x86 {
            verify_rec_seen.push((rec_type, pos, rec_len));
        }

        if (rec_type == 0x81 || rec_type == 0x86) && rec_len >= 20 {
            // mk_dv is at offset 4 of the record (after the 4-byte header)
            let mut dv = [0u8; 16];
            dv.copy_from_slice(&mkb[pos + 4..pos + 20]);
            tracing::debug!(
                target: "freemkv::disc",
                phase = "mkb_mk_dv_found",
                rec_type,
                pos,
                rec_len,
                "mk_dv extracted from MKB"
            );
            return Some(dv);
        }
        pos += rec_len;
    }
    tracing::warn!(
        target: "freemkv::disc",
        phase = "mkb_mk_dv_not_found",
        verify_rec_seen = ?verify_rec_seen,
        scanned_bytes = pos,
        "no 0x81/0x86 record with rec_len>=20 found"
    );
    None
}

/// Find Subset-Difference records (type 0x04) in MKB.
fn mkb_find_subdiff_records(mkb: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }

        if rec_type == 0x04 && rec_len > 4 {
            return Some(mkb[pos + 4..pos + rec_len].to_vec());
        }
        pos += rec_len;
    }
    None
}

/// Find the Media Key Data Record (cvalues table) in an MKB.
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
fn mkb_find_cvalues(mkb: &[u8]) -> Option<Vec<u8>> {
    if let Some(body) = find_record_body(mkb, 0x05) {
        return Some(body);
    }
    find_record_body(mkb, 0x07)
}

/// Walk an MKB and return the payload (header stripped) of the first
/// record matching `rec_type`. Returns `None` if no such record exists or
/// the record is empty.
fn find_record_body(mkb: &[u8], rec_type_wanted: u8) -> Option<Vec<u8>> {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }
        if rec_type == rec_type_wanted && rec_len > 4 {
            return Some(mkb[pos + 4..pos + rec_len].to_vec());
        }
        pos += rec_len;
    }
    None
}

/// Real content length of an MKB: the byte offset where the record stream
/// ends. MKB files (especially `MKB_RW.inf`, but `MKB_RO.inf` too on some
/// discs) are allocated to a fixed size — often ~128 MiB — with the records at
/// the front and the rest zero padding. Walking records (type+len) and stopping
/// at the first padding byte (`type == 0` / zero-length / overrun) gives the
/// actual size so callers can trim off megabytes of zeros before sending or
/// archiving. Returns `mkb.len()` only if the whole buffer parsed as records.
pub fn mkb_content_len(mkb: &[u8]) -> usize {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        // A zero type, a zero/short length, or an overrun = records done, padding begun.
        if rec_type == 0x00 || rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }
        pos += rec_len;
    }
    pos
}

/// Get MKB version from Type and Version Record (type 0x10).
/// Version is a BE u32 at offset 8 of the record body (offset 12 from `pos`).
pub fn mkb_version(mkb: &[u8]) -> Option<u32> {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }

        if rec_type == 0x10 && rec_len >= 12 {
            return Some(u32::from_be_bytes([
                mkb[pos + 8],
                mkb[pos + 9],
                mkb[pos + 10],
                mkb[pos + 11],
            ]));
        }
        pos += rec_len;
    }
    None
}

// ── AACS-G3 key derivation (subset-difference tree) ─────────────────────────

/// AACS-G3 seed constant.
const AESG3_SEED: [u8; 16] = [
    0x7B, 0x10, 0x3C, 0x5D, 0xCB, 0x08, 0xC4, 0xE5, 0x1A, 0x27, 0xB0, 0x17, 0x99, 0x05, 0x3B, 0xD9,
];

/// AACS-G3: derive a subkey from a parent key.
/// seed[15] += inc, then AES-DEC(key, seed) XOR seed.
fn aesg3(key: &[u8; 16], inc: u8) -> [u8; 16] {
    let mut seed = AESG3_SEED;
    seed[15] = seed[15].wrapping_add(inc);
    let mut out = aes_ecb_decrypt(key, &seed);
    for i in 0..16 {
        out[i] ^= seed[i];
    }
    out
}

/// Compute v_mask from a UV value.
fn calc_v_mask(uv: u32) -> u32 {
    let mut v_mask: u32 = 0xFFFF_FFFF;
    while (uv & !v_mask) == 0 && v_mask != 0 {
        v_mask <<= 1;
    }
    v_mask
}

/// Derive processing key from device key using subset-difference tree traversal.
fn calc_pk_from_dk(dk: &[u8; 16], uv: u32, v_mask: u32, dev_key_v_mask: u32) -> [u8; 16] {
    // Initial derivation: left_child = aesg3(dk, 0), pk = aesg3(dk, 1), right_child = aesg3(dk, 2)
    let mut left_child = aesg3(dk, 0);
    let mut pk = aesg3(dk, 1);
    let mut right_child = aesg3(dk, 2);
    let mut current_v_mask = dev_key_v_mask;

    // The subset-difference tree is at most 32 levels deep (u32 mask), so the
    // walk must converge in <= 32 steps. The arithmetic `>> 1` sign-extends
    // current_v_mask, so a v_mask coarser than dev_key_v_mask (reachable from
    // a crafted/corrupt MKB) would otherwise saturate at 0xFFFF_FFFF and spin
    // forever — bound the loop to keep a bad disc from hanging the rip thread.
    let mut steps = 0u32;
    while current_v_mask != v_mask {
        if steps >= 32 {
            break;
        }
        steps += 1;
        // Find the highest unset bit in current_v_mask
        let mut bit_pos: i32 = -1;
        for i in (0..32).rev() {
            if (current_v_mask & (1u32 << i)) == 0 {
                bit_pos = i;
                break;
            }
        }

        let curr_key = if bit_pos < 0 || (uv & (1u32 << bit_pos as u32)) == 0 {
            left_child
        } else {
            right_child
        };

        left_child = aesg3(&curr_key, 0);
        pk = aesg3(&curr_key, 1);
        right_child = aesg3(&curr_key, 2);

        current_v_mask = ((current_v_mask as i32) >> 1) as u32;
    }

    pk
}

/// Derive Media Key from MKB using device keys (subset-difference tree).
pub fn derive_media_key_from_dk(mkb: &[u8], device_keys: &[DeviceKey]) -> Option<[u8; 16]> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;

    // Count UV entries
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    for dk in device_keys {
        let device_number = dk.node as u32;

        // Find applying subset-difference for this device
        for uvs_idx in 0..num_uvs {
            let p_uv = &uvs[1 + 5 * uvs_idx..];
            let u_mask_shift = uvs[5 * uvs_idx]; // byte before the UV value

            if u_mask_shift & 0xC0 != 0 {
                break; // device revoked
            }
            // Shifts of 32..=63 (0x20..=0x3F pass the 0xC0 mask above) would
            // panic in debug / wrap to a wrong mask in release. The MKB byte
            // is disc-controlled, so a crafted/corrupt MKB must not crash the
            // ripper: skip an out-of-range slot rather than `<<` it.
            if u_mask_shift >= 32 {
                continue;
            }

            let uv = u32::from_be_bytes([p_uv[0], p_uv[1], p_uv[2], p_uv[3]]);
            if uv == 0 {
                continue;
            }

            let u_mask: u32 = 0xFFFF_FFFF << u_mask_shift;
            let v_mask = calc_v_mask(uv);

            if ((device_number & u_mask) == (uv & u_mask))
                && ((device_number & v_mask) != (uv & v_mask))
            {
                // Found matching subset-difference — find the right device key.
                // dk.u_mask_shift is a u8 from keydb with no range check;
                // guard the shift the same way as the MKB byte above.
                if dk.u_mask_shift >= 32 {
                    continue;
                }
                let dev_key_v_mask = calc_v_mask(dk.uv);
                let dev_key_u_mask: u32 = 0xFFFF_FFFF << dk.u_mask_shift;

                if u_mask == dev_key_u_mask && (uv & dev_key_v_mask) == (dk.uv & dev_key_v_mask) {
                    // Derive processing key via tree traversal
                    let pk = calc_pk_from_dk(&dk.key, uv, v_mask, dev_key_v_mask);

                    // Validate and derive media key
                    if uvs_idx < cvalues.len() / 16 {
                        let cv = &cvalues[uvs_idx * 16..(uvs_idx + 1) * 16];
                        if let Some(mk) =
                            validate_processing_key(&pk, cv, &uvs[1 + uvs_idx * 5..], &mk_dv)
                        {
                            return Some(mk);
                        }
                    }
                }
            }
        }
    }
    None
}

/// MKB disc structure format code.
const MKB_DISC_STRUCTURE_FORMAT: u8 = 0x83;
/// MKB pack buffer size.
const MKB_PACK_SIZE: usize = 32772;

/// Read MKB from drive via SCSI (REPORT DISC STRUCTURE format 0x83).
/// Returns the concatenated MKB data from all packs.
pub fn read_mkb_from_drive(session: &mut crate::drive::Drive) -> crate::error::Result<Vec<u8>> {
    use crate::scsi::{DataDirection, SCSI_READ_DISC_STRUCTURE};

    let cdb = [
        SCSI_READ_DISC_STRUCTURE,
        0x01,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        MKB_DISC_STRUCTURE_FORMAT,
        (MKB_PACK_SIZE >> 8) as u8,
        (MKB_PACK_SIZE & 0xFF) as u8,
        0x00,
        0x00,
    ];
    let mut buf = vec![0u8; 32772];
    session.scsi_execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000)?;

    let data_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if data_len < 2 {
        return Ok(Vec::new());
    }
    let len = data_len - 2;
    let num_packs = buf[3] as usize;

    let mut mkb = Vec::with_capacity(32768 * num_packs.max(1));
    if len > 0 && len <= 32768 {
        mkb.extend_from_slice(&buf[4..4 + len]);
    }

    // Read remaining packs
    for pack in 1..num_packs {
        let mut cdb = [
            SCSI_READ_DISC_STRUCTURE,
            0x01,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            MKB_DISC_STRUCTURE_FORMAT,
            (MKB_PACK_SIZE >> 8) as u8,
            (MKB_PACK_SIZE & 0xFF) as u8,
            0x00,
            0x00,
        ];
        // Pack number goes in address field
        cdb[2] = ((pack >> 24) & 0xFF) as u8;
        cdb[3] = ((pack >> 16) & 0xFF) as u8;
        cdb[4] = ((pack >> 8) & 0xFF) as u8;
        cdb[5] = (pack & 0xFF) as u8;

        let mut buf = vec![0u8; 32772];
        if session
            .scsi_execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000)
            .is_ok()
        {
            let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
            if len > 2 && len - 2 <= 32768 {
                mkb.extend_from_slice(&buf[4..4 + len - 2]);
            }
        }
    }

    Ok(mkb)
}

// ── Content Certificate parsing ─────────────────────────────────────────────

/// AACS Content Certificate — identifies disc AACS version and features.
#[derive(Debug)]
pub struct ContentCert {
    /// Bus encryption enabled flag
    pub bus_encryption: bool,
    /// Content Certificate ID (6 bytes)
    pub cc_id: [u8; 6],
    /// AACS generation indicated by the certificate type byte.
    ///
    /// Cert type `0x00` → [`AacsVersion::V10`]; any other value →
    /// [`AacsVersion::V20`]. The certificate alone cannot distinguish
    /// V20 from V21 — Variant detection happens after the MKB walk.
    pub version: AacsVersion,
}

/// Parse a Content Certificate (ContentXXX.cer) file.
pub fn parse_content_cert(data: &[u8]) -> Option<ContentCert> {
    if data.len() < 8 {
        return None;
    }

    // Content Certificate format:
    //   [0] certificate type (0x00 = AACS1, 0x01 = AACS2)
    //   [1] bus_encryption_enabled (bit 0)
    //   [2..8] cc_id (6 bytes)
    let version = if data[0] == 0x00 {
        AacsVersion::V10
    } else {
        AacsVersion::V20
    };
    let bus_encryption = (data[1] & 0x01) != 0;
    let mut cc_id = [0u8; 6];
    cc_id.copy_from_slice(&data[2..8]);

    Some(ContentCert {
        bus_encryption,
        cc_id,
        version,
    })
}

// ── Full VUK resolution chain ───────────────────────────────────────────────

/// Result of resolving a disc's VUK.
#[derive(Debug)]
pub struct ResolvedKeys {
    /// Disc hash (SHA1 of Unit_Key_RO.inf)
    pub disc_hash: [u8; 20],
    /// Volume Unique Key. `None` for path 5 — the KEYDB unit-keys
    /// fallback consumes pre-decrypted unit keys directly and has no
    /// VUK to surface.
    pub vuk: Option<[u8; 16]>,
    /// Decrypted unit keys (CPS unit number, key)
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Title → CPS unit index mapping
    pub title_cps_unit: Vec<u16>,
    /// AACS generation that drove the resolution
    pub version: AacsVersion,
    /// Whether bus encryption is enabled (from Content Certificate)
    pub bus_encryption: bool,
    /// Which resolution path succeeded (1=DK, 2=PK, 3=KEYDB derived,
    /// 4=KEYDB VUK, 5=KEYDB unit keys)
    pub key_source: u8,
}

/// Inputs shared by every classical-path resolver. References only —
/// callers retain ownership of all buffers.
pub struct ResolveContext<'a> {
    /// `Unit_Key_RO.inf` raw bytes.
    pub unit_key_ro: &'a [u8],
    /// Content Certificate raw bytes (optional — used for bus-encryption flag).
    pub content_cert: Option<&'a [u8]>,
    /// 16-byte Volume ID from SCSI handshake. `[0u8; 16]` is the
    /// "no VID" sentinel and disables paths 1-3.
    pub volume_id: &'a [u8; 16],
    /// Key sources — checked in array order for disc-keyed lookups,
    /// union'd across all entries for bulk material (DKs, PKs, HCs).
    /// A keydb file, a webservice, an OEM provider can all coexist.
    pub providers: &'a [&'a dyn super::provider::KeyProvider],
    /// MKB raw bytes (optional — paths 1/2 require it).
    pub mkb: Option<&'a [u8]>,
}

/// AACS 1.0 key resolution. Parses `Unit_Key_RO.inf` with 48-byte
/// stride. Tries paths 1 → 4 in order.
pub fn resolve_keys_v1(ctx: &ResolveContext<'_>) -> Option<ResolvedKeys> {
    resolve_keys_classical(ctx, AacsVersion::V10)
}

/// AACS 2.0 key resolution. Parses `Unit_Key_RO.inf` with 64-byte
/// stride. Tries paths 1 → 4 in order. When paths 3/4 succeed against
/// an MKB carrying Variant records (`0x82` / `0x83`), the result's
/// `version` is upgraded to [`AacsVersion::V21`] — derivation still
/// runs through the classical V2 path; the V21-specific Variant chain
/// is wired separately via [`resolve_keys_v21`].
pub fn resolve_keys_v2(ctx: &ResolveContext<'_>) -> Option<ResolvedKeys> {
    let mut resolved = resolve_keys_classical(ctx, AacsVersion::V20)?;
    if let Some(mkb) = ctx.mkb {
        let recs = super::variants::walk_mkb(mkb);
        if super::variants::is_variant_mkb(&recs) {
            resolved.version = AacsVersion::V21;
        }
    }
    Some(resolved)
}

/// AACS 2.1 key resolution via the Media Key Variant chain.
///
/// Paths run in root-of-trust → per-disc-leaf order:
///   1. Variant chain: MKB Variant records + device keys → Km → Kvu
///      (currently unreachable in production — requires an
///      integrator-supplied Key Correction Data constant; see
///      [`super::variants::KEY_CORRECTION_DATA_PLACEHOLDER`])
///   3. KEYDB MK + matching VID → derived VUK (V21 discs already in
///      the keydb decrypt identically to V20)
///   4. KEYDB disc-hash → VUK
///   5. KEYDB disc-hash → pre-decrypted unit keys (no VUK)
///
/// (Numbering preserves the cross-resolver convention; AACS 2.1 has no
/// equivalent of path 2 — there's no host-side PK derivation against a
/// Variant MKB.)
pub fn resolve_keys_v21(ctx: &ResolveContext<'_>) -> Option<ResolvedKeys> {
    let uk_file = parse_unit_key_ro(ctx.unit_key_ro, AacsVersion::V20)?;
    let hash_hex = disc_hash_hex(&uk_file.disc_hash);
    let bus_encryption = ctx
        .content_cert
        .and_then(parse_content_cert)
        .map(|cc| cc.bus_encryption)
        .unwrap_or(false);
    let has_vid = *ctx.volume_id != [0u8; 16];

    let derive_uks = |vuk: &[u8; 16]| -> Vec<(u32, [u8; 16])> {
        uk_file
            .encrypted_keys
            .iter()
            .map(|(num, enc_key)| (*num, decrypt_unit_key(vuk, enc_key)))
            .collect()
    };

    let build =
        |vuk: Option<[u8; 16]>, unit_keys: Vec<(u32, [u8; 16])>, key_source: u8| -> ResolvedKeys {
            ResolvedKeys {
                disc_hash: uk_file.disc_hash,
                vuk,
                unit_keys,
                title_cps_unit: uk_file.title_cps_unit.clone(),
                version: AacsVersion::V21,
                bus_encryption,
                key_source,
            }
        };

    tracing::info!(
        target: "freemkv::disc",
        phase = "resolve_keys_v21_start",
        bus_encryption,
        disc_hash = %hash_hex,
        has_vid,
        mkb_present = ctx.mkb.is_some(),
        "resolve_keys_v21: starting"
    );

    let providers = super::provider::Providers(ctx.providers);

    if has_vid {
        // Path 1: Variant chain (V21's analogue of classical Path 1's
        // DK derivation). Placeholder until KCD constant is supplied.
        if let Some(mkb) = ctx.mkb {
            let recs = super::variants::walk_mkb(mkb);
            let all_dks = providers.device_keys();
            match super::variants::derive_media_key_variant(
                &recs,
                &all_dks,
                &super::variants::KEY_CORRECTION_DATA_PLACEHOLDER,
                ctx.volume_id,
            ) {
                Ok((_km, kvu)) => {
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "resolve_keys_v21_path1_hit",
                        "Variant chain produced Km + Kvu"
                    );
                    return Some(build(Some(kvu), derive_uks(&kvu), 1));
                }
                Err(e) => {
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "resolve_keys_v21_path1_miss",
                        error_code = %e,
                        "Variant chain failed"
                    );
                }
            }
        }

        // Path 3: pre-computed MK + matching VID → derived VUK.
        // Short-circuit: first provider with a matching VID wins.
        if let Some(entry) = providers.lookup_disc_by_vid(ctx.volume_id) {
            // The entry already matched by VID and derive_vuk needs only mk +
            // ctx.volume_id, so a provider that matches by VID without
            // populating disc_id (e.g. a webservice) must not have its MK
            // dropped — gate on the MK alone.
            if let Some(mk) = entry.media_key {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_v21_path3_hit", "MK+VID entry matched volume_id");
                return Some(build(Some(vuk), derive_uks(&vuk), 3));
            }
        }
    } else {
        tracing::debug!(
            target: "freemkv::disc",
            phase = "resolve_keys_v21_no_vid",
            "VID unavailable; paths 1/3 skipped"
        );
    }

    // Paths 4 and 5: hash lookup, prefer V over U on the same entry.
    if let Some(entry) = providers.lookup_disc_by_hash(&uk_file.disc_hash) {
        if let Some(vuk) = entry.vuk {
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_v21_path4_hit", "VUK from KEYDB");
            return Some(build(Some(vuk), derive_uks(&vuk), 4));
        } else if let Some(unit_keys) = match_keydb_unit_keys(&uk_file, &entry.unit_keys) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "resolve_keys_v21_path5_hit",
                uk_count = unit_keys.len(),
                "unit keys from KEYDB (no VUK)"
            );
            return Some(build(None, unit_keys, 5));
        }
    }

    None
}

/// Resolve all AACS keys for a disc using the classical (single-stage
/// Media Key derivation) paths. Used by both V10 and V20.
///
/// Paths run in root-of-trust → per-disc-leaf order. A match at any
/// path returns immediately:
///   1. MKB + device keys → processing key → media key → VUK
///   2. MKB + processing keys → media key → VUK
///   3. KEYDB MK + matching VID → derived VUK
///   4. KEYDB disc-hash → VUK
///   5. KEYDB disc-hash → pre-decrypted unit keys (no VUK)
fn resolve_keys_classical(ctx: &ResolveContext<'_>, version: AacsVersion) -> Option<ResolvedKeys> {
    let bus_encryption = ctx
        .content_cert
        .and_then(parse_content_cert)
        .map(|cc| cc.bus_encryption)
        .unwrap_or(false);

    // Parse Unit_Key_RO.inf at the version-appropriate stride.
    let uk_file = parse_unit_key_ro(ctx.unit_key_ro, version)?;

    let hash_hex = disc_hash_hex(&uk_file.disc_hash);
    let has_vid = *ctx.volume_id != [0u8; 16];

    // Decrypt the disc's encrypted unit keys with a freshly-derived VUK.
    let derive_uks = |vuk: &[u8; 16]| -> Vec<(u32, [u8; 16])> {
        uk_file
            .encrypted_keys
            .iter()
            .map(|(num, enc_key)| (*num, decrypt_unit_key(vuk, enc_key)))
            .collect()
    };

    // Common result constructor — paths 1-4 supply Some(VUK) + derived
    // unit keys; path 5 supplies None + pre-decrypted unit keys from
    // KEYDB.
    let build =
        |vuk: Option<[u8; 16]>, unit_keys: Vec<(u32, [u8; 16])>, key_source: u8| -> ResolvedKeys {
            ResolvedKeys {
                disc_hash: uk_file.disc_hash,
                vuk,
                unit_keys,
                title_cps_unit: uk_file.title_cps_unit.clone(),
                version,
                bus_encryption,
                key_source,
            }
        };

    tracing::info!(
        target: "freemkv::disc",
        phase = "resolve_keys_start",
        version = ?version,
        bus_encryption,
        disc_hash = %hash_hex,
        has_vid,
        mkb_present = ctx.mkb.is_some(),
        "resolve_keys: starting"
    );

    let providers = super::provider::Providers(ctx.providers);

    // Paths 1 and 2 need both MKB and VID. Logged as a single skip when
    // either is absent so operators see one reason, not two.
    if has_vid {
        if let Some(mkb) = ctx.mkb {
            let mk_dv = mkb_find_mk_dv(mkb);
            let subdiff = mkb_find_subdiff_records(mkb);
            let cvalues = mkb_find_cvalues(mkb);
            tracing::debug!(
                target: "freemkv::disc",
                phase = "resolve_keys_mkb_records",
                mk_dv_found = mk_dv.is_some(),
                subdiff_found = subdiff.is_some(),
                subdiff_len = subdiff.as_ref().map(|s| s.len()).unwrap_or(0),
                cvalues_found = cvalues.is_some(),
                cvalues_len = cvalues.as_ref().map(|c| c.len()).unwrap_or(0),
                "MKB record scan results"
            );

            // Path 1: MKB + device keys → media key → VUK
            let all_dks = providers.device_keys();
            if let Some(mk) = derive_media_key_from_dk(mkb, &all_dks) {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path1_hit", "media key derived from device key");
                return Some(build(Some(vuk), derive_uks(&vuk), 1));
            }
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path1_miss", dk_count = all_dks.len(), "DK derivation failed");

            // Path 2: MKB + processing keys → media key → VUK
            let all_pks = providers.processing_keys();
            if let Some(mk) = derive_media_key_from_pk(mkb, &all_pks) {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_hit", "media key derived from processing key");
                return Some(build(Some(vuk), derive_uks(&vuk), 2));
            }
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_miss", pk_count = all_pks.len(), "PK derivation failed");

            // Path 2.5: MK-pool brute. keydb stores Media Keys per-disc, but an
            // MK is MKB-scoped (shared across a pressing/MKB-family). A disc
            // whose own hash/VID isn't keyed can still resolve if ANY stored MK
            // verifies against its MKB. Try every distinct MK via km_verifies;
            // a UNIQUE pass is this disc's Km → derive VUK (needs VID) → UK.
            // One AES-D + magic check per candidate (cheap). mk_dv is hoisted
            // out of the loop so the MKB is not re-walked per candidate.
            let mks = providers.media_keys();
            let mut mk_hits: Vec<[u8; 16]> = Vec::new();
            if let Some(mk_dv) = mkb_find_mk_dv(mkb) {
                for mk in &mks {
                    let verifies = aes_ecb_decrypt(mk, &mk_dv)[..8]
                        == [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
                    if verifies && !mk_hits.contains(mk) {
                        mk_hits.push(*mk);
                        if mk_hits.len() > 1 {
                            break; // ambiguous — bail to avoid a wrong key
                        }
                    }
                }
            }
            if mk_hits.len() == 1 {
                let vuk = derive_vuk(&mk_hits[0], ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_5_hit", mk_pool = mks.len(), "media key from keydb MK-pool brute (km_verifies)");
                // Same class as path 3 (KEYDB MK → derived VUK).
                return Some(build(Some(vuk), derive_uks(&vuk), 3));
            }
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_5_miss", mk_pool = mks.len(), mk_hits = mk_hits.len(), "MK-pool brute: no unique verifying MK");
        } else {
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_no_mkb", "no MKB; paths 1/2 skipped");
        }

        // Path 3: pre-computed MK + matching VID → derived VUK.
        // Short-circuit: first provider with a matching VID wins.
        if let Some(entry) = providers.lookup_disc_by_vid(ctx.volume_id) {
            // The entry already matched by VID and derive_vuk needs only mk +
            // ctx.volume_id, so a provider that matches by VID without
            // populating disc_id (e.g. a webservice) must not have its MK
            // dropped — gate on the MK alone.
            if let Some(mk) = entry.media_key {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path3_hit", "MK+VID entry matched volume_id");
                return Some(build(Some(vuk), derive_uks(&vuk), 3));
            }
        }
        tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path3_miss", "no MK+VID entry matched volume_id");
    } else {
        tracing::debug!(
            target: "freemkv::disc",
            phase = "resolve_keys_no_vid",
            "VID unavailable; paths 1/2/3 require VID and are skipped"
        );
    }

    // Paths 4 and 5: single hash-keyed lookup, prefer V (path 4) over
    // U (path 5). They are not independent checks — path 5 only fires
    // because path 4 had no VUK on the same entry.
    if let Some(entry) = providers.lookup_disc_by_hash(&uk_file.disc_hash) {
        tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_keydb_hit_entry", "disc hash found in provider");
        if let Some(vuk) = entry.vuk {
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path4_hit", "VUK from provider");
            return Some(build(Some(vuk), derive_uks(&vuk), 4));
        } else if let Some(unit_keys) = match_keydb_unit_keys(&uk_file, &entry.unit_keys) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "resolve_keys_path5_hit",
                uk_count = unit_keys.len(),
                "unit keys from provider (no VUK)"
            );
            return Some(build(None, unit_keys, 5));
        }
        tracing::warn!(target: "freemkv::disc", phase = "resolve_keys_keydb_no_keys", "provider entry has neither VUK nor matching unit keys");
    } else {
        tracing::warn!(target: "freemkv::disc", phase = "resolve_keys_keydb_miss", "disc hash NOT found in any provider");
    }

    None
}

/// For path 5: cross-reference the disc's `Unit_Key_RO.inf` CPS-unit
/// numbering against the KEYDB entry's pre-decrypted unit keys. Every
/// CPS unit the disc declares must have a matching entry in KEYDB;
/// partial coverage returns `None` so the resolver doesn't half-decrypt
/// a disc.
fn match_keydb_unit_keys(
    uk_file: &UnitKeyFile,
    keydb_unit_keys: &[(u32, [u8; 16])],
) -> Option<Vec<(u32, [u8; 16])>> {
    if keydb_unit_keys.is_empty() {
        return None;
    }
    let mut matched = Vec::with_capacity(uk_file.encrypted_keys.len());
    for (disc_num, _enc_key) in &uk_file.encrypted_keys {
        let entry = keydb_unit_keys.iter().find(|(n, _)| n == disc_num)?;
        matched.push(*entry);
    }
    Some(matched)
}

#[cfg(test)]
mod tests {
    use super::super::decrypt::{ALIGNED_UNIT_LEN, aes_ecb_encrypt};
    use super::super::keydb::{DiscEntry, KeyDb};
    use super::*;

    /// Get KEYDB path from KEYDB_PATH environment variable. Returns None if not set or not found.
    fn keydb_path() -> Option<std::path::PathBuf> {
        let path = std::path::PathBuf::from(std::env::var("KEYDB_PATH").ok()?);
        if path.exists() { Some(path) } else { None }
    }

    #[test]
    fn derive_media_key_from_dk_survives_out_of_range_u_mask_shift() {
        // Regression: a crafted/corrupt MKB with a Subset-Difference
        // u_mask_shift of 32..=63 (passes the 0xC0 revoked-marker check but
        // overflows `0xFFFF_FFFF << shift`) used to panic in debug / compute a
        // wrong mask in release. The walk must now skip the bad slot and
        // return cleanly (no panic) on disc-controlled bytes.
        let mut mkb: Vec<u8> = Vec::new();
        // 0x81 record: 4-byte header + 16-byte mk_dv body (rec_len = 20).
        mkb.extend_from_slice(&[0x81, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);
        // 0x04 Subset-Difference: one 5-byte entry with u_mask_shift = 0x30
        // (48 — out of range, but 0x30 & 0xC0 == 0 so the revoke check passes).
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0x30, 0x00, 0x00, 0x00, 0x01]);
        // 0x05 cvalues: one 16-byte entry (rec_len = 20).
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);

        let dk = DeviceKey {
            key: [0x11; 16],
            node: 1,
            uv: 1,
            u_mask_shift: 0x30, // also out of range on the device-key side
        };

        // Must not panic; no valid derivation is expected from this junk.
        let _ = derive_media_key_from_dk(&mkb, &[dk]);
    }

    #[test]
    fn test_vuk_derivation() {
        // Pick any UHD entry with a known MK, VID, and VUK from KEYDB.
        // VUK = AES-DEC(MK, VID) XOR VID
        let path = match keydb_path() {
            Some(p) => p,
            None => return,
        };

        let db = KeyDb::load(&path).unwrap();

        // Find a disc with both MK, disc_id, and VUK so we can verify derivation
        let entry = db
            .disc_entries
            .values()
            .find(|e| e.media_key.is_some() && e.disc_id.is_some() && e.vuk.is_some())
            .expect("No disc with MK + VID + VUK");

        let mk = entry.media_key.unwrap();
        let vid = entry.disc_id.unwrap();
        let expected_vuk = entry.vuk.unwrap();

        let derived = derive_vuk(&mk, &vid);
        assert_eq!(
            derived, expected_vuk,
            "VUK derivation failed for disc: {} (hash {})",
            entry.title, entry.disc_hash
        );
        eprintln!("VUK derivation verified for: {}", entry.title);
    }

    #[test]
    fn test_decrypt_unit_key_from_vuk() {
        // Test the full chain: VUK → decrypt encrypted unit key → unit key
        // Use a known disc from KEYDB that has both VUK and unit keys
        let path = match keydb_path() {
            Some(p) => p,
            None => return,
        };

        let db = KeyDb::load(&path).unwrap();

        // Find a disc with VUK and unit keys
        let entry = db
            .disc_entries
            .values()
            .find(|e| e.vuk.is_some() && !e.unit_keys.is_empty())
            .expect("No disc with VUK + unit keys");

        eprintln!(
            "Testing unit key decrypt for: {} ({})",
            entry.title, entry.disc_hash
        );
        eprintln!("  VUK: {:02X?}", entry.vuk.unwrap());
        for (num, key) in &entry.unit_keys {
            eprintln!("  Unit key {}: {:02X?}", num, key);
        }

        // The unit keys in KEYDB are already decrypted — we can verify the chain
        // by encrypting with VUK and then decrypting
        let vuk = entry.vuk.unwrap();
        for (num, expected_uk) in &entry.unit_keys {
            let encrypted = aes_ecb_encrypt(&vuk, expected_uk);
            let decrypted = decrypt_unit_key(&vuk, &encrypted);
            assert_eq!(
                &decrypted, expected_uk,
                "Unit key {} roundtrip failed for {}",
                num, entry.title
            );
        }
        eprintln!("  All {} unit key roundtrips passed", entry.unit_keys.len());
    }

    #[test]
    fn test_decrypt_real_unit() {
        // Try decrypting a real encrypted aligned unit from a UHD sample.
        // This disc is AACS 2.0 (BEE) so unit key alone won't work —
        // we need bus decryption first. But this verifies the pipeline.
        // Path comes from ENCRYPTED_UNIT_PATH (same env-driven pattern as the
        // KEYDB_PATH / MKB_SAMPLE_DIR fixtures); no-ops in CI when unset.
        let unit_path = match std::env::var("ENCRYPTED_UNIT_PATH").ok() {
            Some(p) => std::path::PathBuf::from(p),
            None => return,
        };
        if !unit_path.exists() {
            return;
        }

        let original = std::fs::read(&unit_path).unwrap();
        assert_eq!(original.len(), ALIGNED_UNIT_LEN);
        assert!(
            super::super::decrypt::is_aacs_scrambled(&original),
            "Unit should be encrypted"
        );

        let kp = match keydb_path() {
            Some(p) => p,
            None => return,
        };
        let db = KeyDb::load(&kp).unwrap();

        // Candidate entries: any UHD entry that carries unit keys.
        let candidate_entries: Vec<&DiscEntry> = db
            .disc_entries
            .values()
            .filter(|e| !e.unit_keys.is_empty())
            .collect();

        eprintln!("Found {} entries with unit keys", candidate_entries.len());

        // Try each entry's unit keys
        for entry in &candidate_entries {
            let keys: Vec<[u8; 16]> = entry.unit_keys.iter().map(|(_, k)| *k).collect();
            let mut unit = original.clone();

            if let Some(res) = super::super::decrypt::decrypt_unit_try_keys(&mut unit, &keys) {
                eprintln!(
                    "SUCCESS: Decrypted with entry {} ({res:?})",
                    entry.disc_hash
                );
                // Count TS sync bytes
                let ts = (0..32).filter(|&i| unit[4 + i * 192] == 0x47).count();
                eprintln!("  TS sync bytes: {}/32", ts);
                return;
            }
        }

        // Expected: none work because this is AACS 2.0 and needs bus decryption first
        eprintln!("No unit key worked (expected for AACS 2.0 BEE disc — needs read_data_key)");
    }

    #[test]
    fn test_disc_hash() {
        // SHA1 of a known byte sequence
        let data = b"test unit key ro inf data";
        let hash = disc_hash(data);
        assert_ne!(hash, [0u8; 20]);
        // Same input → same hash
        assert_eq!(hash, disc_hash(data));
    }

    #[test]
    fn test_disc_hash_hex() {
        let hash = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13,
        ];
        let hex = disc_hash_hex(&hash);
        assert_eq!(hex, "0x000102030405060708090A0B0C0D0E0F10111213");
    }

    #[test]
    fn test_parse_unit_key_ro_synthetic() {
        // Build a synthetic Unit_Key_RO.inf
        // Header: uk_pos at offset 0 (BE32), points to key storage
        // Keys at uk_pos + 48 (16 bytes each, 48-byte stride for AACS 1.0)
        let mut data = vec![0u8; 256];

        // uk_pos = 0x60 (96)
        data[0] = 0x00;
        data[1] = 0x00;
        data[2] = 0x00;
        data[3] = 0x60;

        // Header fields at 16-18
        data[16] = 1; // app_type = BD-ROM
        data[17] = 1; // num_bdmv_dir
        data[18] = 0; // no SKB

        // Title mapping at 20-25
        data[20] = 0;
        data[21] = 1; // first_play = CPS unit 1
        data[22] = 0;
        data[23] = 1; // top_menu = CPS unit 1
        data[24] = 0;
        data[25] = 1; // num_titles = 1
        // Title 0 entry: 2 bytes pad + CPS unit
        data[28] = 0;
        data[29] = 1; // CPS unit 1

        // Key storage at offset 0x60
        let uk_pos = 0x60usize;
        data[uk_pos] = 0;
        data[uk_pos + 1] = 2; // 2 unit keys

        // Key 1 at uk_pos + 48
        let key1_pos = uk_pos + 48;
        for i in 0..16 {
            data[key1_pos + i] = 0xAA;
        }

        // Key 2 at uk_pos + 48 + 48
        let key2_pos = key1_pos + 48;
        for i in 0..16 {
            data[key2_pos + i] = 0xBB;
        }

        let parsed = parse_unit_key_ro(&data, AacsVersion::V10).unwrap();
        assert_eq!(parsed.app_type, 1);
        assert_eq!(parsed.num_bdmv_dir, 1);
        assert_eq!(parsed.version, AacsVersion::V10);
        assert_eq!(parsed.encrypted_keys.len(), 2);
        assert_eq!(parsed.encrypted_keys[0].0, 1); // CPS unit 1
        assert_eq!(parsed.encrypted_keys[0].1, [0xAA; 16]);
        assert_eq!(parsed.encrypted_keys[1].0, 2); // CPS unit 2
        assert_eq!(parsed.encrypted_keys[1].1, [0xBB; 16]);
    }

    #[test]
    fn mkb_version_recognizes_type_0x10() {
        // Type-and-Version record: type=0x10, rec_len=12 (BE24).
        // Body is 8 bytes; the version u32 sits at offset 8 of the record.
        let mkb = [
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];
        assert_eq!(mkb_version(&mkb), Some(77));
    }

    #[test]
    fn mkb_content_len_trims_trailing_padding() {
        // Two real records (0x10 type/version + 0x86 verify), then 128 KiB of
        // zero padding (the fixed-region tail). Content length must stop at the
        // end of the records, not include the padding.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4D,
        ];
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&[0xAB; 16]);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        let records_len = mkb.len();
        mkb.extend(std::iter::repeat(0u8).take(128 * 1024)); // padding
        assert_eq!(mkb_content_len(&mkb), records_len);
        // No padding → returns the full length.
        assert_eq!(mkb_content_len(&mkb[..records_len]), records_len);
        // Empty → 0.
        assert_eq!(mkb_content_len(&[]), 0);
    }

    #[test]
    fn mkb_version_returns_none_on_empty() {
        assert_eq!(mkb_version(&[]), None);
        assert_eq!(mkb_version(&[0x10, 0x00]), None);
        // Type 0x10 record but rec_len < 12 → no version available.
        let short = [0x10, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01];
        assert_eq!(mkb_version(&short), None);
    }

    #[test]
    fn mkb_find_mk_dv_recognizes_type_0x81() {
        // First: type-0x10 type/version record (12 bytes), then type-0x81 verify record.
        // Verify record carries a known 16-byte mk_dv at offset 4 of the record body.
        let expected: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        ];
        // type=0x81, rec_len=24 (4-byte header + 16-byte mk_dv + 4-byte trailing zeros)
        mkb.extend_from_slice(&[0x81, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&expected);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        assert_eq!(mkb_find_mk_dv(&mkb), Some(expected));
    }

    #[test]
    fn probe_walk_pk_against_tables_accepts_planted_pk_rejects_corrupt() {
        // Lock in the shared SD walk used by both production
        // (`derive_media_key_from_pk_walked`) and the independent-reproduction
        // harness (`probe::walk_pk_against_tables`). Plant a terminal PK whose
        // derived Media Key satisfies a synthetic verify record; confirm the
        // walk ACCEPTS it against caller-supplied SD/cvalue tables and REJECTS a
        // 1-byte corruption.
        use super::super::decrypt::aes_ecb_encrypt as enc;

        let pk: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let mk: [u8; 16] = [
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ];
        let uv: [u8; 4] = [0x00, 0x00, 0x04, 0x00];

        let mut mk_raw = mk;
        for a in 0..4 {
            mk_raw[12 + a] ^= uv[a];
        }
        let cv = enc(&pk, &mk_raw); // AES-D(pk, cv) == mk_raw
        let mut vd = [0u8; 16];
        vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        let mk_dv = enc(&mk, &vd); // AES-D(mk, mk_dv) starts with sentinel

        // 0x04 SD body: one entry [u_mask_shift=0][uv].
        let mut subdiff = vec![0u8];
        subdiff.extend_from_slice(&uv);

        assert_eq!(
            probe::walk_pk_against_tables(std::slice::from_ref(&pk), &subdiff, &cv, &mk_dv, 1),
            Some(mk),
            "planted terminal PK must verify"
        );
        let mut bad = pk;
        bad[0] ^= 0xFF;
        assert_eq!(
            probe::walk_pk_against_tables(std::slice::from_ref(&bad), &subdiff, &cv, &mk_dv, 1),
            None,
            "corrupted PK must be rejected"
        );
    }

    #[test]
    fn validate_processing_key_round_trip_with_nonzero_uv() {
        // Synthesise a (pk, uv, mk, cvalue, mk_dv) tuple that satisfies the
        // libaacs _validate_pk relation, then confirm validate_processing_key
        // recovers mk. Catches the bugs that landed pre-fix:
        //   * uv XOR step was missing → mk wrong whenever uv != 0
        //   * AES-128E + 12-zero check instead of AES-128D + magic
        use super::super::decrypt::{aes_ecb_decrypt as dec, aes_ecb_encrypt as enc};

        let pk: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let mk: [u8; 16] = [
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ];
        let uv: [u8; 4] = [0x00, 0x00, 0x04, 0x00];

        // cvalue is what AES-128E(pk, mk') gives, where mk' = mk with the
        // last-4-bytes-uv XOR pre-undone:
        //   mk_raw[12..16] = mk[12..16] XOR uv  (so the validate step XORs
        //   uv back in and recovers mk).
        let mut mk_raw = mk;
        for a in 0..4 {
            mk_raw[12 + a] ^= uv[a];
        }
        let cvalue = enc(&pk, &mk_raw);

        // mk_dv is the encryption (under the correct mk) of the verify
        // magic, padded with arbitrary bytes — when decrypted with mk we
        // recover the magic.
        let mut plaintext_vd = [0u8; 16];
        plaintext_vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        // Trailing 8 bytes are don't-cares in the magic check.
        plaintext_vd[8..].copy_from_slice(&[0x11; 8]);
        let mk_dv = enc(&mk, &plaintext_vd);
        // Sanity: decrypting mk_dv with mk yields the magic.
        let _check = dec(&mk, &mk_dv);

        let recovered = validate_processing_key(&pk, &cvalue, &uv, &mk_dv)
            .expect("validate_processing_key must accept a correct pk + uv pair");
        assert_eq!(recovered, mk, "recovered mk must match the planted mk");

        // And a wrong pk must be rejected.
        let mut wrong_pk = pk;
        wrong_pk[0] ^= 0xFF;
        assert!(validate_processing_key(&wrong_pk, &cvalue, &uv, &mk_dv).is_none());

        // And a uv mismatch must be rejected.
        let wrong_uv = [0x00u8, 0x00, 0x00, 0x00];
        assert!(validate_processing_key(&pk, &cvalue, &wrong_uv, &mk_dv).is_none());
    }

    // ── MKB cvalue-record selection (issue #259 / #281) ─────────────────
    //
    // The cvalue (Media Key Data) table is record 0x05; the
    // Subset-Difference index is record 0x04. This matches libaacs
    // (`mkb_cvalues` → 0x05, `mkb_subdiff_records` → 0x04). Record 0x07
    // (Explicit Subset-Difference Record) is NOT the cvalue table. On real
    // in-drive AACS 2.x UHD MKBs 0x07 is small (~96 entries) while the 0x05
    // table is large (181270 entries, 1:1 with 0x04). An earlier
    // `mkb_find_cvalues` preferred 0x07, which under-tested the SD walk and
    // broke the DK→walk path. The selector must prefer 0x05.

    /// Build a 4-byte MKB record header (type + 3-byte big-endian total
    /// length, header included) and append `body`.
    fn mkb_record(rec_type: u8, body: &[u8]) -> Vec<u8> {
        let total = 4 + body.len();
        let mut rec = Vec::with_capacity(total);
        rec.push(rec_type);
        rec.push(((total >> 16) & 0xFF) as u8);
        rec.push(((total >> 8) & 0xFF) as u8);
        rec.push((total & 0xFF) as u8);
        rec.extend_from_slice(body);
        rec
    }

    /// Synthesize an AACS-2.x-shaped MKB carrying BOTH a small 0x07 record
    /// and the real 0x05 cvalue table, with 0x07 placed first so a
    /// "0x07-first" selector would pick the wrong record. The 0x05 table
    /// has `n` 16-byte entries (1:1 with the `n`-entry 0x04 SD index); the
    /// 0x07 decoy has `decoy` 16-byte entries.
    fn synth_aacs2_mkb(n: usize, decoy: usize) -> Vec<u8> {
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&mkb_record(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&mkb_record(0x86, &[0xABu8; 16]));
        let mut sd = Vec::with_capacity(n * 5);
        for i in 0..n {
            sd.push(0x00); // u_mask_shift, top bits clear → not revoked
            sd.extend_from_slice(&((i as u32) + 1).to_be_bytes());
        }
        mkb.extend_from_slice(&mkb_record(0x04, &sd));
        mkb.extend_from_slice(&mkb_record(0x07, &vec![0x11u8; decoy * 16])); // decoy first
        mkb.extend_from_slice(&mkb_record(0x05, &vec![0x22u8; n * 16])); // real cvalues
        mkb
    }

    #[test]
    fn cvalue_selection_prefers_0x05_over_0x07() {
        // AACS-2.x layout: large 0x05 (1:1 with 0x04) + smaller decoy 0x07
        // placed earlier in the record stream.
        let n = 1500;
        let decoy = 96;
        let mkb = synth_aacs2_mkb(n, decoy);

        let sd = probe::mkb_subdiff(&mkb).expect("0x04 present");
        let r05 = probe::mkb_record_body(&mkb, 0x05).expect("0x05 present");
        let r07 = probe::mkb_record_body(&mkb, 0x07).expect("0x07 present");
        let selected = mkb_find_cvalues(&mkb).expect("cvalues selected");

        assert_eq!(sd.len() / 5, n, "0x04 SD index entry count");
        assert_eq!(r05.len() / 16, n, "0x05 cvalue entry count");
        assert_eq!(r07.len() / 16, decoy, "0x07 decoy entry count");

        // The fix: selection MUST pick 0x05 (the large 1:1 table), NOT the
        // 0x07 decoy a "0x07-first" rule would return.
        assert_eq!(
            selected.len() / 16,
            n,
            "cvalue selection must use the large 0x05 table, not the {decoy}-entry 0x07 decoy"
        );
        assert_eq!(
            selected, r05,
            "selected body must be the 0x05 record verbatim"
        );
        assert_eq!(
            selected.len() / 16,
            sd.len() / 5,
            "cvalue table must be 1:1 with the 0x04 Subset-Difference index"
        );
    }

    #[test]
    fn cvalue_selection_falls_back_to_0x07_when_no_0x05() {
        // Malformed/legacy MKB with only a 0x07 record and no 0x05: the
        // selector falls back to 0x07 rather than returning None.
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&mkb_record(0x10, &[0, 0, 0, 0x10, 0, 0, 0, 1]));
        mkb.extend_from_slice(&mkb_record(0x86, &[0xCDu8; 16]));
        mkb.extend_from_slice(&mkb_record(0x04, &[0x00, 0, 0, 0, 1]));
        let only07 = vec![0x33u8; 16];
        mkb.extend_from_slice(&mkb_record(0x07, &only07));

        assert!(probe::mkb_record_body(&mkb, 0x05).is_none());
        let selected = mkb_find_cvalues(&mkb).expect("falls back to 0x07");
        assert_eq!(selected, only07, "fallback returns the 0x07 body");
    }

    /// Locate a captured MKB sample under the optional `MKB_SAMPLE_DIR`.
    /// Returns `None` (skip) when the directory or file is absent.
    fn mkb_sample(rel: &str) -> Option<std::path::PathBuf> {
        let dir = std::env::var("MKB_SAMPLE_DIR").ok()?;
        let p = std::path::Path::new(&dir).join(rel);
        if p.exists() { Some(p) } else { None }
    }

    #[test]
    fn real_aacs2_samples_select_large_0x05_not_small_0x07() {
        // Real in-drive AACS 2.x UHD MKBs carry BOTH a small 0x07
        // Explicit-Subset-Difference record (96 16-byte entries) AND the
        // large 0x05 Media Key Data / cvalue table (181270 entries, 1:1
        // with the 0x04 index). The production selector must return the
        // LARGE 0x05 body, not the small 0x07 one. This is the exact
        // regression #259 found. Skips when no sample dir is present.
        let samples = [
            "sample-a/MKB_RO.inf",
            "sample-b/MKB_RO.inf",
            "sample-c/MKB_RO.inf",
        ];
        let mut checked = 0;
        for rel in samples {
            let path = match mkb_sample(rel) {
                Some(p) => p,
                None => continue,
            };
            let data = std::fs::read(&path).expect("read sample MKB");

            let r05 = probe::mkb_record_body(&data, 0x05)
                .unwrap_or_else(|| panic!("{rel}: expected a 0x05 Media Key Data record"));
            let r07 = probe::mkb_record_body(&data, 0x07)
                .unwrap_or_else(|| panic!("{rel}: expected a 0x07 record"));
            let sd = probe::mkb_subdiff(&data)
                .unwrap_or_else(|| panic!("{rel}: expected a 0x04 Subset-Difference index"));

            let n05 = r05.len() / 16;
            let n07 = r07.len() / 16;

            // The discriminating facts the bug report cited.
            assert!(
                n05 > n07 * 100,
                "{rel}: 0x05 ({n05}) must dwarf 0x07 ({n07})"
            );
            assert_eq!(n05, 181270, "{rel}: full 0x05 cvalue table size");
            assert_eq!(n07, 96, "{rel}: small 0x07 record size");

            // Production selection must be the large 0x05 table.
            let selected = mkb_find_cvalues(&data)
                .unwrap_or_else(|| panic!("{rel}: cvalue selection returned None"));
            assert_eq!(
                selected, r05,
                "{rel}: selector must return the large 0x05 body, not 0x07"
            );

            // And it is 1:1 with the 0x04 SD index the walk iterates: the
            // walk's UV count (take_while top-2-bits clear) lines up with
            // the cvalue count to within the trailing padding entry.
            let uv_entries = sd
                .chunks(5)
                .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
                .count();
            assert!(
                uv_entries >= n05 - 2 && uv_entries <= n05,
                "{rel}: 0x04 UV count ({uv_entries}) should match 0x05 cvalue count ({n05})"
            );

            eprintln!(
                "{rel}: 0x05={n05} cvalues, 0x07={n07}, 0x04 UVs={uv_entries} — selected 0x05"
            );
            checked += 1;
        }
        if checked == 0 {
            eprintln!("no MKB samples present; skipping real-sample assertion");
        }
    }

    #[test]
    fn mkb_find_mk_dv_recognizes_type_0x86() {
        // AACS 2.0 form uses type 0x86 for the verify record.
        let expected: [u8; 16] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
            0x0B, 0x0C,
        ];
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4D,
        ];
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&expected);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        assert_eq!(mkb_find_mk_dv(&mkb), Some(expected));
    }

    #[test]
    fn test_resolve_keys_vuk_path() {
        // Test the full resolve chain using VUK path
        let path = match keydb_path() {
            Some(p) => p,
            None => return,
        };
        let db = KeyDb::load(&path).unwrap();

        // Find any BD entry that carries a VUK and unit keys, then exercise
        // the lookup-by-hash + VUK-derivation chain against it.
        let entry = db
            .disc_entries
            .values()
            .find(|e| e.vuk.is_some() && !e.unit_keys.is_empty() && e.disc_id.is_some());
        if entry.is_none() {
            return;
        }
        let entry = entry.unwrap();
        let vuk = entry.vuk.unwrap();
        let vid = entry.disc_id.unwrap();
        let hash_hex = format!("0x{}", entry.disc_hash.trim_start_matches("0x"));

        // We need the actual Unit_Key_RO.inf from the disc to compute disc hash.
        // Since we don't have it, we can at least test that the KEYDB lookup
        // works with a known hash.
        let found = db.find_disc(&hash_hex);
        assert!(found.is_some());
        assert_eq!(found.unwrap().vuk, Some(vuk));

        // Verify VUK derivation if we have MK + VID
        if let Some(mk) = entry.media_key {
            let derived = derive_vuk(&mk, &vid);
            assert_eq!(derived, vuk, "VUK derivation mismatch");
            eprintln!("VUK derivation verified");
        }
    }

    /// Build a minimal Unit_Key_RO.inf with `num_unit_keys = 1`. The
    /// disc hash won't be in any synthetic keydb so path 1 misses,
    /// which lets us isolate the path-2/3/4 short-circuit behavior.
    fn minimal_unit_key_ro() -> Vec<u8> {
        let mut data = vec![0u8; 256];
        // uk_pos = 0x60
        data[3] = 0x60;
        data[16] = 1; // app_type = BD-ROM
        data[17] = 1; // num_bdmv_dir
        let uk_pos = 0x60usize;
        data[uk_pos + 1] = 1; // 1 unit key
        // Key at uk_pos + 48 — value doesn't matter, just needs to fit.
        for i in 0..16 {
            data[uk_pos + 48 + i] = 0xCC;
        }
        data
    }

    #[test]
    fn resolve_keys_skips_paths_2_through_4_when_vid_is_zero() {
        // No VID -> paths 2/3/4 cannot succeed. The function must
        // return None WITHOUT touching the MKB / device keys, so we
        // can pass an MKB that would otherwise cause expensive
        // derivation work — it must not be consumed.
        let uk_ro = minimal_unit_key_ro();
        let zero_vid = [0u8; 16];

        // Populate keydb with a non-matching VID entry (path 2 would
        // miss anyway) plus dummy processing/device keys (paths 3/4
        // would also miss, but the short-circuit means they're never
        // attempted).
        let mut keydb = KeyDb::empty();
        keydb.disc_entries.insert(
            "0xDEADBEEF".to_string(),
            DiscEntry {
                disc_hash: "0xDEADBEEF".to_string(),
                title: "fixture".to_string(),
                media_key: Some([0x11u8; 16]),
                disc_id: Some([0x22u8; 16]),
                vuk: None,
                unit_keys: Vec::new(),
            },
        );
        keydb.processing_keys.push([0u8; 16]);

        let providers: &[&dyn super::super::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &zero_vid,
            providers,
            mkb: None,
        };
        let result = resolve_keys_v1(&ctx);
        assert!(
            result.is_none(),
            "resolve_keys with VID=0 and no matching disc-hash entry must return None"
        );
    }

    #[test]
    fn resolve_keys_path4_still_runs_when_vid_is_zero() {
        // Path 4 (disc-hash → VUK) doesn't need VID. Confirm the
        // short-circuit doesn't block it: install a keydb entry whose
        // disc_hash matches the fixture's hash, with a known VUK, and
        // verify resolve_keys returns it with key_source = 4.
        let uk_ro = minimal_unit_key_ro();
        let hash = disc_hash(&uk_ro);
        // `find_disc` lowercases the incoming hash; the entry map is
        // keyed lowercase too, so we have to lowercase here.
        let hash_hex = disc_hash_hex(&hash).to_lowercase();

        let mut keydb = KeyDb::empty();
        let known_vuk = [0xABu8; 16];
        keydb.disc_entries.insert(
            hash_hex.clone(),
            DiscEntry {
                disc_hash: hash_hex,
                title: "fixture".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some(known_vuk),
                unit_keys: Vec::new(),
            },
        );

        let vid = [0u8; 16];
        let providers: &[&dyn super::super::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None,
        };
        let resolved =
            resolve_keys_v1(&ctx).expect("path 4 must run regardless of VID availability");
        assert_eq!(resolved.vuk, Some(known_vuk));
        assert_eq!(resolved.key_source, 4);
    }

    #[test]
    fn resolve_keys_path5_uses_keydb_unit_keys_when_vuk_absent() {
        // Path 5: an entry with no VUK but with pre-decrypted unit
        // keys matching the disc's CPS-unit numbering decrypts the
        // disc directly. Covers the ~4,572 U-only KEYDB entries
        // (mostly MKBv76+ UHDs) that the resolver previously ignored.
        let uk_ro = minimal_unit_key_ro();
        let hash = disc_hash(&uk_ro);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();

        // `minimal_unit_key_ro` declares CPS unit 1; supply a matching
        // pre-decrypted unit key in the KEYDB entry.
        let known_uk = [0xCDu8; 16];
        let mut keydb = KeyDb::empty();
        keydb.disc_entries.insert(
            hash_hex.clone(),
            DiscEntry {
                disc_hash: hash_hex,
                title: "fixture".to_string(),
                media_key: None,
                disc_id: None,
                vuk: None,
                unit_keys: vec![(1, known_uk)],
            },
        );

        let vid = [0u8; 16];
        let providers: &[&dyn super::super::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None,
        };
        let resolved =
            resolve_keys_v1(&ctx).expect("path 5 must succeed when KEYDB carries unit keys");
        assert_eq!(resolved.vuk, None, "path 5 has no VUK to return");
        assert_eq!(resolved.key_source, 5);
        assert_eq!(resolved.unit_keys, vec![(1, known_uk)]);
    }

    #[test]
    fn resolve_keys_path5_rejects_partial_unit_key_coverage() {
        // If the disc declares a CPS unit that's not in the KEYDB
        // entry's unit_keys, path 5 must NOT half-decrypt the disc.
        // The match function returns None and the resolver falls
        // through to None overall (no other paths available in this
        // setup).
        let uk_ro = minimal_unit_key_ro();
        let hash = disc_hash(&uk_ro);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();

        // KEYDB has a key for CPS unit 99, but the disc declares unit 1.
        let mut keydb = KeyDb::empty();
        keydb.disc_entries.insert(
            hash_hex.clone(),
            DiscEntry {
                disc_hash: hash_hex,
                title: "fixture".to_string(),
                media_key: None,
                disc_id: None,
                vuk: None,
                unit_keys: vec![(99, [0xEEu8; 16])],
            },
        );

        let vid = [0u8; 16];
        let providers: &[&dyn super::super::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None,
        };
        assert!(
            resolve_keys_v1(&ctx).is_none(),
            "partial CPS-unit coverage must not produce a half-decrypted result"
        );
    }

    #[test]
    fn resolve_keys_path2_5_mk_pool_brute_resolves_unkeyed_disc() {
        // The Dunkirk case: this disc's own hash/VID are NOT in keydb, but its
        // Media Key IS — filed under a sibling disc that shares its MKB. Path
        // 2.5 must km_verifies that MK against the MKB and resolve.
        use super::super::decrypt::aes_ecb_encrypt as enc;

        let km = [0x11u8; 16];
        let vid = [0x22u8; 16];

        // MKB: 0x10 type/version + 0x86 verify record whose mk_dv decrypts under
        // km to the AACS verify magic, so km_verifies(mkb, km) == true.
        let mut vd = [0u8; 16];
        vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        let mk_dv = enc(&km, &vd);
        let mut mkb = mkb_record(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x4D]);
        mkb.extend_from_slice(&mkb_record(0x86, &mk_dv));
        assert!(
            probe::km_verifies(&mkb, &km),
            "fixture: km must verify the MKB"
        );

        // This disc's inf (its hash will NOT be in keydb).
        let uk_ro = minimal_unit_key_ro();

        // keydb: a SIBLING disc carries our km, keyed by the sibling's own
        // hash + VID (neither matches THIS disc) — so only the MK-pool brute
        // (km_verifies) can find it.
        let mut keydb = KeyDb::empty();
        keydb.disc_entries.insert(
            "0xsibling".to_string(),
            DiscEntry {
                disc_hash: "0xsibling".to_string(),
                title: "sibling".to_string(),
                media_key: Some(km),
                disc_id: Some([0x99u8; 16]),
                vuk: None,
                unit_keys: Vec::new(),
            },
        );

        let providers: &[&dyn super::super::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: Some(&mkb),
        };
        let resolved = resolve_keys_v1(&ctx)
            .expect("MK-pool brute (path 2.5) must resolve a disc whose MK is in keydb");
        assert_eq!(
            resolved.key_source, 3,
            "MK-pool brute is the KEYDB-derived class"
        );
        assert_eq!(
            resolved.vuk,
            Some(derive_vuk(&km, &vid)),
            "VUK must derive from the verified Km + this disc's VID"
        );
    }

    #[test]
    fn test_content_cert_parse() {
        // AACS 1.0 cert
        let mut data = vec![0u8; 16];
        data[0] = 0x00; // AACS 1.0
        data[1] = 0x00; // no bus encryption
        let cc = parse_content_cert(&data).unwrap();
        assert_eq!(cc.version, AacsVersion::V10);
        assert!(!cc.bus_encryption);

        // AACS 2.0 with bus encryption
        data[0] = 0x01; // AACS 2.0
        data[1] = 0x01; // bus encryption enabled
        let cc = parse_content_cert(&data).unwrap();
        assert_eq!(cc.version, AacsVersion::V20);
        assert!(cc.bus_encryption);
    }
}
