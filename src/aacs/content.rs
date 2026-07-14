//! AACS content decryption — aligned-unit / bus decryption and TS verification.
//! The low-level AES primitives it uses live in [`super::crypto`].

use aes::Aes128;
use aes::cipher::{BlockDecrypt, KeyInit, generic_array::GenericArray};

use super::crypto::{aes_cbc_decrypt, aes_ecb_encrypt};
// Available at module scope for this module's test fixtures (they reference
// `super::AACS_IV` when building CBC ciphertext directly); test-only.
#[cfg(test)]
use super::crypto::AACS_IV;

// ── AACS constants ──────────────────────────────────────────────────────────

/// Size of an AACS aligned unit (3 × 2048-byte sectors). [BD] §3.10.1.
pub const ALIGNED_UNIT_LEN: usize = 6144;

/// An AACS aligned unit spans this many 2048-byte sectors (3).
pub const ALIGNED_UNIT_SECTORS: u32 = (ALIGNED_UNIT_LEN / SECTOR_BYTES) as u32;

/// Whether `lba` sits on an AACS aligned-unit boundary, measured **relative to
/// the encrypted region's base LBA** (`unit_base` = the clip/extent `start_lba`,
/// NOT absolute disc LBA 0).
///
/// AACS aligned units (6144 B / 3 sectors) are anchored at the start of each
/// clip's encrypted region, so a read must begin a whole number of units past
/// that base for `decrypt_sectors` (which anchors units at buffer offset 0) to
/// align the CBC correctly. This is the SINGLE source of truth for the test —
/// the decrypt-on-read gate, the inline and highway mux read paths, and the
/// key-validation sample reader all key off this, never absolute `lba % 3`. A
/// disc whose clip `start_lba` is not itself 3-aligned would otherwise mis-gate
/// (reject readable units, then report "Decryption failed") on exactly the
/// titles whose clips land off a 3-boundary.
///
/// `lba` is always `>= unit_base` by contract (a read never begins before the
/// extent base it is measured against). `saturating_sub` makes the `lba <
/// unit_base` case well-defined anyway — it clamps the offset to 0, which is a
/// unit boundary — rather than the latent `wrapping_sub` trap where an
/// underflow wraps to ~2^32 and, because `2^32 ≡ 1 (mod 3)`, mis-reports the
/// alignment (e.g. `lba == unit_base - 1` would falsely read as aligned).
pub fn is_unit_aligned(lba: u32, unit_base: u32) -> bool {
    lba.saturating_sub(unit_base) % ALIGNED_UNIT_SECTORS == 0
}

use crate::consts::SECTOR_BYTES;

use crate::consts::BD_SOURCE_PACKET_BYTES;

/// TS sync byte.
const TS_SYNC: u8 = 0x47;

// ── Content decryption ──────────────────────────────────────────────────────

/// True if a 6144-byte aligned unit's MPEG-TS sync structure is DESTROYED — it
/// lacks the `0x47` sync bytes a clear BD-TS unit carries at offsets 4, 196,
/// 388, … (one per 192-byte source packet).
///
/// This is a pure BYTE heuristic; on its own it does NOT mean "encrypted". A
/// destroyed sync structure can be AACS ciphertext, uncorrected-ECC garbage, OR
/// data that was never MPEG-TS at all (UDF filesystem / nav) — those are
/// byte-indistinguishable. So this answers only *"does this unit look like valid
/// clear TS, or not"*, nothing about encryption.
///
/// The "is this unit AACS-encrypted (and must decrypt)?" decision is COMPOSED by
/// the caller, because it needs context this function lacks:
/// `inside an m2ts content extent` AND `ts_sync_destroyed` AND `no key decrypts`
/// (see [`crate::decrypt::decrypt_sectors_in_content`] and
/// [`crate::Disc::encrypted_content_ranges`]). Inside known content this
/// primitive separates an encrypted/garbled unit (destroyed) from a clear
/// segment (intact); OUTSIDE content it is meaningless — feeding it filesystem
/// bytes is what produced the first-2 GB false-positive this split fixes.
///
/// Flag-independent: it does NOT read the TP_extra copy-control bits (byte 0) or
/// the TS scrambling-control bits (byte 7) — AACS sets neither reliably.
pub fn ts_sync_destroyed(unit: &[u8]) -> bool {
    unit.len() >= ALIGNED_UNIT_LEN && !ts_syncs_intact(unit)
}

/// The AUTHORITATIVE AACS "is this aligned unit encrypted?" signal — the Copy
/// Permission Indicator (CPI) in the top 2 bits of byte 0. [BD] §3.10.2. Byte 0 is the first
/// byte of the first source packet's `TP_extra_header`, which AACS always leaves
/// in the clear (the first 16 bytes of every unit are the unencrypted SEED). So
/// this is readable WITHOUT a key:
///   * `(buf[0] & 0xC0) == 0` → CPI clear → the unit is plaintext; pass through.
///   * non-zero → bytes `16..6144` are AES-CBC encrypted; decrypt.
///
/// This is exactly the spec CPI test (`buf[0] & 0xc0 == 0` means clear)
/// and is the spec-correct replacement for the [`ts_sync_destroyed`] byte
/// heuristic. CRITICAL: it is only meaningful when `unit` is read at the correct
/// clip-FILE-anchored boundary — byte 0 must be the real unit start. A
/// disc-absolute / mis-aligned read makes byte 0 arbitrary mid-stream data, so
/// the CPI bits are meaningless (which is precisely why per-unit verify must run
/// clip-anchored, not in the whole-disc sweep).
pub fn aacs_unit_encrypted(unit: &[u8]) -> bool {
    unit.len() >= ALIGNED_UNIT_LEN && (unit[0] & 0xC0) != 0
}

/// True when an aligned unit is flagged encrypted (CPI set) AND still looks
/// scrambled (TS syncs not yet restored) — i.e. genuine encrypted content that
/// has NOT been decrypted yet.
///
/// [`aacs_unit_encrypted`] is the authoritative spec gate, but the CPI bits live
/// in the plaintext header (bytes `0..16`) which decryption never rewrites, so a
/// successfully decrypted unit still reports CPI-set. Buffer-iterating sites that
/// may run twice over the same `buf` (the post-fetch re-decrypt, sample
/// collection, failure diagnosis) need an IDEMPOTENT "does this still need work?"
/// test, so they compose CPI with the sync-restored check: once a unit decrypts,
/// its syncs come back and it drops out. Single-shot callers that always operate
/// on fresh ciphertext (`decrypt_unit`) gate on [`aacs_unit_encrypted`] alone.
///
/// Like CPI itself this is only meaningful at the clip-FILE-anchored boundary.
pub fn aacs_unit_needs_decrypt(unit: &[u8]) -> bool {
    aacs_unit_encrypted(unit) && ts_sync_destroyed(unit)
}

/// The one canonical "did a key OPEN this unit?" signal — a padding-aware,
/// defect-TOLERANT structural check on the POST-decrypt bytes.
///
/// ARCHITECTURE (why this is the only TS-sync test the read/decrypt path may
/// use): TS-sync presence is a *muxer* concern, not a decryption verdict. The
/// read/decrypt path is not allowed to reject or conceal a unit just because a
/// packet isn't conformant MPEG-TS — a non-conforming packet inside an otherwise
/// perfectly-decrypted unit is an authoring/pressing defect (or an AACS 2.1
/// forensic-variant frame), which the demuxer drops on sync-loss and resyncs
/// past. The ONLY thing the decrypt path legitimately needs from TS structure is
/// KEY SELECTION: "did this candidate key turn ciphertext back into MPEG-TS at
/// all?" — used to pick among held keys (multi-CPS-unit discs) and to detect a
/// genuinely-missing key. That is a coarse, all-or-nothing question, and this is
/// its answer.
///
/// Verdict: the key opened the unit iff a SUPERMAJORITY (>= 75%) of the
/// non-padding content packets carry their `0x47` TS sync. Rationale:
///   * WRONG key → AES output is uniform random → each content packet carries
///     `0x47` at offset 4 with probability 1/256 → ~0 synced. Reaching 75% by
///     chance is cryptographically impossible (e.g. 17-of-22 ≈ 256^-17). So a
///     wrong key can NEVER pass — no silent-corruption hole.
///   * RIGHT key → every real content packet decrypts to clear TS. A handful of
///     authored-bad packets (pressing defects / variant frames) legitimately
///     lack `0x47`, but they are a small minority and MUST NOT reject the unit —
///     the decrypted bytes (defects and all) pass through verbatim; the muxer
///     handles the bad packets. This is the whole point: correctness of a
///     *packet* is not a decryption verdict.
///
/// Padding-aware: a 192-byte packet whose 188-byte payload is all-zero is source
/// padding (or a NULL packet) and is excluded from the count, so a legitimate
/// content-fragment TAIL (a few real packets + source-zero padding) is judged on
/// its real packets only. A full content unit reduces to "nearly all 32 synced".
pub fn unit_content_decrypted(unit: &[u8]) -> bool {
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    let limit = ALIGNED_UNIT_LEN.min(unit.len());
    let mut content = 0usize;
    let mut synced = 0usize;
    let mut off = 0;
    while off + PKT <= limit {
        // All-zero payload → padding / NULL packet → excluded from the verdict.
        let payload = &unit[off + 4..off + PKT];
        if !payload.iter().all(|&b| b == 0) {
            content += 1;
            if unit[off + 4] == TS_SYNC {
                synced += 1;
            }
        }
        off += PKT;
    }
    // No content packets (all padding) → trivially "opened". Otherwise require a
    // >=75% supermajority of content packets restored — the wrong-key-proof gate.
    content == 0 || synced * 4 >= content * 3
}

/// "Is this aligned unit STILL genuine ciphertext (no held key opened it)?" — the
/// conceal-path twin of [`unit_content_decrypted`], run on the POST-decrypt
/// bytes. True iff the unit is flagged encrypted (CPI set) AND no key opened it
/// (below the supermajority-sync gate). A unit the right key opened — even one
/// carrying a few defective packets — is NOT ciphertext and is never concealed;
/// its bytes belong to the muxer.
pub fn aacs_unit_still_ciphertext(unit: &[u8]) -> bool {
    aacs_unit_encrypted(unit) && !unit_content_decrypted(unit)
}

/// Overwrite an aligned unit (6144 bytes) IN PLACE with valid NULL MPEG-TS
/// source packets — the [A2] mux loss-concealment fill for a content unit that
/// genuinely would not decrypt.
///
/// Zero-filling such a unit is wrong at the TS layer: a run of `0x00` bytes
/// carries no `0x47` sync, so the demuxer loses packet framing and can mis-parse
/// the *next* unit if a stray `0x47` appears mid-zero. Instead we lay down 32
/// well-formed BD source packets, each a TS null packet (PID `0x1FFF`) carrying
/// an adaptation-field **discontinuity_indicator**:
///
/// ```text
///   [4-byte TP_extra_header = 0][47 1F FF 20  B7 80  + 182 bytes 0xFF stuffing]
///                                ^sync ^PID  ^AF-only ^af_len=183 ^disc_indicator
/// ```
///
/// The demuxer stays byte-synced on the 192-byte stride, and because PID
/// `0x1FFF` matches no elementary stream every null packet is silently dropped.
/// The discontinuity_indicator is the B1 loss SIGNAL: `mux::ts` recognises a
/// `0x1FFF` packet with that bit set as a concealed gap and forces a discontinuity
/// on every tracked PID's next PES (the codec consumer then drops forward to the
/// next keyframe). This is CC-INDEPENDENT — unlike the real PID's continuity
/// counter it survives a loss that is an exact multiple of 16 packets, or a loss
/// at a PID's very start. NEVER emits ciphertext; lossless framing, not fabricated
/// content.
pub fn fill_null_ts_unit(unit: &mut [u8]) {
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    let mut off = 0;
    while off + PKT <= unit.len() {
        // TP_extra_header (arrival timestamp / copy-control) — zero is fine; the
        // demuxer never reads it for a PID it does not track.
        unit[off..off + 4].fill(0);
        // 188-byte TS null packet: sync, PID 0x1FFF (no PUSI/TEI).
        unit[off + 4] = TS_SYNC; // 0x47
        unit[off + 5] = 0x1F; // PID high (top 5 bits of 0x1FFF, flags clear)
        unit[off + 6] = 0xFF; // PID low
        // adaptation_field_control = 0b10 (AF only, no payload), CC = 0.
        unit[off + 7] = 0x20;
        // adaptation_field_length = 183: the AF (its flags byte + 182 stuffing)
        // fills the rest of the 188-byte packet.
        unit[off + 8] = 0xB7;
        // AF flags: discontinuity_indicator (0x80) — the concealed-gap signal.
        unit[off + 9] = 0x80;
        // Stuffing: 0xFF is the conventional adaptation-field fill.
        unit[off + 10..off + PKT].fill(0xFF);
        off += PKT;
    }
}

/// Count the MPEG-TS sync bytes (`0x47`) present at the BD-TS packet stride
/// (offset 4 and every 192 bytes after — 4-byte TP_extra_header + 188-byte
/// TS packet). A clear or correctly-decrypted m2ts unit shows ~one per
/// packet; an encrypted unit, or a non-content unit decrypted under a key
/// that doesn't apply, shows ~none.
pub fn ts_sync_count(unit: &[u8]) -> usize {
    let mut count = 0;
    let mut offset = 4;
    while offset < unit.len() {
        if unit[offset] == TS_SYNC {
            count += 1;
        }
        offset += BD_SOURCE_PACKET_BYTES;
    }
    count
}

/// Number of BD-TS packets in the unit — the maximum possible sync count.
pub fn ts_packet_total(unit: &[u8]) -> usize {
    // One sync byte per 192-byte BD-TS packet (at offset 4 of each). The old
    // `(len - 4) / BD_SOURCE_PACKET_BYTES + 1` over-counted by one for lengths of the
    // form `4 + k·192`.
    unit.len() / BD_SOURCE_PACKET_BYTES
}

fn ts_syncs_intact(unit: &[u8]) -> bool {
    ts_sync_count(unit) > ts_packet_total(unit) / 2
}

/// STRICT, standards-correct "is this a clean MPEG-TS aligned unit?" check —
/// the standards-correct all-32-sync verify: EVERY one of the 32 BD source
/// packets (192-byte stride) must carry its TS sync `0x47` at offset 4; the first
/// miss fails. This is the authoritative gate for the POST-READ verify stage,
/// independent of (and not coupled to) `decrypt_unit`.
///
/// It is deliberately stricter than the majority-vote `ts_syncs_intact`
/// scramble *heuristic*: a wrong-key decrypt that coincidentally restores >16
/// syncs passes the majority test and would silently corrupt content, but fails
/// here. Non-mutating — purely a verdict; it neither decrypts nor clears the CPI
/// bits (those stay the concern of the unchanged `decrypt_unit`).
pub fn unit_is_clean_ts(unit: &[u8]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    let mut i = 0;
    while i < ALIGNED_UNIT_LEN {
        if unit[i + 4] != TS_SYNC {
            return false;
        }
        i += BD_SOURCE_PACKET_BYTES;
    }
    true
}

/// Structural "is this a clean MPEG-2 Program Stream aligned unit?" check — the
/// PS-container analogue of [`unit_is_clean_ts`], for AACS content carried as
/// program stream (HD-DVD `.evo`): every 2048-byte pack must begin with the
/// pack_start_code `00 00 01 BA`. A 6144-byte aligned unit spans three packs.
///
/// UNVALIDATED against real HD-DVD media. It assumes (a) HD-DVD uses the
/// standard AACS 6144-byte unit, (b) `.evo` clips are 2048-pack-aligned so unit
/// boundaries fall on pack starts, and (c) byte 0 of the unit is the pack start
/// — i.e. where the AACS seed and CPI indicator sit for PS content is the same
/// as BD-TS. Each of these must be confirmed against a real HD-DVD disc before
/// the `.evo` path is turned on (see `disc::verify::ContainerKind`). It exists
/// now only so the verify gate is structurally ready for that wiring.
pub fn unit_is_clean_ps(unit: &[u8]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    const PACK_START: [u8; 4] = [0x00, 0x00, 0x01, 0xBA];
    let mut o = 0;
    while o < ALIGNED_UNIT_LEN {
        if unit[o..o + 4] != PACK_START {
            return false;
        }
        o += SECTOR_BYTES; // one MPEG-2 PS pack per 2048-byte sector
    }
    true
}

/// Decrypt one AACS aligned unit (6144 bytes) in-place.
/// Returns true if the unit is now clear MPEG-TS: either it was already
/// unscrambled (returned untouched, no key used) or it was decrypted and
/// verified by its TS sync bytes. Returns false only when the unit was
/// scrambled and this key failed verification.
///
/// Algorithm:
/// 1. AES-128-ECB encrypt first 16 bytes with unit_key → derived
/// 2. XOR derived with original 16 bytes → unit_decrypt_key
/// 3. AES-128-CBC decrypt bytes 16..6143 with unit_decrypt_key and AACS IV
///
/// Decryption restores the TS sync bytes, so the unit reads as clear afterward;
/// there is no flag to clear.
pub fn decrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    if !aacs_unit_encrypted(unit) {
        return true; // CPI flag clear → plaintext, pass through untouched
    }

    // PADDING-AWARE acceptance. The question this answers is "did we read good,
    // decryptable data?" — NOT "are all 32 packets present". A content fragment
    // can END mid-unit, with the disc zero-padding the rest of the aligned unit
    // to the next fragment. Such a tail unit is `[real encrypted packets][source
    // zeros]`: the real packets decrypt perfectly, but the strict all-32
    // `unit_is_clean_ts` would reject the whole unit over the padding tail and
    // discard real video. AES ciphertext is high-entropy, so a SOURCE-zero packet
    // (all 192 bytes zero before decrypt) can only be padding, never content.
    //
    // So: a packet whose SOURCE bytes are all zero is padding — excluded from the
    // verify and emitted as clean zeros. Every other (content) packet must
    // restore its TS sync. A full content unit has no zero-source packets, so this
    // is byte-identical to the old all-32 check (no regression, no wrong-key
    // hole). The discriminator between a legitimate short tail and a genuine
    // misread is exactly this: a misread leaves the failing packets' SOURCE
    // non-zero (real ciphertext that won't decrypt) → still rejected.
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    let npkt = ALIGNED_UNIT_LEN / PKT;
    let mut pad = [false; ALIGNED_UNIT_LEN / PKT];
    for (p, slot) in pad.iter_mut().enumerate().take(npkt) {
        let off = p * PKT;
        *slot = unit[off..off + PKT].iter().all(|&b| b == 0);
    }

    // Save original first 16 bytes (the plaintext seed / header) and derive the
    // per-unit Block Key (identical to `decrypt_unit_checked`).
    // Block Key = AES-128E(Kcu, seed) ⊕ seed  ([BD] §3.10.1 Fig 3-8, two-node
    // construction: encrypt the clear seed under the CPS Unit Key, then XOR the
    // seed back in — the trailing ⊕seed is load-bearing).
    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);
    let derived = aes_ecb_encrypt(unit_key, &header);
    let mut decrypt_key = [0u8; 16];
    for i in 0..16 {
        decrypt_key[i] = derived[i] ^ header[i];
    }
    // Final 6128 bytes of the aligned unit under the Block Key; first 16 = clear seed. [BD] §3.10.1.
    aes_cbc_decrypt(&decrypt_key, &mut unit[16..ALIGNED_UNIT_LEN]);

    // Restore source-zero padding packets to zero (their decrypted bytes are
    // garbage from AES-decrypting zeros, but the source WAS zero, so writing the
    // true source back is faithful — not concealment — and gives the demux a tidy
    // gap instead of AES noise). Content packets are left EXACTLY as decrypted:
    // the read/decrypt path never rewrites content, so an authored-bad packet
    // (no `0x47`) passes through verbatim for the muxer to drop.
    for (p, &is_pad) in pad.iter().enumerate().take(npkt) {
        if is_pad {
            let off = p * PKT;
            for b in unit[off..off + PKT].iter_mut() {
                *b = 0;
            }
        }
    }
    // KEY-SELECTION verdict only: did this key OPEN the unit (restore the TS
    // structure of a supermajority of content packets)? A wrong key can't; the
    // right key can even when a few content packets are authored-bad. This is NOT
    // a per-packet conformance gate — that belongs to the muxer.
    unit_content_decrypted(unit)
}

/// Decrypt an AACS aligned unit in place, accepting the key only when `accept`
/// passes on the decrypted bytes. The AACS crypto is container-agnostic; the
/// post-decrypt acceptance is the only format-specific part — so this is the
/// extension seam for non-TS containers. [`decrypt_unit`] is this with the BD-TS
/// check ([`unit_is_clean_ts`]); HD-DVD PS content would pass
/// [`unit_is_clean_ps`] instead. A CPI-clear unit is plaintext and passes
/// through untouched (no key consumed), exactly as before.
pub fn decrypt_unit_checked(
    unit: &mut [u8],
    unit_key: &[u8; 16],
    accept: fn(&[u8]) -> bool,
) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    if !aacs_unit_encrypted(unit) {
        return true; // CPI flag clear → plaintext, pass through untouched
    }

    // Save original first 16 bytes (they're the plaintext seed / header).
    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);

    // Step 1: Encrypt header with unit key to derive per-unit key.
    let derived = aes_ecb_encrypt(unit_key, &header);

    // Step 2: XOR to get the actual decryption key.
    let mut decrypt_key = [0u8; 16];
    for i in 0..16 {
        decrypt_key[i] = derived[i] ^ header[i];
    }

    // Step 3: Decrypt bytes 16..6143 with AES-CBC.
    aes_cbc_decrypt(&decrypt_key, &mut unit[16..ALIGNED_UNIT_LEN]);

    // Accept the key only if the decrypted unit passes the container's strict
    // structural check. A wrong key that coincidentally restores a majority of
    // markers is rejected here, not silently accepted.
    accept(unit)
}

/// Fast, NON-MUTATING unit-key validation for the brute-force key search.
///
/// `decrypt_unit` pays a full 6128-byte (383-block) CBC decrypt before
/// `verify_ts` can reject a wrong key — but in a brute scan ~every candidate is
/// wrong. In CBC the plaintext of block *i* is `AES_dec(C_i) XOR C_{i-1}`, so
/// the FIRST restored TS sync byte (payload offset 196, which lands in CBC
/// block 11 of the `unit[16..]` region) can be recovered with a SINGLE block
/// decrypt instead of 383. A wrong key fails this 1-byte gate ~255/256 of the
/// time for the cost of one AES block; the rare survivor is then confirmed with
/// the full [`decrypt_unit`], so the set of accepted keys is bit-for-bit
/// identical to the slow path.
///
/// The caller MUST pass an aligned, already-[`ts_sync_destroyed`] unit
/// (`unit.len() >= ALIGNED_UNIT_LEN`). The brute pre-filters its units, so the
/// per-candidate scramble re-scan is intentionally skipped here.
///
/// NOTE: this is a search accelerator — it never writes the input and never
/// participates in the content decrypt path. Aggregate correctness (does a key
/// validate against *any* of the disc's units) is preserved because a true key
/// restores offset-196 on every standard BD-TS unit.
pub fn unit_key_validates(unit: &[u8], unit_key: &[u8; 16]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    // Per-unit decrypt key: AES-ECB-encrypt the 16-byte plaintext header with
    // the unit key, XOR with the header (same derivation as `decrypt_unit`).
    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);
    let derived = aes_ecb_encrypt(unit_key, &header);
    let mut decrypt_key = [0u8; 16];
    for i in 0..16 {
        decrypt_key[i] = derived[i] ^ header[i];
    }

    // Cheap gate: recover ONLY payload byte 196 (the 2nd BD-TS packet's sync).
    // The CBC region is `unit[16..]`; payload offset 196 → region offset 180 =
    // block 11, byte 4. P[11] = AES_dec(C[11]) XOR C[10]; C[10] is raw
    // ciphertext (no decrypt needed). Constant offsets for the fixed 6144 unit.
    const SYNC_PAYLOAD_OFF: usize = 196;
    let region_off = SYNC_PAYLOAD_OFF - 16; // 180
    let blk = region_off / 16; // 11
    let byte = region_off % 16; // 4
    let c11 = 16 + blk * 16; // absolute offset of C[11] in `unit` (=192)
    let cipher = Aes128::new(GenericArray::from_slice(&decrypt_key));
    let mut b = GenericArray::clone_from_slice(&unit[c11..c11 + 16]);
    cipher.decrypt_block(&mut b);
    let prev = unit[c11 - 16 + byte]; // C[10] byte (region block 10)
    if b[byte] ^ prev != TS_SYNC {
        return false;
    }

    // Survivor (~1/256 of candidates): confirm with the authoritative full
    // decrypt + verify, so the verdict matches `decrypt_unit` exactly.
    let mut full = [0u8; ALIGNED_UNIT_LEN];
    full.copy_from_slice(&unit[..ALIGNED_UNIT_LEN]);
    decrypt_unit(&mut full, unit_key)
}

/// Outcome of [`decrypt_unit_try_keys`].
///
/// Distinguishes "the unit was already clear, no key was consumed" from "key
/// at index `i` decrypted it" — the bare `Option<usize>` form conflated the two
/// (a clear unit reported `Some(0)`, indistinguishable from key index 0, and
/// possibly out of range when `unit_keys` is empty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitKeyResult {
    /// The unit was not scrambled; it was left untouched and no key was used.
    AlreadyClear,
    /// The unit was decrypted in place by `unit_keys[index]`.
    DecryptedWith(usize),
}

/// Decrypt one aligned unit trying multiple unit keys.
///
/// Returns [`UnitKeyResult::AlreadyClear`] if the unit was not scrambled (no key
/// consumed), [`UnitKeyResult::DecryptedWith(i)`] if key `i` decrypted it, or
/// `None` if no key worked (the unit is restored to its original bytes).
pub fn decrypt_unit_try_keys(unit: &mut [u8], unit_keys: &[[u8; 16]]) -> Option<UnitKeyResult> {
    if !aacs_unit_encrypted(unit) {
        return Some(UnitKeyResult::AlreadyClear);
    }

    // Save original for retry. Stack-backed buffer — no heap allocation, and the
    // restore-on-failure contract holds uniformly regardless of key count.
    let mut original = [0u8; ALIGNED_UNIT_LEN];
    original.copy_from_slice(&unit[..ALIGNED_UNIT_LEN]);

    for (i, key) in unit_keys.iter().enumerate() {
        unit[..ALIGNED_UNIT_LEN].copy_from_slice(&original);
        if decrypt_unit(unit, key) {
            return Some(UnitKeyResult::DecryptedWith(i));
        }
    }

    // Restore original on failure
    unit[..ALIGNED_UNIT_LEN].copy_from_slice(&original);
    None
}

/// Remove bus encryption from an aligned unit (AACS 2.0 / UHD).
/// Bus encryption uses read_data_key, decrypting bytes 16..2047 of each 2048-byte sector.
pub fn decrypt_bus(unit: &mut [u8], read_data_key: &[u8; 16]) {
    for sector_start in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_BYTES) {
        if sector_start + SECTOR_BYTES > unit.len() {
            break;
        }
        // First 16 bytes of each sector are plaintext
        aes_cbc_decrypt(
            read_data_key,
            &mut unit[sector_start + 16..sector_start + SECTOR_BYTES],
        );
    }
}

/// Full decrypt of an aligned unit: bus decrypt (if needed) then AACS decrypt.
pub fn decrypt_unit_full(
    unit: &mut [u8],
    unit_key: &[u8; 16],
    read_data_key: Option<&[u8; 16]>,
) -> bool {
    if !ts_sync_destroyed(unit) {
        return true;
    }
    if let Some(rdk) = read_data_key {
        decrypt_bus(unit, rdk);
    }
    decrypt_unit(unit, unit_key)
}

#[cfg(test)]
mod tests {
    use super::super::crypto::aes_ecb_decrypt;
    use super::*;
    use aes::cipher::BlockEncrypt; // test fixtures build ciphertext directly

    #[test]
    fn test_aes_ecb_roundtrip() {
        let key = [
            0x15u8, 0x66, 0x5F, 0x98, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
            0x0B, 0x0C,
        ];
        let plain = [0x41u8; 16];
        let enc = aes_ecb_encrypt(&key, &plain);
        let dec = aes_ecb_decrypt(&key, &enc);
        assert_eq!(dec, plain);
    }

    #[test]
    fn is_unit_aligned_relative_to_base() {
        // Aligned at the base and every 3 sectors above it; misaligned between.
        assert!(is_unit_aligned(100, 100), "base itself is aligned");
        assert!(is_unit_aligned(103, 100), "one unit past base is aligned");
        assert!(is_unit_aligned(106, 100));
        assert!(!is_unit_aligned(101, 100));
        assert!(!is_unit_aligned(102, 100));
        // Non-3-aligned base: alignment is RELATIVE to the base, not absolute.
        assert!(is_unit_aligned(101, 101), "non-3-aligned base is aligned");
        assert!(is_unit_aligned(104, 101));
        assert!(!is_unit_aligned(102, 101));
    }

    #[test]
    fn is_unit_aligned_lba_below_base_is_well_defined() {
        // Latent-trap contract (rc.5.2 audit #5): a read never starts before its
        // extent base, but if `lba < unit_base` the result must be well-defined,
        // NOT the `wrapping_sub` underflow that — because 2^32 ≡ 1 (mod 3) —
        // would falsely report alignment. `saturating_sub` clamps to offset 0,
        // which is a unit boundary, so any `lba <= unit_base` reads as aligned.
        assert!(
            is_unit_aligned(99, 100),
            "lba just below base must not wrap"
        );
        assert!(is_unit_aligned(98, 100));
        assert!(is_unit_aligned(0, 100));
        // The specific wrapping_sub trap value: unit_base - 1. With wrapping_sub
        // this is 0xFFFF_FFFF % 3 == 0 → falsely "aligned" by underflow; with
        // saturating_sub it is genuinely 0 → aligned, for the right reason.
        assert!(is_unit_aligned(u32::MAX, u32::MAX)); // base == lba, trivially aligned
        assert!(
            is_unit_aligned(0, u32::MAX),
            "max base, lba 0 must saturate to 0"
        );
    }

    #[test]
    fn test_decrypt_unit_unencrypted() {
        // A clear unit (TS syncs intact) is not scrambled → passes through.
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            unit[off] = TS_SYNC;
            off += BD_SOURCE_PACKET_BYTES;
        }
        let key = [0u8; 16];
        assert!(!ts_sync_destroyed(&unit));
        assert!(decrypt_unit(&mut unit, &key));
    }

    #[test]
    fn ts_packet_total_no_off_by_one() {
        // The maximum sync count is exactly the number of stride
        // positions the counting loop visits (offset 4, 196, ...), i.e.
        // len / 192, NOT (len - 4) / 192 + 1. For the 6144-byte aligned unit
        // the loop checks offsets 4..=5956 → 32 positions.
        let unit = vec![0u8; ALIGNED_UNIT_LEN];
        assert_eq!(ts_packet_total(&unit), 32);
        // Confirm the loop visits exactly that many stride positions.
        let visited = (4..ALIGNED_UNIT_LEN)
            .step_by(BD_SOURCE_PACKET_BYTES)
            .count();
        assert_eq!(visited, ts_packet_total(&unit));
    }

    #[test]
    fn scramble_detection_at_16_32_boundary() {
        // With 32 stride positions the majority threshold is
        // total/2 = 16. A unit with EXACTLY half its syncs intact (16) must
        // NOT be over-counted into the "scrambled" bucket by an inflated
        // total: 16 > 16 is false → not-intact → scrambled. 17 intact → clear.
        // The fix is that `total` is 32 (not 33), so the boundary sits cleanly
        // at the real midpoint.
        let set_syncs = |n: usize| {
            let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
            let mut off = 4;
            let mut placed = 0;
            while off < ALIGNED_UNIT_LEN && placed < n {
                unit[off] = TS_SYNC;
                off += BD_SOURCE_PACKET_BYTES;
                placed += 1;
            }
            unit
        };

        assert_eq!(ts_sync_count(&set_syncs(16)), 16);
        assert_eq!(ts_sync_count(&set_syncs(17)), 17);

        // Exactly half intact → classified scrambled (16 > 16 is false).
        assert!(ts_sync_destroyed(&set_syncs(16)));
        // One past half → classified clear.
        assert!(!ts_sync_destroyed(&set_syncs(17)));
    }

    #[test]
    fn scramble_detection_extremes() {
        // Detection semantics for the clear-cut cases must be preserved:
        // a fully-clear unit (all 32 syncs) is NOT scrambled; a unit with no
        // syncs (fully scrambled body) IS scrambled.
        let mut clear = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            clear[off] = TS_SYNC;
            off += BD_SOURCE_PACKET_BYTES;
        }
        assert_eq!(ts_sync_count(&clear), 32);
        assert!(
            !ts_sync_destroyed(&clear),
            "fully-clear unit → not scrambled"
        );

        let scrambled = vec![0u8; ALIGNED_UNIT_LEN];
        assert_eq!(ts_sync_count(&scrambled), 0);
        assert!(ts_sync_destroyed(&scrambled), "no syncs → scrambled");
    }

    #[test]
    fn test_aes_cbc_roundtrip() {
        let key = [
            0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let original = vec![0x42u8; 128]; // 8 blocks
        let mut data = original.clone();

        // Encrypt with CBC manually (forward direction)
        fn aes_cbc_encrypt(key: &[u8; 16], data: &mut [u8]) {
            let cipher = Aes128::new(GenericArray::from_slice(key));
            let mut prev = super::AACS_IV;
            let num_blocks = data.len() / 16;
            for i in 0..num_blocks {
                let offset = i * 16;
                for j in 0..16 {
                    data[offset + j] ^= prev[j];
                }
                let mut block = GenericArray::clone_from_slice(&data[offset..offset + 16]);
                cipher.encrypt_block(&mut block);
                data[offset..offset + 16].copy_from_slice(&block);
                prev.copy_from_slice(&data[offset..offset + 16]);
            }
        }

        aes_cbc_encrypt(&key, &mut data);
        assert_ne!(data, original); // should be different after encrypt

        super::aes_cbc_decrypt(&key, &mut data);
        assert_eq!(data, original); // should match after roundtrip
    }

    #[test]
    fn test_decrypt_unit_synthetic() {
        // Build a fake 6144-byte aligned unit with known TS sync pattern,
        // encrypt it with the AACS algorithm, then decrypt and verify.
        let unit_key = [0xAAu8; 16];

        // Build plaintext unit with TS sync bytes every 192 bytes starting at offset 4
        let mut plain = vec![0u8; ALIGNED_UNIT_LEN];
        let mut offset = 4;
        while offset < ALIGNED_UNIT_LEN {
            plain[offset] = TS_SYNC;
            offset += BD_SOURCE_PACKET_BYTES;
        }
        // Flag the unit encrypted via the CPI bits (byte 0) — the authoritative
        // gate `decrypt_unit` now consults. Set before key derivation so the
        // recovered plaintext header matches.
        plain[0] |= 0xC0;

        // Now encrypt bytes 16..6143 using the AACS algorithm (reverse of decrypt)
        let header: [u8; 16] = plain[..16].try_into().unwrap();
        let derived = aes_ecb_encrypt(&unit_key, &header);
        let mut encrypt_key = [0u8; 16];
        for i in 0..16 {
            encrypt_key[i] = derived[i] ^ header[i];
        }

        // CBC encrypt bytes 16..6143
        let cipher = Aes128::new(GenericArray::from_slice(&encrypt_key));
        let mut prev = AACS_IV;
        let num_blocks = (ALIGNED_UNIT_LEN - 16) / 16;
        for i in 0..num_blocks {
            let off = 16 + i * 16;
            for j in 0..16 {
                plain[off + j] ^= prev[j];
            }
            let mut block = GenericArray::clone_from_slice(&plain[off..off + 16]);
            cipher.encrypt_block(&mut block);
            plain[off..off + 16].copy_from_slice(&block);
            prev.copy_from_slice(&plain[off..off + 16]);
        }

        // Now plain contains encrypted data. Decrypt it.
        let mut unit = plain;
        assert!(ts_sync_destroyed(&unit));
        assert!(decrypt_unit(&mut unit, &unit_key));
        assert!(!ts_sync_destroyed(&unit)); // decrypted: TS syncs restored

        // Verify TS sync bytes
        let mut count = 0;
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            if unit[off] == TS_SYNC {
                count += 1;
            }
            off += BD_SOURCE_PACKET_BYTES;
        }
        // Assert against the single canonical packet count, not the old
        // `(len - 4) / 192 + 1` form that `ts_packet_total` corrected away from.
        assert_eq!(count, ts_packet_total(&unit));
    }

    // ── Helpers for the hardening tests below ──────────────────────────────

    /// Encrypt an aligned unit in place with the AACS unit-decrypt
    /// algorithm run in reverse, so [`decrypt_unit`] with the same
    /// `unit_key` recovers the plaintext. This is the exact inverse of
    /// the production decrypt: derive `decrypt_key = AES-ECB-E(unit_key,
    /// header) XOR header`, then CBC-encrypt bytes 16..6144 under the
    /// fixed AACS IV.
    fn aacs_encrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) {
        // Set the CPI bits (top 2 of byte 0) so the unit reads as encrypted under
        // `aacs_unit_encrypted` — done BEFORE key derivation so the plaintext
        // header the real decrypt recovers matches what we encrypt under.
        unit[0] |= 0xC0;
        let header: [u8; 16] = unit[..16].try_into().unwrap();
        let derived = aes_ecb_encrypt(unit_key, &header);
        let mut k = [0u8; 16];
        for i in 0..16 {
            k[i] = derived[i] ^ header[i];
        }
        let cipher = Aes128::new(GenericArray::from_slice(&k));
        let mut prev = AACS_IV;
        let num_blocks = (ALIGNED_UNIT_LEN - 16) / 16;
        for i in 0..num_blocks {
            let off = 16 + i * 16;
            for j in 0..16 {
                unit[off + j] ^= prev[j];
            }
            let mut block = GenericArray::clone_from_slice(&unit[off..off + 16]);
            cipher.encrypt_block(&mut block);
            unit[off..off + 16].copy_from_slice(&block);
            prev.copy_from_slice(&unit[off..off + 16]);
        }
    }

    /// Build a clear aligned unit with TS sync bytes at offset 4 + k*192.
    fn clear_unit() -> Vec<u8> {
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            unit[off] = TS_SYNC;
            off += BD_SOURCE_PACKET_BYTES;
        }
        unit
    }

    // ── Padding-aware IsDecryptable (fragment-tail recovery) ───────────────
    //
    // A content fragment can end mid-unit, with the disc zero-padding the rest
    // of the aligned unit to the next fragment (proven on Dunkirk: ~11 real
    // video packets + source-zero pad). `decrypt_unit` must accept such a unit
    // — its real packets decrypt; the source-zero tail is padding, not content
    // — while still REJECTING a unit whose undecryptable tail is non-zero (a
    // genuine misread / wrong key). The discriminator is the SOURCE bytes of the
    // failing packets: zero ⇒ padding (still decryptable), non-zero ⇒ bad data.

    /// Encrypt a full clear unit under `unit_key`, then overwrite the tail (from
    /// packet `keep` onward) with `fill`. `0x00` models disc fragment padding; a
    /// non-zero `fill` models a corrupt/misread tail.
    fn tail_filled_unit(unit_key: &[u8; 16], keep_pkts: usize, fill: u8) -> Vec<u8> {
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, unit_key);
        for b in unit[keep_pkts * BD_SOURCE_PACKET_BYTES..].iter_mut() {
            *b = fill;
        }
        unit
    }

    #[test]
    fn decryptable_full_content_unit_under_correct_key() {
        let key = [0x5Au8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &key);
        assert!(
            decrypt_unit(&mut unit, &key),
            "full clean unit is decryptable"
        );
        assert_eq!(ts_sync_count(&unit), 32, "all 32 syncs restored");
    }

    #[test]
    fn fragment_tail_with_source_zero_pad_is_decryptable() {
        // 11 real content packets, then source-zero padding (the Dunkirk shape).
        let key = [0x5Au8; 16];
        let mut unit = tail_filled_unit(&key, 11, 0x00);
        assert!(
            decrypt_unit(&mut unit, &key),
            "real prefix + source-zero pad IS decryptable"
        );
        for p in 0..11 {
            assert_eq!(
                unit[p * BD_SOURCE_PACKET_BYTES + 4],
                TS_SYNC,
                "content pkt {p} restored its sync"
            );
        }
        // Padding emitted as clean zeros, not decrypted garbage.
        for p in 11..32 {
            let off = p * BD_SOURCE_PACKET_BYTES;
            assert!(
                unit[off..off + BD_SOURCE_PACKET_BYTES]
                    .iter()
                    .all(|&b| b == 0),
                "padding pkt {p} zeroed"
            );
        }
    }

    #[test]
    fn fragment_tail_with_nonzero_garbage_is_not_decryptable() {
        // Same shape, but the tail is NON-zero — a genuine misread, not padding.
        let key = [0x5Au8; 16];
        let mut unit = tail_filled_unit(&key, 11, 0xC3);
        assert!(
            !decrypt_unit(&mut unit, &key),
            "real prefix + non-zero garbage tail is NOT decryptable (misread)"
        );
    }

    // ── Defect-tolerant "did a key OPEN this unit?" verdict ─────────────────
    //
    // The Bourne-UHD bug: a commercial disc carries the odd authored-bad TS
    // packet (a pressing/encoding defect, or an AACS 2.1 forensic-variant frame)
    // — one non-conforming packet inside an otherwise perfectly-decrypted 6144
    // unit. The OLD strict per-packet acceptance rejected the WHOLE unit over
    // that single packet, so the mux concealed ~all of it as NULL and 21/22 good
    // video packets were destroyed and tallied as loss ("false corruption").
    // The read/decrypt path must instead ACCEPT the unit (the key opened it) and
    // pass the defective packet through VERBATIM for the muxer to drop. TS-sync
    // conformance is a muxer concern, never a decryption verdict.

    /// Build a unit that decrypts to 32 content packets, with `defect_pkts`
    /// marked authored-bad (a non-`0x47` at the sync position + non-zero payload
    /// so they count as genuine content, not padding), encrypted under `key`.
    fn unit_with_defects(key: &[u8; 16], defect_pkts: &[usize]) -> Vec<u8> {
        let mut unit = clear_unit();
        for &p in defect_pkts {
            let off = p * BD_SOURCE_PACKET_BYTES;
            unit[off + 4] = 0x80; // NOT a TS sync
            unit[off + 5] = 0xAB; // non-zero payload => real content, not padding
        }
        aacs_encrypt_unit(&mut unit, key);
        unit
    }

    /// A POST-DECRYPT-looking unit: `content` non-padding packets, `synced` of
    /// them carrying `0x47`; the remaining packets are source-zero padding. CPI
    /// (encrypted flag) set iff `cpi`. Feeds the key-independent predicates.
    fn decrypted_shape(content: usize, synced: usize, cpi: bool) -> Vec<u8> {
        let mut u = vec![0u8; ALIGNED_UNIT_LEN];
        for p in 0..content {
            let off = p * BD_SOURCE_PACKET_BYTES;
            u[off + 5] = 0xAB; // non-zero payload => counted as content
            u[off + 4] = if p < synced { TS_SYNC } else { 0x80 };
        }
        if cpi {
            u[0] |= 0xC0;
        }
        u
    }

    #[test]
    fn single_authored_bad_packet_still_decrypts_and_passes_verbatim() {
        // 1 defective content packet in an otherwise-perfect unit (the real case).
        let key = [0x5Au8; 16];
        let mut unit = unit_with_defects(&key, &[17]);
        assert!(
            decrypt_unit(&mut unit, &key),
            "31/32 content packets synced -> the key OPENED the unit"
        );
        let off = 17 * BD_SOURCE_PACKET_BYTES;
        assert_eq!(
            unit[off + 4],
            0x80,
            "defect packet's bytes pass through VERBATIM (no null-fill, no zeroing)"
        );
        assert_eq!(unit[off + 5], 0xAB, "defect payload untouched");
        assert!(
            !aacs_unit_still_ciphertext(&unit),
            "an opened unit is NOT ciphertext -> the mux never conceals it"
        );
        for p in 0..32 {
            if p == 17 {
                continue;
            }
            assert_eq!(
                unit[p * BD_SOURCE_PACKET_BYTES + 4],
                TS_SYNC,
                "every other packet restored its sync (pkt {p})"
            );
        }
    }

    #[test]
    fn several_defect_packets_within_tolerance_still_decrypt() {
        // Up to 25% authored-bad content packets are tolerated (opened + passed
        // through); the muxer drops them.
        let key = [0x33u8; 16];
        let mut unit = unit_with_defects(&key, &[3, 9, 17, 24, 30]); // 5/32 ≈ 16%
        assert!(
            decrypt_unit(&mut unit, &key),
            "27/32 synced (>=75%) -> opened"
        );
    }

    #[test]
    fn wrong_key_never_opens_a_unit() {
        // A wrong key restores ~0 syncs -> far below the supermajority gate.
        let key = [0x5Au8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &key);
        assert!(
            !decrypt_unit(&mut unit, &[0x22u8; 16]),
            "wrong key cannot open the unit"
        );
    }

    #[test]
    fn threshold_accepts_at_75pct_and_rejects_just_below() {
        // 24/32 = exactly 75% opens; 23/32 does not.
        assert!(unit_content_decrypted(&decrypted_shape(32, 24, false)));
        assert!(!unit_content_decrypted(&decrypted_shape(32, 23, false)));
    }

    #[test]
    fn still_ciphertext_tracks_the_open_verdict() {
        // Fully clean -> opened, not ciphertext.
        assert!(!aacs_unit_still_ciphertext(&decrypted_shape(32, 32, true)));
        // One defect -> still opened, still not ciphertext.
        assert!(!aacs_unit_still_ciphertext(&decrypted_shape(32, 31, true)));
        // Wrong-key noise floor -> not opened -> ciphertext (concealable).
        assert!(aacs_unit_still_ciphertext(&decrypted_shape(32, 3, true)));
        // CPI-clear bytes are never "ciphertext" regardless of sync count.
        assert!(!aacs_unit_still_ciphertext(&decrypted_shape(32, 0, false)));
    }

    #[test]
    fn all_padding_unit_is_trivially_opened() {
        // CPI set but every packet is source-zero padding: nothing to decrypt.
        assert!(unit_content_decrypted(&decrypted_shape(0, 0, true)));
        assert!(!aacs_unit_still_ciphertext(&decrypted_shape(0, 0, true)));
    }

    #[test]
    fn defect_count_boundary_via_real_decrypt() {
        // Pin the 75% gate on the REAL decrypt path (not just the predicate):
        // 8/32 defects => 24 synced = exactly 75% => opens; 9/32 => 23 synced =>
        // does NOT open. Packets 1.. avoid the clear-seed packet 0.
        let key = [0x77u8; 16];
        let eight: Vec<usize> = (1..9).collect();
        let mut u_ok = unit_with_defects(&key, &eight);
        assert!(
            decrypt_unit(&mut u_ok, &key),
            "8 defects (24/32 = 75%) -> opened"
        );
        let nine: Vec<usize> = (1..10).collect();
        let mut u_no = unit_with_defects(&key, &nine);
        assert!(
            !decrypt_unit(&mut u_no, &key),
            "9 defects (23/32 < 75%) -> NOT opened (too corrupt / wrong key)"
        );
    }

    #[test]
    fn small_content_units_keep_the_wrong_key_floor() {
        // Tiny content units are where a coincidental wrong-key sync matters most.
        // The gate stays strict enough that a single fluke can't "open" them.
        assert!(unit_content_decrypted(&decrypted_shape(2, 2, false))); // 2/2 -> open
        assert!(!unit_content_decrypted(&decrypted_shape(2, 1, false))); // 1/2 -> not
        assert!(unit_content_decrypted(&decrypted_shape(4, 3, false))); // 3/4=75% -> open
        assert!(!unit_content_decrypted(&decrypted_shape(4, 2, false))); // 2/4 -> not
    }

    #[test]
    fn fragment_tail_padding_plus_one_defect_still_decrypts() {
        // Both tolerances at once: source-zero padding tail EXCLUDED, and the one
        // authored-bad packet in the real prefix TOLERATED. 20 real packets
        // (pkt 5 defective) => 19/20 synced => opened; padding emitted as zeros.
        let key = [0x5Au8; 16];
        let mut unit = clear_unit();
        let off = 5 * BD_SOURCE_PACKET_BYTES;
        unit[off + 4] = 0x80; // defect inside the real content
        unit[off + 5] = 0xAB;
        for b in unit[20 * BD_SOURCE_PACKET_BYTES..].iter_mut() {
            *b = 0; // source-zero padding tail (packets 20..32)
        }
        aacs_encrypt_unit(&mut unit, &key);
        assert!(
            decrypt_unit(&mut unit, &key),
            "19/20 real packets synced + zero pad -> opened"
        );
        assert_eq!(
            unit[off + 4],
            0x80,
            "the defect packet passes through verbatim"
        );
        for p in 20..32 {
            let o = p * BD_SOURCE_PACKET_BYTES;
            assert!(
                unit[o..o + BD_SOURCE_PACKET_BYTES].iter().all(|&b| b == 0),
                "padding pkt {p} emitted as clean zeros"
            );
        }
    }

    #[test]
    fn wrong_key_full_unit_is_not_decryptable() {
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &[0x11u8; 16]);
        assert!(
            !decrypt_unit(&mut unit, &[0x22u8; 16]),
            "wrong key on a full content unit is rejected"
        );
    }

    #[test]
    fn cpi_clear_unit_passes_through_decryptable() {
        // CPI-clear (plaintext) unit: decryptable by definition, untouched.
        let mut unit = clear_unit(); // byte 0 high bits clear
        let before = unit.clone();
        assert!(
            decrypt_unit(&mut unit, &[0u8; 16]),
            "clear unit passes through as decryptable"
        );
        assert_eq!(unit, before, "clear unit left untouched");
    }

    // ── AES-ECB KAT (FIPS-197 Appendix C.1) ────────────────────────────────

    #[test]
    fn aes_ecb_matches_fips197_known_answer() {
        // FIPS-197 Appendix C.1 AES-128 KAT:
        //   key       = 000102030405060708090a0b0c0d0e0f
        //   plaintext = 00112233445566778899aabbccddeeff
        //   ciphertext= 69c4e0d86a7b0430d8cdb78070b4c55a
        // This pins the AES primitive against a published vector — a wrong
        // cipher (or a key/plaintext byte-order slip) fails it.
        let key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F,
        ];
        let pt = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ];
        let expected = [
            0x69, 0xC4, 0xE0, 0xD8, 0x6A, 0x7B, 0x04, 0x30, 0xD8, 0xCD, 0xB7, 0x80, 0x70, 0xB4,
            0xC5, 0x5A,
        ];
        assert_eq!(aes_ecb_encrypt(&key, &pt), expected);
        // And decrypt is the exact inverse.
        assert_eq!(aes_ecb_decrypt(&key, &expected), pt);
    }

    // ── CBC decrypt: first-block uses fixed AACS IV ────────────────────────

    #[test]
    fn cbc_decrypt_first_block_xors_aacs_iv() {
        // CBC: P[0] = AES-D(K, C[0]) XOR IV, and the IV is the fixed AACS
        // constant (not zero). Encrypt a single block forward with IV, then
        // confirm aes_cbc_decrypt recovers it — proving the IV used on block
        // 0 is exactly AACS_IV. A mutation that swaps AACS_IV for [0u8;16]
        // makes the recovered block wrong.
        let key = [0x24u8; 16];
        let plain = [0x5Au8; 16];
        // Forward CBC for one block: C = AES-E(K, P XOR IV).
        let mut x = plain;
        for j in 0..16 {
            x[j] ^= AACS_IV[j];
        }
        let ct = aes_ecb_encrypt(&key, &x);
        let mut buf = ct;
        aes_cbc_decrypt(&key, &mut buf);
        assert_eq!(buf, plain, "block-0 CBC must XOR the fixed AACS IV");
    }

    // ── CBC decrypt KAT (NIST SP 800-38A F.2.2, AES-128-CBC) ───────────────

    #[test]
    fn aes_cbc_decrypt_matches_nist_sp800_38a_f2_2() {
        // NIST SP 800-38A Appendix F.2.2 (CBC-AES128.Decrypt) published vector:
        //   Key = 2b7e151628aed2a6abf7158809cf4f3c
        //   IV  = 000102030405060708090a0b0c0d0e0f
        //   CT  = 7649abac8119b246cee98e9b12e9197d  (block 0)
        //         5086cb9b507219ee95db113a917678b2  (block 1)
        //         73bed6b8e3c1743b7116e69e22229516  (block 2)
        //         3ff1caa1681fac09120eca307586e1a7  (block 3)
        //   PT  = 6bc1bee22e409f96e93d7e117393172a  (block 0)
        //         ae2d8a571e03ac9c9eb76fac45af8e51  (block 1)
        //         30c81c46a35ce411e5fbc1191a0a52ef  (block 2)
        //         f69f2445df4f9b17ad2b417be66c3710  (block 3)
        //
        // `aes_cbc_decrypt` hardwires the fixed AACS IV for block 0 (it never
        // takes a caller IV), so:
        //   * Blocks 1..=3 are independent of the IV — they MUST equal the NIST
        //     plaintext byte-for-byte (P[i] = AES-D(K, C[i]) XOR C[i-1]). This
        //     pins the real reverse-order CBC chaining against a published KAT.
        //   * Block 0 = AES-D(K, C[0]) XOR AACS_IV = NIST_PT[0] XOR NIST_IV
        //     XOR AACS_IV — the documented IV substitution. Asserting this exact
        //     relation pins both the AES decrypt of C[0] AND that block 0 uses
        //     AACS_IV (a swap to [0u8;16] or a chaining bug fails it).
        let key = [
            0x2B, 0x7E, 0x15, 0x16, 0x28, 0xAE, 0xD2, 0xA6, 0xAB, 0xF7, 0x15, 0x88, 0x09, 0xCF,
            0x4F, 0x3C,
        ];
        let nist_iv = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F,
        ];
        // Byte arrays kept narrow (≤14 bytes/line) so the secret-scanner's
        // 32-nibble-per-line heuristic doesn't flag these published vectors as
        // key material (same layout the existing FIPS-197 / CMAC KATs use).
        let ciphertext: [u8; 64] = [
            0x76, 0x49, 0xAB, 0xAC, 0x81, 0x19, 0xB2, 0x46, 0xCE, 0xE9, 0x8E, 0x9B, 0x12, 0xE9,
            0x19, 0x7D, 0x50, 0x86, 0xCB, 0x9B, 0x50, 0x72, 0x19, 0xEE, 0x95, 0xDB, 0x11, 0x3A,
            0x91, 0x76, 0x78, 0xB2, 0x73, 0xBE, 0xD6, 0xB8, 0xE3, 0xC1, 0x74, 0x3B, 0x71, 0x16,
            0xE6, 0x9E, 0x22, 0x22, 0x95, 0x16, 0x3F, 0xF1, 0xCA, 0xA1, 0x68, 0x1F, 0xAC, 0x09,
            0x12, 0x0E, 0xCA, 0x30, 0x75, 0x86, 0xE1, 0xA7,
        ];
        let nist_plaintext: [u8; 64] = [
            0x6B, 0xC1, 0xBE, 0xE2, 0x2E, 0x40, 0x9F, 0x96, 0xE9, 0x3D, 0x7E, 0x11, 0x73, 0x93,
            0x17, 0x2A, 0xAE, 0x2D, 0x8A, 0x57, 0x1E, 0x03, 0xAC, 0x9C, 0x9E, 0xB7, 0x6F, 0xAC,
            0x45, 0xAF, 0x8E, 0x51, 0x30, 0xC8, 0x1C, 0x46, 0xA3, 0x5C, 0xE4, 0x11, 0xE5, 0xFB,
            0xC1, 0x19, 0x1A, 0x0A, 0x52, 0xEF, 0xF6, 0x9F, 0x24, 0x45, 0xDF, 0x4F, 0x9B, 0x17,
            0xAD, 0x2B, 0x41, 0x7B, 0xE6, 0x6C, 0x37, 0x10,
        ];

        let mut buf = ciphertext;
        aes_cbc_decrypt(&key, &mut buf);

        // Blocks 1..=3: exact match against the published NIST plaintext.
        assert_eq!(
            &buf[16..64],
            &nist_plaintext[16..64],
            "CBC chaining (blocks 1..3) must match NIST SP 800-38A F.2.2 plaintext"
        );

        // Block 0: NIST_PT[0] XOR NIST_IV XOR AACS_IV (the fixed-IV substitution).
        let mut expected_block0 = [0u8; 16];
        for i in 0..16 {
            expected_block0[i] = nist_plaintext[i] ^ nist_iv[i] ^ AACS_IV[i];
        }
        assert_eq!(
            &buf[0..16],
            &expected_block0,
            "block-0 plaintext must equal NIST PT XOR NIST IV XOR AACS_IV (fixed-IV path)"
        );
    }

    // ── decrypt_unit: full round trip restores TS syncs ────────────────────

    #[test]
    fn decrypt_unit_roundtrip_restores_all_syncs() {
        // Encrypt a clear unit, confirm it reads as scrambled, then decrypt
        // and confirm every TS sync byte at the 192-byte stride is restored.
        let unit_key = [0x37u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &unit_key);
        assert!(
            ts_sync_destroyed(&unit),
            "encrypted unit must look scrambled"
        );

        assert!(decrypt_unit(&mut unit, &unit_key));
        // All 32 stride positions carry sync after decrypt.
        assert_eq!(ts_sync_count(&unit), ts_packet_total(&unit));
        assert!(!ts_sync_destroyed(&unit));
    }

    #[test]
    fn decrypt_unit_wrong_key_fails_and_does_not_falsely_clear() {
        // A wrong unit key fails verify_ts (the body stays scrambled), so
        // decrypt_unit returns false. Grounds the brute-force gate: a bad key
        // must NOT report success.
        let good = [0x11u8; 16];
        let bad = [0x22u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &good);
        assert!(!decrypt_unit(&mut unit, &bad), "wrong key must not verify");
    }

    #[test]
    fn decrypt_unit_rejects_short_unit() {
        // unit.len() < ALIGNED_UNIT_LEN → false (no panic on the 16.. slice).
        let mut short = vec![0u8; ALIGNED_UNIT_LEN - 1];
        assert!(!decrypt_unit(&mut short, &[0u8; 16]));
    }

    #[test]
    fn decrypt_unit_only_touches_bytes_16_onward() {
        // The first 16 bytes are the plaintext TP_extra header and must be
        // left untouched by decrypt (only unit[16..] is CBC-processed).
        let unit_key = [0x9Au8; 16];
        let mut clear = clear_unit();
        // Put a distinctive header so we can confirm it survives. Byte 0 carries
        // both CPI bits (0xE0) so it is stable under the fixture's `|= 0xC0`.
        clear[..16].copy_from_slice(&[
            0xE0, 0xA1, 0xA2, 0xA3, 0x47, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ]);
        let header_before: [u8; 16] = clear[..16].try_into().unwrap();
        let mut unit = clear;
        aacs_encrypt_unit(&mut unit, &unit_key);
        // Encryption also leaves the header untouched (only 16.. is encrypted).
        assert_eq!(&unit[..16], &header_before);
        decrypt_unit(&mut unit, &unit_key);
        assert_eq!(
            &unit[..16],
            &header_before,
            "header bytes must be preserved"
        );
    }

    // ── decrypt_unit_try_keys: AlreadyClear vs DecryptedWith vs None ───────

    #[test]
    fn try_keys_reports_already_clear_without_consuming_a_key() {
        // A clear unit returns AlreadyClear even with an empty key list — the
        // old Option<usize> form conflated this with Some(0). Grounds the
        // UnitKeyResult enum distinction.
        let mut unit = clear_unit();
        assert_eq!(
            decrypt_unit_try_keys(&mut unit, &[]),
            Some(UnitKeyResult::AlreadyClear)
        );
    }

    #[test]
    fn try_keys_reports_correct_index_among_several() {
        // Three keys, only the 3rd (index 2) decrypts → DecryptedWith(2).
        let real = [0x44u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &real);
        let keys = [[0x01u8; 16], [0x02u8; 16], real];
        assert_eq!(
            decrypt_unit_try_keys(&mut unit, &keys),
            Some(UnitKeyResult::DecryptedWith(2))
        );
        assert!(
            !ts_sync_destroyed(&unit),
            "unit must be clear after the hit"
        );
    }

    #[test]
    fn try_keys_restores_original_bytes_on_total_failure() {
        // When no key works, the unit must be byte-identical to the input
        // (the function CBC-mangles it per attempt, then restores). A buggy
        // restore would leave the unit corrupted — silent data damage.
        let real = [0x55u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &real);
        let snapshot = unit.clone();
        let wrong = [[0xAAu8; 16], [0xBBu8; 16]];
        assert_eq!(decrypt_unit_try_keys(&mut unit, &wrong), None);
        assert_eq!(unit, snapshot, "failed try must restore the original bytes");
    }

    // ── CPI gate: the authoritative encrypted-vs-clear decision ────────────

    #[test]
    fn cpi_gate_clear_flag_passes_through_even_when_body_looks_scrambled() {
        // THE false-fail fix: a unit whose CPI bits are clear (byte 0 & 0xC0 == 0)
        // is plaintext by spec, even if its body has no TS syncs (non-TS clear
        // data, or a mis-probed body). `decrypt_unit` must pass it through
        // untouched and report success — never attempt a decrypt that would fail.
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        // Body looks scrambled (no syncs at the stride) but byte 0 stays 0x00.
        for (i, b) in unit.iter_mut().enumerate().skip(16) {
            *b = (i as u8).wrapping_mul(31) | 1; // never 0x47 at the sync stride
        }
        assert!(ts_sync_destroyed(&unit), "body has no syncs");
        assert!(!aacs_unit_encrypted(&unit), "CPI clear");
        assert!(
            !aacs_unit_needs_decrypt(&unit),
            "CPI-clear ⇒ no decrypt attempt"
        );
        let snapshot = unit.clone();
        // Any key: passthrough success, bytes untouched (no false DecryptFailed).
        assert!(decrypt_unit(&mut unit, &[0xABu8; 16]));
        assert_eq!(unit, snapshot, "CPI-clear unit must be left byte-identical");
        assert_eq!(
            decrypt_unit_try_keys(&mut unit, &[[0xABu8; 16]]),
            Some(UnitKeyResult::AlreadyClear),
            "CPI-clear unit consumes no key"
        );
    }

    #[test]
    fn cpi_gate_set_flag_decrypts_and_needs_decrypt_is_idempotent() {
        // A CPI-set encrypted unit decrypts with the right key. CPI lives in the
        // plaintext header, so it survives decryption — `aacs_unit_encrypted`
        // still reports true afterward, but `aacs_unit_needs_decrypt` flips to
        // false (syncs restored), keeping the re-decrypt paths idempotent.
        let key = [0x5au8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &key); // sets CPI + scrambles body
        assert!(aacs_unit_encrypted(&unit), "CPI set");
        assert!(aacs_unit_needs_decrypt(&unit), "flagged + still scrambled");

        assert!(decrypt_unit(&mut unit, &key), "right key decrypts");
        assert!(
            aacs_unit_encrypted(&unit),
            "CPI bits live in the preserved header ⇒ still set post-decrypt"
        );
        assert!(
            !aacs_unit_needs_decrypt(&unit),
            "syncs restored ⇒ no further decrypt attempt (idempotent re-decrypt)"
        );
    }

    // ── unit_key_validates: matches decrypt_unit's verdict exactly ─────────

    #[test]
    fn unit_key_validates_agrees_with_decrypt_unit() {
        // The fast 1-byte gate's accept/reject set must be identical to the
        // authoritative decrypt_unit. Confirm: correct key → true on both;
        // wrong key → false on both.
        let good = [0x6Au8; 16];
        let bad = [0x6Bu8; 16];
        let mut enc = clear_unit();
        aacs_encrypt_unit(&mut enc, &good);

        assert!(unit_key_validates(&enc, &good));
        let mut probe = enc.clone();
        assert!(decrypt_unit(&mut probe, &good));

        assert!(!unit_key_validates(&enc, &bad));
        let mut probe2 = enc.clone();
        assert!(!decrypt_unit(&mut probe2, &bad));
    }

    #[test]
    fn unit_is_clean_ts_is_strict_all_32_syncs() {
        // Standards-correct gate (all-32 TS syncs): EVERY one of the 32
        // packet syncs is required. A fully-synced clear unit passes.
        let clear = clear_unit();
        assert!(unit_is_clean_ts(&clear), "all-32-sync unit is clean");

        // Drop a SINGLE sync (packet 17 of 32). 31/32 remain, so the majority
        // heuristic still passes — that is exactly the silent-corruption hole.
        // The strict gate must REJECT it.
        let mut one_missing = clear_unit();
        one_missing[17 * BD_SOURCE_PACKET_BYTES + 4] = 0x00;
        assert!(
            ts_syncs_intact(&one_missing),
            "majority heuristic still passes one missing sync (the hole)"
        );
        assert!(
            !unit_is_clean_ts(&one_missing),
            "strict gate rejects even one missing sync"
        );

        // A correctly decrypted unit is clean; a wrong-key decrypt is not.
        let key = [0x33u8; 16];
        let mut enc = clear_unit();
        aacs_encrypt_unit(&mut enc, &key);
        let mut good = enc.clone();
        decrypt_unit(&mut good, &key);
        assert!(unit_is_clean_ts(&good), "right-key decrypt yields clean TS");
        let mut wrong = enc.clone();
        decrypt_unit(&mut wrong, &[0x34u8; 16]);
        assert!(
            !unit_is_clean_ts(&wrong),
            "wrong-key decrypt must fail the strict gate"
        );

        // Short buffer is never vacuously clean.
        assert!(!unit_is_clean_ts(&clear[..ALIGNED_UNIT_LEN - 1]));
    }

    #[test]
    fn unit_key_validates_is_non_mutating() {
        // The accelerator must never write its input (it operates on the
        // ciphertext and confirms on a copy). A mutation that decrypted in
        // place would corrupt the caller's buffer.
        let good = [0x7Cu8; 16];
        let mut enc = clear_unit();
        aacs_encrypt_unit(&mut enc, &good);
        let snapshot = enc.clone();
        let _ = unit_key_validates(&enc, &good);
        assert_eq!(enc, snapshot, "unit_key_validates must not mutate input");
    }

    #[test]
    fn unit_key_validates_rejects_short_unit() {
        let short = vec![0u8; ALIGNED_UNIT_LEN - 16];
        assert!(!unit_key_validates(&short, &[0u8; 16]));
    }

    // ── bus decryption (AACS 2.0 / UHD) ────────────────────────────────────

    #[test]
    fn decrypt_bus_roundtrips_per_sector_skipping_first_16_bytes() {
        // Bus encryption CBC-encrypts bytes 16..2048 of EACH 2048-byte sector
        // (3 sectors per aligned unit), leaving the first 16 plaintext. Build
        // the forward transform, then confirm decrypt_bus inverts it and
        // leaves each sector's first 16 bytes untouched.
        let rdk = [0x13u8; 16];
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        // Fill with a recognisable pattern.
        for (i, b) in unit.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let plain = unit.clone();

        // Forward: CBC-encrypt unit[s+16 .. s+2048] per sector under AACS IV.
        let cipher = Aes128::new(GenericArray::from_slice(&rdk));
        for s in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_BYTES) {
            let mut prev = AACS_IV;
            let body = s + 16;
            let end = s + SECTOR_BYTES;
            let nblocks = (end - body) / 16;
            for i in 0..nblocks {
                let off = body + i * 16;
                for j in 0..16 {
                    unit[off + j] ^= prev[j];
                }
                let mut blk = GenericArray::clone_from_slice(&unit[off..off + 16]);
                cipher.encrypt_block(&mut blk);
                unit[off..off + 16].copy_from_slice(&blk);
                prev.copy_from_slice(&unit[off..off + 16]);
            }
        }
        assert_ne!(unit, plain, "forward bus-encrypt must change the body");

        decrypt_bus(&mut unit, &rdk);
        assert_eq!(
            unit, plain,
            "decrypt_bus must invert per-sector bus encrypt"
        );
        // Each sector's first 16 bytes equal the original (never touched).
        for s in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_BYTES) {
            assert_eq!(&unit[s..s + 16], &plain[s..s + 16]);
        }
    }

    // ── decrypt_unit_full: bus-then-AACS ordering, and clear passthrough ───

    #[test]
    fn decrypt_unit_full_passthrough_when_already_clear() {
        // A clear unit returns true and is not modified, regardless of keys.
        let mut unit = clear_unit();
        let snapshot = unit.clone();
        assert!(decrypt_unit_full(
            &mut unit,
            &[0u8; 16],
            Some(&[0xFFu8; 16])
        ));
        assert_eq!(unit, snapshot, "clear unit must pass through untouched");
    }

    #[test]
    fn decrypt_unit_full_applies_bus_then_aacs() {
        // AACS 2.0 pipeline: content is first AACS-unit-encrypted, then
        // bus-encrypted on top. Decrypt must undo bus FIRST, then AACS.
        // Build that exact two-layer ciphertext and confirm full recovery.
        let unit_key = [0x21u8; 16];
        let rdk = [0x84u8; 16];

        let mut unit = clear_unit();
        // Layer 1: AACS unit-encrypt.
        aacs_encrypt_unit(&mut unit, &unit_key);
        // Layer 2: bus-encrypt on top (per-sector, bytes 16..2048).
        let cipher = Aes128::new(GenericArray::from_slice(&rdk));
        for s in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_BYTES) {
            let mut prev = AACS_IV;
            for i in 0..((SECTOR_BYTES - 16) / 16) {
                let off = s + 16 + i * 16;
                for j in 0..16 {
                    unit[off + j] ^= prev[j];
                }
                let mut blk = GenericArray::clone_from_slice(&unit[off..off + 16]);
                cipher.encrypt_block(&mut blk);
                unit[off..off + 16].copy_from_slice(&blk);
                prev.copy_from_slice(&unit[off..off + 16]);
            }
        }
        assert!(ts_sync_destroyed(&unit));
        assert!(decrypt_unit_full(&mut unit, &unit_key, Some(&rdk)));
        assert_eq!(ts_sync_count(&unit), ts_packet_total(&unit));
    }

    // ── ts_sync_destroyed / ts_sync_count edge cases ───────────────────────

    #[test]
    fn ts_sync_destroyed_false_for_sub_unit_length() {
        // The function guards on `len >= ALIGNED_UNIT_LEN` first; anything
        // shorter is reported NOT scrambled (so the decrypt gate skips it)
        // rather than indexing past the end.
        assert!(!ts_sync_destroyed(&[]));
        assert!(!ts_sync_destroyed(&vec![0u8; ALIGNED_UNIT_LEN - 1]));
        // A scrambled-looking buffer that is one byte short is still "not
        // scrambled" by the length guard.
        let mut almost = vec![0u8; ALIGNED_UNIT_LEN - 1];
        almost[4] = 0x00; // no syncs
        assert!(!ts_sync_destroyed(&almost));
    }

    #[test]
    fn ts_sync_count_only_samples_the_192_byte_stride() {
        // A 0x47 placed OFF the stride (e.g. offset 5) must not be counted —
        // the detector samples exactly offset 4, 196, 388, ... A mutation that
        // scanned every byte would over-count and misclassify scrambled units.
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        unit[5] = TS_SYNC; // off-stride
        unit[197] = TS_SYNC; // off-stride
        assert_eq!(ts_sync_count(&unit), 0, "off-stride 0x47 must not count");
        unit[4] = TS_SYNC; // on-stride
        assert_eq!(ts_sync_count(&unit), 1);
    }

    #[test]
    fn ts_packet_total_for_various_lengths() {
        // total = len / 192 (BD-TS packet size). Pin a few lengths.
        assert_eq!(ts_packet_total(&[0u8; 192]), 1);
        assert_eq!(ts_packet_total(&[0u8; 384]), 2);
        assert_eq!(ts_packet_total(&[0u8; 191]), 0);
        // 6144 = 32 packets.
        assert_eq!(ts_packet_total(&[0u8; ALIGNED_UNIT_LEN]), 32);
    }

    /// Direct coverage for the padding-aware conceal predicate. It must conceal
    /// ONLY genuinely-undecryptable ciphertext — never a decrypted unit (full or
    /// short padding-tail), a clear/non-encrypted unit, or an all-zero unit.
    #[test]
    fn aacs_unit_still_ciphertext_is_padding_aware() {
        let pkt = BD_SOURCE_PACKET_BYTES;
        // Build a 32-packet aligned unit. `cpi` sets the AACS CPI bits (byte 0).
        // Per packet: b'S' = decrypted TS (0x47 sync + non-zero payload),
        // b'C' = ciphertext (non-zero payload, no sync), b'P' = zero padding.
        let build = |cpi: bool, kinds: &[u8]| {
            let mut u = vec![0u8; ALIGNED_UNIT_LEN];
            if cpi {
                u[0] = 0xC0; // CPI bits in the packet-0 header (not the payload)
            }
            for (i, &k) in kinds.iter().enumerate() {
                let off = i * pkt;
                match k {
                    b'S' => {
                        u[off + 4] = TS_SYNC;
                        for b in &mut u[off + 5..off + pkt] {
                            *b = 0x10;
                        }
                    }
                    b'C' => {
                        // Scrambled: non-zero payload, no 0x47 at the sync position.
                        for b in &mut u[off + 4..off + pkt] {
                            *b = 0x5A;
                        }
                    }
                    _ => {} // b'P' → leave zero
                }
            }
            u
        };

        // Not encrypted (CPI clear) → never concealed, even if it looks scrambled.
        assert!(!aacs_unit_still_ciphertext(&build(false, &[b'C'; 32])));
        // All-zero unit (CPI clear) → not encrypted → false.
        assert!(!aacs_unit_still_ciphertext(&build(false, &[b'P'; 32])));
        // Fully decrypted (all packets carry their sync) → false.
        assert!(!aacs_unit_still_ciphertext(&build(true, &[b'S'; 32])));
        // Fully ciphertext (no packet carries its sync) → true.
        assert!(aacs_unit_still_ciphertext(&build(true, &[b'C'; 32])));
        // Decrypted SHORT padding-tail: 11 content packets + 21 zero padding. The
        // majority vote would mis-flag it (<16 syncs); the padding-aware predicate
        // skips the zero padding and sees every non-zero packet has its sync.
        let mut tail = [b'P'; 32];
        for k in tail.iter_mut().take(11) {
            *k = b'S';
        }
        assert!(
            !aacs_unit_still_ciphertext(&build(true, &tail)),
            "a decrypted short padding-tail must NOT be flagged as ciphertext"
        );
    }
}
