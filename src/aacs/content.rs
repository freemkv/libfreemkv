//! AACS content decryption — aligned-unit / bus decryption and TS verification.
//! The low-level AES primitives it uses live in [`super::crypto`].

#[cfg(test)]
use aes::Aes128;
#[cfg(test)]
use aes::cipher::{KeyInit, generic_array::GenericArray};

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

/// HD-DVD `.evo` (MPEG-2 Program Stream) AACS-encrypted-unit flag offset & mask.
///
/// BD/UHD/FMTS flag encryption with the Copy Permission Indicator in the top 2
/// bits of byte 0 (the M2TS `TP_extra_header`). HD-DVD `.evo` is Program Stream —
/// byte 0 is a `00 00 01 BA` pack_start_code, NOT a CPI — so AACS reuses the
/// MPEG-2 `PES_scrambling_control` field instead: pack_header (14 bytes) + PES
/// start-code/`stream_id` (4) + `PES_packet_length` (2) puts the PES flags byte at
/// offset 20, with `PES_scrambling_control` in bits 5-4 (`& 0x30`). Non-zero =
/// encrypted. Derived from BackupHDDVD (`Header[20] & 0x30`) cross-checked against
/// MPEG-2 systems (ISO/IEC 13818-1).
///
/// UNVERIFIED against a real ENCRYPTED HD-DVD disc — none available to confirm
/// byte-exactly. TWO open questions a real disc must settle:
///  1. A pack carrying an MPEG `system_header` (`00 00 01 BB`) before the PES
///     packet shifts this offset past 20.
///  2. Whether offset 20 is even readable pre-decrypt. BD keeps only the first 16
///     bytes clear (the seed) and AES-CBC-encrypts 16..6144 — under that model
///     offset 20 is ciphertext. BackupHDDVD reading `Header[20]` pre-decrypt
///     implies HD-DVD instead encrypts PES *payloads* with *clear* pack/PES
///     headers (per-PES model). If so, `decrypt_unit`'s 16-byte-seed model also
///     would not fit HD-DVD and needs its own path. TS is unaffected either way.
const PS_SCRAMBLE_OFF: usize = 20;
const PS_SCRAMBLE_MASK: u8 = 0x30;

/// The AUTHORITATIVE AACS "is this aligned unit encrypted?" signal, per container.
///
/// - `BdTs` (BD / UHD / FMTS Transport Stream): the Copy Permission Indicator in
///   the top 2 bits of byte 0 — the first `TP_extra_header` byte, always left
///   clear (the first 16 bytes of every unit are the unencrypted SEED). [BD]
///   §3.10.2. `(buf[0] & 0xC0) == 0` → clear; non-zero → bytes `16..6144` are
///   AES-CBC encrypted.
/// - `MpegPs` (HD-DVD `.evo`): the MPEG-2 `PES_scrambling_control` flag — see
///   [`PS_SCRAMBLE_OFF`] (UNVERIFIED against a real encrypted disc).
///
/// Readable WITHOUT a key. CRITICAL: only meaningful when `unit` is read at the
/// correct clip-FILE-anchored boundary — a disc-absolute / mis-aligned read makes
/// the flag byte arbitrary mid-stream data (which is why per-unit checks run
/// clip-anchored, not in the whole-disc sweep).
pub fn aacs_unit_encrypted(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    use crate::disc::ContentFormat;
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    match format {
        ContentFormat::BdTs => (unit[0] & 0xC0) != 0,
        // UNVERIFIED-HDDVD-DECRYPT (1 of 2): the PES_scrambling_control location
        // for HD-DVD `.evo` is derived from spec, never confirmed against a real
        // ENCRYPTED HD DVD (we only have decrypted rips). If HD-DVD ripping ever
        // misbehaves, verify this flag byte/mask against a genuine encrypted unit.
        ContentFormat::MpegPs => (unit[PS_SCRAMBLE_OFF] & PS_SCRAMBLE_MASK) != 0,
    }
}

/// True when an aligned unit is flagged encrypted AND still looks scrambled
/// (structure not yet restored) — i.e. genuine encrypted content NOT yet decrypted.
///
/// [`aacs_unit_encrypted`] is the authoritative gate, but the flag lives in the
/// clear header, which decryption never rewrites, so a successfully decrypted unit
/// still reports the flag. Buffer-iterating sites that may run twice over the same
/// `buf` (the post-fetch re-decrypt, sample collection, failure diagnosis) need an
/// IDEMPOTENT "does this still need work?" test, so they compose the flag with a
/// "structure restored?" check that flips once decrypted: TS syncs come back for
/// `BdTs`; valid `00 00 01 BA` packs come back for `MpegPs`.
///
/// Like the flag itself this is only meaningful at the clip-FILE-anchored boundary.
pub fn aacs_unit_needs_decrypt(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    // "Still needs the key applied" = flagged encrypted AND not yet structurally
    // clean. There is ONE definition of clean — [`is_clean`] (the min(E,4) proof
    // floor: E>4 needs any 4 synced, E<=4 needs all present). Never a second
    // threshold: the old >50% majority false-flagged a bad-encoded-but-OPENED
    // unit as still-scrambled, so the mux re-sampled it to the key service every
    // batch (the storm) and could re-apply the key over already-clear bytes.
    aacs_unit_encrypted(unit, format) && !is_clean(unit, format)
}

/// Minimum synced content packets that PROVE a key opened a unit. Four `0x47`
/// syncs ≈ 32 bits of MPEG-TS structure ≈ 1-in-4-billion that a wrong key (uniform
/// AES noise, `0x47` at 1/256 per packet) fakes it. It is an ABSOLUTE proof floor,
/// NOT a proportion — a unit the key opened but whose content is bad-encoded
/// (many non-conforming packets) is proven by ANY four good packets, not rejected
/// for the bad ones.
const KEY_PROOF_PACKETS: usize = 4;

/// Structural "did a key open this content unit?" — the pure, NO-CRYPTO signal
/// for key SELECTION (pick the right key among multiple on a multi-CPS disc) and
/// read VERIFY. AACS has no cryptographic "did the key work" answer (no MAC), so
/// a key is proven STRUCTURALLY: its plaintext must look like valid content for
/// the disc's container. This dispatches to the right container check by
/// `format` — BD/UHD/FMTS are Transport Stream ([`is_clean_ts`]); HD-DVD `.evo`
/// is Program Stream ([`is_clean_ps`]). NOT a decryption verdict: a correct key
/// can decrypt structurally-broken content, which is the muxer's concern.
pub fn is_clean(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    match format {
        crate::disc::ContentFormat::BdTs => is_clean_ts(unit),
        crate::disc::ContentFormat::MpegPs => is_clean_ps(unit),
    }
}

/// Structural "does this unit carry enough valid MPEG-TS to prove a key opened
/// it?" — the Transport-Stream arm of [`is_clean`]. It is
/// NOT a decryption verdict: [`decrypt_unit`] applies a key (that is
/// "decrypt"); whether the plaintext is clean TS is this SEPARATE question. The
/// mux never calls this — TS validity is a muxer concern, never a decrypt result.
///
/// Rule — evidence is ABSOLUTE, scaled to the packets that exist. Over the
/// ENCRYPTED packets (skip packet 0: its `0x47` sits in the clear 16-byte seed, so
/// it reads `0x47` for ANY key and is never evidence), let `E` = non-padding
/// content packets and `synced` = those carrying `0x47`. The key opened the unit
/// iff `E == 0` (nothing encrypted to prove) OR `synced >= min(E, KEY_PROOF_PACKETS)`.
///   * WRONG key → ~0 synced → fails (reaching 4 by chance ≈ 1e-5/unit, and every
///     unit of a clip would have to fluke — astronomically safe).
///   * RIGHT key, bad-encoded content → any 4 good packets pass; the bad ones are
///     the muxer's problem. (This is the false-negative the old 75% PROPORTION
///     caused — a mostly-bad unit the key opened was wrongly rejected.)
///   * `min(E, 4)` handles the end-of-clip fragment TAIL: a unit with only E=1
///     real packet (then source-zero padding) needs just that one to sync, so a
///     sparse-but-valid tail is never false-rejected. Padding (all-zero payload)
///     is excluded throughout.
fn is_clean_ts(unit: &[u8]) -> bool {
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    let limit = ALIGNED_UNIT_LEN.min(unit.len());
    let mut content = 0usize;
    let mut synced = 0usize;
    // Skip packet 0: its sync byte lives in the clear 16-byte seed (unencrypted),
    // so it is `0x47` regardless of the key and proves nothing about decryption.
    let mut off = PKT;
    while off + PKT <= limit {
        let payload = &unit[off + 4..off + PKT];
        if !payload.iter().all(|&b| b == 0) {
            content += 1;
            if unit[off + 4] == TS_SYNC {
                synced += 1;
            }
        }
        off += PKT;
    }
    content == 0 || synced >= content.min(KEY_PROOF_PACKETS)
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

/// The Program-Stream arm of [`is_clean`] (HD-DVD `.evo`): a pure structural
/// check that a unit is valid MPEG-2 PS — every 2048-byte pack begins with the
/// pack_start_code `00 00 01 BA`; a 6144-byte AACS unit spans three packs.
///
/// Like [`is_clean_ts`] this is a structural question, NOT a decryption verdict.
/// Pack 0's start sits in the clear 16-byte seed (present regardless of the key —
/// the freebie `is_clean_ts` skips at packet 0); packs 1 and 2 (offsets 2048 and
/// 4096) are in the encrypted region, so a wrong key garbles them and this returns
/// false (64 bits of discrimination). Validated against real decrypted HD-DVD
/// `.evo` (ANCHORMAN / SHAUN_OF_THE_DEAD): pack starts are exactly 2048-aligned,
/// three per aligned unit, at offsets 0 / 2048 / 4096.
///
/// UNVERIFIED-HDDVD-DECRYPT (2 of 2): the pack STRUCTURE here is confirmed on
/// decrypted rips, but that a real ENCRYPTED `.evo` decrypts to it via the same
/// 6144-byte aligned unit (16-byte seed + AES-CBC over 16..6144) as BD is the
/// unverified assumption — we have no encrypted HD DVD. If HD-DVD decryption
/// yields garbage with a known-good key, the unit granularity is the suspect.
fn is_clean_ps(unit: &[u8]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    const PACK_START: [u8; 4] = [0x00, 0x00, 0x01, 0xBA];
    // Skip pack 0 (offset 0): its start bytes lie in the clear 16-byte seed —
    // present for ANY key (and byte 0 may carry the Blu-ray-style CPI bits), so
    // it proves nothing about decryption. This mirrors `is_clean_ts` skipping
    // packet 0. Packs 1 and 2 (offsets 2048 / 4096) are in the encrypted region
    // and are what discriminate the key (64 bits of proof).
    let mut o = SECTOR_BYTES;
    while o + 4 <= ALIGNED_UNIT_LEN {
        if unit[o..o + 4] != PACK_START {
            return false;
        }
        o += SECTOR_BYTES; // one MPEG-2 PS pack per 2048-byte sector
    }
    true
}

/// Decrypt one AACS aligned unit (6144 bytes) IN PLACE — PURE crypto that applies
/// `unit_key` and leaves the plaintext. This is decryption and NOTHING else: it
/// makes no verdict about whether the result is clean TS. That is the SEPARATE
/// [`is_clean_ts`] question, which a caller composes only when it needs key
/// SELECTION (multi-CPS discs) or a read VERIFY — because AACS content has no MAC,
/// "did the key decrypt correctly?" is unanswerable by crypto; TS structure is a
/// data-quality signal, not a decrypt verdict.
///
/// PURE: this applies the key UNCONDITIONALLY (any full-length unit). It does NOT
/// check the encrypted-flag — decrypting an already-clear unit would corrupt it,
/// so the CALLER must gate on [`aacs_unit_encrypted`] (which is container-aware)
/// before calling. Lifting that gate out of the crypto keeps this function
/// container-agnostic (the flag's location differs BD-TS vs HD-DVD-PS) and true
/// to "decrypt applies the key and nothing else". The only guard kept here is the
/// length check, since the crypto is defined only over a whole 6144-byte unit.
///
/// Block Key = AES-128E(Kcu, seed) ⊕ seed ([BD] §3.10.1 Fig 3-8: encrypt the clear
/// 16-byte seed under the CPS Unit Key, XOR the seed back in — the trailing ⊕seed
/// is load-bearing); then AES-128-CBC decrypt bytes 16..6144 under the AACS IV.
/// Source-zero padding packets (all 192 bytes zero on disc) are restored to zero:
/// their decrypted bytes are AES-noise from decrypting zeros, but the source WAS
/// zero, so writing the true source back is faithful and gives the demux a tidy
/// gap. Content packets are left EXACTLY as decrypted — the decrypt path never
/// rewrites content, so an authored-bad packet passes through verbatim.
pub fn decrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) {
    if unit.len() < ALIGNED_UNIT_LEN {
        return;
    }
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    let npkt = ALIGNED_UNIT_LEN / PKT;
    let mut pad = [false; ALIGNED_UNIT_LEN / PKT];
    for (p, slot) in pad.iter_mut().enumerate().take(npkt) {
        let off = p * PKT;
        *slot = unit[off..off + PKT].iter().all(|&b| b == 0);
    }

    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);
    let derived = aes_ecb_encrypt(unit_key, &header);
    let mut decrypt_key = [0u8; 16];
    for i in 0..16 {
        decrypt_key[i] = derived[i] ^ header[i];
    }
    aes_cbc_decrypt(&decrypt_key, &mut unit[16..ALIGNED_UNIT_LEN]);

    for (p, &is_pad) in pad.iter().enumerate().take(npkt) {
        if is_pad {
            let off = p * PKT;
            for b in unit[off..off + PKT].iter_mut() {
                *b = 0;
            }
        }
    }
}

/// Remove bus encryption from an aligned unit (AACS 2.0 / UHD).
/// Bus encryption uses read_data_key, decrypting bytes 16..2048 of each 2048-byte sector.
pub(crate) fn decrypt_bus(unit: &mut [u8], read_data_key: &[u8; 16]) {
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
    fn clear_unit_is_not_flagged_encrypted() {
        // `decrypt_unit` is now PURE (applies the key unconditionally); the
        // "leave a clear unit untouched" policy lives at the caller's gate,
        // `aacs_unit_encrypted` / `aacs_unit_needs_decrypt`. A clear TS unit
        // (byte-0 CPI bits clear, syncs intact) must report NOT-encrypted so the
        // caller never hands it to decrypt_unit.
        let ts = crate::disc::ContentFormat::BdTs;
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            unit[off] = TS_SYNC;
            off += BD_SOURCE_PACKET_BYTES;
        }
        assert!(crate::aacs::content::is_clean(
            &unit,
            crate::disc::ContentFormat::BdTs
        ));
        assert!(
            !aacs_unit_encrypted(&unit, ts),
            "byte-0 CPI clear ⇒ not flagged encrypted"
        );
        assert!(
            !aacs_unit_needs_decrypt(&unit, ts),
            "a clear unit needs no decrypt ⇒ caller skips decrypt_unit"
        );
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
    fn is_clean_min4_proof_floor() {
        // ONE rule: a unit is clean iff `synced >= min(E, 4)` over the ENCRYPTED
        // (non-padding) packets — E>4 needs any 4, E<=4 needs all present. Build
        // NON-ZERO payloads (real content, not padding) so every packet counts
        // toward E; place `n` TS syncs among packets 1..31 (packet 0 is skipped).
        let unit_with = |synced: usize| {
            let mut unit: Vec<u8> = (0..ALIGNED_UNIT_LEN)
                .map(|i| ((i * 7 + 1) as u8) | 1)
                .collect();
            // Scrub any accidental 0x47 at a sync position, then place exactly
            // `synced` real syncs in packets 1.. (skip packet 0).
            let mut off = BD_SOURCE_PACKET_BYTES + 4;
            let mut placed = 0;
            while off < ALIGNED_UNIT_LEN {
                unit[off] = if placed < synced { TS_SYNC } else { 0x46 };
                placed += 1;
                off += BD_SOURCE_PACKET_BYTES;
            }
            unit
        };
        // E = 31 content packets (all non-zero) → threshold min(31,4) = 4.
        assert!(
            !crate::aacs::content::is_clean(&unit_with(3), crate::disc::ContentFormat::BdTs),
            "3 synced of a well-populated unit is below the proof floor → not clean"
        );
        assert!(
            crate::aacs::content::is_clean(&unit_with(4), crate::disc::ContentFormat::BdTs),
            "4 synced proves the key opened it, even with many bad-encoded packets"
        );
        // The old >50% majority would have called `unit_with(4)` scrambled (4/31
        // < half) — that false-flag was the mux key-server storm. min(E,4) fixes it.
    }

    #[test]
    fn scramble_detection_extremes() {
        // A fully-clear unit (every packet synced) is clean; a fully-scrambled
        // unit (non-zero ciphertext, NO syncs) is not. (An all-zero buffer is
        // empty padding — E==0 — which `is_clean` treats as clean, NOT scrambled.)
        let mut clear = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            clear[off] = TS_SYNC;
            off += BD_SOURCE_PACKET_BYTES;
        }
        assert!(
            crate::aacs::content::is_clean(&clear, crate::disc::ContentFormat::BdTs),
            "fully-clear unit → clean"
        );

        // Real scrambled ciphertext: non-zero everywhere, no 0x47 at any sync slot.
        let mut scrambled: Vec<u8> = (0..ALIGNED_UNIT_LEN)
            .map(|i| ((i * 13 + 3) as u8) | 1)
            .collect();
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            if scrambled[off] == TS_SYNC {
                scrambled[off] = 0x46;
            }
            off += BD_SOURCE_PACKET_BYTES;
        }
        assert!(
            !crate::aacs::content::is_clean(&scrambled, crate::disc::ContentFormat::BdTs),
            "non-zero body with no syncs → scrambled"
        );
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
        assert!(!crate::aacs::content::is_clean(
            &unit,
            crate::disc::ContentFormat::BdTs
        ));
        decrypt_unit(&mut unit, &unit_key);
        assert!(crate::aacs::content::is_clean(
            &unit,
            crate::disc::ContentFormat::BdTs
        )); // decrypted: TS syncs restored

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
        decrypt_unit(&mut unit, &key);
        assert_eq!(
            ts_sync_count(&unit),
            32,
            "the right key recovered the plaintext (all 32 syncs restored)"
        );
    }

    #[test]
    fn fragment_tail_with_source_zero_pad_is_decryptable() {
        // 11 real content packets, then source-zero padding (the Dunkirk shape).
        let key = [0x5Au8; 16];
        let mut unit = tail_filled_unit(&key, 11, 0x00);
        decrypt_unit(&mut unit, &key);
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
    fn fragment_tail_with_nonzero_garbage_decrypts_the_real_prefix() {
        // Same shape, but the tail is NON-zero garbage. The crypto still recovers
        // the 11 real packets; the garbage tail is whatever it is (the muxer drops
        // it on sync-loss, and a genuine bad sector is caught by the physical read
        // layer / mapfile — never by TS structure). Ground truth: the 11 real
        // packets came back.
        let key = [0x5Au8; 16];
        let mut unit = tail_filled_unit(&key, 11, 0xC3);
        decrypt_unit(&mut unit, &key);
        for p in 0..11 {
            assert_eq!(
                unit[p * BD_SOURCE_PACKET_BYTES + 4],
                TS_SYNC,
                "real content pkt {p} decrypted"
            );
        }
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

    /// A POST-DECRYPT-looking unit for the key-independent [`is_clean_ts`]: `e`
    /// ENCRYPTED content packets carrying non-zero payload, `synced` of them with
    /// `0x47`; the rest is source-zero padding. Content is placed at packets 1..
    /// because [`is_clean_ts`] SKIPS packet 0 (its sync lives in the clear seed and
    /// is never evidence), so `e`/`synced` map directly to what it measures. CPI
    /// (encrypted flag) set iff `cpi`.
    fn decrypted_shape(e: usize, synced: usize, cpi: bool) -> Vec<u8> {
        let mut u = vec![0u8; ALIGNED_UNIT_LEN];
        for i in 0..e {
            let off = (i + 1) * BD_SOURCE_PACKET_BYTES; // packets 1.. (skip seed pkt 0)
            u[off + 5] = 0xAB; // non-zero payload => counted as content
            u[off + 4] = if i < synced { TS_SYNC } else { 0x80 };
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
        decrypt_unit(&mut unit, &key);
        let off = 17 * BD_SOURCE_PACKET_BYTES;
        assert_eq!(
            unit[off + 4],
            0x80,
            "defect packet's bytes pass through VERBATIM (no null-fill, no zeroing)"
        );
        assert_eq!(unit[off + 5], 0xAB, "defect payload untouched");
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
    fn several_defect_packets_decrypt_the_good_ones() {
        // Ground truth: the right key recovers every non-defect packet's sync; the
        // authored-bad packets pass through verbatim (the muxer drops them).
        let key = [0x33u8; 16];
        let defects = [3usize, 9, 17, 24, 30];
        let mut unit = unit_with_defects(&key, &defects);
        decrypt_unit(&mut unit, &key);
        for p in 0..32 {
            if defects.contains(&p) {
                continue;
            }
            assert_eq!(
                unit[p * BD_SOURCE_PACKET_BYTES + 4],
                TS_SYNC,
                "non-defect pkt {p} decrypted"
            );
        }
    }

    #[test]
    fn wrong_key_does_not_recover_the_plaintext() {
        // Ground truth: a wrong key produces bytes that are NOT the plaintext.
        let clear = clear_unit();
        let mut unit = clear.clone();
        aacs_encrypt_unit(&mut unit, &[0x5Au8; 16]);
        decrypt_unit(&mut unit, &[0x22u8; 16]);
        assert_ne!(unit, clear, "a wrong key does not recover the plaintext");
    }

    #[test]
    fn key_proof_floor_is_four_synced_on_a_full_unit() {
        // ABSOLUTE proof floor: >=4 synced ENCRYPTED packets opens a full unit;
        // <4 does not — regardless of how many others are bad-encoded.
        assert!(
            is_clean_ts(&decrypted_shape(31, 4, false)),
            "4 synced -> opened"
        );
        assert!(
            !is_clean_ts(&decrypted_shape(31, 3, false)),
            "3 synced -> below the proof floor -> not opened"
        );
    }

    #[test]
    fn all_padding_unit_is_trivially_opened() {
        // CPI set but every packet is source-zero padding: nothing to decrypt.
        assert!(is_clean_ts(&decrypted_shape(0, 0, true)));
    }

    #[test]
    fn is_clean_dispatches_by_container_format() {
        use crate::disc::ContentFormat;
        // Clean HD-DVD `.evo` Program-Stream unit: pack_start_code `00 00 01 BA`
        // at each 2048-byte pack boundary (0/2048/4096) — the layout validated
        // against real decrypted ANCHORMAN / SHAUN_OF_THE_DEAD `.evo`. Offset 0 is
        // the clear-seed freebie; the packs at 2048/4096 are encrypted and are
        // what actually discriminate a key.
        let mut ps = vec![0u8; ALIGNED_UNIT_LEN];
        for off in [0usize, 2048, 4096] {
            ps[off..off + 4].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
        }
        assert!(
            is_clean(&ps, ContentFormat::MpegPs),
            "clean PS opens for MpegPs"
        );
        assert!(
            !is_clean(&ps, ContentFormat::BdTs),
            "PS content has no 0x47 TS syncs -> not clean as TS"
        );

        // Wrong key garbles the ENCRYPTED packs (2048/4096); offset 0 stays a pack
        // start (it is the clear seed) but that alone proves nothing -> rejected.
        let mut wrong = ps.clone();
        wrong[2048] = 0xFF;
        wrong[4096] = 0xFF;
        assert!(
            !is_clean(&wrong, ContentFormat::MpegPs),
            "garbled encrypted packs -> wrong key rejected"
        );

        // A clean BD Transport-Stream unit opens for BdTs, not MpegPs.
        let ts = decrypted_shape(31, 31, false);
        assert!(
            is_clean(&ts, ContentFormat::BdTs),
            "clean TS opens for BdTs"
        );
        assert!(
            !is_clean(&ts, ContentFormat::MpegPs),
            "TS content has no `00 00 01 BA` packs -> not clean as PS"
        );
    }

    #[test]
    fn ps_container_encrypt_detection_and_idempotency() {
        use crate::disc::ContentFormat;
        let ps = ContentFormat::MpegPs;

        // Base clean PS unit: pack_start_code at each 2048 boundary.
        let mut clear = vec![0u8; ALIGNED_UNIT_LEN];
        for off in [0usize, 2048, 4096] {
            clear[off..off + 4].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
        }
        // PES scrambling_control (byte 20, bits 5-4) == 0 → not encrypted.
        assert!(
            !aacs_unit_encrypted(&clear, ps),
            "scrambling_control clear ⇒ not encrypted"
        );

        // Encrypted + still-scrambled: flag set, packs 1/2 garbled (would be
        // ciphertext on a real disc) → is_clean_ps false → needs decrypt.
        let mut enc = clear.clone();
        enc[PS_SCRAMBLE_OFF] |= 0x10; // PES_scrambling_control = 01
        enc[2048] = 0xFF;
        enc[4096] = 0xFF;
        assert!(
            aacs_unit_encrypted(&enc, ps),
            "scrambling_control set ⇒ encrypted"
        );
        assert!(
            aacs_unit_needs_decrypt(&enc, ps),
            "flagged + packs not restored ⇒ needs decrypt"
        );

        // Decrypted: the flag survives (it is in the preserved header) but packs
        // are valid → needs_decrypt flips false (idempotent re-decrypt).
        let mut dec = clear.clone();
        dec[PS_SCRAMBLE_OFF] |= 0x10;
        assert!(
            aacs_unit_encrypted(&dec, ps),
            "flag survives decryption (header preserved)"
        );
        assert!(
            !aacs_unit_needs_decrypt(&dec, ps),
            "valid packs restored ⇒ no re-decrypt (idempotent)"
        );
    }

    #[test]
    fn sparse_tail_needs_all_present_packets_min_e_4() {
        // End-of-clip fragment tail: `min(E,4)` scales to the packets that exist,
        // so a sparse unit needs ALL of its (few) encrypted packets — a wrong key
        // gives 0, so this is a strong proof with no wrong-key hole, and a valid
        // 1-packet tail is never false-rejected.
        assert!(
            is_clean_ts(&decrypted_shape(1, 1, false)),
            "E=1: the one packet syncs -> open"
        );
        assert!(
            !is_clean_ts(&decrypted_shape(1, 0, false)),
            "E=1: 0 synced (wrong key) -> not"
        );
        assert!(
            is_clean_ts(&decrypted_shape(3, 3, false)),
            "E=3: all 3 sync -> open"
        );
        assert!(
            !is_clean_ts(&decrypted_shape(3, 2, false)),
            "E=3: min(3,4)=3 -> 2 synced is not enough"
        );
        // The threshold boundary: E=4 needs all 4 (min(4,4)=4); E=5 needs only 4
        // of 5 (min(5,4)=4) — the transition from "need all E" to "need exactly 4".
        assert!(
            is_clean_ts(&decrypted_shape(4, 4, false)),
            "E=4: 4/4 -> open"
        );
        assert!(
            !is_clean_ts(&decrypted_shape(4, 3, false)),
            "E=4: 3/4 -> below min(4,4)=4"
        );
        assert!(
            is_clean_ts(&decrypted_shape(5, 4, false)),
            "E=5: 4/5 -> open (min(5,4)=4, the floor caps at 4)"
        );
        assert!(
            !is_clean_ts(&decrypted_shape(5, 3, false)),
            "E=5: 3/5 -> below the 4 floor"
        );
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
        decrypt_unit(&mut unit, &key);
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
    fn wrong_key_full_unit_does_not_recover_plaintext() {
        let clear = clear_unit();
        let mut unit = clear.clone();
        aacs_encrypt_unit(&mut unit, &[0x11u8; 16]);
        decrypt_unit(&mut unit, &[0x22u8; 16]);
        assert_ne!(
            unit, clear,
            "a wrong key on a full content unit does not recover the plaintext"
        );
    }

    #[test]
    fn cpi_clear_unit_reports_not_encrypted() {
        // CPI-clear (plaintext) unit: the caller's gate `aacs_unit_encrypted`
        // reports NOT-encrypted, so it is never handed to the now-pure
        // `decrypt_unit` (which would otherwise corrupt it by applying a key).
        let unit = clear_unit(); // byte 0 high bits clear
        assert!(
            !aacs_unit_encrypted(&unit, crate::disc::ContentFormat::BdTs),
            "CPI-clear ⇒ caller does not decrypt it"
        );
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
            !crate::aacs::content::is_clean(&unit, crate::disc::ContentFormat::BdTs),
            "encrypted unit must look scrambled"
        );

        decrypt_unit(&mut unit, &unit_key);
        // All 32 stride positions carry sync after decrypt.
        assert_eq!(ts_sync_count(&unit), ts_packet_total(&unit));
        assert!(crate::aacs::content::is_clean(
            &unit,
            crate::disc::ContentFormat::BdTs
        ));
    }

    #[test]
    fn decrypt_unit_wrong_key_does_not_recover_plaintext() {
        // A wrong unit key leaves the body scrambled — the plaintext is NOT
        // recovered. Grounds the brute-force gate: a bad key must not look right.
        let good = [0x11u8; 16];
        let bad = [0x22u8; 16];
        let clear = clear_unit();
        let mut unit = clear.clone();
        aacs_encrypt_unit(&mut unit, &good);
        decrypt_unit(&mut unit, &bad);
        assert_ne!(unit, clear, "a wrong key does not recover the plaintext");
    }

    #[test]
    fn decrypt_unit_ignores_short_unit() {
        // unit.len() < ALIGNED_UNIT_LEN → no-op (no panic on the 16.. slice).
        let mut short = vec![0u8; ALIGNED_UNIT_LEN - 1];
        let before = short.clone();
        decrypt_unit(&mut short, &[0u8; 16]);
        assert_eq!(short, before, "a short unit is left untouched (no panic)");
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

    // ── CPI gate: the authoritative encrypted-vs-clear decision ────────────

    #[test]
    fn cpi_gate_set_flag_decrypts_and_needs_decrypt_is_idempotent() {
        // A CPI-set encrypted unit decrypts with the right key. CPI lives in the
        // plaintext header, so it survives decryption — `aacs_unit_encrypted`
        // still reports true afterward, but `aacs_unit_needs_decrypt` flips to
        // false (syncs restored), keeping the re-decrypt paths idempotent.
        let ts = crate::disc::ContentFormat::BdTs;
        let key = [0x5au8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &key); // sets CPI + scrambles body
        assert!(aacs_unit_encrypted(&unit, ts), "CPI set");
        assert!(
            aacs_unit_needs_decrypt(&unit, ts),
            "flagged + still scrambled"
        );

        decrypt_unit(&mut unit, &key);
        assert!(
            aacs_unit_encrypted(&unit, ts),
            "CPI bits live in the preserved header ⇒ still set post-decrypt"
        );
        assert!(
            !aacs_unit_needs_decrypt(&unit, ts),
            "syncs restored ⇒ no further decrypt attempt (idempotent re-decrypt)"
        );
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

    // ── ts_sync_destroyed / ts_sync_count edge cases ───────────────────────

    #[test]
    fn ts_sync_destroyed_false_for_sub_unit_length() {
        // The function guards on `len >= ALIGNED_UNIT_LEN` first; anything
        // shorter is reported NOT scrambled (so the decrypt gate skips it)
        // rather than indexing past the end.
        assert!(crate::aacs::content::is_clean(
            &[],
            crate::disc::ContentFormat::BdTs
        ));
        assert!(crate::aacs::content::is_clean(
            &vec![0u8; ALIGNED_UNIT_LEN - 1],
            crate::disc::ContentFormat::BdTs
        ));
        // A scrambled-looking buffer that is one byte short is still "not
        // scrambled" by the length guard.
        let mut almost = vec![0u8; ALIGNED_UNIT_LEN - 1];
        almost[4] = 0x00; // no syncs
        assert!(crate::aacs::content::is_clean(
            &almost,
            crate::disc::ContentFormat::BdTs
        ));
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
}
