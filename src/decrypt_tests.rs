use super::*;

// HD DVD: a buffer of `n` packs, an NV_PCK (CPI naming Title Key 2) every 4th from pack
// `nav_at`, the rest audio packs encrypted per pack under `kt` (`[HD]` §4.3.2).
fn hd_buf(n: usize, nav_at: usize, kt: &[u8; 16]) -> (Vec<u8>, Vec<u8>) {
    use crate::aacs::hddvd::encrypt_pack;
    use crate::aacs::hddvd::tests::{audio_pack, cpi, nav_pack};
    let c = cpi(2, 7);
    let plain: Vec<u8> = (0..n)
        .flat_map(|i| match i >= nav_at && (i - nav_at).is_multiple_of(4) {
            true => nav_pack(&c),
            false => audio_pack(i as u8 | 1),
        })
        .collect();
    let mut enc = plain.clone();
    for (i, p) in enc.chunks_mut(2048).enumerate() {
        if !(i >= nav_at && (i - nav_at).is_multiple_of(4)) {
            encrypt_pack(p, kt, &c);
        }
    }
    (plain, enc)
}

fn hd_keys(keys: &[(u32, [u8; 16])]) -> DecryptKeys {
    DecryptKeys::Aacs {
        unit_keys: keys.to_vec(),
        format: crate::disc::ContentFormat::MpegPs,
    }
}

#[test]
fn hddvd_packs_decrypt_under_the_title_key_their_cpi_names() {
    let kt = [0x61; 16];
    let (plain, mut buf) = hd_buf(12, 0, &kt);
    let map = AacsKeyMap::from_ranges(vec![(0, 100, 0)]);
    let keys = hd_keys(&[(1, [0x99; 16]), (2, kt)]);
    let r = decrypt_hddvd_packs(&mut buf, &keys, 0, &map, None, None).unwrap();
    assert_eq!(buf, plain);
    assert_eq!(r.blanked, 0);
    assert_eq!(r.last_cpi.map(|c| c.title_key_ptr()), Some(2));
}

#[test]
fn hddvd_packs_before_the_first_nav_use_the_lead_cpi_or_are_blanked() {
    use crate::aacs::hddvd::tests::cpi;
    let kt = [0x61; 16];
    let map = AacsKeyMap::from_ranges(vec![(0, 100, 0)]);
    let keys = hd_keys(&[(2, kt)]);
    let (plain, enc) = hd_buf(9, 2, &kt);
    let mut buf = enc.clone();
    decrypt_hddvd_packs(&mut buf, &keys, 0, &map, None, Some(cpi(2, 7))).unwrap();
    assert_eq!(buf, plain);
    let mut buf = enc.clone();
    let r = decrypt_hddvd_packs(&mut buf, &keys, 0, &map, None, None).unwrap();
    assert_eq!(r.blanked, 2);
    assert!(buf[..2 * 2048].iter().all(|&b| b == 0));
    assert_eq!(buf[2 * 2048..], plain[2 * 2048..]);
}

#[test]
fn hddvd_ciphertext_no_held_key_opens_fails_loud() {
    let kt = [0x61; 16];
    let (_, enc) = hd_buf(8, 0, &kt);
    let map = AacsKeyMap::from_ranges(vec![(0, 100, 0)]);
    // Title Key 2 not held.
    let mut buf = enc.clone();
    let r = decrypt_hddvd_packs(&mut buf, &hd_keys(&[(1, kt)]), 0, &map, None, None);
    assert!(matches!(r, Err(crate::error::Error::DecryptFailed)));
    // Encrypted packs outside every keyed range.
    let mut buf = enc.clone();
    let outside = AacsKeyMap::from_ranges(vec![(500, 600, 0)]);
    let r = decrypt_hddvd_packs(&mut buf, &hd_keys(&[(2, kt)]), 0, &outside, None, None);
    assert!(matches!(r, Err(crate::error::Error::DecryptFailed)));
    // KEY_VF 01: a Segment Key, not held.
    let mut buf = enc.clone();
    buf[60] = 0x40;
    let r = decrypt_hddvd_packs(&mut buf, &hd_keys(&[(2, kt)]), 0, &map, None, None);
    assert!(matches!(r, Err(crate::error::Error::DecryptFailed)));
}

#[test]
fn hddvd_packs_outside_the_content_ranges_pass_through() {
    let kt = [0x61; 16];
    let (_, enc) = hd_buf(8, 0, &kt);
    let mut buf = enc.clone();
    let map = AacsKeyMap::from_ranges(vec![(0, 100, 0)]);
    decrypt_hddvd_packs(
        &mut buf,
        &hd_keys(&[(2, kt)]),
        0,
        &map,
        Some(&[(50, 10)]),
        None,
    )
    .unwrap();
    assert_eq!(buf, enc);
}

/// Build a clear-TS region: a 0x47 sync byte at offset 4 of every 192-byte
/// BD-TS packet (matching `ts_sync_count`'s probe stride), filler elsewhere.
/// Reads as NOT scrambled.
fn clear_ts_region(len: usize) -> Vec<u8> {
    let mut v: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31)).collect();
    let mut off = 4;
    while off < len {
        v[off] = 0x47;
        off += 192;
    }
    v
}

/// Build a scrambled region: the 192-byte-stride sync positions are NOT
/// 0x47 (encrypted content destroys them), so it reads as scrambled.
fn scrambled_region(len: usize) -> Vec<u8> {
    let mut v: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31)).collect();
    let mut off = 4;
    while off < len {
        // Force a non-sync byte at every probe position.
        v[off] = 0xA5;
        off += 192;
    }
    // Flag every aligned unit's CPI bits (byte 0) so it reads as encrypted
    // under the authoritative `aacs_unit_encrypted`/`aacs_unit_needs_decrypt`
    // gate — real encrypted content always carries these.
    let mut u = 0;
    while u < len {
        v[u] |= 0xC0;
        u += aacs::content::ALIGNED_UNIT_LEN;
    }
    v
}

// ── `decrypt_sectors_in_content` (now a legacy alias of `decrypt_sectors`) ──

// `DecryptKeys::None` is a no-op; a forensic read plan is NOT the extents it was given —
// the mux's provenance guard's precondition.
#[test]
fn a_forensic_read_plan_drops_units_the_full_extents_include() {
    let full = vec![crate::disc::Extent {
        start_lba: 1000,
        sector_count: 60,
    }];

    // No forensic segment: the plan IS the extents, byte for byte, so
    // provenance stays trustworthy on an ordinary disc.
    let plain = AacsKeyMap::from_ranges_phased(vec![(1000, 1060, 5, Phase::All)]);
    assert_eq!(
        plain.read_plan(&full, 3),
        full,
        "a non-forensic map must return the extents unchanged"
    );

    // With alternate phases, units are omitted — fewer sectors are read
    // than the spans describe.
    let phased = AacsKeyMap::from_ranges_phased(vec![(1000, 1060, 5, Phase::Even)]);
    let plan = phased.read_plan(&full, 3);
    let planned: u32 = plan.iter().map(|e| e.sector_count).sum();
    let whole: u32 = full.iter().map(|e| e.sector_count).sum();
    assert!(
        planned < whole,
        "a forensic segment must drop units: planned {planned} of {whole}"
    );
    assert_ne!(
        plan, full,
        "the plan differs from the extents, which is exactly what the mux \
             detects before deciding whether a byte offset means anything"
    );
}

#[test]
fn content_gate_none_keys_is_noop() {
    let mut keys = DecryptKeys::None;
    let original = scrambled_region(aacs::content::ALIGNED_UNIT_LEN);
    let mut buf = original.clone();
    let dropped = decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 3)]).unwrap();
    assert_eq!(dropped, 0);
    assert_eq!(buf, original);
}

// CSS ignores the content gate (it lives in the AACS arm) and always reports `0` — the read
// stays scheme-agnostic.
#[test]
fn content_gate_css_keys_is_noop() {
    let mut keys = DecryptKeys::Css { title_key: [0; 5] };
    let mut buf = vec![0u8; 2048];
    let dropped = decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 3)]).unwrap();
    assert_eq!(
        dropped, 0,
        "CSS arm returns 0; content gate is a no-op for CSS"
    );
}

// `decrypt_sectors_in_content` is the live read path for every mapped rip and must actually
// DECRYPT — the `_is_noop` tests above only pin `0`, which an `Ok(0)` stub also returns.
#[test]
fn content_gate_css_actually_descrambles_the_buffer() {
    const RUN_START: usize = 0x59;
    const SEED_OFFSET: usize = 0x54;
    const PERIOD: usize = 8;
    let title_key = [0x11u8, 0x22, 0x33, 0x44, 0x55];

    let mut plaintext = vec![0u8; 2048];
    plaintext[0x00..0x04].copy_from_slice(&css::PACK_START);
    plaintext[4] = 0x44; // '01': a 13818-1 pack
    plaintext[0x14] = 0x10; // CSS scramble flag (DVD-Video sector header)
    crate::css::dvd_pack_header(&mut plaintext, 0xE0);
    let pat: Vec<u8> = (0..PERIOD)
        .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
        .collect();
    for (i, b) in plaintext.iter_mut().enumerate().skip(RUN_START) {
        *b = pat[i % PERIOD];
    }
    plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05]);

    let mut buf = plaintext.clone();
    css::lfsr::scramble_sector(&title_key, &mut buf);
    let ciphertext = buf.clone();
    assert_ne!(
        &ciphertext[0x80..],
        &plaintext[0x80..],
        "fixture malformed — the sector was not actually scrambled"
    );

    let mut keys = DecryptKeys::Css { title_key };
    decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 1)])
        .expect("CSS descramble must not fail");

    // Report the first differing offset rather than dumping 1.9 KB.
    let mismatch = (0x80..2048).find(|&i| buf[i] != plaintext[i]);
    assert!(
        mismatch.is_none(),
        "the scrambled body must come back as the plaintext it was built \
             from; first mismatch at offset {mismatch:?} (buf={:#04x} \
             expected={:#04x}) — a wrapper that decrypts nothing leaves the \
             ciphertext in place and the caller muxes scrambled MPEG",
        buf[mismatch.unwrap_or(0x80)],
        plaintext[mismatch.unwrap_or(0x80)],
    );
}

// The AACS arm of the same entry point must fail LOUD: reaching it means a reader was built
// without installing its key map, and must not be softened into a success with a zero
// count.
#[test]
fn content_gate_aacs_keys_fail_loud_not_ok_zero() {
    let mut keys = DecryptKeys::Aacs {
        unit_keys: vec![(1, [0xAB; 16])],
        format: crate::disc::ContentFormat::BdTs,
    };
    let original = scrambled_region(aacs::content::ALIGNED_UNIT_LEN);
    let mut buf = original.clone();
    let r = decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 3)]);
    assert!(
        matches!(r, Err(crate::error::Error::DecryptFailed)),
        "AACS without an installed key map must be DecryptFailed, got {r:?}"
    );
    assert_eq!(
        buf, original,
        "and it must not have half-decrypted the buffer on the way out"
    );
}

// Build a crackable scrambled CSS sector: a periodic run in the clear
// header continues past 0x80 so `crack_title_key` recovers the key.
// Distinct `seed`s give two sectors different cribs (two VOB regions).
fn crackable_css_sector(title_key: &[u8; 5], seed: &[u8; 5]) -> Vec<u8> {
    const RUN_START: usize = 0x59;
    const SEED_OFFSET: usize = 0x54;
    const PERIOD: usize = 8;
    let mut plaintext = vec![0u8; 2048];
    plaintext[0x00..0x04].copy_from_slice(&css::PACK_START);
    plaintext[4] = 0x44; // '01': a 13818-1 pack
    plaintext[0x14] = 0x10; // scramble flag
    crate::css::dvd_pack_header(&mut plaintext, 0xE0);
    let pat: Vec<u8> = (0..PERIOD)
        .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
        .collect();
    for (i, b) in plaintext.iter_mut().enumerate().skip(RUN_START) {
        *b = pat[i % PERIOD];
    }
    plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(seed);
    css::lfsr::scramble_sector(title_key, &mut plaintext);
    plaintext
}

// CHARACTERIZATION: the CSS arm's per-region re-crack when the cached title key goes stale
// at a VOB region boundary. Delicate logic the recovery refactor moves next.
#[test]
fn css_region_change_recracks_the_title_key() {
    let key_a = [0x11, 0x22, 0x33, 0x44, 0x55];
    let key_b = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
    let sector_a = crackable_css_sector(&key_a, &[0x01, 0x02, 0x03, 0x04, 0x05]);
    let sector_b = crackable_css_sector(&key_b, &[0x09, 0x08, 0x07, 0x06, 0x05]);

    // Expected plaintext bodies: each sector descrambled under its true key.
    let mut plain_a = sector_a.clone();
    css::lfsr::descramble_sector(&key_a, &mut plain_a);
    let mut plain_b = sector_b.clone();
    css::lfsr::descramble_sector(&key_b, &mut plain_b);

    let mut buf = Vec::with_capacity(4096);
    buf.extend_from_slice(&sector_a);
    buf.extend_from_slice(&sector_b);

    // Cache primed to region A's key (as if A was the last crack). CSS
    // descramble-and-rekey lives in `css::descramble_region` (the recovery
    // seam calls it); the region change must re-crack region B's key.
    let mut ended = key_a;
    css::descramble_region(&mut buf, &mut ended).expect("descramble");

    assert_eq!(
        &buf[0x80..2048],
        &plain_a[0x80..2048],
        "sector 0 rides the cached key (crib matches, no re-crack)"
    );
    assert_eq!(
        &buf[2048 + 0x80..4096],
        &plain_b[0x80..2048],
        "sector 1 re-cracks its own region key and descrambles correctly"
    );
    // The cache must have advanced to a key that descrambles region B.
    let mut check_b = sector_b.clone();
    css::lfsr::descramble_sector(&ended, &mut check_b);
    assert_eq!(
        &check_b[0x80..2048],
        &plain_b[0x80..2048],
        "the ended cache key must round-trip region B's body"
    );
}

// Whole leading unit + a SCRAMBLED trailing partial flagged encrypted: an
// encrypted unit split across an extent boundary cannot be CBC-decrypted
// standalone, so mapped decrypt must fail loud, not emit it as clear.
#[test]
fn aacs_scrambled_trailing_partial_is_rejected() {
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, [0xAB; 16])],
        format: crate::disc::ContentFormat::BdTs,
    };
    // One CLEAR leading unit (passes through) + a 4096-byte (two-sector) tail
    // whose seed byte flags it encrypted, inside the mapped range.
    let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
    let mut tail = scrambled_region(4096);
    tail[0] |= 0xC0; // CPI bits → flagged encrypted on the partial
    buf.extend_from_slice(&tail);

    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
    let err = decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
        .expect_err("scrambled encrypted trailing partial must be rejected");
    assert_eq!(
        err.code(),
        crate::error::Error::DecryptFailed.code(),
        "scrambled trailing partial must fail with DecryptFailed"
    );
}

// A CLEAR trailing partial is legitimate content and must pass through byte-for-byte, not
// just return `Ok` — a corrupting mutant that still returns `Ok` must fail this.
#[test]
fn aacs_clear_trailing_partial_passes_through() {
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, [0xAB; 16])],
        format: crate::disc::ContentFormat::BdTs,
    };
    let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
    let mut tail = clear_ts_region(4096);
    tail[0] &= 0x3F; // ensure the CPI bits are clear
    buf.extend_from_slice(&tail);
    let snapshot = buf.clone();
    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
    decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
        .expect("a clear trailing partial is legitimate content");
    assert_eq!(
        buf, snapshot,
        "a clear trailing partial must pass through byte-for-byte, not just return Ok"
    );
}

// ── DecryptKeys::None and is_encrypted ─────────────────────────────────

// DecryptKeys::None is a pure no-op: the buffer must return byte-for-byte
// unchanged with Ok. The `None => {}` arm does nothing.
#[test]
fn none_keys_is_noop() {
    let mut buf: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
    let snapshot = buf.clone();
    decrypt_sectors(&mut buf, &mut DecryptKeys::None, 0).expect("None is always Ok");
    assert_eq!(buf, snapshot, "None must not touch the buffer");
}

// is_encrypted reflects the variant: None -> false, Css/Aacs -> true.
// Grounding: `!matches!(self, DecryptKeys::None)`.
#[test]
fn is_encrypted_matches_variant() {
    assert!(!DecryptKeys::None.is_encrypted());
    assert!(DecryptKeys::Css { title_key: [0; 5] }.is_encrypted());
    assert!(
        DecryptKeys::Aacs {
            unit_keys: vec![(0, [0; 16])],
            format: crate::disc::ContentFormat::BdTs,
        }
        .is_encrypted()
    );
}

// ── CSS dispatch (DecryptKeys::Css) ────────────────────────────────────

// Build a CSS-scrambled sector via `scramble_sector` (the true inverse
// of `descramble_sector`), so decrypt_sectors descrambles it back.
fn make_css_sector(title_key: &[u8; 5], seed: &[u8; 5], body_fill: u8) -> (Vec<u8>, Vec<u8>) {
    let mut sector = vec![body_fill; 2048];
    // A real scrambled DVD sector is an MPEG-2 PS pack starting with the pack
    // start code; the descrambler requires it before trusting byte 0x14. Without
    // it the fixture is a shape that can't occur on disc, masking a rejecting gate.
    sector[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    sector[4] = 0x44; // '01': a 13818-1 pack
    sector[0x0D] = 0xF8; // pack_stuffing_length 0
    sector[0x14] = 0x30; // scramble flag (bits 4-5)
    crate::css::dvd_pack_header(&mut sector, 0xE0);
    sector[0x54..0x59].copy_from_slice(seed);
    let plaintext = sector.clone();
    css::lfsr::scramble_sector(title_key, &mut sector);
    (sector, plaintext)
}

// The CSS path descrambles each 2048-byte sector with the title key: a
// scrambled sector run through decrypt_sectors must come back to its
// plaintext body, proving the title key is actually applied.
#[test]
fn css_descrambles_with_title_key() {
    let mut title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    let seed = [0xDE, 0xAD, 0xBE, 0xEF, 0x42];
    let (mut sector, plaintext) = make_css_sector(&title_key, &seed, 0xA5);
    // CSS descramble lives in `css::descramble_region` (the recovery seam
    // calls it); `decrypt_sectors` only flags CSS sectors for recovery.
    css::descramble_region(&mut sector, &mut title_key).expect("descramble");
    assert_eq!(
        &sector[0x80..2048],
        &plaintext[0x80..2048],
        "CSS body must round-trip to plaintext"
    );
    // Flag cleared by the descrambler.
    assert_eq!(
        sector[0x14] & 0x30,
        0,
        "scramble flag cleared after CSS decrypt"
    );
}

// The CSS path processes EACH 2048-byte sector independently: two
// scrambled sectors in one buffer must both round-trip, pinning that
// the loop reaches every sector, not just the first.
#[test]
fn css_processes_every_sector_in_buffer() {
    let title_key = [0x01, 0x02, 0x03, 0x04, 0x05];
    let (s0, p0) = make_css_sector(&title_key, &[0x11, 0x22, 0x33, 0x44, 0x55], 0x3C);
    let (s1, p1) = make_css_sector(&title_key, &[0x66, 0x77, 0x88, 0x99, 0xAA], 0xC3);
    let mut buf = s0;
    buf.extend_from_slice(&s1);
    let mut title_key = title_key;
    css::descramble_region(&mut buf, &mut title_key).expect("descramble");
    assert_eq!(
        &buf[0x80..2048],
        &p0[0x80..2048],
        "sector 0 body must round-trip"
    );
    assert_eq!(
        &buf[2048 + 0x80..4096],
        &p1[0x80..2048],
        "sector 1 body must round-trip (loop must reach the 2nd sector)"
    );
}

// Build a CSS sector whose clear header ends in a periodic run that
// continues into the encrypted region, so `crack_title_key` can recover
// a key from it. Returns (scrambled_sector, plaintext_body).
fn make_crackable_css_sector(
    title_key: &[u8; 5],
    seed: &[u8; 5],
    period: usize,
) -> (Vec<u8>, Vec<u8>) {
    let mut plaintext = vec![0u8; 2048];
    // Real scrambled DVD sectors are MPEG-2 PS packs; the scramble policy
    // requires the pack start code as well as the flag bits.
    plaintext[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    plaintext[4] = 0x44; // '01': a 13818-1 pack
    plaintext[0x14] = 0x10; // scramble flag
    crate::css::dvd_pack_header(&mut plaintext, 0xE0);
    // Periodic run from 0x59 (just above the seed) through 0x80 and on into
    // the encrypted region; phase anchored to offset 0 so it is continuous
    // across the 0x80 boundary.
    let pat: Vec<u8> = (0..period)
        .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
        .collect();
    for (i, b) in plaintext.iter_mut().enumerate().skip(0x59) {
        *b = pat[i % period];
    }
    plaintext[0x54..0x59].copy_from_slice(seed); // seed sits below the run
    let body = plaintext.clone();
    css::lfsr::scramble_sector(title_key, &mut plaintext);
    (plaintext, body)
}

// CSS keys are per-VTS/VOB region: must re-crack when the cached key stops descrambling,
// not blindly reapply it — the bug that pixelated every DVD rip.
#[test]
fn css_rekeys_when_title_key_region_changes() {
    let key_a = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    let key_b = [0x07, 0x5A, 0xC3, 0x10, 0x88]; // a DIFFERENT region's key
    let (s0, p0) = make_crackable_css_sector(&key_a, &[0x11, 0x22, 0x33, 0x44, 0x55], 4);
    let (s1, p1) = make_crackable_css_sector(&key_b, &[0x66, 0x77, 0x88, 0x99, 0xAA], 4);
    // Precondition: each sector must be crackable on its own (the rekey
    // depends on it). If this fails the fixture, not the path, is at fault.
    assert_eq!(
        crate::css::keyless::crack_title_key(&s0),
        Some(key_a),
        "fixture s0 must crack to key_a standalone"
    );
    assert_eq!(
        crate::css::keyless::crack_title_key(&s1),
        Some(key_b),
        "fixture s1 must crack to key_b standalone"
    );
    let mut buf = s0;
    buf.extend_from_slice(&s1);

    // Cache primed to key_a only — exactly what the one-shot scan crack yields.
    let mut title_key = key_a;
    css::descramble_region(&mut buf, &mut title_key).expect("descramble");

    assert_eq!(
        &buf[0x80..2048],
        &p0[0x80..2048],
        "region A sector descrambles with the cached (primed) key"
    );
    assert_eq!(
        &buf[2048 + 0x80..4096],
        &p1[0x80..2048],
        "region B sector must descramble after the path re-cracks its own key"
    );
    // The cache must have advanced to region B's key.
    assert_eq!(
        title_key, key_b,
        "cache must hold region B's key after the rekey"
    );
}

// The CSS path leaves UNSCRAMBLED sectors (flag clear) byte-for-byte
// untouched — descramble_sector early-returns on a zero flag; a clear
// sector mixed into the buffer must not be corrupted.
#[test]
fn css_leaves_clear_sector_unchanged() {
    let title_key = [0x01, 0x02, 0x03, 0x04, 0x05];
    let mut sector = vec![0x77u8; 2048];
    sector[0x14] = 0x00; // not scrambled
    let snapshot = sector.clone();
    let mut keys = DecryptKeys::Css { title_key };
    decrypt_sectors(&mut sector, &mut keys, 0).unwrap();
    assert_eq!(sector, snapshot, "clear CSS sector must be left untouched");
}

// CSS decrypt always returns Ok (it cannot fail — descrambling is XOR,
// no key validity check), even for an empty buffer.
#[test]
fn css_empty_buffer_is_ok() {
    let mut buf: Vec<u8> = Vec::new();
    let mut keys = DecryptKeys::Css { title_key: [0; 5] };
    assert!(decrypt_sectors(&mut buf, &mut keys, 0).is_ok());
}

// ── AACS unit-key index selection ──────────────────────────────────────

// A map that selects a key index OUTSIDE the held pool must fail loud,
// validating `decrypt_sectors_mapped`'s up-front bounds check.
#[test]
fn aacs_mapped_out_of_range_key_idx_errors() {
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, [0xAB; 16])],
        format: crate::disc::ContentFormat::BdTs,
    };
    let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 5)]); // idx 5, pool holds 1 key
    let err = decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
        .expect_err("map index 5 is out of range for a 1-key pool");
    assert_eq!(
        err.code(),
        crate::error::Error::DecryptFailed.code(),
        "out-of-range mapped key index must be DecryptFailed"
    );
}

/// A non-empty map over an EMPTY unit_keys pool has no key to satisfy its
/// selected index → DecryptFailed (via the same bounds check).
#[test]
fn aacs_mapped_empty_unit_keys_errors() {
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![],
        format: crate::disc::ContentFormat::BdTs,
    };
    let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
    let err = decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
        .expect_err("empty unit_keys cannot satisfy map idx 0");
    assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
}

/// SAFETY NET: reaching the CSS/`None` wrapper (`decrypt_sectors`) with AACS
/// keys means a reader was built with no map — a bug. It must fail loud, never
/// apply a guessed key. (AACS decrypts exclusively via `decrypt_sectors_mapped`.)
#[test]
fn aacs_via_unmapped_decrypt_sectors_fails_loud() {
    let mut keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, [0xAB; 16])],
        format: crate::disc::ContentFormat::BdTs,
    };
    let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
    let err = decrypt_sectors(&mut buf, &mut keys, 0)
        .expect_err("AACS through the unmapped path must fail loud");
    assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
}

// ── Multi-CPS-unit key selection ──────────────────────────────────────

/// Encrypt an aligned unit so `aacs::content::decrypt_unit` with the same key
/// recovers the plaintext, flagging it encrypted first (bytes 0..16 are the key
/// seed, so the flag must be set before the crypto runs).
fn aacs_encrypt_unit_for_test(unit: &mut [u8], unit_key: &[u8; 16]) {
    unit[0] |= 0xC0;
    assert!(
        aacs::content::encrypt_unit(unit, unit_key),
        "a full-length unit must encrypt"
    );
}

/// Build a clear aligned unit with TS sync bytes placed at the BD-TS stride
/// (offset 4 + k*192) so `is_clean` reports true and
/// `decrypt_unit` verifies it as clear after decryption.
fn clear_ts_unit() -> Vec<u8> {
    let mut unit = vec![0u8; aacs::content::ALIGNED_UNIT_LEN];
    let mut off = 4;
    while off < aacs::content::ALIGNED_UNIT_LEN {
        unit[off] = 0x47;
        off += 192;
    }
    unit
}

// A `SectorSource` that returns a fixed wire buffer for any read (the bytes a
// bus-encrypted drive would put on the transport), reporting the full span —
// the input to the drive-owned bus-removal stream in the ordering tests below.
struct WireSource {
    bytes: Vec<u8>,
}
impl crate::sector::SectorSource for WireSource {
    fn capacity_sectors(&self) -> u32 {
        (self.bytes.len() / 2048) as u32
    }
    fn read_sectors(
        &mut self,
        _lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        let n = (count as usize * 2048).min(self.bytes.len());
        buf[..n].copy_from_slice(&self.bytes[..n]);
        Ok(n)
    }
}

// AACS 2.0 bus-then-unit ordering, proven at the SINGLE de-bus point: the
// drive-owned bus-removal stream strips the bus layer AT READ TIME, then the
// downstream mapped decrypt strips the unit-key layer (no bus key threaded).
#[test]
fn bus_removal_then_mapped_unit_decrypt_recovers_plaintext_end_to_end() {
    use crate::sector::SectorSource;
    use crate::sector::bus_removal::{BusRemovalSectorSource, BusStage};

    let unit_key = [0x5Au8; 16];
    let rdk = [0x91u8; 16];
    let mut clear = clear_ts_unit();
    clear[0] |= 0xC0; // the encrypted flag lives in the clear seed
    // Inner layer: unit-key encryption. Outer layer: drive bus-encryption —
    // exactly the bytes a bus-encrypted drive puts on the wire.
    let mut wire = clear.clone();
    aacs_encrypt_unit_for_test(&mut wire, &unit_key);
    aacs::content::encrypt_bus(&mut wire, &rdk);

    // Read the wire through the drive-owned bus-removal stream (host-key
    // stage): the ONE place bus decryption is applied.
    let mut src =
        BusRemovalSectorSource::new(WireSource { bytes: wire }, BusStage::AacsHostKey(rdk));
    let mut buf = vec![0u8; aacs::content::ALIGNED_UNIT_LEN];
    let n = src.read_sectors(0, 3, &mut buf, false).unwrap();
    assert_eq!(n, aacs::content::ALIGNED_UNIT_LEN);
    // Bus removed, but the CPS unit-key layer is still in place — not yet clear.
    assert!(
        !aacs::content::is_clean(&buf, crate::disc::ContentFormat::BdTs),
        "bus removed at read, but the CPS unit-key layer must still be scrambled"
    );

    // The downstream mapped decrypt applies ONLY the unit key over the
    // already-de-bussed content.
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, unit_key)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
    decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
        .expect("unit-key decrypt over de-bussed content must succeed");
    assert_eq!(
        buf,
        aacs::content::cpi_cleared(clear),
        "bus removal (at read) then unit-key decrypt must recover the plaintext exactly"
    );
}

// Necessity: the SAME wire bytes read through a PASSTHROUGH stage keep the bus
// layer, so unit-key decrypt alone runs over bus-encrypted data and cannot
// recover clear — the bus key is genuinely required, and de-bus precedes it.
#[test]
fn without_bus_removal_mapped_unit_decrypt_cannot_clear_bus_encrypted_content() {
    use crate::sector::SectorSource;
    use crate::sector::bus_removal::{BusRemovalSectorSource, BusStage};

    let unit_key = [0x5Au8; 16];
    let rdk = [0x91u8; 16];
    let mut clear = clear_ts_unit();
    clear[0] |= 0xC0;
    let mut wire = clear.clone();
    aacs_encrypt_unit_for_test(&mut wire, &unit_key);
    aacs::content::encrypt_bus(&mut wire, &rdk);

    // Passthrough: no bus key, so the wire is delivered still-bus-encrypted.
    let mut src = BusRemovalSectorSource::new(WireSource { bytes: wire }, BusStage::Passthrough);
    let mut buf = vec![0u8; aacs::content::ALIGNED_UNIT_LEN];
    src.read_sectors(0, 3, &mut buf, false).unwrap();
    // Snapshot the still-bus-encrypted wire as delivered, so we can prove the
    // unit-key decrypt actually TOUCHED the buffer (not a silent no-op) — a
    // bare `!= clear` also passes on an untouched buffer.
    let wire_delivered = buf.clone();

    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, unit_key)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
    // Phase::All never runs the forensic verify, so this returns Ok either
    // way; the point is the BYTES are not the plaintext without bus removal.
    let _ = decrypt_sectors_mapped(&mut buf, &keys, 0, &map);
    assert_ne!(
        buf, clear,
        "without bus removal first, unit-key decrypt alone cannot recover clear"
    );
    assert_ne!(
        buf, wire_delivered,
        "unit-key decrypt must have actually run over the buffer (not a no-op); \
             it just can't recover clear without bus removal first"
    );
}

// ── FMTS phase-aware map ──────────────────────────────────────────────────

/// `entry_for` returns Some((idx, phase, range_start)) inside a range;
/// `from_ranges` is All, `from_ranges_phased` carries the phase; an uncovered
/// LBA is `None` (pass through).
#[test]
fn aacskeymap_phase_entry_for() {
    let all = AacsKeyMap::from_ranges(vec![(100, 200, 3)]);
    assert_eq!(all.entry_for(150), Some((3, Phase::All, 100)));
    assert_eq!(all.entry_for(50), None);

    let phased = AacsKeyMap::from_ranges_phased(vec![(100, 200, 3, Phase::Odd)]);
    assert_eq!(phased.entry_for(150), Some((3, Phase::Odd, 100)));
    assert_eq!(phased.entry_for(250), None);
    assert_eq!(phased.key_idx_for(150), Some(3));
}

/// A map with no forensic (Even/Odd) range is the common disc: `read_plan`
/// returns the extents unchanged, so nothing but FMTS is affected.
#[test]
fn read_plan_non_forensic_is_unchanged() {
    use crate::disc::Extent;
    let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3
    let ext = vec![
        Extent {
            start_lba: 1000,
            sector_count: 300,
        },
        Extent {
            start_lba: 5000,
            sector_count: 60,
        },
    ];
    // A non-forensic map (empty, or multi-CPS All) leaves the plan untouched.
    assert_eq!(AacsKeyMap::from_ranges(vec![]).read_plan(&ext, us), ext);
    let multi = AacsKeyMap::from_ranges(vec![(1000, 1150, 2)]);
    assert_eq!(multi.read_plan(&ext, us), ext);
}

// FMTS: a forensic Even segment drops exactly its alternate (odd) units,
// while default content stays one coalesced sequential run.
#[test]
fn read_plan_forensic_reads_only_our_phase_units() {
    use crate::disc::Extent;
    let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3
    // One extent, 100 units [1000, 1300). A 10-unit Even forensic segment at
    // LBA [1030, 1060): kept even units are ix 0,2,4,6,8 → LBA 1030,1036,1042,
    // 1048,1054; dropped odd units → 1033,1039,1045,1051,1057.
    let ext = vec![Extent {
        start_lba: 1000,
        sector_count: 300,
    }];
    let map = AacsKeyMap::from_ranges_phased(vec![(1030, 1060, 5, Phase::Even)]);
    let plan = map.read_plan(&ext, us);
    let expected = vec![
        Extent {
            start_lba: 1000,
            sector_count: 33,
        }, // 1000..1030 default + the ix-0 even unit at 1030
        Extent {
            start_lba: 1036,
            sector_count: 3,
        },
        Extent {
            start_lba: 1042,
            sector_count: 3,
        },
        Extent {
            start_lba: 1048,
            sector_count: 3,
        },
        Extent {
            start_lba: 1054,
            sector_count: 3,
        },
        Extent {
            start_lba: 1060,
            sector_count: 240,
        }, // default resumes, coalesced to the extent end
    ];
    assert_eq!(plan, expected);
    // Exactly the 5 odd units (15 sectors) are omitted; nothing else.
    let kept: u32 = plan.iter().map(|e| e.sector_count).sum();
    assert_eq!(
        kept,
        300 - 5 * us,
        "only the alternate-phase units are dropped"
    );
    // Every kept LBA is one the decrypt loop would decrypt (All or our parity),
    // and no dropped LBA is: the plan and the decrypt gate agree unit-for-unit.
    for e in &plan {
        let mut off = 0;
        while off < e.sector_count {
            let lba = e.start_lba + off;
            if let Some((_, phase @ (Phase::Even | Phase::Odd), rs)) = map.entry_for(lba) {
                let is_odd = ((lba - rs) / us) % 2 == 1;
                assert!(
                    is_odd == matches!(phase, Phase::Odd),
                    "plan kept an alternate-phase unit at LBA {lba}"
                );
            }
            off += us;
        }
    }
}

// A forensic range does NOT start on an aligned-unit boundary, so `unit_ix = (lba -
// range_start) / us` must still get the parity right for an unaligned `range_start`.
#[test]
fn read_plan_phase_parity_is_measured_from_an_unaligned_range_start() {
    use crate::disc::Extent;
    let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3

    // Case A — range_start is itself unaligned (1001 % 3 == 2), so unit offsets
    // are 0, 3, 6, ... Under this shape `(lba + range_start) / us` shifts every
    // index by an ODD amount and inverts the kept half.
    let ext = vec![Extent {
        start_lba: 1001,
        sector_count: 12,
    }];
    let map = AacsKeyMap::from_ranges_phased(vec![(1001, 1013, 5, Phase::Even)]);
    assert_eq!(
        map.read_plan(&ext, us),
        vec![
            Extent {
                start_lba: 1001,
                sector_count: 3
            }, // ix 0, even
            Extent {
                start_lba: 1007,
                sector_count: 3
            }, // ix 2, even
        ],
        "unit index must be measured as (lba - range_start), so the kept \
             units are the even-indexed ones counting from the range start"
    );

    // Case B — the extent begins one unit-remainder from the range start (1001 -
    // 1000 = 1), so offsets are 1, 4, 7, 10. Here `(lba - range_start) * us`
    // inverts the halves: multiplying only preserves parity for aligned offsets.
    let ext = vec![Extent {
        start_lba: 1001,
        sector_count: 12,
    }];
    let map = AacsKeyMap::from_ranges_phased(vec![(1000, 1013, 5, Phase::Even)]);
    assert_eq!(
        map.read_plan(&ext, us),
        vec![
            Extent {
                start_lba: 1001,
                sector_count: 3
            }, // (1001-1000)/3 = 0, even
            Extent {
                start_lba: 1007,
                sector_count: 3
            }, // (1007-1000)/3 = 2, even
        ],
        "the offset must be DIVIDED by the unit size to become a unit index"
    );
}

// An extent whose last whole unit is an alternate-phase unit must still drop it — the tail
// guard is only for a REMNANT shorter than a unit.
#[test]
fn read_plan_gates_the_last_whole_unit_of_an_extent_not_just_the_remnant() {
    use crate::disc::Extent;
    let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3

    // Two whole units, no remnant. ix 0 is even (kept), ix 1 is odd (dropped)
    // and it is the LAST thing in the extent.
    let ext = vec![Extent {
        start_lba: 1000,
        sector_count: 6,
    }];
    let map = AacsKeyMap::from_ranges_phased(vec![(1000, 1006, 5, Phase::Even)]);
    assert_eq!(
        map.read_plan(&ext, us),
        vec![Extent {
            start_lba: 1000,
            sector_count: 3
        }],
        "the trailing odd-phase unit is a whole unit and must be dropped; the \
             short-tail guard is for a remnant SMALLER than a unit"
    );

    // And the remnant case the guard is actually for: 4 sectors = one whole
    // unit plus a 1-sector tail. The tail is ordinary content and is kept
    // even though the unit before it was dropped.
    let ext = vec![Extent {
        start_lba: 1000,
        sector_count: 4,
    }];
    let map = AacsKeyMap::from_ranges_phased(vec![(1000, 1004, 5, Phase::Odd)]);
    assert_eq!(
        map.read_plan(&ext, us),
        vec![Extent {
            start_lba: 1003,
            sector_count: 1
        }],
        "a sub-unit remnant is ordinary content and is always read"
    );
}

// Every scheme that CANNOT prove a key answers the same way — the property `decrypt_span`
// exists to hold. CSS is deliberately excluded (self-recovers heuristically; cannot PROVE a
// key wrong).
#[test]
fn every_scheme_gives_the_same_verdict_when_no_key_can_be_proven() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;

    // AACS, encrypted, no map installed at all.
    let mut aacs_keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, [0xAAu8; 16])],
        format: ContentFormat::BdTs,
    };
    let mut buf = vec![0u8; ul];
    let aacs_no_map = decrypt_span(&mut buf, &mut aacs_keys, 0, None, None)
        .expect_err("an AACS reader with no key map cannot prove any key");

    // AACS, encrypted, mapped but the unit falls outside every range.
    let mut orphan = clear_ts_unit();
    aacs_encrypt_unit_for_test(&mut orphan, &[0xCCu8; 16]);
    let mut buf = orphan.to_vec();
    let empty = AacsKeyMap::from_ranges(vec![]);
    let aacs_unmapped = decrypt_span(&mut buf, &mut aacs_keys, 0, Some(&empty), None)
        .expect_err("an encrypted unit no range covers cannot be keyed");

    let want = crate::error::Error::DecryptFailed.code();
    for (what, e) in [
        ("AACS, no map", aacs_no_map),
        ("AACS, unit outside every range", aacs_unmapped),
    ] {
        assert_eq!(
            e.code(),
            want,
            "{what}: every scheme must refuse identically, or one of them is \
                 quietly emitting data it could not decrypt"
        );
    }

    // And clear media is NOT a refusal — the shared policy must not turn
    // "nothing to decrypt" into an error.
    let mut none_keys = DecryptKeys::None;
    let mut buf = vec![0u8; 2048];
    assert!(
        decrypt_span(&mut buf, &mut none_keys, 0, None, None).is_ok(),
        "clear media has no key to prove and must pass through"
    );
}

// An ENCRYPTED unit outside every key-map range must fail, not pass through as ciphertext —
// a CLEAR one outside every range still must pass through untouched. Both directions
// asserted.
#[test]
fn an_encrypted_unit_outside_every_key_range_fails_instead_of_passing_through() {
    use crate::disc::ContentFormat;
    let key = [0xAAu8; 16];
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;

    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key)],
        format: ContentFormat::BdTs,
    };
    // The map covers unit 0 only. Unit 1 is the orphan.
    let map = AacsKeyMap::from_ranges(vec![(0, usz, 0)]);

    // Clear orphan: untouched, no error. This is nav/filesystem.
    let mut clear_buf = vec![0u8; 2 * ul];
    let mut u0 = clear_ts_unit();
    aacs_encrypt_unit_for_test(&mut u0, &key);
    clear_buf[..ul].copy_from_slice(&u0);
    clear_buf[ul..].copy_from_slice(&clear_ts_unit());
    let orphan_before = clear_buf[ul..].to_vec();
    decrypt_sectors_mapped(&mut clear_buf, &keys, 0, &map)
        .expect("a CLEAR unit outside the map is ordinary nav and must pass");
    assert_eq!(
        &clear_buf[ul..],
        &orphan_before[..],
        "a clear out-of-range unit must be left byte-identical"
    );

    // Encrypted orphan: must fail loud.
    let mut enc_buf = vec![0u8; 2 * ul];
    let mut v0 = clear_ts_unit();
    aacs_encrypt_unit_for_test(&mut v0, &key);
    enc_buf[..ul].copy_from_slice(&v0);
    let mut orphan = clear_ts_unit();
    aacs_encrypt_unit_for_test(&mut orphan, &[0xCCu8; 16]);
    enc_buf[ul..].copy_from_slice(&orphan);

    let err = decrypt_sectors_mapped(&mut enc_buf, &keys, 0, &map)
        .expect_err("an encrypted unit we hold no key for must not be emitted");
    assert_eq!(
        err.code(),
        crate::error::Error::DecryptFailed.code(),
        "same verdict CSS and the split-unit branch give for 'no provable key'"
    );
}

/// Phase::Even → only even-index units in the range are decrypted; the odd
/// (alternate variant) half is left BYTE-FOR-BYTE as ciphertext for the muxer.
#[test]
fn mapped_phase_even_decrypts_even_leaves_odd_ciphertext() {
    use crate::disc::ContentFormat;
    let key_a = [0xAAu8; 16];
    let key_b = [0xBBu8; 16];
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    let mut buf = vec![0u8; 8 * ul];
    let mut odd_cipher = Vec::new();
    for i in 0..8 {
        let mut u = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut u, if i % 2 == 0 { &key_a } else { &key_b });
        if i % 2 == 1 {
            odd_cipher.push(u.clone());
        }
        buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
    }
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key_a)],
        format: ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 8 * usz, 0, Phase::Even)]);
    decrypt_sectors_mapped(&mut buf, &keys, 0, &map).expect("even phase decrypts clean");
    for i in 0..8 {
        let u = &buf[i * ul..(i + 1) * ul];
        if i % 2 == 0 {
            assert!(
                aacs::content::is_clean(u, ContentFormat::BdTs),
                "even unit {i} decrypted to clean TS"
            );
        } else {
            assert_eq!(
                u,
                odd_cipher[i / 2].as_slice(),
                "odd unit {i} left as ciphertext"
            );
        }
    }
}

// `with_content_ranges` contract: an encrypted unit whose LBA is OUTSIDE the disc's
// content extents (clear filesystem / BDMV nav) must pass through untouched — never
// decrypted, verified, or counted as loss. Pre-fix the content map was ignored, so this unit was decrypted (mangled).
#[test]
fn mapped_decrypt_skips_units_outside_content_ranges() {
    let unit_key = [0x33u8; 16];
    let mut clear = clear_ts_unit();
    clear[0] |= 0xC0; // encrypted flag lives in the clear seed
    let mut ciphertext = clear.clone();
    aacs_encrypt_unit_for_test(&mut ciphertext, &unit_key);

    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, unit_key)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);

    // Unit at LBA 0 is OUTSIDE the content ranges → passed through untouched.
    let mut outside = ciphertext.clone();
    decrypt_sectors_mapped_in_content(&mut outside, &keys, 0, &map, Some(&[(100, 3)]))
        .expect("an out-of-content unit passes through");
    assert_eq!(
        outside, ciphertext,
        "unit outside content ranges must not be decrypted"
    );

    // The SAME unit at LBA 0 INSIDE the content ranges → decrypted to plaintext.
    let mut inside = ciphertext.clone();
    decrypt_sectors_mapped_in_content(&mut inside, &keys, 0, &map, Some(&[(0, 3)]))
        .expect("an in-content unit decrypts");
    assert_eq!(
        inside,
        aacs::content::cpi_cleared(clear),
        "unit inside content ranges must decrypt to plaintext"
    );
}

// Exercise the rayon parallel branch of `apply_aacs_map`: a buffer of far more than
// `PARALLEL_MIN_UNITS` units on the multi-thread path must recover EVERY unit byte-for-
// byte — i.e. `par_chunks_mut` keeps each `idx_in_buf` correct (a race/off-by-one corrupts units).
#[test]
fn mapped_decrypt_parallel_path_recovers_every_unit() {
    use crate::disc::ContentFormat;
    let key = [0x77u8; 16];
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    let key2 = [0x78u8; 16];
    // Force the parallel branch (else a 1-core runner / FREEMKV_THREADS=1 runs serial).
    set_decrypt_threads(4);
    assert!(decrypt_threads() > 1);
    let n = PARALLEL_MIN_UNITS * 4; // well past the parallel threshold
    let half = n / 2;
    // DISTINCT plaintext per unit and a second key for the upper half, so a
    // unit landing at the wrong index or under the wrong key is detectable.
    let clear_of = |i: usize| {
        let mut c = clear_ts_unit();
        c[0] |= 0xC0; // the CPI/encrypted flag is part of the preserved clear seed
        c[1] = i as u8; // per-unit seed byte → a distinct block key
        c[200] = i as u8; // and a distinct payload byte
        c
    };
    let mut buf = vec![0u8; n * ul];
    for i in 0..n {
        let mut u = clear_of(i);
        aacs_encrypt_unit_for_test(&mut u, if i < half { &key } else { &key2 });
        buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
    }
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key), (1, key2)],
        format: ContentFormat::BdTs,
    };
    let split = (half as u32) * usz;
    let map = AacsKeyMap::from_ranges(vec![(0, split, 0), (split, (n as u32) * usz, 1)]);
    decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
        .expect("the parallel mapped decrypt must succeed");
    for i in 0..n {
        assert_eq!(
            &buf[i * ul..(i + 1) * ul],
            aacs::content::cpi_cleared(clear_of(i)).as_slice(),
            "unit {i} must recover to its own plaintext on the parallel path"
        );
    }
    set_decrypt_threads(0);
}

// A failed pool build is remembered: later calls go serial without re-running the builder.
#[test]
fn failed_pool_build_is_cached_not_retried() {
    let slot = RwLock::new(None);
    let failed = std::sync::atomic::AtomicBool::new(false);
    let calls = std::cell::Cell::new(0u32);
    for _ in 0..3 {
        let got = pool_or_build(&slot, &failed, || {
            calls.set(calls.get() + 1);
            None
        });
        assert!(got.is_none());
    }
    assert_eq!(
        calls.get(),
        1,
        "the failing builder must run once, not per call"
    );
    // A successful build is stored and reused.
    let slot = RwLock::new(None);
    let ok = std::sync::atomic::AtomicBool::new(false);
    let build = || rayon::ThreadPoolBuilder::new().num_threads(1).build().ok();
    let first = pool_or_build(&slot, &ok, build).expect("pool builds");
    let second = pool_or_build(&slot, &ok, || None).expect("pool reused");
    assert!(Arc::ptr_eq(&first, &second));
}

// Overlapping ranges are made disjoint (later start wins) so entry_for stays sound.
#[test]
fn nested_key_range_keeps_outer_tail_and_phase_anchor() {
    let map =
        AacsKeyMap::from_ranges_phased(vec![(0, 100, 0, Phase::Even), (20, 60, 1, Phase::All)]);
    assert_eq!(map.key_idx_for(80), Some(0));
    assert_eq!(map.key_idx_for(40), Some(1));
    assert_eq!(map.entry_for(80), Some((0, Phase::Even, 0)));
    assert_eq!(map.entry_for(10), Some((0, Phase::Even, 0)));
}

/// Every sector of every input range resolves to some key.
#[test]
fn every_sector_of_every_input_range_gets_a_key() {
    let input = vec![
        (0, 100, 0),
        (20, 30, 1),
        (25, 60, 2),
        (90, 120, 3),
        (110, 115, 4),
    ];
    let map = AacsKeyMap::from_ranges(input.clone());
    for (s, e, _) in input {
        for lba in s..e {
            assert!(map.key_idx_for(lba).is_some(), "lba {lba} lost its key");
        }
    }
    assert_eq!(map.key_idx_for(60), Some(0));
    assert_eq!(map.key_idx_for(116), Some(3));
    assert_eq!(map.key_idx_for(120), None);
}

#[test]
fn overlapping_key_ranges_are_made_disjoint() {
    let map = AacsKeyMap::from_ranges(vec![(0, 100, 0), (20, 30, 1), (25, 60, 2), (60, 60, 3)]);
    let r = map.ranges();
    assert!(
        r.windows(2).all(|w| w[0].1 <= w[1].0),
        "not disjoint: {r:?}"
    );
    assert_eq!(map.key_idx_for(10), Some(0));
    assert_eq!(map.key_idx_for(22), Some(1));
    assert_eq!(map.key_idx_for(40), Some(2));
    assert_eq!(map.key_indices(), &[0, 1, 2]);
}

// The mapped descramble indexes the committed key pool POSITIONALLY, so the ORDER of the
// `Vec<UnitKey>` a `KeySource` returns is load-bearing — nothing here searches the pool.
#[test]
fn mapped_key_selection_is_positional_so_pool_order_matters() {
    use crate::disc::ContentFormat;
    let key_a = [0xAAu8; 16];
    let key_b = [0xBBu8; 16];
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    // Unit 0 encrypted under key_a, unit 1 under key_b.
    let build = || {
        let mut buf = vec![0u8; 2 * ul];
        for (i, k) in [key_a, key_b].iter().enumerate() {
            let mut u = clear_ts_unit();
            aacs_encrypt_unit_for_test(&mut u, k);
            buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
        }
        buf
    };
    // Map: unit 0 → pool position 0, unit 1 → pool position 1.
    let map = AacsKeyMap::from_ranges_phased(vec![
        (0, usz, 0, Phase::Even),
        (usz, 2 * usz, 1, Phase::Even),
    ]);
    // Pool in CPS-unit order: each range gets its own key, both come clean.
    let mut buf = build();
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key_a), (1, key_b)],
        format: ContentFormat::BdTs,
    };
    decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
        .expect("pool in CPS-unit order decrypts clean");
    // SAME keys, SAME CPS-unit numbers, swapped POSITIONS. If the number were
    // what mattered (or if the path searched the pool) this would be
    // equivalent; positional indexing makes it decrypt both units wrong.
    let mut buf = build();
    let swapped = DecryptKeys::Aacs {
        unit_keys: vec![(1, key_b), (0, key_a)],
        format: ContentFormat::BdTs,
    };
    assert!(
        decrypt_sectors_mapped(&mut buf, &swapped, 0, &map).is_err(),
        "a reordered pool must fail loud — key selection is positional, so the \
             ORDER a KeySource returns its keys in is part of the contract"
    );
}

/// The correct-phase safety `is_clean` fires loud: even units whose mapped key is
/// wrong do NOT come clean → `DecryptFailed` (not silent corruption). A wrong key
/// fails every unit it keys (here both even ones); a lone failure is damage.
#[test]
fn mapped_phase_verify_fails_loud_on_wrong_key() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    let mut buf = vec![0u8; 4 * ul];
    for i in 0..4 {
        let mut u = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut u, &[0xAAu8; 16]); // encrypted under A
        buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
    }
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, [0xCCu8; 16])], // map slot points at the WRONG key
        format: ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 4 * usz, 0, Phase::Even)]);
    assert!(matches!(
        decrypt_sectors_mapped(&mut buf, &keys, 0, &map),
        Err(crate::error::Error::DecryptFailed)
    ));
}

/// The wrong-key verdict is per key: a read spanning a good FMTS range and a wrong-keyed
/// one stops, even though the good range's units verify.
#[test]
fn mapped_phase_wrong_key_stops_beside_a_good_range() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    let good = [0xAAu8; 16];
    let mut buf = vec![0u8; 8 * ul];
    for i in 0..8 {
        let mut u = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut u, &good);
        buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
    }
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, good), (1, [0xCCu8; 16])],
        format: ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges_phased(vec![
        (0, 4 * usz, 0, Phase::Even),
        (4 * usz, 8 * usz, 1, Phase::Even),
    ]);
    assert!(matches!(
        decrypt_sectors_mapped(&mut buf, &keys, 0, &map),
        Err(crate::error::Error::DecryptFailed)
    ));
}

/// Option A (E7013): one damaged forensic unit under each of two keys (garbled heads that
/// kept flag and sync), no other forensic unit in the read, is damage: blanked, never E7013.
#[test]
fn mapped_phase_lone_damage_under_two_keys_is_blanked_not_e7013() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    let (key_a, key_b) = ([0xAAu8; 16], [0xBBu8; 16]);
    let mut buf = vec![0u8; 4 * ul];
    for (i, k) in [key_a, key_a, key_b, key_b].iter().enumerate() {
        let mut u = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut u, k);
        if i % 2 == 0 {
            crate::test_util::damage_unit_seed(&mut u);
            u[4] = 0x47;
        }
        buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
    }
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key_a), (1, key_b)],
        format: ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges_phased(vec![
        (0, 2 * usz, 0, Phase::Even),
        (2 * usz, 4 * usz, 1, Phase::Even),
    ]);
    let blanked = decrypt_sectors_mapped_in_content(&mut buf, &keys, 0, &map, None)
        .expect("damage, not E7013");
    assert_eq!(blanked, 2);
    assert!(buf[..ul].iter().all(|&b| b == 0));
    assert!(buf[2 * ul..3 * ul].iter().all(|&b| b == 0));
}

/// Two damaged forensic units beside verifying ones are damage: blanked and counted. Only
/// a key with NO verifying unit is a wrong key.
#[test]
fn mapped_phase_two_damaged_units_beside_verified_ones_are_blanked() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    let key = [0xAAu8; 16];
    // Even-phase units sit at even indices 0, 2, 4, 6; the odd ones are left alone.
    let mut buf = vec![0u8; 7 * ul];
    for i in 0..7 {
        let mut u = clear_ts_unit();
        if i % 2 == 0 {
            aacs_encrypt_unit_for_test(&mut u, &key);
            if i < 4 {
                crate::test_util::damage_unit_seed(&mut u);
                u[4] = 0x47;
            }
        }
        buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
    }
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key)],
        format: ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 7 * usz, 0, Phase::Even)]);
    let blanked = decrypt_sectors_mapped_in_content(&mut buf, &keys, 0, &map, None)
        .expect("damage beside verified units is not E7013");
    assert_eq!(blanked, 2);
}

/// One failing forensic unit that ANOTHER held key opens is a wrong key assignment, not
/// damage: E7013 even though it is the read's only failure.
#[test]
fn mapped_phase_a_unit_another_key_opens_is_a_wrong_key_not_damage() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let usz = (ul / 2048) as u32;
    let (key_a, key_b) = ([0xAAu8; 16], [0xBBu8; 16]);
    let mut buf = clear_ts_unit();
    aacs_encrypt_unit_for_test(&mut buf, &key_b);
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key_a), (1, key_b)],
        format: ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges_phased(vec![(0, usz, 0, Phase::Even)]);
    assert!(matches!(
        decrypt_sectors_mapped_in_content(&mut buf, &keys, 0, &map, None),
        Err(crate::error::Error::DecryptFailed)
    ));
}

/// HD DVD (`MpegPs`) units carry no TS sync at byte 4: none is damage, none is touched.
#[test]
fn blank_damaged_units_leaves_mpegps_untouched() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    // Unflagged, no sync at byte 4, not clean PS: damage if judged as BD-TS.
    let mut buf: Vec<u8> = (0..2 * ul + 100)
        .map(|i| (i as u8).wrapping_mul(31) | 1)
        .collect();
    for u in 0..2 {
        buf[u * ul + 20] = 0;
    }
    let before = buf.clone();
    let n = blank_damaged_units(&mut buf, 0, ContentFormat::MpegPs, &|_| true, true);
    assert_eq!(n, 0);
    assert_eq!(buf, before);
}

/// Option A (E7013): a garbage seed with its CPI bits clear lost the TS sync every BD-TS
/// unit carries at byte 4 (KS-2): damage, blanked and counted; clear and zero units are not.
#[test]
fn blank_damaged_units_counts_a_garbage_seed_with_cpi_clear() {
    use crate::disc::ContentFormat;
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let mut garbage = clear_ts_unit();
    aacs_encrypt_unit_for_test(&mut garbage, &[0xAAu8; 16]);
    crate::test_util::damage_unit_seed(&mut garbage);
    garbage[0] &= 0x3F;
    let mut buf = [garbage, clear_ts_unit(), vec![0u8; ul]].concat();
    let n = blank_damaged_units(&mut buf, 0, ContentFormat::BdTs, &|_| true, false);
    assert_eq!(n, 1);
    assert!(buf[..ul].iter().all(|&b| b == 0));
    assert_eq!(&buf[ul..2 * ul], &clear_ts_unit()[..]);
}

/// Phase::All (multi-CPS / base) decrypts EVERY unit and never runs the verify
/// — the common-disc path is byte-for-byte unchanged.
#[test]
fn mapped_all_phase_decrypts_every_unit() {
    use crate::disc::ContentFormat;
    let key = [0x11u8; 16];
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let mut buf = vec![0u8; 4 * ul];
    for i in 0..4 {
        let mut u = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut u, &key);
        buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
    }
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key)],
        format: ContentFormat::BdTs,
    };
    decrypt_sectors_mapped(
        &mut buf,
        &keys,
        0,
        &AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]),
    )
    .expect("all-phase decrypts");
    for i in 0..4 {
        assert!(
            aacs::content::is_clean(&buf[i * ul..(i + 1) * ul], ContentFormat::BdTs),
            "unit {i} decrypted (All)"
        );
    }
}

// ── decrypt_threads resolution (read-only; no global mutation) ─────────

// Default thread count is always usable: >=1, never above MAX_THREADS. Reads only; safe
// alongside other tests.
#[test]
fn decrypt_threads_within_valid_pool_range() {
    let n = decrypt_threads();
    assert!(n >= 1, "decrypt thread count must be at least 1, got {n}");
    assert!(
        n <= MAX_THREADS,
        "decrypt thread count must not exceed MAX_THREADS ({MAX_THREADS}), got {n}"
    );
}

// The FMTS phase gate picks which half of an interleaved forensic segment we decrypt;
// getting its index arithmetic wrong silently decrypts the alternate variant into garbage.
#[test]
fn phase_gate_selects_only_our_parity_of_a_forensic_segment() {
    use super::{Phase, unit_is_our_phase};
    // A range starting at LBA 30, 3 sectors per aligned unit: units are at
    // 30, 33, 36, 39, ... with indices 0, 1, 2, 3, ...
    let ours = |lba, phase| unit_is_our_phase(lba, 30, 3, phase);

    // Even phase takes indices 0, 2, 4 -> LBAs 30, 36, 42.
    assert!(ours(30, Phase::Even));
    assert!(!ours(33, Phase::Even));
    assert!(ours(36, Phase::Even));
    assert!(!ours(39, Phase::Even));

    // Odd phase is the exact complement.
    for lba in [30, 33, 36, 39, 42, 45] {
        assert_ne!(
            ours(lba, Phase::Even),
            ours(lba, Phase::Odd),
            "LBA {lba} must belong to exactly one parity"
        );
    }

    // Non-forensic content: the whole range is ours.
    for lba in [30, 33, 36, 39] {
        assert!(ours(lba, Phase::All));
    }

    // The index must be RANGE-RELATIVE: `(lba - start)`, not `(lba + start)`.
    // Pin a case where they genuinely disagree: (5-1)/2 = 2 (even, ours) but
    // (5+1)/2 = 3 (odd, not ours).
    assert!(
        unit_is_our_phase(5, 1, 2, Phase::Even),
        "the index must be measured from the range start"
    );

    // The `/ unit_sectors` -> `* unit_sectors` mutant is EQUIVALENT here and
    // deliberately not chased: aligned offsets are k*u, and dividing vs.
    // multiplying give the same parity since `unit_sectors` (=3) is always odd.
    assert!(!unit_is_our_phase(33, 30, 3, Phase::Even));
}

// A malformed key map (unit below range start, or zero unit size) must return a DEFINED
// answer, not merely avoid panicking.
#[test]
fn phase_gate_does_not_panic_on_a_malformed_map() {
    use super::{Phase, unit_is_our_phase};
    // Unit below its own range start: saturating_sub clamps to 0, and unit
    // 0 is even.
    assert!(unit_is_our_phase(10, 100, 3, Phase::Even));
    // Zero unit size: max(1) makes the divisor 1, so the index is the raw
    // offset 70 — even.
    assert!(unit_is_our_phase(100, 30, 0, Phase::Even));
    // Both malformations at once: offset 0 over divisor 1 is unit 0, even.
    assert!(unit_is_our_phase(5, 5, 0, Phase::Even));
}

// Content-range lookup over SEVERAL sorted ranges: before the first, inside
// each, in the gaps, on every boundary, and past the last.
#[test]
fn content_range_lookup_handles_multiple_ranges_and_their_boundaries() {
    let ranges = [(10u32, 5u32), (20, 1), (100, 50)];
    let inside = [10, 14, 20, 100, 149];
    let outside = [0, 9, 15, 19, 21, 99, 150, u32::MAX];
    for lba in inside {
        assert!(lba_in_content_ranges(lba, &ranges), "{lba} is content");
    }
    for lba in outside {
        assert!(!lba_in_content_ranges(lba, &ranges), "{lba} is not content");
    }
}

// The per-unit content gate inside ONE multi-unit buffer: units in content
// decrypt, the unit in the gap between two ranges passes through untouched.
#[test]
fn content_gate_is_per_unit_within_a_multi_unit_buffer() {
    let key = [0x44u8; 16];
    let ul = aacs::content::ALIGNED_UNIT_LEN;
    let mut clear = clear_ts_unit();
    clear[0] |= 0xC0;
    let mut enc = clear.clone();
    aacs_encrypt_unit_for_test(&mut enc, &key);
    let mut buf = [enc.clone(), enc.clone(), enc.clone()].concat();
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
    // Units at LBA 300, 303, 306: content is 300..303 and 306..309.
    decrypt_sectors_mapped_in_content(&mut buf, &keys, 300, &map, Some(&[(300, 3), (306, 3)]))
        .expect("mixed buffer decrypts");
    let plain = aacs::content::cpi_cleared(clear);
    assert_eq!(&buf[..ul], plain.as_slice(), "unit 0 is content");
    assert_eq!(&buf[ul..2 * ul], enc.as_slice(), "unit 1 sits in the gap");
    assert_eq!(&buf[2 * ul..], plain.as_slice(), "unit 2 is content");
}
