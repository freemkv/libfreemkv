use crate::disc::ContentFormat;

// BEHAVIOR 2 — phase-tie default: all four arms of the even/odd clean-count decision.
#[test]
fn resolve_tie_phase_covers_all_arms() {
    use crate::decrypt::Phase;
    // Non-tie: the clean half is the index's real variant.
    assert_eq!(
        super::resolve_tie_phase(5, 2).unwrap(),
        Phase::Even,
        "even majority → Even"
    );
    assert_eq!(
        super::resolve_tie_phase(2, 5).unwrap(),
        Phase::Odd,
        "odd majority → Odd"
    );
    // Padding tie (both halves clean, > 0): parity immaterial → default Even.
    assert_eq!(
        super::resolve_tie_phase(3, 3).unwrap(),
        Phase::Even,
        "even == odd > 0 → default Even"
    );
    assert_eq!(super::resolve_tie_phase(1, 1).unwrap(), Phase::Even);
    // Neither half clean (even == odd == 0): no evidence.
    assert_eq!(super::resolve_tie_phase(0, 0), None);
}

// ── Fix 1: FMTS phase-probe read-fault vs wrong-key distinction ─────────

// Build a 6144-byte aligned unit of CLEAN MPEG-TS then AACS-encrypt it under key.
fn encrypted_clean_unit(key: &[u8; 16]) -> Vec<u8> {
    use crate::aacs::content::ALIGNED_UNIT_LEN;
    let mut u = vec![0u8; ALIGNED_UNIT_LEN];
    let mut off = 0;
    while off + 192 <= ALIGNED_UNIT_LEN {
        u[off + 4] = 0x47; // TS sync at the BD-TS packet stride
        for b in &mut u[off + 5..off + 192] {
            *b = 0xAB; // non-zero payload so is_clean counts it as content
        }
        off += 192;
    }
    // Flag encrypted BEFORE encrypting: bytes 0..16 are the key seed.
    u[0] |= 0xC0;
    assert!(
        crate::aacs::content::encrypt_unit(&mut u, key),
        "a full-length unit must encrypt"
    );
    u
}

fn a_segment(index: u16) -> crate::aacs::segment::Segment {
    crate::aacs::segment::Segment {
        index,
        start_spn: 0,
        end_spn: 100,
    }
}

// A probe whose EVERY read faults must classify as ReadFault, NOT WrongKey.
#[test]
fn probe_index_phase_all_faults_is_read_fault_not_wrong_key() {
    let segs = vec![a_segment(1)];
    let key = [0x11u8; 16];
    let got = super::probe_index_phase(
        &segs,
        1,
        8,
        16,
        ContentFormat::BdTs,
        &key,
        |_seg, _unit| None, // every read faults
    );
    assert_eq!(
        got,
        super::IndexProbe::ReadFault,
        "all-faulted probe is a recoverable read fault, never a wrong key"
    );
}

/// Reads SUCCEED but decrypt to NEITHER clean parity (ciphertext under a key we
/// do NOT hold) → [`IndexProbe::WrongKey`]. This is the genuine-missing-key path
/// the caller MUST keep as a hard `FmtsKeyMissing`.
#[test]
fn probe_index_phase_reads_succeed_but_no_clean_phase_is_wrong_key() {
    let segs = vec![a_segment(1)];
    let cipher = encrypted_clean_unit(&[0xAAu8; 16]); // encrypted under key A
    let probe_key = [0xBBu8; 16]; // ... probed under the WRONG key B
    let got = super::probe_index_phase(
        &segs,
        1,
        8,
        16,
        ContentFormat::BdTs,
        &probe_key,
        |_seg, _unit| Some(cipher.clone()),
    );
    assert_eq!(
        got,
        super::IndexProbe::WrongKey,
        "reads that decrypt to no clean parity under the probed key are a wrong key"
    );
}

/// Reads succeed and the EVEN units decrypt clean under this index's key while
/// the ODD units are (unencrypted) padding → [`IndexProbe::Phase`]`(Even)`.
#[test]
fn probe_index_phase_resolves_clean_even_phase() {
    use crate::aacs::content::ALIGNED_UNIT_LEN;
    use crate::decrypt::Phase;
    let segs = vec![a_segment(1)];
    let key = [0x33u8; 16];
    let even_unit = encrypted_clean_unit(&key);
    let got = super::probe_index_phase(
        &segs,
        1,
        8,
        16,
        ContentFormat::BdTs,
        &key,
        // even unit index → clean ciphertext under `key`; odd → zero padding
        // (aacs_unit_encrypted false → not counted).
        |_seg, unit| {
            if unit % 2 == 0 {
                Some(even_unit.clone())
            } else {
                Some(vec![0u8; ALIGNED_UNIT_LEN])
            }
        },
    );
    assert_eq!(
        got,
        super::IndexProbe::Phase(Phase::Even),
        "clean even units + padding odd → Even phase"
    );
}

// Read-fault TOLERANCE: first same-index segment faults every read, second decrypts clean
// -> probe falls through.
#[test]
fn probe_index_phase_falls_through_faulting_segment_to_next() {
    use crate::decrypt::Phase;
    let mut faulting = a_segment(1);
    faulting.start_spn = 1; // distinguish the two same-index segments
    let good = a_segment(1);
    let segs = vec![faulting, good];
    let key = [0x44u8; 16];
    let clean = encrypted_clean_unit(&key);
    let got = super::probe_index_phase(
        &segs,
        1,
        8,
        16,
        ContentFormat::BdTs,
        &key,
        // The faulting segment (start_spn == 1) reads None; the good one reads a
        // clean even unit / padding odd.
        |seg, unit| {
            if seg.start_spn == 1 {
                None
            } else if unit % 2 == 0 {
                Some(clean.clone())
            } else {
                Some(vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN])
            }
        },
    );
    assert_eq!(
        got,
        super::IndexProbe::Phase(Phase::Even),
        "a faulting first segment must not block resolving from the next same-index one"
    );
}

// A READABLE but unclean same-index segment (wrong-key signature) must not
// stop the probe: a later same-index segment can still anchor the phase.
#[test]
fn probe_index_phase_falls_through_unclean_segment_to_next() {
    use crate::decrypt::Phase;
    let mut unclean = a_segment(1);
    unclean.start_spn = 1;
    let segs = vec![unclean, a_segment(1)];
    let key = [0x55u8; 16];
    let clean = encrypted_clean_unit(&key);
    let other = encrypted_clean_unit(&[0x66u8; 16]); // decrypts to junk under `key`
    let got = super::probe_index_phase(&segs, 1, 8, 16, ContentFormat::BdTs, &key, |seg, unit| {
        if seg.start_spn == 1 {
            Some(other.clone())
        } else if unit % 2 == 0 {
            Some(clean.clone())
        } else {
            Some(vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN])
        }
    });
    assert_eq!(
        got,
        super::IndexProbe::Phase(Phase::Even),
        "an unclean first segment must not end the probe as WrongKey"
    );
}
