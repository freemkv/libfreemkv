use super::*;
use crate::mux::ts::PesPacket;

fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
    PesPacket {
        source: None,
        pid: 0x1100,
        pts,
        dts: None,
        data,
        discontinuity: false,
    }
}

// Non-degenerate substream count/directory size test.
#[test]
fn mlp_num_substreams_is_the_top_nibble_of_major_sync_byte_16() {
    // Byte 16 of the major sync: top nibble is num_substreams, bottom nibble
    // is a different field, so it must not leak into the answer.
    for n in 0..16u8 {
        let mut ms = vec![0u8; 17];
        ms[16] = (n << 4) | 0x0F;
        assert_eq!(
            mlp_num_substreams(&ms),
            Some(n),
            "num_substreams is the high nibble only"
        );
    }
    // Real counts: 1 substream for 2.0/5.1 core-only, 4 for the 7.1/Atmos
    // layouts this crate has to mux.
    let mut ms = vec![0u8; 20];
    ms[16] = 0x40;
    assert_eq!(mlp_num_substreams(&ms), Some(4));

    // A major sync too short to contain byte 16 yields no answer — never a
    // defaulted count, which would arm the parity check against garbage.
    assert_eq!(mlp_num_substreams(&[0u8; 16]), None);
}

#[test]
fn mlp_substr_header_size_counts_the_extraword_entries() {
    // Directory entry: 2 bytes, plus 2 more when the entry's top bit
    // (extraword) is set. Build a 4-substream directory that mixes both.
    const HDR: usize = 4;
    let mut au = vec![0xAAu8; HDR];
    au.extend_from_slice(&[0x00, 0x11]); // plain          → 2
    au.extend_from_slice(&[0x80, 0x22, 0x01, 0x02]); // extraword → 4
    au.extend_from_slice(&[0x00, 0x33]); // plain          → 2
    au.extend_from_slice(&[0x80, 0x44, 0x03, 0x04]); // extraword → 4
    au.extend_from_slice(&[0xFFu8; 8]); // payload past the directory
    assert_eq!(
        mlp_substr_header_size(&au, HDR, 4),
        Some(12),
        "2 + 4 + 2 + 4"
    );

    // Same AU, fewer declared substreams → only that many entries counted.
    assert_eq!(mlp_substr_header_size(&au, HDR, 1), Some(2));
    assert_eq!(mlp_substr_header_size(&au, HDR, 2), Some(6));
    assert_eq!(mlp_substr_header_size(&au, HDR, 0), Some(0));

    // An all-plain directory is 2 bytes per substream.
    let plain = vec![0x00u8; HDR + 8];
    assert_eq!(mlp_substr_header_size(&plain, HDR, 4), Some(8));

    // A directory that runs past the AU has no answer: the parity window
    // would otherwise be placed over bytes that are not there.
    let truncated = &au[..HDR + 9];
    assert_eq!(mlp_substr_header_size(truncated, HDR, 4), None);
    assert_eq!(mlp_substr_header_size(&plain, HDR, 5), None);
}

fn make_truehd_unit(size_bytes: usize) -> Vec<u8> {
    let words = size_bytes / 2;
    let mut data = vec![0u8; size_bytes];
    data[0] = ((words >> 8) & 0x0F) as u8;
    data[1] = (words & 0xFF) as u8;
    data
}

// Make a synthetic major-sync AU pass the decodability gate (CRC + parity).
fn finalize_major_sync(au: &mut [u8]) {
    const MSHDR: usize = 28; // no extension (byte 25 clear)
    // num_substreams = 1 → major-sync byte 16 (AU[20]) top nibble.
    au[20] = (au[20] & 0x0F) | 0x10;
    // Substream directory entry at AU[4+MSHDR] = AU[32]: extraword flag clear.
    au[32] &= 0x7F;
    // Major-sync checksum, built EXACTLY as `mlp_major_sync_crc_ok` verifies it
    // (the MLP checksum16): swap_bytes(crc16_mlp(body)) ^ LE word before
    // the trailer, stored little-endian in the trailer.
    let body_end = 4 + MSHDR - 4; // AU[4..28]
    let crc = super::crc16_mlp(&au[4..body_end]).swap_bytes()
        ^ u16::from_le_bytes([au[body_end], au[body_end + 1]]);
    au[4 + MSHDR - 2] = (crc & 0xFF) as u8;
    au[4 + MSHDR - 1] = (crc >> 8) as u8;
    // Parity: choose the AU check nibble (AU[0] high bits) so the header +
    // directory fold to 0xF. The length low nibble (AU[0] low bits) is kept.
    let hi = au[0] & 0x0F;
    let p0 = (hi ^ au[1] ^ au[2] ^ au[3]) ^ (au[32] ^ au[33]);
    let c = ((p0 >> 4) ^ (p0 & 0x0F) ^ 0x0F) & 0x0F;
    au[0] = (c << 4) | hi;
}

/// Give a synthetic NON-major-sync AU a valid header parity nibble (1
/// substream, directory at AU[4..6]), so it passes the gate once a preceding
/// major sync has established `num_substreams`.
fn finalize_normal_parity(au: &mut [u8]) {
    au[4] &= 0x7F; // no extraword
    let hi = au[0] & 0x0F;
    let p0 = (hi ^ au[1] ^ au[2] ^ au[3]) ^ (au[4] ^ au[5]);
    let c = ((p0 >> 4) ^ (p0 & 0x0F) ^ 0x0F) & 0x0F;
    au[0] = (c << 4) | hi;
}

fn valid_major_sync() -> Vec<u8> {
    let mut u = make_truehd_unit(200);
    u[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
    finalize_major_sync(&mut u);
    u
}

fn valid_normal_au() -> Vec<u8> {
    let mut u = make_truehd_unit(200);
    finalize_normal_parity(&mut u);
    u
}

#[test]
fn corrupt_major_sync_drops_forward_to_next_valid() {
    // MLP state carries across AUs, so a corrupt AU is dropped FORWARD to the
    // next valid major sync. Sequence: valid MS, corrupt MS, normal AU, valid
    // MS — only the two valid syncs survive (the normal AU is poisoned collateral).
    let mut parser = TrueHdParser::new();
    let ms1 = valid_major_sync();
    let mut ms_bad = valid_major_sync();
    ms_bad[10] ^= 0xFF; // corrupt a CRC-covered header byte
    let normal = valid_normal_au(); // clean parity, but arrives mid-resync
    let ms2 = valid_major_sync();

    let mut data = ms1.clone();
    data.extend_from_slice(&ms_bad);
    data.extend_from_slice(&normal);
    data.extend_from_slice(&ms2);
    let mut frames = parser.parse(&make_pes(data, Some(90000)));
    frames.extend(parser.flush());

    assert_eq!(frames.len(), 2, "only the two valid major syncs survive");
    assert!(frames[0].keyframe && frames[1].keyframe);
    assert_eq!(
        parser.dropped_frames(),
        2,
        "corrupt MS + poisoned normal AU"
    );
}

#[test]
fn discontinuity_resyncs_forward_to_next_major_sync() {
    // A discontinuity leaves MLP's cross-AU state stale, so it arms the same
    // drop-forward the corruption path uses: drop post-gap AUs until the next
    // CRC-valid major sync. Fixed 559 "restart header sync" errors on a real disc.
    let mut parser = TrueHdParser::new();

    // Establish a baseline so num_substreams is known and the gate is live.
    let mut pre = valid_major_sync();
    pre.extend_from_slice(&valid_normal_au());
    let pre_frames = parser.parse(&make_pes(pre, Some(90000)));
    assert_eq!(pre_frames.len(), 2, "baseline: major sync + one normal AU");

    // Post-gap PES: two normal AUs, then a valid major sync, then a normal AU.
    // `discontinuity: true` says packets were lost before this PES.
    let mut post = valid_normal_au();
    post.extend_from_slice(&valid_normal_au());
    post.extend_from_slice(&valid_major_sync());
    post.extend_from_slice(&valid_normal_au());
    let post_pes = PesPacket {
        source: None,
        pid: 0x1100,
        pts: Some(90000 + 4 * 900),
        dts: None,
        data: post,
        discontinuity: true,
    };
    let mut frames = parser.parse(&post_pes);
    frames.extend(parser.flush());

    // Only the major sync and the AU after it survive; the two leading
    // post-gap normal AUs are dropped for lack of a re-init point.
    assert_eq!(
        frames.len(),
        2,
        "resume only at the major sync + what follows it (got {})",
        frames.len()
    );
    assert!(
        frames[0].keyframe,
        "the first surviving post-gap frame MUST be a major sync (re-init \
             point), never a mid-stream AU"
    );
    assert!(
        !frames[1].keyframe,
        "the AU after the re-init point is a normal AU"
    );
    assert_eq!(
        parser.dropped_frames(),
        2,
        "the two post-gap AUs before the major sync are dropped as collateral"
    );
}

#[test]
fn crc_failed_head_major_sync_is_kept_not_track_killed() {
    // REGRESSION (every TrueHD title after the checksum gate landed): a
    // checksum-failed major sync at stream head (no baseline yet) must NOT
    // arm drop-forward, or it silently drops the whole track (OOM decoder).
    let mut parser = TrueHdParser::new();
    let mut ms_bad = valid_major_sync();
    ms_bad[10] ^= 0xFF; // break a checksum-covered header byte → checksum fails
    let mut data = ms_bad;
    for _ in 0..6 {
        data.extend_from_slice(&valid_normal_au());
    }
    let mut frames = parser.parse(&make_pes(data, Some(90000)));
    frames.extend(parser.flush());
    assert_eq!(
        frames.len(),
        7,
        "no baseline yet: the CRC-failed head major sync + all following AUs are \
             kept, not dropped (got {})",
        frames.len()
    );
    assert_eq!(
        parser.dropped_frames(),
        0,
        "nothing dropped without a validated baseline to protect"
    );
    // And the invariant still holds ONCE a baseline exists: after a genuinely
    // valid major sync, a later corrupt one IS dropped (see
    // `corrupt_major_sync_drops_forward_to_next_valid`).
}

// Independent bitwise CRC-16 oracle (poly 0x002D), not tautological with `crc16_mlp`.
fn ref_crc16_2d(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x002D
            } else {
                crc << 1
            };
        }
    }
    crc
}

#[test]
fn extended_major_sync_crc_validates_and_rejects() {
    // COVERAGE GAP: the extended major-sync header path (ms[25]&1 set) had
    // zero test coverage — every other fixture builds only the basic header.
    // Build one with an independent oracle (ref_crc16_2d) to catch the regression.
    assert_eq!(
        ref_crc16_2d(b"123456789"),
        0x4FF7,
        "oracle anchored to catalogue"
    );

    // n = 3 extension words → mshdr = 28 + 2 + 2*3 = 36.
    let n = 3usize;
    let mshdr = 28 + 2 + 2 * n;
    assert_eq!(mshdr, 36);
    let mut ms = vec![0u8; 40]; // slack past the 36-byte header
    // Non-trivial, varied body so the CRC is a meaningful function of it.
    for (i, b) in ms.iter_mut().enumerate().take(mshdr - 4) {
        *b = (0x37u8).wrapping_add((i as u8).wrapping_mul(0x53));
    }
    ms[25] |= 1; // extension flag → selects the extended header size
    ms[26] = (ms[26] & 0x0F) | ((n as u8) << 4); // extension word count in high nibble

    // The 2-byte "penultimate" word (between the CRC-covered body and the
    // trailer). Chosen non-zero and non-palindromic so the LE/BE distinction
    // is observable.
    ms[mshdr - 4] = 0x12;
    ms[mshdr - 3] = 0x34;

    // Oracle: checksum16 = crc16_2D(body).swap_bytes() ^ le16(penultimate),
    // computed with the INDEPENDENT ref CRC, then stored LITTLE-ENDIAN.
    let le_word = u16::from_le_bytes([ms[mshdr - 4], ms[mshdr - 3]]);
    let trailer = ref_crc16_2d(&ms[..mshdr - 4]).swap_bytes() ^ le_word;
    ms[mshdr - 2] = (trailer & 0xFF) as u8;
    ms[mshdr - 1] = (trailer >> 8) as u8;
    assert_ne!(
        ms[mshdr - 2],
        ms[mshdr - 1],
        "trailer bytes must differ so the LE/BE swap below is a real distinction"
    );

    // The extended header size is computed from ms[25]/ms[26].
    assert_eq!(
        mlp_major_sync_header_size(&ms),
        Some(mshdr),
        "extended header size = 28 + 2 + 2*n"
    );
    // The validator accepts the independently-built extended major sync.
    assert!(
        mlp_major_sync_crc_ok(&ms, mshdr),
        "valid extended major-sync checksum must validate"
    );

    // A single corrupted body byte must be rejected.
    let mut corrupt = ms.clone();
    corrupt[10] ^= 0xFF;
    assert!(
        !mlp_major_sync_crc_ok(&corrupt, mshdr),
        "a corrupted extended major sync must be rejected"
    );

    // The endianness regression: the SAME checksum stored big-endian must be
    // rejected. A validator that reads the trailer big-endian (the shipped
    // bug) would instead accept this and reject the correct LE form above.
    let mut swapped = ms.clone();
    swapped.swap(mshdr - 2, mshdr - 1);
    assert!(
        !mlp_major_sync_crc_ok(&swapped, mshdr),
        "a big-endian-stored trailer must be rejected (little-endian is load-bearing)"
    );
}

#[test]
fn parity_failure_is_dropped() {
    // A normal AU whose header parity is broken (after a major sync sets
    // num_substreams) is undecodable → dropped.
    let mut parser = TrueHdParser::new();
    let ms1 = valid_major_sync();
    let mut bad = valid_normal_au();
    // A single-nibble flip: MLP's nibble-fold parity is blind
    // to a full-byte flip, which changes both nibbles equally and cancels.
    bad[2] ^= 0x01;
    let ms2 = valid_major_sync();
    let mut data = ms1;
    data.extend_from_slice(&bad);
    data.extend_from_slice(&ms2);
    let mut frames = parser.parse(&make_pes(data, Some(90000)));
    frames.extend(parser.flush());
    assert_eq!(frames.len(), 2, "the parity-broken AU is dropped");
    assert_eq!(parser.dropped_frames(), 1);
    // The drop is a SILENCE GAP whose length is what the CLI reports as lost
    // audio: one AU = 40 samples at 48 kHz = 40/48000s = 833_333 ns. A count
    // without a rate-aware duration understates or invents the loss.
    assert_eq!(
        parser.dropped_duration_ns(),
        833_333,
        "one dropped AU = 40 samples at 48 kHz"
    );
}

#[test]
fn drop_forward_preserves_av_sync_no_shift() {
    // THE INVARIANT: the resumed major sync keeps the exact PTS it would have
    // had with no drop — base + 3 AU durations (MS1, corrupt-MS, normal, MS2)
    // — so the drop is a silence gap, never a shift.
    let mut parser = TrueHdParser::new();
    let ms1 = valid_major_sync();
    let mut ms_bad = valid_major_sync();
    ms_bad[10] ^= 0xFF;
    let normal = valid_normal_au();
    let ms2 = valid_major_sync();
    let mut data = ms1;
    data.extend_from_slice(&ms_bad);
    data.extend_from_slice(&normal);
    data.extend_from_slice(&ms2);
    let mut frames = parser.parse(&make_pes(data, Some(90000)));
    frames.extend(parser.flush());
    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[1].pts_ns - frames[0].pts_ns,
        3 * AU_DURATION_NS,
        "resumed major sync keeps its true timeline (gap, not shift)"
    );
}

#[test]
fn transient_corruptions_do_not_poison_whole_track() {
    // Regression (audit HIGH): drop-forward must not amplify a couple of
    // transient errors into a false whole-track poison — even resync runs
    // past 200 AUs must leave the track un-poisoned and keep good audio after.
    let mut parser = TrueHdParser::new();
    let mut data = valid_major_sync();
    // Corruption #1 then a long run of normal AUs (all collateral-dropped
    // while resyncing — no major sync to re-init on).
    let mut bad1 = valid_normal_au();
    bad1[2] ^= 0x01; // single-nibble parity break
    data.extend_from_slice(&bad1);
    for _ in 0..210 {
        data.extend_from_slice(&valid_normal_au());
    }
    // A valid major sync resumes; the good AUs after it MUST be kept.
    data.extend_from_slice(&valid_major_sync());
    for _ in 0..5 {
        data.extend_from_slice(&valid_normal_au());
    }
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert!(
        !parser.tally.is_poisoned(),
        "two transient errors must not poison the track"
    );
    // MS1 + resumed MS2 + the 5 good AUs after it survive.
    assert_eq!(
        frames.len(),
        7,
        "post-resync good audio is kept, not poisoned away"
    );
    assert!(
        parser.dropped_frames() > 200,
        "the resync run was still counted for reporting"
    );
}

#[test]
fn corrupt_major_sync_rate_nibble_does_not_shift_pts() {
    // Regression (audit MED): a corrupt major sync's rate nibble must NOT
    // refine au_duration_ns — the rate is only trustworthy after the CRC
    // validates, else the resumed 48kHz audio is shifted (not gapped).
    let mut parser = TrueHdParser::new();
    let ms1 = valid_major_sync(); // 48 kHz
    let mut ms_bad = valid_major_sync();
    // Set the rate nibble (top nibble of format_info = au[8]) to 0x8 (44.1k).
    // au[8] is CRC-covered, so this also breaks the major-sync CRC → corrupt.
    ms_bad[8] = (ms_bad[8] & 0x0F) | 0x80;
    let ms2 = valid_major_sync(); // 48 kHz
    let mut data = ms1;
    data.extend_from_slice(&ms_bad);
    data.extend_from_slice(&ms2);
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 2, "corrupt MS dropped; MS1 and MS2 survive");
    assert_eq!(
        frames[1].pts_ns - frames[0].pts_ns,
        2 * AU_DURATION_NS,
        "resumed audio keeps the 48 kHz cadence — the corrupt MS's 44.1k rate was ignored"
    );
}

#[test]
fn too_short_major_sync_does_not_clear_resync() {
    // Regression (audit LOW): while resyncing, a major sync too short to hold
    // (and CRC-validate) its header must NOT be treated as a clean resync
    // point — the runt is dropped and only a real validated major sync resumes.
    let mut parser = TrueHdParser::new();
    let ms1 = valid_major_sync();
    let mut bad = valid_normal_au();
    bad[2] ^= 0x01; // parity break → triggers resync
    // An 8-byte "major sync": length=4 words, sync at bytes 4..8, too short
    // to hold the 28-byte major-sync header.
    let runt = vec![0x00, 0x04, 0x00, 0x00, 0xF8, 0x72, 0x6F, 0xBA];
    let ms2 = valid_major_sync();
    let mut data = ms1;
    data.extend_from_slice(&bad);
    data.extend_from_slice(&runt);
    data.extend_from_slice(&ms2);
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 2, "the runt major sync did not resume decode");
    for f in &frames {
        assert_eq!(
            f.data.len(),
            200,
            "only the real 200-byte major syncs survive"
        );
    }
}

#[test]
fn au_shorter_than_directory_after_baseline_arms_resync() {
    // With a proven baseline, a 2-byte AU can't hold its substream directory:
    // it is corruption, so it is dropped and the next normal AU is dropped forward.
    let mut parser = TrueHdParser::new();
    let mut data = valid_major_sync();
    data.extend_from_slice(&[0x00, 0x01]);
    data.extend_from_slice(&valid_normal_au());
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 1, "runt AU and its follower are dropped");
    assert!(parser.dropped_frames() >= 1);
}

#[test]
fn clean_truehd_stream_drops_nothing() {
    // A run of valid AUs passes untouched — zero false positives (the CRC and
    // parity are verified against real TrueHD output).
    let mut parser = TrueHdParser::new();
    let mut data = valid_major_sync();
    for _ in 0..5 {
        data.extend_from_slice(&valid_normal_au());
    }
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 6);
    assert_eq!(parser.dropped_frames(), 0);
}

fn make_ac3_frame() -> Vec<u8> {
    // Minimal AC-3 frame: sync 0x0B77, fscod=0 (48kHz), frmsizecod=0 (64 words = 128 bytes)
    let mut data = vec![0u8; 128];
    data[0] = 0x0B;
    data[1] = 0x77;
    data[4] = 0x00; // fscod=0, frmsizecod=0
    data
}

#[test]
fn parse_empty_pes() {
    let mut parser = TrueHdParser::new();
    let pes = make_pes(Vec::new(), Some(0));
    assert!(parser.parse(&pes).is_empty());
}

#[test]
fn parse_single_unit() {
    let mut parser = TrueHdParser::new();
    let unit = make_truehd_unit(200);
    let pes = make_pes(unit, Some(90000));
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data.len(), 200);
}

#[test]
fn parse_unit_spanning_two_pes() {
    let mut parser = TrueHdParser::new();
    let unit = make_truehd_unit(200);
    let mid = 100;

    let pes1 = make_pes(unit[..mid].to_vec(), Some(90000));
    assert!(parser.parse(&pes1).is_empty());

    let pes2 = make_pes(unit[mid..].to_vec(), Some(93000));
    let frames = parser.parse(&pes2);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data.len(), 200);
}

#[test]
fn discontinuity_drops_truncated_partial() {
    // B1: a partial TrueHD unit is buffered, then a concealed gap (PES marked
    // discontinuity) carries a fresh unit. The truncated partial must be
    // dropped — splicing it corrupts AU framing; PTS re-seeds from the post-gap PES.
    let mut parser = TrueHdParser::new();

    // PES 1: first 150 bytes of a 300-byte unit (length prefix says 300, only
    // 150 present) → held, nothing emitted.
    let partial = make_truehd_unit(300);
    let pes1 = make_pes(partial[..150].to_vec(), Some(90000));
    assert!(parser.parse(&pes1).is_empty(), "partial unit held");

    // Concealed gap: a fresh 200-byte unit at a forward PTS jump.
    let fresh = make_truehd_unit(200);
    let pes2 = PesPacket {
        source: None,
        pid: 0x1100,
        pts: Some(180000),
        dts: None,
        data: fresh.clone(),
        discontinuity: true,
    };
    let frames = parser.parse(&pes2);
    assert_eq!(frames.len(), 1, "exactly one clean unit across the gap");
    assert_eq!(
        frames[0].data.len(),
        200,
        "emitted unit is the fresh 200-byte one, not a 300-byte splice"
    );
    assert_eq!(
        frames[0].data, fresh,
        "unit bytes are the fresh post-gap unit"
    );
    assert_eq!(
        frames[0].pts_ns,
        pts_to_ns(180000),
        "cadence re-bases to the post-gap PTS across the cleared buffer"
    );
}

// A long zero-header run must be drained in ~1 call, not one 4-byte
// drain per header (each drain shifts the tail — O(run_len^2) otherwise).
#[test]
fn a_long_zero_word_run_is_drained_once_not_per_word() {
    const ZERO_WORDS: usize = 8192; // 32 KiB of zero headers
    let mut parser = TrueHdParser::new();
    let mut data = vec![0u8; ZERO_WORDS * 4];
    // A real unit follows, so the run has a clean end to detect.
    data.extend_from_slice(&make_truehd_unit(100));
    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1, "the trailing real unit is still emitted");
    assert_eq!(frames[0].data.len(), 100);
    assert!(
        parser.zero_run_drains <= 2,
        "expected the whole zero run to be drained in ~1 call, got {} drains for {ZERO_WORDS} zero words",
        parser.zero_run_drains
    );
}

#[test]
fn parse_multiple_units_incrementing_pts() {
    let mut parser = TrueHdParser::new();
    let mut data = make_truehd_unit(100);
    data.extend_from_slice(&make_truehd_unit(120));
    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data.len(), 100);
    assert_eq!(frames[1].data.len(), 120);
    assert_eq!(frames[1].pts_ns - frames[0].pts_ns, AU_DURATION_NS);
}

#[test]
fn pes_pts_lagging_the_au_cadence_never_emits_backward() {
    // Regression: the per-AU cadence is sample-accurate, but a PES boundary
    // can carry a PTS that lags it slightly (muxer rounding jitter). An
    // unconditional reset snapped the next AU below the prior one — clamp forward-only.
    let mut parser = TrueHdParser::new();
    let au = make_truehd_unit(100);
    // PES1: three complete AUs at pts 90000 — buffer empties, cadence runs
    // ahead to 90000_ns + 3*AU_DURATION_NS.
    let mut d1 = au.clone();
    d1.extend_from_slice(&au);
    d1.extend_from_slice(&au);
    let f1 = parser.parse(&make_pes(d1, Some(90000)));
    assert_eq!(f1.len(), 3);
    let last1 = f1.last().unwrap().pts_ns;
    // PES2's PTS (90001) maps to fewer ns than the running cadence — pre-fix
    // this snapped backward.
    let f2 = parser.parse(&make_pes(au.clone(), Some(90001)));
    assert_eq!(f2.len(), 1);
    assert!(
        f2[0].pts_ns >= last1,
        "AU pts must not go backward when PES PTS lags the cadence: got {} after {}",
        f2[0].pts_ns,
        last1
    );
}

#[test]
fn clip_boundary_pts_reset_is_adopted_not_clamped() {
    // Regression (multi-clip non-monotonic audio-DTS band): a non-seamless
    // clip boundary resets PES PTS near zero (large backward step, not
    // jitter) and must be ADOPTED raw, like DTS/AC-3, or audio strands at the prior tail.
    let mut parser = TrueHdParser::new();
    let au = make_truehd_unit(100);
    // Clip 1: an AU at PES PTS = 10s (90000 ticks/s → 900_000 ticks). Buffer
    // empties, so the next PES seeds a fresh base.
    let clip1_pts = 90_000 * 10; // 10 s in 90 kHz ticks
    let f1 = parser.parse(&make_pes(au.clone(), Some(clip1_pts)));
    assert_eq!(f1.len(), 1);
    let last1 = f1[0].pts_ns;
    assert_eq!(last1, pts_to_ns(clip1_pts));
    // Clip 2: PES PTS resets to 0 — 10 s backward, far beyond the 3 s
    // discontinuity threshold. Must be adopted, not clamped to the cadence.
    let f2 = parser.parse(&make_pes(au.clone(), Some(0)));
    assert_eq!(f2.len(), 1);
    assert_eq!(
        f2[0].pts_ns, 0,
        "clip-boundary PTS reset must be adopted raw (got {}, expected the \
             reset value 0 — clamping to the previous clip's cadence is the bug)",
        f2[0].pts_ns
    );
    assert!(
        f2[0].pts_ns < last1,
        "the reset frame must land below the previous clip's tail, not above it"
    );
}

#[test]
fn skip_interleaved_ac3() {
    let mut parser = TrueHdParser::new();
    let ac3 = make_ac3_frame();
    let truehd = make_truehd_unit(200);
    let mut data = ac3;
    data.extend_from_slice(&truehd);
    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data.len(), 200);
}

#[test]
fn continuation_pes_pts_does_not_override_au_in_progress() {
    // An AU split across two PES packets: the first PES (pts 90000) begins
    // it, the second (pts 99999) merely continues it. The emitted AU must
    // keep the first PES's PTS, not the continuation's.
    let mut parser = TrueHdParser::new();
    let unit = make_truehd_unit(200);
    let mid = 100;

    let pes1 = make_pes(unit[..mid].to_vec(), Some(90000));
    assert!(parser.parse(&pes1).is_empty(), "AU held mid-assembly");

    // Continuation PES carries a later PTS that must be ignored for this AU.
    let pes2 = make_pes(unit[mid..].to_vec(), Some(99999));
    let frames = parser.parse(&pes2);
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].pts_ns,
        pts_to_ns(90000),
        "AU keeps the PTS of the PES that began it, not the continuation PES"
    );
}

#[test]
fn new_au_after_empty_buffer_takes_new_pes_pts() {
    // After an AU fully drains (buffer empty), the next PES legitimately
    // seeds a fresh PTS base.
    let mut parser = TrueHdParser::new();
    let f1 = parser.parse(&make_pes(make_truehd_unit(200), Some(90000)));
    assert_eq!(f1.len(), 1);
    assert_eq!(f1[0].pts_ns, pts_to_ns(90000));

    // Buffer is now empty; a new PES with a new PTS starts a new AU.
    let f2 = parser.parse(&make_pes(make_truehd_unit(200), Some(180000)));
    assert_eq!(f2.len(), 1);
    assert_eq!(
        f2[0].pts_ns,
        pts_to_ns(180000),
        "new AU after empty buffer adopts the new PES PTS"
    );
}

#[test]
fn zero_length_au_drains_full_header() {
    // A zero-length AU header (4 bytes) must be skipped whole. Draining only
    // 2 would misread the timing bytes (0x01 0x90 = 400 words = 800 bytes) as
    // a bogus length, stalling the parser waiting for bytes that never come.
    let mut parser = TrueHdParser::new();
    let mut data = vec![0x00, 0x00, 0x01, 0x90]; // length=0, timing=0x0190
    data.extend_from_slice(&make_truehd_unit(200));
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 1, "real unit parses after zero-length header");
    assert_eq!(frames[0].data.len(), 200);
}

#[test]
fn unmappable_ac3_header_resyncs_not_stalls() {
    // A permanently unmappable 0x0B77 header (reserved fscod==3) must NOT
    // stall the parser — it used to be treated as "incomplete, wait" and
    // break forever. Now it resyncs (drains 2 bytes) so a clean unit behind it is emitted.
    let mut parser = TrueHdParser::new();
    // Unmappable AC-3-looking head: 0x0B77, byte4 fscod=3 (0xC0).
    let mut data = vec![0x0B, 0x77, 0x00, 0x00, 0xC0, 0x00];
    // A clean TrueHD AU follows.
    data.extend_from_slice(&make_truehd_unit(200));
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(
        frames.len(),
        1,
        "TrueHD AU behind a bad header is recovered"
    );
    assert_eq!(frames[0].data.len(), 200);
    assert!(parser.acc.is_empty(), "buffer fully consumed, no stall");
}

#[test]
fn truehd_au_with_0b77_head_not_stolen_by_ac3() {
    // A TrueHD AU whose first two bytes are 0x0B 0x77 must NOT be misrouted
    // to the AC-3 path — the AC-3 size closes the boundary wrong; secondary
    // corroboration rejects it since the computed end isn't followed by a real sync.
    let mut parser = TrueHdParser::new();
    // 5870-byte AU starting with 0x0B 0x77. Byte 4 = 0x00 → AC-3 would
    // size it as fscod=0, frmsizecod=0 → 128 bytes. The bytes at offset 128
    // are zeros (next_words==0) → not corroborated → kept as TrueHD.
    let mut unit = vec![0u8; 5870];
    unit[0] = 0x0B; // 0xB high nibble of the 12-bit length, check nibble 0
    unit[1] = 0x77; // low byte of length 0xB77
    let frames = parser.parse(&make_pes(unit, Some(90000)));
    assert_eq!(frames.len(), 1, "0x0B77-headed TrueHD AU kept whole");
    assert_eq!(
        frames[0].data.len(),
        5870,
        "AU sized by TrueHD length, not AC-3 frame size"
    );
}

#[test]
fn codec_private_none() {
    let parser = TrueHdParser::new();
    assert!(parser.codec_private().is_none());
}

#[test]
fn truehd_channels_71_from_8ch_presentation() {
    // 8ch presentation assignment bits 0-4 (LR,C,LFE,LsRs,back-LR) = 2+1+1+2+2 = 8.
    let format_info = 0x1F; // low 13 bits = 0x1F
    assert_eq!(truehd_channels(format_info), Some(8));
}

#[test]
fn truehd_channels_51_from_6ch_presentation() {
    // No 8ch presentation; 6ch bits 0-3 (LR,C,LFE,LsRs) = 2+1+1+2 = 6.
    let format_info = 0xF << 15; // 6ch field = 0xF, 8ch field = 0
    assert_eq!(truehd_channels(format_info), Some(6));
}

#[test]
fn truehd_channels_scan_finds_major_sync() {
    // [junk][major sync 0xF8726FBA][format_info: 8ch=0x1F -> 7.1]
    let mut data = vec![0xAA, 0xBB];
    data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
    data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
    assert_eq!(truehd_channels_from_stream(&data), Some(8));
}

// --- truehd_channels: per-bit mask channel counts (MLP channel table) ---

#[test]
fn truehd_channels_8ch_single_bit_counts() {
    // THD_8CH = [2,1,1,2,2,2,2,1,1,2,2,1,1]. A single set bit must yield
    // exactly that bit's channel count. Bit 0 → 2 (L/R pair), bit 1 → 1 (C),
    // bit 2 → 1 (LFE), bit 7 → 1.
    assert_eq!(truehd_channels(1 << 0), Some(2));
    assert_eq!(truehd_channels(1 << 1), Some(1));
    assert_eq!(truehd_channels(1 << 2), Some(1));
    assert_eq!(truehd_channels(1 << 7), Some(1));
}

#[test]
fn truehd_channels_8ch_all_bits_set() {
    // All 13 8ch bits set = 2+1+1+2+2+2+2+1+1+2+2+1+1 = 20. ch8 field is the
    // low 13 bits (0x1FFF).
    assert_eq!(truehd_channels(0x1FFF), Some(20));
}

#[test]
fn truehd_channels_6ch_used_only_when_8ch_zero() {
    // The 8ch presentation takes priority; the 6ch field (bits 15-19) is read
    // ONLY when ch8 == 0. THD_6CH = [2,1,1,2,2]. Set 6ch bit 0 (→2) while
    // 8ch is zero: 6ch field value 1 at shift 15.
    assert_eq!(truehd_channels(1 << 15), Some(2));
    // All 5 6ch bits = 2+1+1+2+2 = 8 (bit 4 is the Lvh/Rvh pair). 0x1F << 15.
    assert_eq!(truehd_channels(0x1F << 15), Some(8));
}

#[test]
fn truehd_channels_8ch_wins_over_6ch_when_both_present() {
    // When BOTH fields are non-zero, the richer 8ch presentation is used.
    // 8ch = bit0 (→2), 6ch = all bits (would be 7) → result must be 2, the
    // 8ch count, proving the `if ch8 != 0` branch wins.
    let fi = (1u32 << 0) | (0x1F << 15);
    assert_eq!(truehd_channels(fi), Some(2));
}

#[test]
fn truehd_lfe_reads_the_chosen_presentation() {
    assert_eq!(truehd_lfe(0x0F), 1, "8ch LFE bit");
    assert_eq!(truehd_lfe(0x100F), 2, "8ch LFE + LFE2");
    assert_eq!(truehd_lfe(0x4B), 0, "7.0 has no LFE");
    assert_eq!(truehd_lfe(0xF << 15), 1, "6ch LFE bit when 8ch is empty");
    assert_eq!(truehd_lfe(0x03 | (0xF << 15)), 0, "8ch wins over 6ch");
    assert_eq!(truehd_lfe(0x1B << 15), 0, "6ch mask with every bit but LFE");
    assert_eq!(truehd_lfe(0x04 << 15), 1, "6ch LFE bit alone");
    assert_eq!(truehd_lfe(0), 0);
}

#[test]
fn truehd_channels_none_when_both_fields_zero() {
    // No presentation flags set → None (can't determine layout).
    assert_eq!(truehd_channels(0), None);
    // Bits outside both fields (e.g. bit 13, bit 14, bits 20-31) don't count
    // as a presentation and must still yield None.
    assert_eq!(truehd_channels(1 << 13), None);
    assert_eq!(truehd_channels(1 << 20), None);
}

// --- truehd_channels_from_stream: major-sync variant bit + scan ---

#[test]
fn channels_from_stream_rejects_mlp_sync_0xfb() {
    // 0xF8726FBB is the MLP stream type, NOT TrueHD (0xF8726FBA). Its next
    // word holds quantization/MLP rate fields, not TrueHD's rate nibble +
    // channel masks; decoding it as TrueHD reads channels from unrelated bits.
    let mut data = vec![0x00];
    data.extend_from_slice(&0xF872_6FBBu32.to_be_bytes());
    data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
    assert_eq!(
        truehd_channels_from_stream(&data),
        None,
        "an MLP (0xBB) major sync must not be decoded as TrueHD format_info"
    );
    // The same bytes under the TrueHD stream type DO decode.
    let mut data = vec![0x00];
    data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
    data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
    assert_eq!(truehd_channels_from_stream(&data), Some(8));
}

#[test]
fn mlp_sync_0xfb_yields_no_sample_rate_or_atmos() {
    // Same split for the shared scan: an MLP major sync must not produce a
    // TrueHD rate (bits 31..28 of an MLP header are a quantization code, not
    // the rate) nor an Atmos verdict.
    let mut data = vec![0x00];
    data.extend_from_slice(&0xF872_6FBBu32.to_be_bytes());
    // ratebits nibble 0x1 would decode as 96 kHz under the TrueHD layout.
    data.extend_from_slice(&0x1000_001Fu32.to_be_bytes());
    data.extend_from_slice(&[0x00; 12]);
    assert!(
        truehd_sync_info_from_stream(&data).is_none(),
        "no TrueHD sync info from an MLP major sync"
    );
    assert_eq!(truehd_sample_rate_from_stream(&data), None);
    // TrueHD stream type, identical trailing bytes → the rate IS decoded.
    let mut data = vec![0x00];
    data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
    data.extend_from_slice(&0x1000_001Fu32.to_be_bytes());
    data.extend_from_slice(&[0x00; 12]);
    assert_eq!(truehd_sample_rate_from_stream(&data), Some(96000));
}

#[test]
fn channels_from_stream_none_without_major_sync() {
    // No major sync anywhere → None, no panic, scan terminates.
    let data = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    assert_eq!(truehd_channels_from_stream(&data), None);
}

#[test]
fn channels_from_stream_too_short_for_format_info() {
    // Sync present but fewer than 8 bytes total → the `p + 8 <= len` guard
    // prevents reading format_info out of bounds → None.
    let data = 0xF872_6FBAu32.to_be_bytes().to_vec(); // 4 bytes only
    assert_eq!(truehd_channels_from_stream(&data), None);
}

#[test]
fn channels_from_stream_unaligned_sync() {
    // The scan advances 1 byte at a time, so a major sync at an odd offset
    // is still found. Place it at offset 3.
    let mut data = vec![0xAA, 0xBB, 0xCC];
    data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
    data.extend_from_slice(&(0x1Fu32).to_be_bytes());
    assert_eq!(truehd_channels_from_stream(&data), Some(8));
}

// --- AU length field: 12-bit mask, partial AU, is_major_sync keyframe ---

#[test]
fn au_length_uses_low_12_bits_only() {
    // unit_words = ((b0<<8)|b1) & 0xFFF. The top 4 bits of b0 (the MLP
    // check/access-unit nibble) must NOT inflate the length. b0 = 0xF1
    // (nibble 0xF, low 0x1), b1 = 0x00 → words = 0x100 = 256 → 512 bytes.
    let mut parser = TrueHdParser::new();
    let mut unit = vec![0u8; 512];
    unit[0] = 0xF1; // high nibble 0xF must be masked off
    unit[1] = 0x00;
    let f = parser.parse(&make_pes(unit, Some(90000)));
    assert_eq!(f.len(), 1);
    assert_eq!(
        f[0].data.len(),
        512,
        "length sized from low 12 bits (0x100 words), nibble masked"
    );
}

#[test]
fn au_with_major_sync_is_keyframe() {
    // An AU whose bytes 4-7 hold the major sync (0xF8726FBA, low bit masked)
    // is a restart point → keyframe. Build a >=8-byte AU with the sync at
    // offset 4. words = 100 → 200 bytes.
    let mut parser = TrueHdParser::new();
    let mut unit = make_truehd_unit(200);
    unit[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
    finalize_major_sync(&mut unit);
    let f = parser.parse(&make_pes(unit, Some(90000)));
    assert_eq!(f.len(), 1);
    assert!(f[0].keyframe, "major-sync AU must be flagged keyframe");
}

#[test]
fn au_without_major_sync_is_not_keyframe() {
    // A plain AU (no major sync at offset 4) is not a keyframe.
    let mut parser = TrueHdParser::new();
    let f = parser.parse(&make_pes(make_truehd_unit(200), Some(90000)));
    assert_eq!(f.len(), 1);
    assert!(!f[0].keyframe);
}

#[test]
fn major_sync_variant_bit_also_keyframe() {
    // The restart-point check masks the low sync bit, so 0xF8726FBB (MLP)
    // counts as a major sync too — both types re-init the decoder, so both
    // are keyframes (format_info decode alone is 0xBA-only; see the sibling test).
    let mut parser = TrueHdParser::new();
    let mut unit = make_truehd_unit(200);
    unit[4..8].copy_from_slice(&0xF872_6FBBu32.to_be_bytes());
    finalize_major_sync(&mut unit);
    let f = parser.parse(&make_pes(unit, Some(90000)));
    assert_eq!(f.len(), 1);
    assert!(f[0].keyframe, "major-sync variant 0xFB also a keyframe");
}

#[test]
fn incomplete_au_waits_does_not_emit_short() {
    // The AU length declares more bytes than buffered → parser must wait, not
    // emit a truncated AU. words=300 (0x12C) → 600 bytes declared, only 100
    // present. 300 exercises both length bytes (high nibble 0x1, low 0x2C).
    let mut parser = TrueHdParser::new();
    let mut data = vec![0u8; 100];
    let words = 300usize;
    data[0] = ((words >> 8) & 0x0F) as u8; // 0x01
    data[1] = (words & 0xFF) as u8; // 0x2C → 300 words = 600 bytes
    let f = parser.parse(&make_pes(data, Some(90000)));
    assert!(
        f.is_empty(),
        "must not emit fewer bytes than the length field"
    );
    assert_eq!(parser.acc.len(), 100, "partial AU retained");
}

/// Largest AU the 12-bit length field can declare: 0xFFF words × 2.
const MAX_AU_BYTES: usize = 0xFFF * 2; // 8190

#[test]
fn buffer_stays_bounded_across_many_partial_pes() {
    // Malformed/never-completing input must keep the buffer bounded. The
    // bound that actually holds is MAX_AU_BYTES (8190), not MAX_TRUEHD_BUF —
    // asserting only `<= MAX_TRUEHD_BUF` is vacuous; assert the reachable ceiling instead.
    let mut parser = TrueHdParser::new();
    let mut worst = 0usize;
    for _ in 0..200 {
        let mut frag = vec![0u8; MAX_AU_BYTES - 1];
        frag[0] = 0xFF;
        frag[1] = 0xFF;
        let _ = parser.parse(&make_pes(frag, Some(0)));
        worst = worst.max(parser.acc.len());
        assert!(
            parser.acc.len() < MAX_AU_BYTES,
            "reassembly buffer exceeded the AU-length ceiling: {} >= {}",
            parser.acc.len(),
            MAX_AU_BYTES
        );
        assert!(
            parser.acc.len() <= MAX_TRUEHD_BUF,
            "reassembly buffer exceeded cap: {} > {}",
            parser.acc.len(),
            MAX_TRUEHD_BUF
        );
    }
    // The fixture must genuinely load the buffer, not self-drain: if this
    // trips, the test is measuring nothing.
    assert!(
        worst >= MAX_AU_BYTES - 8,
        "fixture must drive the buffer to the ceiling, peaked at {worst}"
    );
}

// --- ac3_boundary_corroborated: the AC-3-vs-TrueHD disambiguation ---

#[test]
fn ac3_corroborated_when_frame_fills_buffer() {
    // frame_bytes >= buf.len() → the AC-3 frame ends the buffer → corroborated.
    let buf = vec![0u8; 128];
    assert!(ac3_boundary_corroborated(&buf, 128));
    assert!(ac3_boundary_corroborated(&buf, 200));
}

#[test]
fn ac3_corroborated_when_next_is_ac3_sync() {
    // Bytes after the frame begin with 0x0B 0x77 → another AC-3 frame →
    // corroborated.
    let mut buf = vec![0u8; 130];
    buf[128] = 0x0B;
    buf[129] = 0x77;
    assert!(ac3_boundary_corroborated(&buf, 128));
}

#[test]
fn ac3_corroborated_when_next_is_plausible_truehd_au() {
    // Bytes after the frame form a plausible TrueHD AU header (non-zero
    // 12-bit length within 32 KiB) → corroborated. next_words = 0x100 = 256
    // → 512 bytes <= 32768.
    let mut buf = vec![0u8; 130];
    buf[128] = 0x01; // (0x01<<8)|0x00 & 0xFFF = 0x100
    buf[129] = 0x00;
    assert!(ac3_boundary_corroborated(&buf, 128));
}

#[test]
fn ac3_not_corroborated_when_next_zero_length() {
    // Bytes after the frame are zeros → next_words == 0 → NOT a plausible
    // TrueHD AU and not an AC-3 sync → NOT corroborated (treat as TrueHD).
    let buf = vec![0u8; 130]; // all zero after frame_bytes=128
    assert!(!ac3_boundary_corroborated(&buf, 128));
}

#[test]
fn ac3_corroborated_when_too_few_trailing_bytes() {
    // Fewer than 2 bytes follow the frame → can't judge → accept (next call
    // sees the continuation). frame_bytes=128, buf=129 → 1 trailing byte.
    let buf = vec![0u8; 129];
    assert!(ac3_boundary_corroborated(&buf, 128));
}

#[test]
fn ac3_frame_at_head_needs_more_when_buffer_short() {
    let ac3 = make_ac3_frame(); // 128 bytes
    let size = |bytes: &[u8]| {
        let mut p = TrueHdParser::new();
        p.acc.seed(bytes);
        p.ac3_frame_at_head()
    };
    // Too short to read the header, then a valid header whose frame is not all here.
    assert!(matches!(size(&ac3[..5]), Ac3Size::NeedMore));
    assert!(matches!(size(&ac3[..100]), Ac3Size::NeedMore));
    assert!(matches!(size(&ac3), Ac3Size::Frame(128)));
    // frmsizecod 55 is out of the table: resync, not wait.
    let mut bad = ac3.clone();
    bad[4] = 55;
    assert!(matches!(size(&bad), Ac3Size::Unmappable));

    // An AC-3 frame split across PES is held, then skipped whole: the TrueHD unit that
    // follows it comes out intact.
    let mut parser = TrueHdParser::new();
    assert!(
        parser
            .parse(&make_pes(ac3[..100].to_vec(), Some(0)))
            .is_empty()
    );
    let mut rest = ac3[100..].to_vec();
    rest.extend_from_slice(&make_truehd_unit(200));
    let f = parser.parse(&make_pes(rest, None));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].data.len(), 200);
}

// --- #2 sample rate from the major-sync rate nibble ---

// `format_info` with given `ratebits` (top nibble) + a 7.1 8-channel
// mask (ch8 = 0x1F) in the low 13 bits, co-located in one real word.
fn format_info_with(ratebits: u32) -> u32 {
    ((ratebits & 0xF) << 28) | 0x1F
}

#[test]
fn sample_rate_whitelist_real_rates() {
    assert_eq!(truehd_sample_rate_hz(format_info_with(0x0)), Some(48000));
    assert_eq!(truehd_sample_rate_hz(format_info_with(0x1)), Some(96000));
    assert_eq!(truehd_sample_rate_hz(format_info_with(0x2)), Some(192000));
    assert_eq!(truehd_sample_rate_hz(format_info_with(0x8)), Some(44100));
    assert_eq!(truehd_sample_rate_hz(format_info_with(0x9)), Some(88200));
    assert_eq!(truehd_sample_rate_hz(format_info_with(0xA)), Some(176400));
}

#[test]
fn sample_rate_unknown_rate_falls_back_to_none() {
    // 0xF is the explicit invalid code; 0x3/0xB are formula-only, not
    // whitelisted; 0x7/0xE are reserved. None may produce a rate — the host
    // must fall back to its container value, never write a wrong SamplingFrequency.
    for bad in [0x3u32, 0x7, 0xB, 0xC, 0xD, 0xE, 0xF] {
        assert_eq!(
            truehd_sample_rate_hz(format_info_with(bad)),
            None,
            "ratebits {bad:#x} must not yield a rate"
        );
    }
}

#[test]
fn sample_rate_nibble_does_not_disturb_channel_decode() {
    // Internal-consistency guard: with the 96kHz nibble AND a 7.1 mask in
    // the same word, rate reads 96000 and channels still read 8 — proving
    // the rate nibble (bits 31..28) and channel masks (bits 19..0) don't collide.
    let fi = format_info_with(0x1);
    assert_eq!(truehd_sample_rate_hz(fi), Some(96000));
    assert_eq!(truehd_channels(fi), Some(8));
}

#[test]
fn sample_rate_from_stream_scans_major_sync() {
    // [junk][0xF8726FBA][format_info: ratebits=0x1 (96k), ch8=0x1F]
    let mut data = vec![0xAA, 0xBB];
    data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
    data.extend_from_slice(&format_info_with(0x1).to_be_bytes());
    assert_eq!(truehd_sample_rate_from_stream(&data), Some(96000));
}

#[test]
fn sample_rate_from_stream_none_without_sync() {
    let data = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    assert_eq!(truehd_sample_rate_from_stream(&data), None);
}

// --- #3 per-AU duration: family-aware, 48 kHz family byte-identical ---

#[test]
fn au_duration_48k_family_unchanged() {
    // 48 / 96 / 192 kHz (ratebits 0x0/0x1/0x2) all keep the exact current
    // 833_333 constant — the common case must never shift.
    for rb in [0x0u32, 0x1, 0x2] {
        assert_eq!(truehd_au_duration_ns(format_info_with(rb)), 833_333);
    }
}

#[test]
fn au_duration_441k_family_is_907029() {
    // 44.1 / 88.2 / 176.4 kHz (ratebits 0x8/0x9/0xA) → 907_029 ns.
    for rb in [0x8u32, 0x9, 0xA] {
        assert_eq!(truehd_au_duration_ns(format_info_with(rb)), 907_029);
    }
}

#[test]
fn au_duration_unknown_rate_keeps_default() {
    // An unrecognised/garbage rate nibble must not pick the 44.1 k value
    // (note 0xF & 8 != 0): it falls back to the 833_333 default.
    for rb in [0x3u32, 0x7, 0xB, 0xF] {
        assert_eq!(truehd_au_duration_ns(format_info_with(rb)), 833_333);
    }
}

#[test]
fn parser_44k_major_sync_sets_907029_increment() {
    // Two AUs: the first carries a major sync with ratebits=0x8 (44.1 k).
    // After the parser reads it, the per-AU PTS increment must be 907_029.
    let mut parser = TrueHdParser::new();
    let mut a1 = make_truehd_unit(200);
    a1[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes()); // major sync
    a1[8..12].copy_from_slice(&format_info_with(0x8).to_be_bytes()); // 44.1 k
    finalize_major_sync(&mut a1);
    let mut a2 = make_truehd_unit(200);
    finalize_normal_parity(&mut a2);
    let mut data = a1;
    data.extend_from_slice(&a2);
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[1].pts_ns - frames[0].pts_ns,
        907_029,
        "44.1 k-family AU increments by 907_029 once the major sync is read"
    );
}

#[test]
fn parser_48k_major_sync_keeps_833333_increment() {
    // Regression: a 48 k-family (ratebits=0x0) major sync keeps the exact
    // current 833_333 increment.
    let mut parser = TrueHdParser::new();
    let mut a1 = make_truehd_unit(200);
    a1[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
    a1[8..12].copy_from_slice(&format_info_with(0x0).to_be_bytes()); // 48 k
    finalize_major_sync(&mut a1);
    let mut a2 = make_truehd_unit(200);
    finalize_normal_parity(&mut a2);
    let mut data = a1;
    data.extend_from_slice(&a2);
    let frames = parser.parse(&make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[1].pts_ns - frames[0].pts_ns, 833_333);
}

// --- #1 Atmos detection from num_substreams (msync[16] >> 4) ---

/// Build a demuxed chunk with one major sync whose 17th sync byte (offset
/// 16 from the 0xF8) has top nibble `num_substreams`. The AU is padded past
/// byte 16 so the substream count is reachable.
fn major_sync_with_substreams(num_substreams: u8) -> Vec<u8> {
    let mut data = vec![0x00, 0x00]; // leading junk; scan is byte-aligned
    let sync_off = data.len();
    data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes()); // bytes [off..off+4]
    data.extend_from_slice(&format_info_with(0x0).to_be_bytes()); // format_info
    // Pad up to and including byte `sync_off + 16`.
    while data.len() <= sync_off + 16 {
        data.push(0x00);
    }
    data[sync_off + 16] = (num_substreams & 0xF) << 4;
    data
}

#[test]
fn atmos_true_when_four_substreams() {
    // num_substreams = 4 → byte 16 = 0x40 → Atmos object substream present.
    let data = major_sync_with_substreams(4);
    assert_eq!(truehd_is_atmos_from_stream(&data), Some(true));
}

#[test]
fn atmos_false_when_three_substreams() {
    // num_substreams = 3 (plain 7.1 TrueHD) → byte 16 = 0x30 → not Atmos.
    let data = major_sync_with_substreams(3);
    assert_eq!(truehd_is_atmos_from_stream(&data), Some(false));
}

#[test]
fn atmos_none_when_au_too_short_for_substream_byte() {
    // Major sync present but the chunk ends before byte sync_off+16 → None,
    // never a false Atmos. Sync at offset 0; only format_info follows.
    let mut data = 0xF872_6FBAu32.to_be_bytes().to_vec();
    data.extend_from_slice(&format_info_with(0x0).to_be_bytes()); // 8 bytes total
    assert_eq!(truehd_is_atmos_from_stream(&data), None);
}

#[test]
fn atmos_none_without_major_sync() {
    let data = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    assert_eq!(truehd_is_atmos_from_stream(&data), None);
}

#[test]
fn sync_info_combines_channels_rate_and_atmos() {
    // One scan yields all three facts: 7.1 channels, 96 kHz, 4 substreams.
    let data = {
        let mut d = vec![0x00, 0x00];
        let off = d.len();
        d.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
        d.extend_from_slice(&format_info_with(0x1).to_be_bytes()); // 96k + 7.1
        while d.len() <= off + 16 {
            d.push(0x00);
        }
        d[off + 16] = 0x40; // 4 substreams
        d
    };
    let info = truehd_sync_info_from_stream(&data).expect("major sync found");
    assert_eq!(truehd_channels(info.format_info), Some(8));
    assert_eq!(truehd_sample_rate_hz(info.format_info), Some(96000));
    assert_eq!(info.is_atmos, Some(true));
}

/// Same rule for TrueHD: a unit spanning two packets belongs to the one
/// that carried its first byte.
#[test]
fn an_access_unit_carries_the_source_of_the_packet_it_began_in() {
    let mut parser = TrueHdParser::new();
    let unit = make_truehd_unit(512);

    let mut p1 = make_pes(unit[..200].to_vec(), Some(90_000));
    p1.source = Some(crate::pes::SourcePos::at_byte(1_000));
    let first = parser.parse(&p1);
    assert!(first.is_empty(), "partial unit held");

    let mut p2 = make_pes(unit[200..].to_vec(), Some(180_000));
    p2.source = Some(crate::pes::SourcePos::at_byte(9_000));
    let frames = parser.parse(&p2);
    assert!(!frames.is_empty(), "the completed unit is emitted");
    assert_eq!(
        frames[0].source.map(|s| s.byte),
        Some(1_000),
        "the unit belongs to the packet its FIRST byte came from"
    );
}
