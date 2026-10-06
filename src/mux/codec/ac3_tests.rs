use super::*;

fn make_ac3_frame(fscod: u8, frmsizecod: u8) -> Vec<u8> {
    let size = AC3_FRAME_SIZES[frmsizecod as usize][fscod as usize] * 2;
    let mut frame = vec![0u8; size];
    frame[0] = 0x0B;
    frame[1] = 0x77;
    frame[4] = (fscod << 6) | frmsizecod;
    frame[5] = 0x08 << 3; // bsid = 8 (AC-3)
    finalize_ac3_crc(&mut frame);
    frame
}

// Sets the trailing CRC word so the frame's [2..] residue is zero (passes
// the decodability gate); leaves the crc1 field (bytes 2-3) untouched.
fn finalize_ac3_crc(frame: &mut [u8]) {
    let n = frame.len();
    if n < 4 {
        return;
    }
    let c = crc16_ansi(&frame[2..n - 2]);
    frame[n - 2] = (c >> 8) as u8;
    frame[n - 1] = (c & 0xFF) as u8;
}

// The per-PES working buffer must be REUSED, not reallocated (`parse` runs ~10^5 times per
// audio track).
#[test]
fn the_working_buffer_is_reused_across_packets_not_reallocated() {
    let mut parser = Ac3Parser::new();
    let frame = make_ac3_frame(0, 0);

    parser.parse(&PesPacket {
        source: None,
        pid: 0x1100,
        pts: Some(90_000),
        dts: None,
        data: frame.clone(),
        discontinuity: false,
    });
    let cap_after_first = parser.scratch.capacity();
    assert!(
        cap_after_first > 0,
        "the buffer must be handed back to the parser, not dropped"
    );

    // Discriminator: LARGE packet then small. A reused buffer keeps the large
    // capacity; a fresh `Vec` drops to the small size. Equal-sized packets
    // can't tell those apart — an earlier to_vec()-per-call bug passed that test.
    let big = [frame.clone(), frame.clone(), frame.clone(), frame.clone()].concat();
    parser.parse(&PesPacket {
        source: None,
        pid: 0x1100,
        pts: None,
        dts: None,
        data: big.clone(),
        discontinuity: false,
    });
    let cap_after_big = parser.scratch.capacity();
    assert!(
        cap_after_big >= big.len(),
        "the buffer must have grown to hold the large packet"
    );

    parser.parse(&PesPacket {
        source: None,
        pid: 0x1100,
        pts: None,
        dts: None,
        data: frame.clone(),
        discontinuity: false,
    });
    assert_eq!(
        parser.scratch.capacity(),
        cap_after_big,
        "after a small packet the buffer must STILL hold the large \
             capacity — a fresh allocation per call would have shrunk to the \
             small packet's size"
    );
}

#[test]
fn parse_empty_pes() {
    let mut parser = Ac3Parser::new();
    let pes = PesPacket {
        source: None,
        pid: 0,
        pts: None,
        dts: None,
        data: vec![],
        discontinuity: false,
    };
    assert!(parser.parse(&pes).is_empty());
}

#[test]
fn parse_single_frame() {
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2); // 48kHz, 80 words = 160 bytes
    let pes = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data: frame_data.clone(),
        discontinuity: false,
    };
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data.len(), 160);
}

#[test]
fn parse_frame_spanning_two_pes() {
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2); // 160 bytes
    let mid = 80;

    // First PES: first half of frame
    let pes1 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data: frame_data[..mid].to_vec(),
        discontinuity: false,
    };
    let frames1 = parser.parse(&pes1);
    assert!(frames1.is_empty(), "partial frame should not emit");

    // Second PES: second half
    let pes2 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(93000),
        dts: None,
        data: frame_data[mid..].to_vec(),
        discontinuity: false,
    };
    let frames2 = parser.parse(&pes2);
    assert_eq!(frames2.len(), 1);
    assert_eq!(frames2[0].data.len(), 160);
}

#[test]
fn discontinuity_drops_truncated_partial() {
    // B1: a partial frame is buffered, then a discontinuity-marked PES brings a
    // fresh complete frame. The partial must be DROPPED, not spliced, or the
    // parser emits one corrupt frame from [stale partial | head of fresh].
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2); // 160 bytes, starts with 0x0B77

    // First PES: only the first half of a frame (no boundary marker).
    let pes1 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data: frame_data[..80].to_vec(),
        discontinuity: false,
    };
    assert!(
        parser.parse(&pes1).is_empty(),
        "partial frame should not emit"
    );

    // Concealed gap: a fresh whole frame, marked discontinuity.
    let fresh = make_ac3_frame(0, 2);
    let pes2 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(99000),
        dts: None,
        data: fresh.clone(),
        discontinuity: true,
    };
    let frames = parser.parse(&pes2);
    assert_eq!(frames.len(), 1, "exactly one clean frame across the gap");
    assert_eq!(
        frames[0].data, fresh,
        "emitted frame is the fresh post-gap frame, not a spliced partial"
    );
}

#[test]
fn empty_discontinuity_pes_still_drops_partial() {
    // Defensive ordering: the discontinuity clear runs BEFORE the empty-data
    // guard, so even an empty-payload discontinuity PES drops the stranded
    // partial instead of leaking the signal and splicing on the next PES.
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2); // 160 bytes

    // Partial first half buffered.
    let pes1 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data: frame_data[..80].to_vec(),
        discontinuity: false,
    };
    assert!(parser.parse(&pes1).is_empty());

    // Empty-payload discontinuity PES: must still clear the partial.
    let gap = PesPacket {
        source: None,
        pid: 0,
        pts: None,
        dts: None,
        data: vec![],
        discontinuity: true,
    };
    assert!(parser.parse(&gap).is_empty(), "empty PES emits nothing");

    // A fresh whole frame (no discontinuity now): if the partial had leaked,
    // this would splice into a frankenstein; instead it emits cleanly.
    let fresh = make_ac3_frame(0, 2);
    let pes2 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(99000),
        dts: None,
        data: fresh.clone(),
        discontinuity: false,
    };
    let frames = parser.parse(&pes2);
    assert_eq!(frames.len(), 1, "one clean frame, partial was dropped");
    assert_eq!(
        frames[0].data, fresh,
        "no splice — partial did not leak past the empty gap PES"
    );
}

#[test]
fn skip_garbage_before_sync() {
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2);
    let mut data = vec![0xDE, 0xAD, 0xBE, 0xEF]; // garbage
    data.extend_from_slice(&frame_data);
    let pes = PesPacket {
        source: None,
        pid: 0,
        pts: None,
        dts: None,
        data,
        discontinuity: false,
    };
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data.len(), 160);
}

#[test]
fn sync_word_split_across_pes_is_preserved() {
    // A frame whose 0x0B77 syncword straddles the PES boundary (0x0B at the
    // tail of PES 1, 0x77 at the head of PES 2) must still be emitted whole.
    // Previously the lone trailing 0x0B was dropped and the frame lost.
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2); // 160 bytes, starts with 0x0B 0x77

    // PES 1: a complete frame, then a single 0x0B (first half of next sync).
    let mut pes1_data = frame_data.clone();
    pes1_data.push(0x0B);
    let pes1 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data: pes1_data,
        discontinuity: false,
    };
    let frames1 = parser.parse(&pes1);
    assert_eq!(frames1.len(), 1, "first complete frame emitted");

    // PES 2: 0x77 (second half of sync) + rest of the second frame.
    let mut pes2_data = vec![0x77];
    pes2_data.extend_from_slice(&frame_data[2..]);
    let pes2 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(93000),
        dts: None,
        data: pes2_data,
        discontinuity: false,
    };
    let frames2 = parser.parse(&pes2);
    assert_eq!(frames2.len(), 1, "split-sync frame must be recovered");
    assert_eq!(frames2[0].data.len(), 160);
}

#[test]
fn buffer_stays_bounded_across_many_garbage_pes() {
    // The carry-over buffer must never grow without bound: carry-from-`pos`
    // drops pre-sync junk, and a never-completing frame is bounded by the
    // 8192-byte frame cap and the MAX_AC3_BUF resync guard.
    let mut parser = Ac3Parser::new();
    for i in 0..256 {
        // Vary the trailing byte so we also exercise the lone-0x0B retain.
        let mut data = vec![0x55u8; 8192];
        if i % 3 == 0 {
            *data.last_mut().unwrap() = 0x0B;
        }
        let pes = PesPacket {
            source: None,
            pid: 0,
            pts: None,
            dts: None,
            data,
            discontinuity: false,
        };
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
        assert!(
            parser.acc.len() <= MAX_AC3_BUF,
            "buffer grew to {} (cap {})",
            parser.acc.len(),
            MAX_AC3_BUF
        );
    }
    // After all that garbage the retained tail is at most a single partial
    // syncword byte — never an accumulation of whole PES packets.
    assert!(parser.acc.len() <= 1, "retained {} bytes", parser.acc.len());
}

#[test]
fn split_sync_below_cap_is_still_retained() {
    // The cap must not break the normal split-sync straddle: a short tail
    // ending in 0x0B (well under the cap) is retained so the next PES can
    // complete the syncword.
    let mut parser = Ac3Parser::new();
    let data = vec![0x00, 0x00, 0x0B];
    let pes = PesPacket {
        source: None,
        pid: 0,
        pts: None,
        dts: None,
        data,
        discontinuity: false,
    };
    assert!(parser.parse(&pes).is_empty());
    assert_eq!(
        parser.acc.as_slice(),
        vec![0x0B],
        "lone trailing 0x0B retained"
    );
}

#[test]
fn flush_emits_complete_buffered_frame_at_eos() {
    // A complete final frame sitting in the carry-over buffer with no
    // following PES must be drained by flush() at EOS — the bug was that
    // ac3 inherited the no-op default flush and dropped the last frame.
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2);
    parser.acc.seed(&frame_data.clone());
    parser.flush_pts_ns = pts_to_ns(99000);
    let f = parser.flush();
    assert_eq!(f.len(), 1, "complete buffered frame drained at EOS");
    assert_eq!(f[0].data.len(), 160);
    assert_eq!(f[0].pts_ns, pts_to_ns(99000), "flush uses carried PTS");
    assert!(f[0].duration_ns.is_some(), "flush sets duration");
    assert!(parser.acc.is_empty(), "buffer consumed by flush");
}

#[test]
fn flush_carries_running_pts_from_partial_tail() {
    // After a full frame emits in parse, the partial next frame held in the
    // buffer is timed at the running per-frame PTS; flush completing it must
    // use that, not the original PES base.
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2);
    let mut data = frame_data.clone();
    data.extend_from_slice(&frame_data[..40]); // partial frame 2 held
    let pes = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data,
        discontinuity: false,
    };
    let f = parser.parse(&pes);
    assert_eq!(f.len(), 1, "frame 1 emitted in parse");
    let dur = f[0].duration_ns.unwrap() as i64;
    // The held partial's flush PTS should be base + one frame duration.
    assert_eq!(parser.flush_pts_ns, pts_to_ns(90000) + dur);
}

#[test]
fn flush_drops_partial_tail() {
    // A partial frame (cannot be sized/completed) at EOS is dropped, not
    // emitted truncated.
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2);
    parser.acc.seed(&frame_data[..80]); // half a frame
    assert!(parser.flush().is_empty(), "partial tail dropped");
}

#[test]
fn per_frame_pts_increments_within_one_pes() {
    // Two AC-3 frames in a single PES must get distinct, increasing PTS —
    // one per frame, not the single PES timestamp on both.
    let mut parser = Ac3Parser::new();
    let frame_data = make_ac3_frame(0, 2); // 48kHz, 1536 samples
    let mut data = frame_data.clone();
    data.extend_from_slice(&frame_data);
    let pes = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data,
        discontinuity: false,
    };
    let f = parser.parse(&pes);
    assert_eq!(f.len(), 2);
    assert_eq!(f[0].pts_ns, pts_to_ns(90000), "frame 0 uses PES base PTS");
    // 1536 samples @ 48kHz = 32 ms = 32_000_000 ns.
    let expect = 1536u64 * 1_000_000_000 / 48_000;
    assert_eq!(f[0].duration_ns, Some(expect));
    assert_eq!(
        f[1].pts_ns - f[0].pts_ns,
        expect as i64,
        "frame 1 PTS advances by one frame duration, not equal to frame 0"
    );
}

#[test]
fn frame_duration_ac3_48khz() {
    // AC-3 @ 48kHz: 1536 / 48000 s = 32 ms.
    let frame = make_ac3_frame(0, 2);
    let bsid = get_bsid(&frame);
    assert!(bsid < 11, "test frame is legacy AC-3");
    assert_eq!(frame_duration_ns(&frame, bsid), 32_000_000);
}

#[test]
fn eac3_subheader_sized_frame_is_rejected() {
    // An E-AC-3 sync with frmsiz=0 sizes to a 2-byte "frame"; frmsiz=1 to
    // 4 bytes. Both are sub-header junk that must NOT be emitted as audio.
    // bsid must be >= 11 for the E-AC-3 sizing path. Byte 5 bits 7..3 = bsid.
    let mut parser = Ac3Parser::new();
    // Build an E-AC-3 sync: 0x0B 0x77, frmsiz=0 (bytes 2-3 low bits = 0),
    // bsid=16 (>=11) at byte 5. Pad to a few bytes so find_ac3_sync + sizing
    // run. eac3_frame_size = (0 + 1) * 2 = 2 < MIN_FRAME_BYTES.
    let mut data = vec![0x0B, 0x77, 0x00, 0x00, 0x00, 16 << 3, 0x00, 0x00];
    // Append a real AC-3 frame after the junk so we can confirm the parser
    // resyncs past the junk and still emits the valid frame.
    let good = make_ac3_frame(0, 2);
    data.extend_from_slice(&good);
    let pes = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data,
        discontinuity: false,
    };
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1, "only the real AC-3 frame is emitted");
    assert_eq!(frames[0].data.len(), 160);
}

#[test]
fn eac3_fscod2_reduced_rate_duration() {
    // E-AC-3 with fscod==3 (reduced rate) and fscod2==0 → 24 kHz, not 48; block
    // count is then fixed at 6 → 1536 samples. Byte 4: fscod(2)|fscod2(2)|...,
    // so fscod=3 (0b11), fscod2=0 (0b00) → byte4 = 0b1100_0000 = 0xC0.
    let data = [0x0B, 0x77, 0x00, 0x00, 0xC0, 16 << 3];
    let bsid = get_bsid(&data);
    assert!(bsid >= 11, "test frame is E-AC-3");
    // 1536 samples / 24000 Hz = 64 ms.
    assert_eq!(frame_duration_ns(&data, bsid), 64_000_000);
}

#[test]
fn ac3_frame_size_table() {
    // fscod=0 (48kHz), frmsizecod=0: 64 words = 128 bytes
    assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x00, 0x40]), 128);
    // fscod=0 (48kHz), frmsizecod=2: 80 words = 160 bytes
    assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x02, 0x40]), 160);
}

// --- ac3_frame_size: fscod-indexed table columns + reject paths ---

#[test]
fn ac3_frame_size_44100_uses_second_column() {
    // ATSC A/52 Table 5.18: fscod=1 (44.1 kHz), frmsizecod=0 → 69 words.
    // byte4 = fscod(2)<<6 | frmsizecod(6) = 0b01_000000 = 0x40.
    assert_eq!(
        ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x40, 0x00]),
        69 * 2,
        "44.1kHz column (index 1), 69 words = 138 bytes"
    );
}

#[test]
fn ac3_frame_size_32000_uses_third_column() {
    // A/52 Table 5.18: fscod=2 (32 kHz), frmsizecod=0 → 96 words.
    // byte4 = 0b10_000000 = 0x80.
    assert_eq!(
        ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x80, 0x00]),
        96 * 2,
        "32kHz column (index 2), 96 words = 192 bytes"
    );
}

#[test]
fn ac3_frame_size_reserved_fscod3_is_unmappable() {
    // fscod=3 is RESERVED in AC-3 (A/52 §5.4.1.3). The size function must
    // return 0 (unmappable), never index the table. byte4 = 0b11_000000.
    assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0xC0, 0x00]), 0);
}

#[test]
fn ac3_frame_size_frmsizecod_out_of_range_is_zero() {
    // frmsizecod has 38 valid entries (0..=37). 38..=63 are reserved.
    // frmsizecod=38 (0b100110) with fscod=0 → byte4 = 0x26. Must return 0.
    assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x26, 0x00]), 0);
    // The largest reserved code (63 = 0x3F) likewise.
    assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x3F, 0x00]), 0);
}

#[test]
fn ac3_frame_size_short_input_is_zero() {
    // Fewer than 5 bytes can't carry byte 4 → 0, no panic.
    assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0]), 0);
    assert_eq!(ac3_frame_size(&[]), 0);
}

#[test]
fn ac3_frame_size_max_frmsizecod_37() {
    // Last valid frmsizecod=37 (0b100101), fscod=0 → 1280 words = 2560 bytes.
    // byte4 = 0x25.
    assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x25, 0x00]), 1280 * 2);
}

// --- E-AC-3 frame sizing (frmsiz field bytes 2-3) ---

#[test]
fn eac3_frame_size_formula() {
    // E-AC-3 (A/52 Annex E): frmsiz = byte2[2:0]<<8 | byte3; frame bytes =
    // (frmsiz + 1) * 2. With byte2=0x07 (low 3 bits set) and byte3=0xFF,
    // frmsiz = 0x7FF = 2047 → (2048)*2 = 4096 bytes.
    assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0x07, 0xFF]), 4096);
    // frmsiz=2 → (3)*2 = 6 bytes (== MIN_FRAME_BYTES).
    assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0x00, 0x02]), 6);
}

#[test]
fn eac3_frame_size_short_input_zero() {
    // < 4 bytes can't carry the frmsiz field → 0, no panic.
    assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0x00]), 0);
}

#[test]
fn eac3_frame_size_masks_byte2_to_three_bits() {
    // Only the low 3 bits of byte 2 belong to frmsiz; the upper 5 bits
    // (strmtyp/substreamid) must be masked off. byte2=0xFF, byte3=0x00 →
    // frmsiz = (0xFF & 0x07)<<8 | 0 = 0x700 = 1792 → (1793)*2 = 3586.
    assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0xFF, 0x00]), (1792 + 1) * 2);
}

// --- get_bsid: byte 5 bits 7..3, the AC-3/E-AC-3 selector ---

#[test]
fn get_bsid_extracts_bits_7_3() {
    // bsid lives in byte 5 bits 7..3 (A/52 §5.3.2 BSI). 0b10101_000 = 0xA8 →
    // bsid = 0b10101 = 21.
    assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 0xA8]), 21);
    // Low 3 bits must be ignored: 0x0F (0b00001_111) → bsid = 1.
    assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 0x0F]), 1);
}

#[test]
fn get_bsid_short_input_zero() {
    assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0]), 0);
}

#[test]
fn bsid_11_is_first_eac3_value() {
    // The parser switches to E-AC-3 sizing at bsid >= 11. bsid=10 must use
    // AC-3 sizing, bsid=11 E-AC-3. byte5 = bsid<<3.
    assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 10 << 3]), 10);
    assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 11 << 3]), 11);
}

// --- frame_sample_rate / frame_duration: per-fscod and fscod2 ---

#[test]
fn ac3_duration_44100() {
    // Legacy AC-3 @ 44.1kHz: 1536 / 44100 s. fscod=1 → byte4 bits 7-6 = 01.
    // Build a real frame so the sizing path validates too.
    let frame = make_ac3_frame(1, 0); // fscod=1, frmsizecod=0
    let bsid = get_bsid(&frame);
    assert!(bsid < 11);
    // (1536 * 1e9 + 44100/2) / 44100, rounded to nearest.
    let expect = (1536u64 * 1_000_000_000 + 44_100 / 2) / 44_100;
    assert_eq!(frame_duration_ns(&frame, bsid), expect);
}

#[test]
fn ac3_duration_32000() {
    // 1536 / 32000 s = 48 ms exactly.
    let frame = make_ac3_frame(2, 0); // fscod=2 (32kHz)
    let bsid = get_bsid(&frame);
    assert_eq!(frame_duration_ns(&frame, bsid), 48_000_000);
}

#[test]
fn eac3_fscod2_22050_reduced_rate() {
    // E-AC-3 fscod==3, fscod2==1 → 22.05 kHz (EAC3_REDUCED_RATES[1]).
    // byte4 = fscod(11) | fscod2(01) << 4 = 0b1101_0000 = 0xD0. fscod==3
    // fixes numblks to 6 → 1536 samples.
    let data = [0x0B, 0x77, 0x00, 0x00, 0xD0, 16 << 3];
    let bsid = get_bsid(&data);
    assert!(bsid >= 11);
    let expect = (1536u64 * 1_000_000_000 + 22_050 / 2) / 22_050;
    assert_eq!(frame_duration_ns(&data, bsid), expect);
}

#[test]
fn eac3_fscod2_16000_reduced_rate() {
    // fscod==3, fscod2==2 → 16 kHz. byte4 = 0b1110_0000 = 0xE0.
    let data = [0x0B, 0x77, 0x00, 0x00, 0xE0, 16 << 3];
    let bsid = get_bsid(&data);
    let expect = 1536u64 * 1_000_000_000 / 16_000; // exact
    assert_eq!(frame_duration_ns(&data, bsid), expect);
}

#[test]
fn eac3_fscod2_reserved_index3_falls_back_48k() {
    // fscod==3, fscod2==3 is RESERVED; the code falls back to 48 kHz
    // (EAC3_REDUCED_RATES[3]). byte4 = 0b1111_0000 = 0xF0.
    let data = [0x0B, 0x77, 0x00, 0x00, 0xF0, 16 << 3];
    let bsid = get_bsid(&data);
    let expect = 1536u64 * 1_000_000_000 / 48_000; // 32ms
    assert_eq!(frame_duration_ns(&data, bsid), expect);
}

#[test]
fn ac3_fscod3_does_not_use_fscod2_path() {
    // For LEGACY AC-3 (bsid < 11) fscod==3 is reserved; frame_sample_rate
    // must NOT take the fscod2 branch (that is E-AC-3 only) and must index
    // SAMPLE_RATES[3] = 48000 fallback. Duration = 1536/48000 = 32ms.
    let data = [0x0B, 0x77, 0x00, 0x00, 0xC0, 8 << 3]; // bsid=8 (AC-3)
    let bsid = get_bsid(&data);
    assert!(bsid < 11);
    assert_eq!(frame_duration_ns(&data, bsid), 32_000_000);
}

#[test]
fn frame_sample_rate_short_input_defaults_48k() {
    // < 5 bytes → SAMPLE_RATES[0] = 48000 default (can't read fscod).
    let short = [0x0B, 0x77, 0x00, 0x00];
    let expect = 1536u64 * 1_000_000_000 / 48_000;
    assert_eq!(frame_duration_ns(&short, 8), expect);
}

// --- eac3_samples_per_frame: numblkscod table ---

#[test]
fn eac3_numblkscod_block_counts() {
    // A/52 Annex E numblkscod (byte4 bits 5-4 when fscod != 3):
    //   0→1 block, 1→2, 2→3, 3→6 blocks; each block = 256 samples.
    // fscod=0 keeps the fscod2 path off. byte4 = numblkscod << 4.
    let mk = |numblkscod: u8| [0x0B, 0x77, 0x00, 0x00, numblkscod << 4, 0x00];
    assert_eq!(
        eac3_samples_per_frame(&mk(0)),
        256,
        "numblkscod 0 → 1 block"
    );
    assert_eq!(
        eac3_samples_per_frame(&mk(1)),
        512,
        "numblkscod 1 → 2 blocks"
    );
    assert_eq!(
        eac3_samples_per_frame(&mk(2)),
        768,
        "numblkscod 2 → 3 blocks"
    );
    assert_eq!(
        eac3_samples_per_frame(&mk(3)),
        1536,
        "numblkscod 3 → 6 blocks"
    );
}

#[test]
fn eac3_samples_fscod3_fixed_at_six_blocks() {
    // When fscod==3 (reduced rate), numblks is fixed at 6 regardless of the
    // numblkscod bits. byte4 = 0b11_xx_0000; set the numblkscod bits to 0
    // (would otherwise be 1 block) to prove the fscod==3 override wins.
    let data = [0x0B, 0x77, 0x00, 0x00, 0xC0, 0x00];
    assert_eq!(eac3_samples_per_frame(&data), 6 * 256);
}

#[test]
fn eac3_samples_short_input_defaults_1536() {
    // < 5 bytes → AC3_SAMPLES_PER_FRAME (1536) fallback.
    assert_eq!(eac3_samples_per_frame(&[0x0B, 0x77, 0x00, 0x00]), 1536);
}

// --- frame acceptance / rejection at the size boundaries ---

#[test]
fn eac3_frame_at_min_frame_bytes_passes_sizing_then_crc_gate() {
    // Smallest frame SIZING accepts is MIN_FRAME_BYTES = 6 (frmsiz=2). An
    // all-zero 6-byte frame passes sizing (proven COUNTED as a drop, not
    // size-skipped) but fails CRC; the following real AC-3 frame is emitted.
    let mut parser = Ac3Parser::new();
    // 0x0B 0x77 | byte2=0 byte3=2 (frmsiz=2 → 6 bytes) | byte4=0 | byte5 bsid
    let mut data = vec![0x0B, 0x77, 0x00, 0x02, 0x00, 16 << 3];
    data.truncate(6);
    data.extend_from_slice(&make_ac3_frame(0, 2));
    let f = parser.parse(&make_eac3_pes(data));
    assert_eq!(f.len(), 1, "6-byte frame dropped (CRC), real AC-3 emitted");
    assert_eq!(f[0].data.len(), 160, "the surviving frame is the real AC-3");
    assert_eq!(
        parser.dropped_frames(),
        1,
        "the 6-byte frame reached the gate"
    );
}

#[test]
fn eac3_max_frmsiz_frame_within_window_accepted() {
    // E-AC-3 frmsiz is an 11-bit field, so its max value 0x7FF = 2047 →
    // (2048)*2 = 4096 bytes, inside the MIN_FRAME_BYTES..=8192 accept window
    // and, with a valid CRC, must be emitted.
    let mut parser = Ac3Parser::new();
    let mut frame = vec![0u8; 4096];
    frame[0] = 0x0B;
    frame[1] = 0x77;
    frame[2] = 0x07; // frmsiz high
    frame[3] = 0xFF; // frmsiz low → 0x7FF = 2047 → 4096 bytes
    frame[5] = 16 << 3; // bsid 16 (E-AC-3)
    finalize_ac3_crc(&mut frame); // pass the decodability gate
    // Trailing E-AC-3 AU is HELD at end of call (a dependent substream may
    // follow), so it's closed by flush() at EOS, not in-call.
    let mut f = parser.parse(&make_eac3_pes(frame));
    f.extend(parser.flush());
    assert_eq!(f.len(), 1, "4096-byte E-AC-3 frame within window accepted");
    assert_eq!(f[0].data.len(), 4096);
}

#[test]
fn undersized_sync_skips_two_bytes_and_resyncs() {
    // A sync whose decoded size is below MIN_FRAME_BYTES (here an E-AC-3
    // frmsiz=0 → 2-byte "frame") is rejected by skipping exactly 2 bytes
    // past the sync, then resyncing to the next real frame.
    let mut parser = Ac3Parser::new();
    let mut data = vec![0x0B, 0x77, 0x00, 0x00, 0x00, 16 << 3];
    data.extend_from_slice(&make_ac3_frame(0, 2)); // real frame follows
    let f = parser.parse(&make_eac3_pes(data));
    assert_eq!(f.len(), 1, "junk sync skipped, real frame found");
    assert_eq!(f[0].data.len(), 160);
}

// --- find_ac3_sync ---

#[test]
fn find_ac3_sync_locates_0b77() {
    assert_eq!(find_ac3_sync(&[0xFF, 0x0B, 0x77, 0x00]), Some(1));
    assert_eq!(find_ac3_sync(&[0x0B, 0x77]), Some(0));
}

#[test]
fn find_ac3_sync_lone_0b_at_end_not_matched() {
    // A trailing lone 0x0B (no following 0x77) is not a complete syncword.
    // saturating_sub(1) prevents an out-of-bounds read of data[i+1].
    assert_eq!(find_ac3_sync(&[0xFF, 0xFF, 0x0B]), None);
    assert_eq!(find_ac3_sync(&[0x0B]), None);
    assert_eq!(find_ac3_sync(&[]), None);
}

#[test]
fn find_ac3_sync_0b_without_77_no_false_positive() {
    // 0x0B followed by something other than 0x77 is not a sync.
    assert_eq!(find_ac3_sync(&[0x0B, 0x76, 0x0B, 0x78]), None);
}

// --- flush rejects an oversized declared frame ---

#[test]
fn flush_rejects_frame_extending_past_buffer() {
    // A buffered sync whose decoded frame size exceeds the buffered bytes must
    // be dropped by flush (never emit fewer bytes than declared). Build a real
    // 160-byte AC-3 frame but only buffer 100 bytes.
    let mut parser = Ac3Parser::new();
    let frame = make_ac3_frame(0, 2); // sizes to 160
    parser.acc.seed(&frame[..100]);
    assert!(
        parser.flush().is_empty(),
        "incomplete frame must not be emitted truncated at flush"
    );
}

#[test]
fn flush_with_no_sync_is_empty() {
    // flush on a buffer with no syncword yields nothing and clears.
    let mut parser = Ac3Parser::new();
    parser.acc.seed(&[0xAA, 0xBB, 0xCC]);
    assert!(parser.flush().is_empty());
}

// --- acmod_channels: channel count from the AC-3 BSI bitstream ---

// Builds a minimal AC-3 BSI header (8 bytes) with a given acmod + lfeon,
// writing byte6/7 bits MSB-first in the order acmod_channels reads them.
fn make_bsi(acmod: u8, lfeon: bool) -> Vec<u8> {
    // Collect the bit sequence after byte 6 bit 7: acmod(3), [cmixlev(2)],
    // [surmixlev(2)], [dsurmod(2)], lfeon(1). Mix-level/dsurmod bits are
    // arbitrary (0 here) — only their PRESENCE shifts lfeon's position.
    let mut bits: Vec<u8> = Vec::new();
    for i in (0..3).rev() {
        bits.push((acmod >> i) & 1);
    }
    if (acmod & 0x1) != 0 && acmod != 0x1 {
        bits.push(0);
        bits.push(0); // cmixlev
    }
    if (acmod & 0x4) != 0 {
        bits.push(0);
        bits.push(0); // surmixlev
    }
    if acmod == 0x2 {
        bits.push(0);
        bits.push(0); // dsurmod
    }
    bits.push(lfeon as u8); // lfeon
    // Pack bits MSB-first starting at byte 6.
    let mut frame = vec![0u8; 8];
    frame[0] = 0x0B;
    frame[1] = 0x77;
    frame[5] = 8 << 3; // bsid = 8 (legacy AC-3), bsmod = 0
    for (idx, &b) in bits.iter().enumerate() {
        let bitpos = 6 * 8 + idx;
        if b != 0 {
            frame[bitpos / 8] |= 1 << (7 - (bitpos % 8));
        }
    }
    frame
}

#[test]
fn acmod_channels_stereo_2_0_no_lfe() {
    // acmod=2 (2/0 L,R), no LFE → 2 channels; verifies the channel count comes
    // from the AC-3 bitstream's acmod, independent of any IFO claim (a disc
    // IFO/substream mismatch is a separate SELECTION bug, tracked for rc.5.2).
    assert_eq!(acmod_channels(&make_bsi(2, false)), Some(2));
}

#[test]
fn acmod_channels_5_1() {
    // acmod=7 (3/2 L,C,R,SL,SR) + LFE → 6 channels (5.1).
    assert_eq!(acmod_channels(&make_bsi(7, true)), Some(6));
    // 3/2 without LFE → 5 channels.
    assert_eq!(acmod_channels(&make_bsi(7, false)), Some(5));
}

#[test]
fn acmod_channels_mono_and_dual_mono() {
    // acmod=1 (1/0 centre/mono) → 1; with LFE → 2.
    assert_eq!(acmod_channels(&make_bsi(1, false)), Some(1));
    assert_eq!(acmod_channels(&make_bsi(1, true)), Some(2));
    // acmod=0 (1+1 dual mono) → 2 base channels.
    assert_eq!(acmod_channels(&make_bsi(0, false)), Some(2));
}

#[test]
fn acmod_channels_3_0_and_2_1() {
    // Per A/52 Table 5.8: acmod 4 = 2/1, 5 = 3/1, 6 = 2/2.
    // acmod=4 (2/1 L,R,S) → 3 (surmixlev present, no centre → no cmixlev).
    assert_eq!(acmod_channels(&make_bsi(4, false)), Some(3));
    // acmod=5 (3/1 L,C,R,S) → 4 (centre → cmixlev present, surround →
    // surmixlev present). This is the regression case: index 5 was wrongly
    // 3 in ACMOD_CHANNELS, undercounting a 3/1 stream by one channel.
    assert_eq!(acmod_channels(&make_bsi(5, false)), Some(4));
    // acmod=5 (3/1) + LFE → 5; lfeon position shifts after both cmixlev
    // (centre) and surmixlev (surround) 2-bit fields.
    assert_eq!(acmod_channels(&make_bsi(5, true)), Some(5));
    // acmod=6 (2/2 L,R,SL,SR) → 4 (surmixlev present, no centre); +LFE → 5.
    assert_eq!(acmod_channels(&make_bsi(6, false)), Some(4));
    assert_eq!(acmod_channels(&make_bsi(6, true)), Some(5));
}

#[test]
fn acmod_channels_every_mode_with_and_without_lfe() {
    // A/52 Table 5.8 base counts, spelled independently of ACMOD_CHANNELS. lfeon sits
    // after cmixlev/surmixlev/dsurmod, so a misplaced cursor misreads it.
    let base = [2u8, 1, 2, 3, 3, 4, 4, 5];
    for (acmod, &n) in base.iter().enumerate() {
        let acmod = acmod as u8;
        assert_eq!(
            acmod_channels(&make_bsi(acmod, false)),
            Some(n),
            "acmod {acmod}"
        );
        assert_eq!(
            acmod_channels(&make_bsi(acmod, true)),
            Some(n + 1),
            "acmod {acmod} + LFE"
        );
    }
}

#[test]
fn acmod_channels_short_frame_is_none() {
    // Fewer than 8 bytes cannot carry the BSI bits → None (caller falls
    // back to the IFO-claimed channel count).
    assert_eq!(acmod_channels(&[0x0B, 0x77, 0, 0, 0, 8 << 3]), None);
    assert_eq!(acmod_channels(&[]), None);
}

#[test]
fn acmod_channels_eac3_is_none() {
    // E-AC-3 (bsid >= 11) uses a different BSI layout; acmod_channels
    // declines so the caller keeps the passed count.
    let mut data = make_bsi(2, false);
    data[5] = 16 << 3; // bsid = 16 (E-AC-3)
    assert_eq!(acmod_channels(&data), None);
}

#[test]
fn acmod_channels_parses_real_built_frame() {
    // A frame built by make_ac3_frame (fscod/frmsizecod set, acmod bits 0)
    // decodes acmod=0 → 2 channels (dual mono), confirming the cursor lands
    // on the right bytes for a fully-formed frame, not just a stub header.
    let frame = make_ac3_frame(0, 2);
    // make_ac3_frame leaves byte 6 = 0 → acmod=0, lfeon=0 → 2 channels.
    assert_eq!(acmod_channels(&frame), Some(2));
}

// --- decodability (CRC) gate: keep clean frames, drop corrupt ones ---

/// A structurally-valid AC-3 frame with one payload byte corrupted so its
/// native CRC fails (header/size intact, so the framer delimits it normally).
fn make_corrupt_ac3_frame(fscod: u8, frmsizecod: u8) -> Vec<u8> {
    let mut f = make_ac3_frame(fscod, frmsizecod);
    f[20] ^= 0xFF; // flip a payload byte → CRC no longer zero
    assert!(!frame_crc_ok(&f), "corruption must break the CRC");
    f
}

#[test]
fn crc16_residue_zero_after_finalize_nonzero_after_corruption() {
    // The CRC-16/ANSI residue property the gate relies on: a finalized frame
    // has residue 0 over [2..]; flipping any covered byte makes it nonzero.
    let good = make_ac3_frame(0, 2);
    assert!(frame_crc_ok(&good));
    let bad = make_corrupt_ac3_frame(0, 2);
    assert!(!frame_crc_ok(&bad));
}

#[test]
fn crc_fail_frame_is_dropped_survivors_kept() {
    // good / corrupt / good in one PES: the corrupt middle frame is dropped
    // (CRC), the two clean frames are emitted, and the drop is counted.
    let mut parser = Ac3Parser::new();
    let mut data = make_ac3_frame(0, 2);
    data.extend_from_slice(&make_corrupt_ac3_frame(0, 2));
    data.extend_from_slice(&make_ac3_frame(0, 2));
    let f = parser.parse(&make_eac3_pes(data));
    // Only two of three survive; flush has nothing (all closed in-call).
    assert_eq!(f.len(), 2, "corrupt frame dropped, two clean survive");
    assert_eq!(parser.dropped_frames(), 1);
    assert_eq!(
        parser.dropped_duration_ns(),
        32_000_000,
        "one 32ms frame of silence"
    );
}

#[test]
fn crc_drop_preserves_pts_sync_no_shift() {
    // THE INVARIANT: dropping a corrupt frame must not shift the audio after
    // it. good/corrupt/good in one PES — the trailing clean frame keeps the
    // EXACT PTS it would have had with no drop: a silence gap, not a shift.
    let mut parser = Ac3Parser::new();
    let mut data = make_ac3_frame(0, 2); // f0
    data.extend_from_slice(&make_corrupt_ac3_frame(0, 2)); // dropped
    data.extend_from_slice(&make_ac3_frame(0, 2)); // f2
    let f = parser.parse(&make_eac3_pes(data));
    assert_eq!(f.len(), 2);
    let base = pts_to_ns(90000);
    let frame_dur = 32_000_000i64; // 1536 @ 48k
    assert_eq!(f[0].pts_ns, base, "f0 at PES base");
    assert_eq!(
        f[1].pts_ns,
        base + 2 * frame_dur,
        "surviving frame keeps its true timeline (base + 2 frames) — gap, not shift"
    );
}

#[test]
fn bsid_over_16_is_dropped() {
    // bsid > 16 is out of range (ETSI TS 102 366 defines no bsid above 16).
    // A frame with bsid = 17 that still sizes must be dropped, not emitted.
    let mut frame = vec![0u8; 128];
    frame[0] = 0x0B;
    frame[1] = 0x77;
    frame[3] = 63; // frmsiz = 63 → (63+1)*2 = 128 bytes (E-AC-3 sizing)
    frame[5] = 17 << 3; // bsid = 17 (> 16)
    assert_eq!(get_bsid(&frame), 17);
    let tally = super::super::dropgate::DropTally::new("ac3");
    assert_eq!(ac3_drop_reason(&tally, &frame, 17), Some("bsid"));
}

#[test]
fn clean_stream_drops_nothing() {
    // A stream of valid frames passes untouched — zero false positives.
    let mut parser = Ac3Parser::new();
    let mut data = Vec::new();
    for _ in 0..5 {
        data.extend_from_slice(&make_ac3_frame(0, 2));
    }
    let mut f = parser.parse(&make_eac3_pes(data));
    f.extend(parser.flush());
    assert_eq!(f.len(), 5);
    assert_eq!(parser.dropped_frames(), 0);
}

// --- E-AC-3 substream grouping: one access unit per independent substream ---

/// Builds a synthetic E-AC-3 syncframe of exactly `size` bytes with the given
/// strmtyp/substreamid; fscod/numblkscod/acmod fixed at 48kHz/6 blocks/5.1, bsid 16, CRC
/// finalized.
fn make_eac3_frame(strmtyp: u8, substreamid: u8, size: usize) -> Vec<u8> {
    assert!(size >= MIN_FRAME_BYTES && size.is_multiple_of(2));
    let frmsiz = size / 2 - 1;
    let mut f = vec![0u8; size];
    f[0] = 0x0B;
    f[1] = 0x77;
    f[2] = (strmtyp << 6) | (substreamid << 3) | ((frmsiz >> 8) as u8 & 0x07);
    f[3] = (frmsiz & 0xFF) as u8;
    f[4] = 0x3F;
    f[5] = 16 << 3;
    finalize_ac3_crc(&mut f);
    f
}

#[test]
fn eac3_independent_plus_dependent_is_one_access_unit() {
    // THE FIX: per ETSI TS 102 366 Annex E an AU is the independent substream
    // plus every dependent substream that follows it — both must emerge as ONE
    // frame with the independent PTS and ONE duration (not double it).
    let mut parser = Ac3Parser::new();
    let indep = make_eac3_frame(0, 0, 160);
    let dep = make_eac3_frame(1, 0, 96);
    let indep2 = make_eac3_frame(0, 0, 160);
    let mut data = indep.clone();
    data.extend_from_slice(&dep);
    data.extend_from_slice(&indep2);

    // The next independent substream closes the first access unit in-call;
    // the trailing one is held for a possible dependent in the next PES and
    // closed by flush().
    let f = parser.parse(&make_eac3_pes(data));
    assert_eq!(f.len(), 1, "independent + dependent = exactly one AU");
    let mut expect = indep.clone();
    expect.extend_from_slice(&dep);
    assert_eq!(f[0].data, expect, "AU carries both substreams, in order");
    assert_eq!(
        f[0].pts_ns,
        pts_to_ns(90000),
        "AU is stamped with the INDEPENDENT substream's PTS"
    );
    assert_eq!(
        f[0].duration_ns,
        Some(32_000_000),
        "dependent substream adds no duration (same 32 ms time period)"
    );

    let f2 = parser.flush();
    assert_eq!(f2.len(), 1, "held trailing AU drained at EOS");
    assert_eq!(
        f2[0].pts_ns,
        pts_to_ns(90000) + 32_000_000,
        "next AU advances by ONE frame duration, not two"
    );
    assert_eq!(parser.dropped_frames(), 0, "nothing dropped");
}

#[test]
fn eac3_grouped_timeline_is_not_doubled() {
    // Two complete access units (independent + dependent each) in one PES, plus
    // a third independent substream closing the second. PTS cadence must be
    // one frame duration per AU — else the 2x-runtime / A-V-drift symptom.
    let mut parser = Ac3Parser::new();
    let mut data = Vec::new();
    for _ in 0..2 {
        data.extend_from_slice(&make_eac3_frame(0, 0, 160));
        data.extend_from_slice(&make_eac3_frame(1, 0, 96));
    }
    data.extend_from_slice(&make_eac3_frame(0, 0, 160));
    let mut f = parser.parse(&make_eac3_pes(data));
    f.extend(parser.flush());
    assert_eq!(f.len(), 3, "3 independent substreams → 3 access units");
    let base = pts_to_ns(90000);
    assert_eq!(f[0].pts_ns, base);
    assert_eq!(f[1].pts_ns, base + 32_000_000);
    assert_eq!(f[2].pts_ns, base + 64_000_000);
    assert_eq!(f[0].data.len(), 160 + 96, "AU = independent + dependent");
    assert_eq!(f[1].data.len(), 160 + 96);
}

#[test]
fn plain_ac3_frames_are_not_grouped_or_delayed() {
    // NO REGRESSION for legacy AC-3 (bsid < 11): it has no substream structure
    // (byte 2 is crc1, not strmtyp), so every syncframe is a complete AU,
    // emitted in the SAME call — never merged, never held for a dependent.
    let mut parser = Ac3Parser::new();
    let mut data = Vec::new();
    for _ in 0..3 {
        data.extend_from_slice(&make_ac3_frame(0, 2)); // 160 bytes, bsid=8
    }
    let f = parser.parse(&make_eac3_pes(data));
    assert_eq!(f.len(), 3, "three AC-3 frames, three access units, in-call");
    for (i, fr) in f.iter().enumerate() {
        assert_eq!(fr.data.len(), 160, "frame {i} not merged with a neighbour");
        assert_eq!(fr.pts_ns, pts_to_ns(90000) + i as i64 * 32_000_000);
        assert_eq!(fr.duration_ns, Some(32_000_000));
    }
    assert!(parser.acc.is_empty(), "nothing held back for plain AC-3");
    assert!(parser.flush().is_empty(), "flush has nothing left to drain");
}

#[test]
fn eac3_access_unit_split_across_pes_is_grouped() {
    // The AU boundary is only known at the NEXT independent substream, so a
    // trailing one is held across the PES boundary: the dependent half in the
    // next PES still joins it, keeping the FIRST PES's PTS.
    let mut parser = Ac3Parser::new();
    let indep = make_eac3_frame(0, 0, 160);
    let dep = make_eac3_frame(1, 0, 96);
    let indep2 = make_eac3_frame(0, 0, 160);

    let mut d1 = indep.clone();
    d1.extend_from_slice(&dep[..40]); // dependent substream split mid-frame
    let pes1 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data: d1,
        discontinuity: false,
    };
    assert!(
        parser.parse(&pes1).is_empty(),
        "AU held: its dependent substream may continue in the next PES"
    );

    let mut d2 = dep[40..].to_vec();
    d2.extend_from_slice(&indep2);
    let pes2 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(92880), // base + 32 ms in 90 kHz ticks
        dts: None,
        data: d2,
        discontinuity: false,
    };
    let f = parser.parse(&pes2);
    assert_eq!(f.len(), 1, "the straddling AU emerges whole, exactly once");
    let mut expect = indep.clone();
    expect.extend_from_slice(&dep);
    assert_eq!(f[0].data, expect, "independent + dependent, contiguous");
    assert_eq!(
        f[0].pts_ns,
        pts_to_ns(90000),
        "held AU keeps its own (first PES) PTS, not the second PES's"
    );
}

#[test]
fn eac3_dependent_syncword_split_across_pes_still_groups() {
    // The straddling-syncword path must survive grouping: the dependent
    // substream's 0x0B77 is split (0x0B ends PES 1, 0x77 starts PES 2).
    let mut parser = Ac3Parser::new();
    let indep = make_eac3_frame(0, 0, 160);
    let dep = make_eac3_frame(1, 0, 96);

    let mut d1 = indep.clone();
    d1.push(0x0B);
    let pes1 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data: d1,
        discontinuity: false,
    };
    assert!(
        parser.parse(&pes1).is_empty(),
        "AU held, lone 0x0B retained"
    );

    let mut d2 = vec![0x77];
    d2.extend_from_slice(&dep[2..]);
    let pes2 = PesPacket {
        source: None,
        pid: 0,
        pts: Some(92880),
        dts: None,
        data: d2,
        discontinuity: false,
    };
    assert!(
        parser.parse(&pes2).is_empty(),
        "still held: no next independent substream yet"
    );
    let f = parser.flush();
    assert_eq!(f.len(), 1, "one grouped AU at EOS");
    let mut expect = indep.clone();
    expect.extend_from_slice(&dep);
    assert_eq!(
        f[0].data, expect,
        "split-sync dependent substream recovered"
    );
    assert_eq!(f[0].pts_ns, pts_to_ns(90000));
}

#[test]
fn orphan_dependent_substream_is_skipped() {
    // A dependent substream with no independent parent (stream joined mid-AU)
    // cannot be decoded on its own: skip it instead of shipping it as an
    // access unit, and do not let it consume any of the timeline.
    let mut parser = Ac3Parser::new();
    let mut data = make_eac3_frame(1, 0, 96); // dependent first — no parent
    data.extend_from_slice(&make_ac3_frame(0, 2)); // legacy AC-3 follows
    let f = parser.parse(&make_eac3_pes(data));
    assert_eq!(f.len(), 1, "only the parentable frame is emitted");
    assert_eq!(f[0].data.len(), 160, "the AC-3 frame, not the orphan");
    assert_eq!(
        f[0].pts_ns,
        pts_to_ns(90000),
        "orphan consumed no time — following audio keeps its true PTS"
    );
}

#[test]
fn eac3_additional_independent_substream_stays_in_the_frame_set() {
    // THE FIX: a frame set is substream 0 plus dependents, then OPTIONAL
    // additional independent substreams 1..7 — ALL the SAME time period.
    // Keying the AU boundary on `strmtyp` alone doubled the PTS advance.
    let mut parser = Ac3Parser::new();
    let ind0 = make_eac3_frame(0, 0, 160); // main programme
    let dep0 = make_eac3_frame(1, 0, 96); // its dependent (7.1 extension)
    let ind1 = make_eac3_frame(0, 1, 128); // associated service
    let dep1 = make_eac3_frame(1, 1, 96); // its dependent
    let mut set = ind0.clone();
    set.extend_from_slice(&dep0);
    set.extend_from_slice(&ind1);
    set.extend_from_slice(&dep1);

    // Three consecutive frame sets: the third closes the second, and the
    // third itself is held for a possible continuation and drained by flush().
    let mut data = Vec::new();
    for _ in 0..3 {
        data.extend_from_slice(&set);
    }
    let mut f = parser.parse(&make_eac3_pes(data));
    f.extend(parser.flush());

    assert_eq!(
        f.len(),
        3,
        "one access unit per frame set — NOT one per independent substream"
    );
    let base = pts_to_ns(90000);
    for (i, fr) in f.iter().enumerate() {
        assert_eq!(
            fr.data, set,
            "frame set {i} emerges whole, all four substreams in bitstream order"
        );
        assert_eq!(
            fr.pts_ns,
            base + i as i64 * 32_000_000,
            "frame set {i} advances by ONE 32 ms period, not two"
        );
        assert_eq!(
            fr.duration_ns,
            Some(32_000_000),
            "the additional independent substream adds no duration"
        );
    }
    assert_eq!(parser.dropped_frames(), 0, "nothing dropped");
}

#[test]
fn eac3_stream_joined_mid_frame_set_resyncs_at_substreamid_0() {
    // A stream whose first syncframe is ADDITIONAL substream (substreamid 3)
    // joined a set whose mandatory substreamid-0 was never seen. It's skipped
    // (carries the set's time period, not its own); resyncs at substreamid-0.
    let mut parser = Ac3Parser::new();
    let orphan = make_eac3_frame(0, 3, 128);
    let orphan_dep = make_eac3_frame(1, 3, 96);
    let ind0 = make_eac3_frame(0, 0, 160);
    let mut data = orphan;
    data.extend_from_slice(&orphan_dep);
    data.extend_from_slice(&ind0);
    let mut f = parser.parse(&make_eac3_pes(data));
    f.extend(parser.flush());
    assert_eq!(f.len(), 1, "only the frame set that has substreamid 0");
    assert_eq!(f[0].data, ind0, "the orphan substreams are not shipped");
    assert_eq!(
        f[0].pts_ns,
        pts_to_ns(90000),
        "orphans consumed no time — the resynced audio keeps its true PTS"
    );
}

#[test]
fn eac3_strmtyp2_starts_a_new_access_unit() {
    // strmtyp 2 is an INDEPENDENT substream (Annex E), and strmtyp 3 is
    // reserved — neither may be folded into the preceding access unit.
    let mut parser = Ac3Parser::new();
    let mut data = make_eac3_frame(0, 0, 160);
    data.extend_from_slice(&make_eac3_frame(2, 0, 96));
    data.extend_from_slice(&make_eac3_frame(3, 0, 96));
    let mut f = parser.parse(&make_eac3_pes(data));
    f.extend(parser.flush());
    assert_eq!(f.len(), 3, "strmtyp 0 / 2 / 3 = three access units");
    assert_eq!(f[0].data.len(), 160);
    assert_eq!(f[1].data.len(), 96);
    assert_eq!(f[2].data.len(), 96);
}

#[test]
fn ac3_core_plus_eac3_dependent_is_one_access_unit() {
    // The backwards-compatible Dolby Digital Plus arrangement: a legacy AC-3
    // core syncframe followed by an E-AC-3 dependent substream. The dependent
    // substream must attach to the AC-3 core, not become its own access unit.
    let mut parser = Ac3Parser::new();
    let core = make_ac3_frame(0, 2); // bsid = 8, 160 bytes
    let dep = make_eac3_frame(1, 0, 96);
    let mut data = core.clone();
    data.extend_from_slice(&dep);
    data.extend_from_slice(&make_ac3_frame(0, 2)); // next core closes the AU
    let f = parser.parse(&make_eac3_pes(data));
    assert_eq!(f.len(), 1, "core + dependent = one AU (second core held)");
    let mut expect = core.clone();
    expect.extend_from_slice(&dep);
    assert_eq!(f[0].data, expect);
    assert_eq!(f[0].pts_ns, pts_to_ns(90000), "core's PTS");
    assert_eq!(f[0].duration_ns, Some(32_000_000), "one frame duration");
}

#[test]
fn corrupt_dependent_substream_drops_the_whole_access_unit() {
    // A dependent substream failing its native CRC poisons the whole AU:
    // emitting the independent half alone ships a pair no decoder can
    // reassemble. The drop still accounts for one duration (gap, not shift).
    let mut parser = Ac3Parser::new();
    let mut dep = make_eac3_frame(1, 0, 96);
    dep[20] ^= 0xFF; // break the dependent substream's CRC
    assert!(!frame_crc_ok(&dep));
    let mut data = make_eac3_frame(0, 0, 160);
    data.extend_from_slice(&dep);
    data.extend_from_slice(&make_eac3_frame(0, 0, 160));
    let mut f = parser.parse(&make_eac3_pes(data));
    f.extend(parser.flush());
    assert_eq!(f.len(), 1, "poisoned AU dropped, the clean one survives");
    assert_eq!(parser.dropped_frames(), 1);
    assert_eq!(parser.dropped_duration_ns(), 32_000_000, "one 32 ms gap");
    assert_eq!(
        f[0].pts_ns,
        pts_to_ns(90000) + 32_000_000,
        "survivor keeps its true timeline"
    );
}

// A 256-byte E-AC-3 syncframe with a valid CRC; strmtyp/substreamid go
// into byte 2, which substream_role reads: (0,0) opens, (1,0) extends.
fn eac3_substream_frame(strmtyp: u8, substreamid: u8) -> Vec<u8> {
    const SIZE: usize = 256;
    let frmsiz = SIZE / 2 - 1; // (frmsiz + 1) * 2 == SIZE
    let mut f = vec![0u8; SIZE];
    f[0] = 0x0B;
    f[1] = 0x77;
    f[2] = (strmtyp << 6) | (substreamid << 3) | ((frmsiz >> 8) as u8 & 0x07);
    f[3] = (frmsiz & 0xFF) as u8;
    f[5] = 16 << 3; // bsid 16 → E-AC-3
    finalize_ac3_crc(&mut f);
    f
}

// A held access unit's carry-over bytes must not be re-scanned/re-CRCed from byte 0 on
// every packet (quadratic work).
#[test]
fn a_held_access_unit_is_not_rescanned_from_its_first_frame_every_packet() {
    const DEPENDENTS: usize = 200;

    let mut parser = Ac3Parser::new();
    // Opens the access unit.
    let emitted = parser.parse(&make_eac3_pes(eac3_substream_frame(0, 0)));
    assert!(
        emitted.is_empty(),
        "the access unit is held open, not emitted"
    );
    for _ in 0..DEPENDENTS {
        let f = parser.parse(&make_eac3_pes(eac3_substream_frame(1, 0)));
        assert!(f.is_empty(), "a dependent substream extends the open unit");
    }

    let fed = (DEPENDENTS + 1) as u64;
    assert!(
        parser.frames_scanned <= 2 * fed,
        "the scanner examined {} syncframes for {fed} fed — a held access \
             unit must be resumed, not re-derived",
        parser.frames_scanned
    );

    // ...and the resume must not have cost correctness: the whole frame
    // set is still one access unit, emitted intact at EOS.
    let out = parser.flush();
    assert_eq!(out.len(), 1, "the frame set is a single access unit");
    assert_eq!(
        out[0].data.len(),
        256 * (DEPENDENTS + 1),
        "every substream of the frame set belongs to it"
    );
}

// A concealed gap must drop the HELD access unit, not just the byte buffer: its offsets
// describe the pre-gap bytes and must not be applied to the unrelated post-gap ones.
#[test]
fn a_discontinuity_drops_the_held_access_unit_with_its_bytes() {
    let mut parser = Ac3Parser::new();

    // Open an access unit and extend it, so a HeldAu exists describing
    // offsets into a large buffer.
    assert!(
        parser
            .parse(&make_eac3_pes(eac3_substream_frame(0, 0)))
            .is_empty(),
        "the access unit is held open, not emitted"
    );
    for _ in 0..8 {
        assert!(
            parser
                .parse(&make_eac3_pes(eac3_substream_frame(1, 0)))
                .is_empty(),
            "a dependent substream extends the open unit"
        );
    }

    // The gap. Its post-gap payload is deliberately far SHORTER than the
    // held unit's bytes, so a stale HeldAu indexes past its end.
    let mut gap = make_eac3_pes(eac3_substream_frame(0, 0));
    gap.discontinuity = true;
    let _ = parser.parse(&gap);

    // Whatever comes out, nothing may carry pre-gap bytes: the truncated
    // unit was dropped, so the only access unit that can be emitted is the
    // one opened after the gap.
    let out = parser.flush();
    let total: usize = out.iter().map(|f| f.data.len()).sum();
    assert!(
        total <= 256,
        "a post-gap access unit must not be spliced onto the 9 frames held \
             before the gap; got {total} bytes across {} frame(s)",
        out.len()
    );
}

// An access unit that keeps gaining substreams past MAX_AC3_BUF is dropped WITH its held
// state, so the next PES starts clean instead of resuming a stale HeldAu.
#[test]
fn an_oversized_held_access_unit_is_dropped_with_its_held_state() {
    const PER_PES: usize = 128;
    let mut data = eac3_substream_frame(0, 0);
    for _ in 1..PER_PES {
        data.extend_from_slice(&eac3_substream_frame(1, 0));
    }
    let mut parser = Ac3Parser::new();
    let mut fed = 0usize;
    while fed <= MAX_AC3_BUF {
        assert!(parser.parse(&make_eac3_pes(data.clone())).is_empty());
        fed += data.len();
        // Later PES start with a dependent: it only extends the held unit.
        data = eac3_substream_frame(1, 0).repeat(PER_PES);
    }
    assert_eq!(parser.acc.len(), 0, "the oversized carry-over is dropped");

    // A stale HeldAu (end ~1 MiB) would index past this 256-byte buffer.
    assert!(
        parser
            .parse(&make_eac3_pes(eac3_substream_frame(0, 0)))
            .is_empty()
    );
    let out = parser.flush();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].data.len(), 256, "only the post-guard unit survives");
}

// helper: PES with a generic pts for E-AC-3 tests
fn make_eac3_pes(data: Vec<u8>) -> PesPacket {
    PesPacket {
        source: None,
        pid: 0,
        pts: Some(90000),
        dts: None,
        data,
        discontinuity: false,
    }
}

/// An access unit that began in an earlier packet keeps THAT packet's
/// source offset. The packet that completes it is a different clip at a
/// seam, and taking its offset places the audio in the wrong one.
#[test]
fn an_access_unit_carries_the_source_of_the_packet_it_began_in() {
    let mut parser = Ac3Parser::new();
    let frame = make_ac3_frame(0, 4);

    let mut p1 = PesPacket {
        pid: 0x1100,
        pts: Some(90_000),
        dts: None,
        data: frame[..frame.len() / 2].to_vec(),
        source: Some(crate::pes::SourcePos::at_byte(1_000)),
        discontinuity: false,
    };
    p1.data.truncate(frame.len() / 2);
    assert!(parser.parse(&p1).is_empty(), "partial frame held");

    let mut rest = frame[frame.len() / 2..].to_vec();
    rest.extend_from_slice(&make_ac3_frame(0, 4));
    let p2 = PesPacket {
        pid: 0x1100,
        pts: Some(180_000),
        dts: None,
        data: rest,
        source: Some(crate::pes::SourcePos::at_byte(9_000)),
        discontinuity: false,
    };
    let frames = parser.parse(&p2);
    assert!(!frames.is_empty(), "the completed unit is emitted");
    assert_eq!(
        frames[0].source.map(|s| s.byte),
        Some(1_000),
        "the unit belongs to the packet its FIRST byte came from"
    );
}
// A track that becomes POISONED while an access unit is held open across a PES boundary
// must not emit that unit on resume.
#[test]
fn a_held_access_unit_is_dropped_when_the_track_poisons_before_it_resumes() {
    // One more than the verdict gate: the Nth close is what poisons.
    const CORRUPT_AUS: usize = 200;

    let mut parser = Ac3Parser::new();
    let mut data = Vec::new();
    for _ in 0..CORRUPT_AUS {
        let mut f = eac3_substream_frame(0, 0);
        // Corrupt a payload byte AFTER CRC finalization: header stays intact so
        // the frame is still parsed whole and fails only CRC — a VERIFIED
        // drop, the only kind that feeds the poison verdict.
        f[100] ^= 0xFF;
        assert!(!frame_crc_ok(&f), "the fixture frame must fail its CRC");
        data.extend_from_slice(&f);
    }
    // The clean access unit. Last in the PES, so it is held open.
    let clean = eac3_substream_frame(0, 0);
    assert!(
        frame_crc_ok(&clean),
        "the held unit is individually decodable"
    );
    data.extend_from_slice(&clean);

    let emitted = parser.parse(&make_eac3_pes(data));
    assert!(
        emitted.is_empty(),
        "every corrupt unit is dropped and the clean one is held; got {} frame(s)",
        emitted.len()
    );
    // The state the re-check depends on: the track IS poisoned, and the
    // held unit was opened before that verdict existed.
    assert!(
        parser.tally.is_poisoned(),
        "fixture must actually cross the whole-track poison threshold"
    );

    // Resume. The next PES opens a new unit, which closes the held one.
    let out = parser.parse(&make_eac3_pes(eac3_substream_frame(0, 0)));
    assert!(
        out.is_empty(),
        "an access unit held across the boundary must not be emitted once \
             the track is poisoned; got {} frame(s) totalling {} bytes",
        out.len(),
        out.iter().map(|f| f.data.len()).sum::<usize>()
    );
    // ...and nothing may leak at end of stream either.
    let tail = parser.flush();
    assert!(
        tail.is_empty(),
        "a poisoned track emits nothing at EOS; got {} frame(s)",
        tail.len()
    );
    assert!(
        parser.dropped_frames() > CORRUPT_AUS as u64,
        "the held unit must be ACCOUNTED as a drop, not silently discarded"
    );
}

// An access unit starting in a LATER PES adopts that PES's PTS even when it differs from
// the running cadence, so a real PTS jump is followed rather than drifted past.
#[test]
fn an_access_unit_in_a_later_pes_adopts_its_pts_over_the_cadence() {
    let mut parser = Ac3Parser::new();
    let frame = make_ac3_frame(0, 4);
    let pes = |pts: i64| PesPacket {
        source: None,
        pid: 0,
        pts: Some(pts),
        dts: None,
        data: frame.clone(),
        discontinuity: false,
    };
    let f1 = parser.parse(&pes(90_000));
    assert_eq!(f1[0].pts_ns, pts_to_ns(90_000));
    // Cadence would put this one at 90_000 + 32 ms; the PES says 180_000.
    let f2 = parser.parse(&pes(180_000));
    assert_eq!(f2.len(), 1);
    assert_eq!(f2[0].pts_ns, pts_to_ns(180_000));
}
