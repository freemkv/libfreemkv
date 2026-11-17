use super::*;

// A pack header with no stuffing, SCR and mux rate zero.
fn pack_header() -> Vec<u8> {
    let mut v = PACK_START.to_vec();
    v.extend_from_slice(&[0x44, 0, 0x04, 0, 0x04, 0x01, 0x01, 0x89, 0xC3, 0xF8]);
    v
}

// A PES packet with an empty optional header, `payload` bytes long.
fn pes(sid: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0, 0, 1, sid];
    v.extend_from_slice(&((payload.len() + 3) as u16).to_be_bytes());
    v.extend_from_slice(&[0x80, 0x00, 0x00]);
    v.extend_from_slice(payload);
    v
}

// A padding packet filling the pack to 2048 bytes.
fn pad_to_pack(mut v: Vec<u8>) -> Vec<u8> {
    let left = PACK_LEN - v.len() - 6;
    v.extend_from_slice(&[0, 0, 1, PADDING]);
    v.extend_from_slice(&(left as u16).to_be_bytes());
    v.resize(PACK_LEN, 0xFF);
    v
}

/// A synthetic CPI: KEY_VF `10`, `TITLE_KEY_PTR` = `ptr`, CH_PTR = `ch`.
pub(crate) fn cpi(ptr: u16, ch: u32) -> Cpi {
    let mut c = [0u8; 16];
    c[0] = 0x80;
    c[1..3].copy_from_slice(&ptr.to_be_bytes());
    c[4..8].copy_from_slice(&ch.to_be_bytes());
    c[8] = 0x80;
    Cpi(c)
}

/// An NV_PCK: system header, then a GCI packet carrying `cpi` at GCI data offset 12.
pub(crate) fn nav_pack(cpi: &Cpi) -> Vec<u8> {
    let mut v = pack_header();
    v.extend_from_slice(&[0, 0, 1, SYSTEM_HEADER, 0, 21]);
    v.extend_from_slice(&[0x80; 21]);
    let mut gci = vec![0, 0, 1, PRIVATE_STREAM_2, 0x01, 0x01, GCI_SUB_STREAM_ID];
    gci.extend_from_slice(&[0x11; CPI_IN_GCI]);
    gci.extend_from_slice(&cpi.0);
    gci.resize(6 + 0x101, 0);
    v.extend_from_slice(&gci);
    pad_to_pack(v)
}

/// A plaintext E-AC-3 audio pack: its first access unit (sync `0B 77`) at byte 400, then
/// padding from byte 1800. `seed` varies the Dtk bytes and the payload.
pub(crate) fn audio_pack(seed: u8) -> Vec<u8> {
    let mut v = pack_header();
    let hd_len = 1800 - v.len() - 6 - 3;
    let mut body: Vec<u8> = (0..hd_len).map(|i| (i as u8).wrapping_mul(seed)).collect();
    body[0] = 0xC0;
    body[1] = 1;
    // The pointer counts from its own last byte (body[3], pack byte 26).
    let ptr = 400 - 26;
    body[2..4].copy_from_slice(&(ptr as u16).to_be_bytes());
    body[400 - 23..400 - 21].copy_from_slice(&[0x0B, 0x77]);
    v.extend_from_slice(&pes(PRIVATE_STREAM_1, &body));
    pad_to_pack(v)
}

/// A plaintext video pack whose one PES fills the pack: no structural test applies.
pub(crate) fn video_pack(seed: u8) -> Vec<u8> {
    let mut v = pack_header();
    let n = PACK_LEN - v.len() - 9;
    let body: Vec<u8> = (0..n).map(|i| (i as u8) ^ seed).collect();
    v.extend_from_slice(&pes(0xE0, &body));
    v
}

fn hl_pack() -> Vec<u8> {
    let mut v = pack_header();
    v.extend_from_slice(&[0, 0, 1, PRIVATE_STREAM_2, 0x07, 0xEC, HLI_SUB_STREAM_ID]);
    v.resize(PACK_LEN, 0x5A);
    v
}

const KT: [u8; 16] = [0x42; 16];

#[test]
fn nav_pack_yields_its_cpi_and_fields() {
    let c = cpi(17, 9);
    assert_eq!(classify(&nav_pack(&c)), PackKind::Nav(Some(c)));
    assert_eq!(c.key_vf(), 0b10);
    assert_eq!(c.title_key_ptr(), 17);
}

#[test]
fn content_key_is_aes_g_of_dtk_and_cpi_lsb96() {
    let c = cpi(1, 0x0102_0304);
    let pack = audio_pack(3);
    let mut d = [0u8; 16];
    d[..4].copy_from_slice(&pack[84..88]);
    d[4..].copy_from_slice(&c.0[4..16]);
    assert_eq!(content_key(&KT, &pack, &c), aes_g(&KT, &d));
    // CPI msb bytes (KMI) are not key input; Dtk and CH_PTR are.
    let mut other = c;
    other.0[3] ^= 0xFF;
    assert_eq!(content_key(&KT, &pack, &other), aes_g(&KT, &d));
    assert_ne!(content_key(&KT, &pack, &cpi(1, 5)), aes_g(&KT, &d));
}

#[test]
fn encrypt_keeps_the_clear_head_and_decrypt_restores_the_pack() {
    let c = cpi(3, 77);
    let plain = audio_pack(5);
    let mut p = plain.clone();
    encrypt_pack(&mut p, &KT, &c);
    assert_eq!(classify(&p), PackKind::Scrambled);
    assert_eq!(p[..20], plain[..20]);
    assert_eq!(p[21..CLEAR_LEN], plain[21..CLEAR_LEN]);
    assert_ne!(p[CLEAR_LEN..], plain[CLEAR_LEN..]);
    assert_eq!(p[..4], PACK_START, "an encrypted pack keeps its start code");
    decrypt_pack(&mut p, &KT, &c);
    assert_eq!(p, plain);
    assert_eq!(classify(&p), PackKind::Clear);
}

#[test]
fn a_shifted_dtk_or_wrong_key_does_not_open_the_pack() {
    let c = cpi(3, 77);
    let mut p = audio_pack(5);
    encrypt_pack(&mut p, &KT, &c);
    let mut wrong = p.clone();
    decrypt_pack(&mut wrong, &[0x43; 16], &c);
    assert_eq!(payload_check(&wrong), Some(false));
    // Dtk one byte early (83..87) is the wrong key input.
    let mut shifted = p.clone();
    let mut d = [0u8; 16];
    d[..4].copy_from_slice(&shifted[83..87]);
    d[4..].copy_from_slice(&c.0[4..]);
    aes_cbc_decrypt(&aes_g(&KT, &d), &mut shifted[CLEAR_LEN..]);
    assert_eq!(payload_check(&shifted), Some(false));
    decrypt_pack(&mut p, &KT, &c);
    assert_eq!(payload_check(&p), Some(true));
}

#[test]
fn payload_check_judges_only_bytes_past_the_clear_head() {
    assert_eq!(payload_check(&audio_pack(9)), Some(true));
    assert_eq!(payload_check(&video_pack(9)), None);
    let mut bad = audio_pack(9);
    bad[400] ^= 1;
    assert_eq!(payload_check(&bad), Some(false));
    let mut bad_pad = audio_pack(9);
    bad_pad[1800] = 0x47;
    assert_eq!(payload_check(&bad_pad), Some(false));
}

#[test]
fn scrambling_is_read_per_pack_from_the_pes_header() {
    let mut v = video_pack(1);
    assert_eq!(classify(&v), PackKind::Clear);
    v[20] |= 0x10;
    assert_eq!(classify(&v), PackKind::Scrambled);
    // A system header's byte 20 is rate_bound, never a scrambling flag.
    let mut nav = nav_pack(&cpi(1, 1));
    nav[20] = 0xB2;
    assert!(matches!(classify(&nav), PackKind::Nav(_)));
}

#[test]
fn highlight_packs_need_a_key_only_under_a_valid_key_pointer() {
    let hl = hl_pack();
    assert_eq!(classify(&hl), PackKind::Highlight);
    assert!(needs_key(classify(&hl), Some(&cpi(1, 1))));
    let mut none = cpi(1, 1);
    none.0[0] = 0;
    assert!(!needs_key(classify(&hl), Some(&none)));
    assert!(!needs_key(classify(&hl), None));
}

// A private_stream_1 PES at `q` whose header data is `h` bytes, an AC-3 sub-stream `sub`
// pointing `ptr` bytes on, and `sync` where the pointer lands. Returns the pack.
fn ac3_pes_pack(q: usize, h: u8, sub: u8, ptr: u16, sync: [u8; 2]) -> Vec<u8> {
    let mut p = vec![0u8; PACK_LEN];
    p[q + 6] = 0x80;
    p[q + 8] = h;
    let hd = q + 9 + usize::from(h);
    p[hd] = sub;
    p[hd + 2..hd + 4].copy_from_slice(&ptr.to_be_bytes());
    let at = hd + 3 + usize::from(ptr);
    p[at..at + 2].copy_from_slice(&sync);
    p
}

#[test]
fn ac3_sync_is_checked_where_the_pointer_lands() {
    let ok = ac3_pes_pack(120, 0, 0x80, 10, [0x0B, 0x77]);
    assert_eq!(ac3_sync_check(&ok, 120, 200), Some(true));
    let bad = ac3_pes_pack(120, 0, 0x80, 10, [0x0B, 0x78]);
    assert_eq!(ac3_sync_check(&bad, 120, 200), Some(false));
    // The header data length moves the sub-stream header.
    let long = ac3_pes_pack(120, 5, 0x80, 10, [0x0B, 0x77]);
    assert_eq!(ac3_sync_check(&long, 120, 200), Some(true));
    // The sync word must lie inside the PES packet: its end is q + 6 + len.
    assert_eq!(
        ac3_sync_check(&ok, 120, 18),
        Some(true),
        "ends exactly after the sync"
    );
    assert_eq!(ac3_sync_check(&ok, 120, 17), None, "one byte short");
}

#[test]
fn ac3_sync_is_not_checked_without_a_usable_pointer() {
    let ok = ac3_pes_pack(120, 0, 0x80, 10, [0x0B, 0x77]);
    // No PES optional header.
    let mut no_flags = ok.clone();
    no_flags[126] = 0x00;
    assert_eq!(ac3_sync_check(&no_flags, 120, 200), None);
    // Pointer zero: no access unit starts in this packet.
    let mut zero = ac3_pes_pack(120, 0, 0x80, 10, [0x0B, 0x77]);
    zero[131] = 0;
    zero[132] = 0;
    assert_eq!(ac3_sync_check(&zero, 120, 200), None);
    // Sub-stream ids: 0x80-0x87 and 0xC0-0xC7 only.
    for (sub, want) in [
        (0x87, true),
        (0xC0, true),
        (0xC7, true),
        (0x88, false),
        (0xBF, false),
        (0xC8, false),
        (0x7F, false),
    ] {
        let p = ac3_pes_pack(120, 0, sub, 10, [0x0B, 0x77]);
        assert_eq!(
            ac3_sync_check(&p, 120, 200).is_some(),
            want,
            "sub-stream {sub:#x}"
        );
    }
    // The sync word may not sit inside the always-clear first 128 bytes.
    let at_clear = ac3_pes_pack(100, 0, 0x80, 16, [0x0B, 0x77]);
    assert_eq!(
        ac3_sync_check(&at_clear, 100, 200),
        Some(true),
        "first byte past the clear part"
    );
    let inside = ac3_pes_pack(100, 0, 0x80, 15, [0x0B, 0x77]);
    assert_eq!(ac3_sync_check(&inside, 100, 200), None);
}

#[test]
fn a_nav_packs_cpi_must_fit_its_gci_packet() {
    let pack = vec![0u8; PACK_LEN];
    assert!(nav_cpi(&pack, 0).is_none());
    // A GCI-sized private_stream_2 packet.
    let gci = |len: u16| {
        let mut p = vec![0u8; PACK_LEN];
        p[..4].copy_from_slice(&[0, 0, 1, PRIVATE_STREAM_2]);
        p[4..6].copy_from_slice(&len.to_be_bytes());
        p[6] = GCI_SUB_STREAM_ID;
        for (i, b) in p[7 + CPI_IN_GCI..7 + CPI_IN_GCI + 16]
            .iter_mut()
            .enumerate()
        {
            *b = 0x10 + i as u8;
        }
        p
    };
    let want = (1 + CPI_IN_GCI + 16) as u16;
    let cpi = nav_cpi(&gci(want), 0).expect("a whole GCI carries a CPI");
    assert_eq!(cpi.0[0], 0x10);
    assert_eq!(cpi.0[15], 0x1F);
    assert!(
        nav_cpi(&gci(want - 1), 0).is_none(),
        "GCI too short for the CPI"
    );
    // Packets before the GCI are walked past.
    let mut p = gci(want);
    let mut walked = vec![0u8; PACK_LEN];
    walked[..4].copy_from_slice(&[0, 0, 1, 0xBB]);
    walked[4..6].copy_from_slice(&10u16.to_be_bytes());
    walked[16..16 + 7 + CPI_IN_GCI + 16].copy_from_slice(&p[..7 + CPI_IN_GCI + 16]);
    p = walked;
    assert!(nav_cpi(&p, 0).is_some());
}
