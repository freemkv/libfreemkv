use super::*;

/// Build a 12-byte object record: `object_type` in the top two bits of
/// byte 0, HDMV `id_ref` big-endian at offset 6.
fn hdmv_obj(id_ref: u16) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0] = 1 << 6;
    b[6..8].copy_from_slice(&id_ref.to_be_bytes());
    b
}
fn bdj_obj() -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0] = 2 << 6;
    b
}

fn build(first: [u8; 12], top: [u8; 12], titles: &[[u8; 12]]) -> Vec<u8> {
    let indexes_start = 48u32;
    let mut d = vec![0u8; indexes_start as usize];
    d[0..4].copy_from_slice(b"INDX");
    d[4..8].copy_from_slice(b"0300");
    d[8..12].copy_from_slice(&indexes_start.to_be_bytes());
    d.extend_from_slice(&0u32.to_be_bytes()); // index_len (unused by parser)
    d.extend_from_slice(&first);
    d.extend_from_slice(&top);
    d.extend_from_slice(&(titles.len() as u16).to_be_bytes());
    for t in titles {
        d.extend_from_slice(t);
    }
    d
}

#[test]
fn parses_hdmv_first_play_and_titles() {
    let d = build(hdmv_obj(0), bdj_obj(), &[bdj_obj(), bdj_obj()]);
    let idx = parse(&d).expect("parses");
    assert_eq!(idx.first_play, PlaybackObj::Hdmv { id_ref: 0 });
    assert_eq!(idx.top_menu, PlaybackObj::BdJ);
    assert_eq!(idx.titles.len(), 2);
    assert_eq!(idx.titles[0], PlaybackObj::BdJ);
}

#[test]
fn rejects_bad_magic_and_truncation() {
    assert!(parse(b"NOPE").is_none());
    let d = build(hdmv_obj(3), hdmv_obj(0), &[hdmv_obj(1)]);
    // Truncating below the declared title span must yield None, never panic.
    assert!(parse(&d[..d.len() - 4]).is_none());
}

#[test]
fn rejects_title_count_over_the_cap() {
    let mut d = build(hdmv_obj(0), bdj_obj(), &[bdj_obj()]);
    // num_titles (u16) is at 48 (indexes_start) + 4 (index_len) + 12 + 12 = 76.
    // Overwrite it with MAX_TITLES + 1: the cap must reject before any record
    // read, so an attacker-huge count can't drive a 4097-entry parse/alloc.
    d[76..78].copy_from_slice(&((MAX_TITLES as u16) + 1).to_be_bytes());
    assert!(
        parse(&d).is_none(),
        "title count over MAX_TITLES must be rejected"
    );
}

#[test]
fn rejects_zero_titles() {
    let d = build(hdmv_obj(0), bdj_obj(), &[]);
    assert!(parse(&d).is_none(), "num_titles == 0 must be rejected");
}

#[test]
fn unknown_object_types_parse_as_unknown() {
    let mut none = [0u8; 12]; // object_type 0
    let mut reserved = [0u8; 12];
    reserved[0] = 3 << 6; // object_type 3
    none[0] = 0x3F; // low bits set must not leak into the type
    let d = build(none, reserved, &[reserved]);
    let idx = parse(&d).expect("parses");
    assert_eq!(idx.first_play, PlaybackObj::Unknown);
    assert_eq!(idx.top_menu, PlaybackObj::Unknown);
    assert_eq!(idx.titles[0], PlaybackObj::Unknown);
}
