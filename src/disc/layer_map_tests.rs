use super::*;

// A format-00h DVD answer: header + PFI bytes 0..16 (rest zero).
fn pfi(layer_byte: u8, start: u32, end_data: u32, end_l0: u32) -> Vec<u8> {
    let mut b = vec![0u8; 4 + 2048];
    b[0..2].copy_from_slice(&2050u16.to_be_bytes());
    let p = &mut b[4..];
    p[2] = layer_byte;
    p[4..8].copy_from_slice(&start.to_be_bytes());
    p[8..12].copy_from_slice(&end_data.to_be_bytes());
    p[12..16].copy_from_slice(&end_l0.to_be_bytes());
    b
}

const CAP: u32 = 4_000_000;

#[test]
fn single_layer_dvd_has_no_break() {
    let m = parse_dvd_pfi(&pfi(0x01, 0x30000, 0x26_0000, 0), CAP).unwrap();
    assert_eq!(
        m,
        LayerMap {
            layers: 1,
            layer_break: None,
            track_path: None
        }
    );
}

// OTP DVD-9: layer 0 runs 0x30000..=end_l0; layer 1 PSNs are complements, so the
// data-area end is NOT the break. Break = end_l0 - 0x30000 + 1.
#[test]
fn opposite_track_path_break_is_layer0_end() {
    let end_l0 = 0x30000 + 2_084_960 - 1;
    let m = parse_dvd_pfi(&pfi(0x31, 0x30000, 0xFC_FFFF, end_l0), CAP).unwrap();
    assert_eq!(m.layers, 2);
    assert_eq!(m.track_path, Some(TrackPath::Opposite));
    assert_eq!(m.layer_break, Some(2_084_960));
}

// PTP: End PSN in Layer 0 is 000000h; layer 0's data-area end bounds layer 0.
#[test]
fn parallel_track_path_break_is_layer0_data_end() {
    let end_data = 0x30000 + 2_000_000 - 1;
    let m = parse_dvd_pfi(&pfi(0x21, 0x30000, end_data, 0), CAP).unwrap();
    assert_eq!(m.layers, 2);
    assert_eq!(m.track_path, Some(TrackPath::Parallel));
    assert_eq!(m.layer_break, Some(2_000_000));
}

#[test]
fn implausible_break_is_dropped_but_layer_count_kept() {
    // Break past capacity.
    let m = parse_dvd_pfi(&pfi(0x31, 0x30000, 0, 0x30000 + CAP), CAP).unwrap();
    assert_eq!((m.layers, m.layer_break), (2, None));
    // End before start.
    let m = parse_dvd_pfi(&pfi(0x31, 0x30000, 0, 0x100), CAP).unwrap();
    assert_eq!((m.layers, m.layer_break), (2, None));
}

#[test]
fn empty_short_or_reserved_pfi_is_none() {
    assert_eq!(parse_dvd_pfi(&[0u8; 4100], CAP), None, "zero length field");
    assert_eq!(parse_dvd_pfi(&[0u8; 10], CAP), None);
    // Number of Layers 10b/11b are reserved for DVD.
    assert_eq!(parse_dvd_pfi(&pfi(0x41, 0x30000, 0, 0), CAP), None);
}

fn di(kind: &[u8; 3], layer_nibble: u8) -> Vec<u8> {
    let mut b = vec![0u8; 4100];
    b[0..2].copy_from_slice(&4098u16.to_be_bytes());
    let d = &mut b[4..];
    d[0..2].copy_from_slice(b"DI");
    d[2] = 1;
    d[8..11].copy_from_slice(kind);
    d[12] = (layer_nibble << 4) | 0x01;
    b
}

#[test]
fn bd_di_reports_layer_count() {
    assert_eq!(parse_bd_di(&di(b"BDO", 1)).unwrap().layers, 1);
    assert_eq!(parse_bd_di(&di(b"BDO", 2)).unwrap().layers, 2);
    assert_eq!(parse_bd_di(&di(b"BDU", 3)).unwrap().layers, 3, "BD-66/100");
    let m = parse_bd_di(&di(b"BDR", 2)).unwrap();
    assert_eq!((m.layer_break, m.track_path), (None, None));
}

#[test]
fn bd_di_without_signature_or_with_bad_count_is_none() {
    assert_eq!(parse_bd_di(&[0u8; 4100]), None);
    assert_eq!(parse_bd_di(&di(b"XYZ", 2)), None);
    assert_eq!(parse_bd_di(&di(b"BDO", 0)), None);
    assert_eq!(parse_bd_di(&di(b"BDO", 5)), None);
}

#[test]
fn imgburn_descriptor_is_break_then_image_name() {
    assert_eq!(
        imgburn_dvd_descriptor(2_084_960, "Movie.iso"),
        "LayerBreak=2084960\r\nMovie.iso\r\n"
    );
}

// The break must lie strictly inside the disc.
#[test]
fn break_at_capacity_is_dropped_one_below_is_kept() {
    let m = parse_dvd_pfi(&pfi(0x31, 0x30000, 0, 0x30000 + CAP - 1), CAP).unwrap();
    assert_eq!(m.layer_break, None);
    let m = parse_dvd_pfi(&pfi(0x31, 0x30000, 0, 0x30000 + CAP - 2), CAP).unwrap();
    assert_eq!(m.layer_break, Some(CAP - 1));
    // A one-sector layer 0 is a break of 1.
    let m = parse_dvd_pfi(&pfi(0x31, 0x30000, 0, 0x30000), CAP).unwrap();
    assert_eq!(m.layer_break, Some(1));
}

// The answer needs the 4-byte header and PFI bytes 0..16, and a length field saying so.
#[test]
fn pfi_length_boundaries() {
    let mut b = pfi(0x31, 0x30000, 0, 0x30000 + 99);
    b[0..2].copy_from_slice(&18u16.to_be_bytes());
    assert_eq!(parse_dvd_pfi(&b[..20], CAP).unwrap().layer_break, Some(100));
    assert_eq!(parse_dvd_pfi(&b[..19], CAP), None);
    b[0..2].copy_from_slice(&17u16.to_be_bytes());
    assert_eq!(parse_dvd_pfi(&b[..20], CAP), None);
}

// Each sector field's first byte is reserved: only its low 24 bits address a sector.
#[test]
fn pfi_sector_fields_read_24_bits() {
    let m = parse_dvd_pfi(&pfi(0x31, 0x0003_0000, 0, 0xFF03_0000 + 99), CAP).unwrap();
    assert_eq!(m.layer_break, Some(100));
    let m = parse_dvd_pfi(&pfi(0x21, 0xFF03_0000, 0x0003_0000 + 99, 0), CAP).unwrap();
    assert_eq!(m.layer_break, Some(100));
}

#[test]
fn bd_di_length_signature_and_count_boundaries() {
    let full = di(b"BDW", 4);
    assert_eq!(parse_bd_di(&full).unwrap().layers, 4, "BDXL quad layer");
    assert_eq!(parse_bd_di(&full[..17]).unwrap().layers, 4);
    assert_eq!(parse_bd_di(&full[..16]), None);
    let mut bad_sig = di(b"BDO", 2);
    bad_sig[5] = b'X';
    assert_eq!(parse_bd_di(&bad_sig), None);
}
