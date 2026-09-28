//! Spec guards (keys-upfront-design §7.8): behaviour that is correct by the quoted
//! spec and must not change. Each asserts against the `libfreemkv::spec` const text
//! and the behaviour, and is written to fail on a plausible wrong change. Guards
//! that need crate-private items (or the unit decrypt, which leaves the public API)
//! live next to their code, named the same.

use aes::Aes128;
use aes::cipher::{Array, BlockCipherDecrypt, KeyInit};
use libfreemkv::ContentFormat;
use libfreemkv::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
use libfreemkv::aacs::derive::derive_vuk;
use libfreemkv::aacs::inf::parse_unit_key_ro;
use libfreemkv::aacs::mkb::AacsVersion;
use libfreemkv::spec::keys::*;

fn aes_128d(key: &[u8; 16], block: &[u8; 16]) -> [u8; 16] {
    let mut b: Array<u8, _> = (*block).into();
    Aes128::new(&(*key).into()).decrypt_block(&mut b);
    b.into()
}

// A Unit_Key_RO.inf with the key block at `start` (Num_of_CPS_Unit = `declared`) and
// room for `slots` keys at `stride`; every other byte is a non-zero filler.
fn inf(start: usize, declared: u16, slots: usize, stride: usize) -> Vec<u8> {
    let mut v = vec![0xEEu8; start + 48 + stride * slots];
    v[..4].copy_from_slice(&(start as u32).to_be_bytes());
    v[24..26].copy_from_slice(&0u16.to_be_bytes()); // Num_of_Title
    v[start..start + 2].copy_from_slice(&declared.to_be_bytes());
    v
}

/// per spec; do not change without a spec citation — KS-2 [BD] §3.10.1:
/// "An Aligned Unit consists of 32 MPEG source packets … 6144 bytes … 3 logical sectors."
#[test]
fn aligned_unit_is_6144_bytes_3_sectors_32_packets() {
    let t = KS_2_ALIGNED_UNIT.text;
    assert!(t.contains("32 MPEG source packets") && t.contains("TP_extra_header (4 bytes)"));
    assert!(t.contains("(188 bytes)") && t.contains("6144 bytes") && t.contains("3 logical"));
    assert_eq!(ALIGNED_UNIT_LEN, 6144);
    assert_eq!(ALIGNED_UNIT_SECTORS, 3);
    assert_eq!(ALIGNED_UNIT_LEN / (4 + 188), 32);
    assert_eq!(ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS as usize * 2048);
}

fn unit_with_cpi(bits: u8) -> Vec<u8> {
    let mut u = vec![0u8; ALIGNED_UNIT_LEN];
    u[0] = bits << 6 | 0x15; // the low 6 bits are Arrival_time_stamp, never the flag
    u
}

/// per spec; do not change without a spec citation — KS-5 [BD] §3.10.2:
/// "shall be set to 00₂ if the data is not encrypted" (KS-24: libaacs agrees).
#[test]
fn cpi_00_is_clear() {
    assert!(
        KS_5_CPI
            .text
            .contains("set to 00₂ if the data is not encrypted")
    );
    assert!(
        KS_24_LIBAACS_CPI_CLEAR_UNIT
            .text
            .contains("if (!(buf[0] & 0xc0))")
    );
    assert!(!aacs_unit_encrypted(
        &unit_with_cpi(0b00),
        ContentFormat::BdTs
    ));
}

/// per spec; do not change without a spec citation — KS-5 [BD] §3.10.2:
/// "Copy_permission_indicator shall be set to 11₂ if the data is encrypted".
#[test]
fn cpi_11_is_encrypted() {
    assert!(
        KS_5_CPI
            .text
            .contains("set to 11₂ if the data is encrypted")
    );
    assert!(aacs_unit_encrypted(
        &unit_with_cpi(0b11),
        ContentFormat::BdTs
    ));
}

/// per spec; do not change without a spec citation — KS-5 [BD] §3.10.2:
/// "set to 10₂ or 01₂, the data shall be considered encrypted" (not `== 0xC0`).
#[test]
fn cpi_10_and_01_are_considered_encrypted() {
    assert!(
        KS_5_CPI
            .text
            .contains("10₂ or 01₂, the data shall be considered encrypted")
    );
    assert!(aacs_unit_encrypted(
        &unit_with_cpi(0b10),
        ContentFormat::BdTs
    ));
    assert!(aacs_unit_encrypted(
        &unit_with_cpi(0b01),
        ContentFormat::BdTs
    ));
}

/// per spec; do not change without a spec citation — KS-12 [BD] §3.9.3:
/// "Unit_Key_Block_start_address field (32 bits) indicates the start address".
#[test]
fn unit_key_block_start_address_is_bytes_0_to_4_be() {
    assert!(KS_12_UNIT_KEY_BLOCK_START.text.contains("field (32 bits)"));
    assert!(
        KS_12_UNIT_KEY_BLOCK_START
            .text
            .contains("from the first byte of CPS Unit Key")
    );
    // Start 0x0130: big-endian bytes 00 00 01 30 (little-endian would read 0x3001_0000).
    let mut v = inf(0x130, 2, 2, 48);
    v[16] = 1; // Application_Type: the header begins at byte 16, after 96 reserved bits
    let ukf = parse_unit_key_ro(&v, AacsVersion::V10).expect("parses");
    assert_eq!(
        ukf.encrypted_keys.len(),
        2,
        "Num_of_CPS_Unit read at the BE start address"
    );
    assert_eq!(ukf.app_type, 1);
    v[..4].copy_from_slice(&0x130u32.to_le_bytes());
    assert!(
        parse_unit_key_ro(&v, AacsVersion::V10).is_none(),
        "LE is out of range"
    );
}

/// per spec; do not change without a spec citation — KS-13 [BD] Table 3-13:
/// "Application_Type (= 01₁₆) 8 … Num_of_Title#I 16 … (reserved) 16 … CPS_Unit_number …".
#[test]
fn header_fields_and_title_table_layout() {
    let t = KS_13_UNIT_KEY_FILE_HEADER.text;
    assert!(t.contains("First Playback#I 16 … CPS_Unit_number for Top Menu#I 16"));
    assert!(t.contains("Num_of_Title#I 16 … (reserved) 16 … CPS_Unit_number for Title#J"));
    let mut v = inf(0x40, 3, 3, 48);
    v[16] = 1; // Application_Type @16
    v[17] = 1; // Num_of_BD_Directory @17
    v[18..20].copy_from_slice(&[0, 0]); // flag + reserved
    v[20..22].copy_from_slice(&2u16.to_be_bytes()); // First Playback @20
    v[22..24].copy_from_slice(&3u16.to_be_bytes()); // Top Menu @22
    v[24..26].copy_from_slice(&2u16.to_be_bytes()); // Num_of_Title @24
    v[28..30].copy_from_slice(&1u16.to_be_bytes()); // Title 1: 2 reserved (0xEE) + CPS
    v[32..34].copy_from_slice(&3u16.to_be_bytes()); // Title 2
    let ukf = parse_unit_key_ro(&v, AacsVersion::V10).expect("parses");
    assert_eq!((ukf.app_type, ukf.num_bdmv_dir), (1, 1));
    assert_eq!(
        ukf.title_cps_unit,
        vec![1, 2, 0, 2],
        "FP, TM, titles (0-based)"
    );
}

/// per spec; do not change without a spec citation — KS-14 [BD] §3.9.3:
/// "Num_of_CPS_Unit field (16 bits) indicates the number of CPS Units on the disc."
#[test]
fn num_of_cps_unit_is_the_declared_count() {
    assert!(
        KS_14_UNIT_KEY_BLOCK
            .text
            .contains("Num_of_CPS_Unit field (16 bits) indicates")
    );
    for declared in [1u16, 2, 5] {
        let v = inf(0x40, declared, declared as usize, 48);
        let ukf = parse_unit_key_ro(&v, AacsVersion::V10).expect("parses");
        let nums: Vec<u32> = ukf.encrypted_keys.iter().map(|k| k.0).collect();
        assert_eq!(
            nums,
            (1..=declared as u32).collect::<Vec<_>>(),
            "declared {declared}"
        );
    }
    // A file that declares 3 units but holds only 2 key slots is malformed, never "2 units".
    let mut short = inf(0x40, 3, 2, 48);
    short.truncate(0x40 + 48 + 48 + 16);
    assert!(parse_unit_key_ro(&short, AacsVersion::V10).is_none());
}

/// per spec; do not change without a spec citation — KS-14 [BD] Table 3-15:
/// "MAC of PMSN#I 128 … MAC of Device Binding Nonce#I 128 … Encrypted CPS Unit Key … 128".
#[test]
fn v10_key_entry_is_48_bytes_encrypted_key_last() {
    let t = KS_14_UNIT_KEY_BLOCK.text;
    assert!(t.contains("Num_of_CPS_Unit 16 … (reserved) 112 … MAC of PMSN#I 128"));
    assert!(t.contains("MAC of Device Binding Nonce#I 128 … Encrypted CPS Unit Key"));
    let start = 0x40;
    let mut v = inf(start, 2, 2, 48);
    for i in 0..2 {
        let entry = start + 16 + 48 * i;
        v[entry..entry + 16].fill(0xA0 + i as u8); // MAC of PMSN
        v[entry + 16..entry + 32].fill(0xB0 + i as u8); // MAC of Device Binding Nonce
        v[entry + 32..entry + 48].fill(0xC0 + i as u8); // Encrypted CPS Unit Key
    }
    let ukf = parse_unit_key_ro(&v, AacsVersion::V10).expect("parses");
    assert_eq!(ukf.encrypted_keys, vec![(1, [0xC0; 16]), (2, [0xC1; 16])]);
}

/// per spec; do not change without a spec citation — KS-11 [BD] §3.9.2:
/// "CPS_Unit_number values are defined in ascending order, starting from one."
#[test]
fn cps_unit_numbers_are_one_based() {
    assert!(KS_11_CPS_NUMBER_FROM_ONE.text.contains("starting from one"));
    let mut v = inf(0x40, 3, 3, 48);
    v[20..22].copy_from_slice(&1u16.to_be_bytes()); // FP: CPS 1 → index 0
    v[22..24].copy_from_slice(&3u16.to_be_bytes()); // TM: CPS n → index n-1
    v[24..26].copy_from_slice(&2u16.to_be_bytes());
    v[28..30].copy_from_slice(&0u16.to_be_bytes()); // 0: not a CPS number → clamped to 0
    v[32..34].copy_from_slice(&4u16.to_be_bytes()); // > n: out of range → clamped to 0
    let ukf = parse_unit_key_ro(&v, AacsVersion::V10).expect("parses");
    assert_eq!(ukf.title_cps_unit, vec![0, 2, 0, 0]);
    let first = ukf.encrypted_keys.first().expect("keys");
    assert_eq!(first.0, 1, "the first key is CPS unit number 1");
}

/// per spec; do not change without a spec citation — KS-16 [PV] §3.3 "Kvu = AES-G(Km, IDv)";
/// KS-17 [CM] §2.1.3 "AES-G(x1, x2) = AES-128D(x1, x2) ⊕ x2."
#[test]
fn kvu_is_aes_g_of_km_and_vid() {
    assert_eq!(KS_16_KVU.text, "Kvu = AES-G(Km, IDv)");
    assert_eq!(KS_17_AES_G.text, "AES-G(x1, x2) = AES-128D(x1, x2) ⊕ x2.");
    let km: [u8; 16] = core::array::from_fn(|i| (i as u8).wrapping_mul(29) ^ 0x5C);
    let vid: [u8; 16] = core::array::from_fn(|i| 0xF0 - i as u8);
    let mut want = aes_128d(&km, &vid);
    for (w, x) in want.iter_mut().zip(vid) {
        *w ^= x;
    }
    assert_eq!(derive_vuk(&km, &vid), want);
    assert_ne!(
        derive_vuk(&km, &vid),
        aes_128d(&km, &vid),
        "the ⊕ IDv step is there"
    );
    assert_ne!(derive_vuk(&vid, &km), want, "Km is the key, IDv the data");
}

/// per evidence (no public spec); do not change without evidence proving otherwise —
/// KS-25: "the 64-byte Unit_Key_RO.inf stride" of AACS 2.x (qa UHD fixture).
#[test]
fn v20_stride_64_evidence() {
    assert_eq!(
        KS_25_AACS2_EVIDENCE.kind,
        libfreemkv::spec::QuoteKind::Evidence
    );
    assert!(
        KS_25_AACS2_EVIDENCE
            .text
            .contains("64-byte Unit_Key_RO.inf stride")
    );
    let start = 0x40;
    let mut v = inf(start, 2, 2, 64);
    v[start + 48..start + 64].fill(0xC0);
    v[start + 48 + 64..start + 64 + 64].fill(0xC1);
    for version in [AacsVersion::V20, AacsVersion::V21] {
        let ukf = parse_unit_key_ro(&v, version).expect("parses");
        assert_eq!(ukf.encrypted_keys, vec![(1, [0xC0; 16]), (2, [0xC1; 16])]);
    }
    let v10 = parse_unit_key_ro(&v, AacsVersion::V10).expect("parses");
    assert_ne!(
        v10.encrypted_keys[1].1, [0xC1; 16],
        "48 is the AACS 1.0 stride only"
    );
}

/// per evidence (no public spec); do not change without evidence proving otherwise —
/// KS-27: the HD DVD book was "removed from this AACS website due to inactivity".
#[test]
fn hddvd_flag_offset_is_unverified() {
    assert_eq!(
        KS_27_HDDVD_EVIDENCE.kind,
        libfreemkv::spec::QuoteKind::Evidence
    );
    let src = include_str!("../src/aacs/content.rs");
    let lines: Vec<&str> = src.lines().collect();
    let at = lines
        .iter()
        .position(|l| l.starts_with("const PS_SCRAMBLE_OFF"))
        .expect("PS_SCRAMBLE_OFF is defined in aacs/content.rs");
    let context = lines[at.saturating_sub(3)..at].join("\n");
    assert!(
        context.contains("UNVERIFIED"),
        "the UNVERIFIED marker left PS_SCRAMBLE_OFF"
    );
}
