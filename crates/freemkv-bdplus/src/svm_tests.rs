use super::*;

fn section(body: usize, padded: bool) -> Vec<u8> {
    let used = HEADER_SIZE + body + TRAILER_SIZE;
    let mut bytes = vec![0; if padded { SECTION_SIZE } else { used }];
    bytes[..8].copy_from_slice(b"BDSVM_CC");
    bytes[8..13].copy_from_slice(&[1, 0, 0, 0, 1]);
    bytes[13..17].copy_from_slice(&[7, 0xd7, 7, 31]);
    bytes[20..24].copy_from_slice(&(body as u32).to_be_bytes());
    bytes[HEADER_SIZE..HEADER_SIZE + body].fill(0x42);
    bytes[HEADER_SIZE + body..used].fill(0x77);
    bytes
}

#[test]
fn all_regions_round_trip_and_share_storage() {
    let mut bytes = section(0x3000, true);
    // Nonzero alignment and unresolved header fields must survive parsing.
    bytes[SECTION_SIZE - 1] = 0x99;
    bytes[17..20].copy_from_slice(&[4, 5, 6]);
    bytes.extend(section(27, false));
    let bytes: Arc<[u8]> = bytes.into();
    let svm = Svm::parse(bytes.clone()).unwrap();
    assert!(Arc::ptr_eq(&bytes, &svm.shared_bytes()));
    assert!(Arc::ptr_eq(
        &svm.shared_bytes(),
        &svm.clone().shared_bytes()
    ));
    assert_eq!(svm.section_count(), 2);
    assert!(svm.section(2).is_none());
    assert!(svm.section(usize::MAX).is_none());
    let primary = svm.primary();
    assert_eq!(primary.offset(), 0);
    assert_eq!(primary.header().magic(), b"BDSVM_CC");
    assert_eq!(
        primary.header().code_date(),
        SvmDate {
            year: 2007,
            month: 7,
            day: 31
        }
    );
    assert_eq!(primary.header().unknown_08_0c(), &[1, 0, 0, 0, 1]);
    assert_eq!(primary.header().unknown_11_13(), &[4, 5, 6]);
    assert_eq!(primary.body().as_bytes().len(), 0x3000);
    assert_eq!(primary.body().prefix().len(), 4096);
    assert_eq!(primary.body().payload().len(), 8192);
    assert_eq!(
        [primary.body().prefix(), primary.body().payload()].concat(),
        primary.body().as_bytes()
    );
    assert_eq!(primary.trailer(), &[0x77; 40]);
    assert_eq!(primary.padding().last(), Some(&0x99));
    assert_eq!(svm.section(1).unwrap().offset(), SECTION_SIZE);
    let mut rebuilt = Vec::new();
    for s in svm.sections() {
        rebuilt.extend_from_slice(s.header().as_bytes());
        rebuilt.extend_from_slice(s.body().as_bytes());
        rebuilt.extend_from_slice(s.trailer());
        rebuilt.extend_from_slice(s.padding());
    }
    assert_eq!(rebuilt, svm.as_bytes());
    assert!(!format!("{svm:?}").contains("77777777"));
}

#[test]
fn invalid_framing_reports_section_offset() {
    for n in 0..HEADER_SIZE + TRAILER_SIZE {
        assert!(Svm::parse(vec![0; n]).is_err());
    }
    let valid = section(1, false);
    for n in 0..valid.len() {
        assert!(Svm::parse(&valid[..n]).is_err());
    }
    let mut oversized = section(0, false);
    oversized[20..24].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(Svm::parse(oversized).is_err());
    let mut second = section(0, true);
    second.extend_from_slice(b"bad");
    let error = Svm::parse(second).unwrap_err();
    assert!(matches!(
        error,
        BdPlusError::SvmFormat {
            offset: SECTION_SIZE
        }
    ));
}

#[test]
fn format_bounds_are_not_online_identity_bounds() {
    // Empty and small bodies are structurally representable. They are not
    // necessarily executable, nor eligible for an online identity.
    for len in [0, 1, 4095, 4096, SECTION_SIZE - HEADER_SIZE - TRAILER_SIZE] {
        let svm = Svm::parse(section(len, false)).unwrap();
        assert_eq!(svm.primary().body().as_bytes().len(), len);
        assert_eq!(svm.primary().body().prefix().len(), len.min(4096));
        assert_eq!(
            svm.primary().body().payload().len(),
            len.saturating_sub(4096)
        );
        assert!(svm.primary().padding().is_empty());
    }
    let mut bytes = section(0, false);
    bytes[8..20].fill(0xff);
    let svm = Svm::parse(bytes.clone()).unwrap();
    assert_eq!(svm.as_bytes(), bytes);
    assert_eq!(svm.primary().header().code_date().month, 255);
}

/// Real capture stays outside the repository. Run explicitly with
/// `FREEMKV_BDPLUS_SVM=/path/00000.svm cargo test --lib castaway_svm -- --ignored`.
#[test]
#[ignore = "requires the user's complete Cast Away SVM capture"]
fn castaway_svm_all_sections_round_trip() {
    let bytes =
        std::fs::read(std::env::var_os("FREEMKV_BDPLUS_SVM").expect("FREEMKV_BDPLUS_SVM")).unwrap();
    let svm = Svm::parse(bytes).unwrap();
    assert_eq!(svm.as_bytes().len(), 20 * 1024 * 1024);
    assert_eq!(svm.section_count(), 10);
    let mut rebuilt = Vec::new();
    for (i, s) in svm.sections().enumerate() {
        assert_eq!(s.offset(), i * SECTION_SIZE);
        assert_eq!(
            s.header().code_date(),
            SvmDate {
                year: 2007,
                month: 7,
                day: 31
            }
        );
        assert_eq!(s.header().unknown_08_0c(), &[1, 0, 0, 0, 1]);
        assert_eq!(s.header().unknown_11_13(), &[0; 3]);
        assert_eq!(
            s.body().as_bytes().len(),
            if i == 0 { 522948 } else { 2097088 }
        );
        assert_eq!(s.padding().len(), if i == 0 { 1574140 } else { 0 });
        assert!(s.padding().iter().all(|b| *b == 0));
        rebuilt.extend_from_slice(s.header().as_bytes());
        rebuilt.extend_from_slice(s.body().as_bytes());
        rebuilt.extend_from_slice(s.trailer());
        rebuilt.extend_from_slice(s.padding());
    }
    assert_eq!(rebuilt, svm.as_bytes());
}
