use super::*;

fn svm_with_sections(bodies: &[&[u8]]) -> Svm {
    let mut bytes = Vec::new();
    for (index, body) in bodies.iter().enumerate() {
        let section_start = bytes.len();
        assert_eq!(section_start, index * crate::svm::SECTION_SIZE);
        bytes.extend_from_slice(b"BDSVM_CC");
        bytes.extend_from_slice(&[0; 12]);
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&[0; crate::svm::TRAILER_SIZE]);
        if index + 1 < bodies.len() {
            bytes.resize((index + 1) * crate::svm::SECTION_SIZE, 0);
        }
    }
    Svm::parse(bytes).unwrap()
}

#[test]
fn library_resolves_each_named_file_and_section() {
    let mut library = BdPlusContentLibrary::default();
    library.insert(1, svm_with_sections(&[b"first-section", b"second-section"]));
    library.insert(2, svm_with_sections(&[b"other-file"]));
    assert!(library.contains(1));
    assert_eq!(library.len(), 2);
    assert_eq!(library.section_body(1, 0).unwrap(), b"first-section");
    assert_eq!(library.section_body(1, 1).unwrap(), b"second-section");
    assert_eq!(library.section_body(2, 0).unwrap(), b"other-file");
}

#[test]
fn library_distinguishes_missing_file_from_missing_section() {
    let mut library = BdPlusContentLibrary::default();
    library.insert(1, svm_with_sections(&[b"only-one"]));
    assert_eq!(
        library.section_body(1, 7),
        Err(BdPlusError::MissingContent {
            name: 1,
            section: 7,
        })
    );
    assert_eq!(
        library.section_body(9, 0),
        Err(BdPlusError::MissingContent {
            name: 9,
            section: 0,
        })
    );
}
