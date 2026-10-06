use super::*;

// Magics as literals (<linux/magic.h>), not the constants above, so a wrong
// constant cannot pass by agreeing with itself.
#[test]
fn classify_f_type_maps_magics() {
    assert_eq!(classify_f_type(0x6969), FsType::Nfs);
    for magic in [0xEF53, 0x5846_5342, 0x9123_683E, 0x0102_1994] {
        assert_eq!(classify_f_type(magic), FsType::Local, "{magic:#x}");
    }
    assert_eq!(classify_f_type(0), FsType::Unknown);
    assert_eq!(classify_f_type(0x6969 + 1), FsType::Unknown);
}
