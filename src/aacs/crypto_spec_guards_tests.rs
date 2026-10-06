use super::AACS_IV;
use crate::spec::keys::KS_19_IV0;

/// per spec; do not change without a spec citation — KS-19 [CM] §2.1.2: "iv0, which
/// is: 0BA0F8DDFEA61FB3D8DF9F566A050F78₁₆" (the value is read from the quote).
#[test]
fn cbc_iv_is_iv0() {
    let hex = KS_19_IV0
        .text
        .rsplit(' ')
        .next()
        .and_then(|h| h.strip_suffix("₁₆"))
        .expect("KS-19 ends with the iv0 value");
    let iv0: Vec<u8> = (0..32)
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    assert_eq!(hex.len(), 32);
    assert_eq!(AACS_IV.to_vec(), iv0);
}
