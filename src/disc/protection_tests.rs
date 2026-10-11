use super::*;
use crate::disc::{AacsState, ContentFormat, DiscRegion};

fn disc(format: DiscFormat) -> Disc {
    Disc {
        volume_id: "VOL".into(),
        meta_title: None,
        format,
        capacity_sectors: 1000,
        capacity_bytes: 1000 * 2048,
        layers: 1,
        titles: Vec::new(),
        region: DiscRegion::Unknown,
        aacs: None,
        css: None,
        encrypted: false,
        aacs_error: None,
        css_error: None,
        bdplus: None,
        content_format: ContentFormat::BdTs,
    }
}

fn aacs(version: u8, disc_hash: &str, volume_id: [u8; 16]) -> AacsState {
    AacsState {
        version,
        bus_encryption: true,
        mkb_version: None,
        disc_hash: disc_hash.into(),
        volume_id,
        uk_ro: Vec::new(),
        mkb: Vec::new(),
    }
}

fn css() -> crate::css::CssState {
    crate::css::CssState {
        title_key: [1; 5],
        crack_span: None,
    }
}

#[test]
fn intrinsic_id_is_the_scanned_disc_hash_verbatim() {
    let mut d = disc(DiscFormat::BluRay);
    assert_eq!(d.intrinsic_id(), None);
    d.aacs = Some(aacs(1, "0xABcd", [0; 16]));
    assert_eq!(d.intrinsic_id(), Some("0xABcd"));
}

#[test]
fn aacs_volume_id_ignores_an_all_zero_vid() {
    let mut d = disc(DiscFormat::Uhd);
    assert_eq!(d.aacs_volume_id(), None);
    d.aacs = Some(aacs(2, "", [0; 16]));
    assert_eq!(d.aacs_volume_id(), None);
    d.aacs = Some(aacs(2, "", [7; 16]));
    assert_eq!(d.aacs_volume_id(), Some([7; 16]));
}

#[test]
fn only_aacs_state_needs_a_key_lookup() {
    let mut d = disc(DiscFormat::BluRay);
    d.encrypted = true;
    assert!(
        !d.needs_key_lookup(),
        "encrypted without state has nothing to look up"
    );
    d.aacs = Some(aacs(1, "", [0; 16]));
    assert!(d.needs_key_lookup());
    let mut dvd = disc(DiscFormat::Dvd);
    dvd.css = Some(css());
    assert!(!dvd.needs_key_lookup());
}

#[test]
fn keys_per_title_is_a_dvd_without_aacs() {
    assert!(disc(DiscFormat::Dvd).keys_per_title());
    let mut d = disc(DiscFormat::Dvd);
    d.aacs = Some(aacs(1, "", [0; 16]));
    assert!(!d.keys_per_title());
    for f in [
        DiscFormat::BluRay,
        DiscFormat::Uhd,
        DiscFormat::Fmts,
        DiscFormat::HdDvd,
        DiscFormat::Unknown,
    ] {
        assert!(!disc(f).keys_per_title(), "{f:?}");
    }
}

#[test]
fn protection_label_classifies_the_scheme() {
    let clear = disc(DiscFormat::BluRay);
    assert_eq!(clear.protection_label(), ProtectionLabel::Clear);

    let mut failed_css = disc(DiscFormat::Dvd);
    failed_css.encrypted = true;
    failed_css.css_error = Some(Error::CssKeyMissing);
    assert_eq!(failed_css.protection_label(), ProtectionLabel::Css);
    failed_css.css_error = None;
    failed_css.css = Some(css());
    assert_eq!(failed_css.protection_label(), ProtectionLabel::Css);

    let generation = |f: DiscFormat, state: Option<AacsState>| {
        let mut d = disc(f);
        d.encrypted = true;
        d.aacs = state;
        d.protection_label()
    };
    let aacs_label = |major, minor| ProtectionLabel::Aacs { major, minor };
    assert_eq!(generation(DiscFormat::Fmts, None), aacs_label(2, 1));
    assert_eq!(generation(DiscFormat::Uhd, None), aacs_label(2, 0));
    assert_eq!(generation(DiscFormat::BluRay, None), aacs_label(1, 0));
    assert_eq!(generation(DiscFormat::HdDvd, None), aacs_label(1, 0));
    let v2 = || Some(aacs(2, "", [0; 16]));
    assert_eq!(generation(DiscFormat::BluRay, v2()), aacs_label(2, 0));
    assert_eq!(generation(DiscFormat::Unknown, v2()), aacs_label(2, 0));
    assert_eq!(generation(DiscFormat::Dvd, None), ProtectionLabel::Unknown);
    assert_eq!(
        generation(DiscFormat::Unknown, None),
        ProtectionLabel::Unknown
    );
}

#[test]
fn protection_failure_prefers_the_css_error() {
    let mut d = disc(DiscFormat::BluRay);
    assert!(d.protection_failure().is_none());
    d.aacs_error = Some(Error::AacsVidUnavailable);
    let code = |d: &Disc| d.protection_failure().map(|e| e.code());
    assert_eq!(code(&d), Some(Error::AacsVidUnavailable.code()));
    d.css_error = Some(Error::CssKeyMissing);
    assert_eq!(code(&d), Some(Error::CssKeyMissing.code()));
}
