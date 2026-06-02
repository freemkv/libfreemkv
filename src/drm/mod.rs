//! Top-level DRM scheme dispatch.
//!
//! Four content-protection schemes ride through a single
//! detect-then-load pipeline:
//!
//! | Scheme              | Discriminator                                  |
//! |---------------------|------------------------------------------------|
//! | [`DrmScheme::Css`]  | DVD probe sector flagged scrambled             |
//! | [`DrmScheme::Aacs10`] | Content cert type byte `0x00`                |
//! | [`DrmScheme::Aacs20`] | Content cert type byte `!= 0x00`, no Variant |
//! | [`DrmScheme::Aacs21`] | Content cert + MKB records `0x82` / `0x83`   |
//!
//! Detection happens from a [`DrmProbe`] (raw inputs the caller has
//! already extracted from the disc); resolution runs through a
//! [`DrmContext`] (the full set of inputs the loaders need).
//!
//! The AACS 2.1 arm is wired but disabled. The dispatcher leaves
//! [`crate::aacs::resolve_keys_v21`] reachable as a library entry point
//! for fixture-driven validation, but production consumers go through
//! [`DrmScheme::load`], which short-circuits V21 to `None` until the
//! Variant chain has a real Variant-scheme disc to validate against.

use crate::aacs;
use crate::css;

/// Which content-protection scheme governs a disc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrmScheme {
    /// DVD Content Scramble System.
    Css,
    /// AACS 1.0 — original BD-ROM.
    Aacs10,
    /// AACS 2.0 — UHD-BD, classical Media Key chain.
    Aacs20,
    /// AACS 2.1 — UHD-BD with Media Key Variant chain.
    Aacs21,
}

/// Inputs to [`DrmScheme::detect`]. All borrows — caller retains
/// ownership.
pub struct DrmProbe<'a> {
    /// 2048-byte sample sector from inside a DVD title's extents. Used
    /// only for CSS scramble-flag detection. `None` for non-DVD discs.
    pub dvd_sample_sector: Option<&'a [u8]>,
    /// Content Certificate file bytes (typically `/AACS/Content000.cer`).
    /// `None` when the disc has no AACS directory.
    pub content_cert: Option<&'a [u8]>,
    /// MKB file bytes (typically `/AACS/MKB_RW.inf`). Required to
    /// distinguish AACS 2.0 from AACS 2.1.
    pub mkb: Option<&'a [u8]>,
}

/// Inputs to [`DrmScheme::load`]. Carries everything needed by either
/// the AACS or CSS loader.
pub struct DrmContext<'a> {
    /// AACS resolver inputs — required when the scheme is any AACS
    /// variant.
    pub aacs: Option<aacs::ResolveContext<'a>>,
    /// CSS resolver inputs — required when the scheme is [`DrmScheme::Css`].
    pub css: Option<css::CssContext<'a>>,
}

/// Resolved key material, tagged by scheme.
#[derive(Debug)]
pub enum ResolvedScheme {
    Css(css::CssState),
    Aacs(aacs::ResolvedKeys),
}

impl DrmScheme {
    /// Detect which DRM scheme protects the disc described by `probe`.
    ///
    /// Returns `None` for unencrypted media. The order is intentional:
    /// CSS is checked first (DVD-format probe), then AACS (Blu-ray
    /// format).
    pub fn detect(probe: &DrmProbe<'_>) -> Option<DrmScheme> {
        // CSS — DVD probe sector carries the scramble flag.
        if let Some(sector) = probe.dvd_sample_sector {
            if css::is_scrambled(sector) {
                return Some(DrmScheme::Css);
            }
        }

        // AACS — content cert type byte distinguishes V10 from V20+.
        // V21 promotion requires MKB Variant records.
        let cc = probe.content_cert.and_then(aacs::parse_content_cert)?;
        match cc.version {
            aacs::AacsVersion::V10 => Some(DrmScheme::Aacs10),
            aacs::AacsVersion::V20 | aacs::AacsVersion::V21 => {
                if let Some(mkb) = probe.mkb {
                    let recs = aacs::variants::walk_mkb(mkb);
                    if aacs::variants::is_variant_mkb(&recs) {
                        return Some(DrmScheme::Aacs21);
                    }
                }
                Some(DrmScheme::Aacs20)
            }
        }
    }

    /// Run key resolution for this scheme against `ctx`.
    ///
    /// Returns `None` when the scheme's resolver could not produce keys
    /// (missing context, KEYDB miss, failed crypto walk, etc.) or when
    /// the scheme itself is gated off (see the inline comment on the
    /// `Aacs21` arm).
    pub fn load(self, ctx: &mut DrmContext<'_>) -> Option<ResolvedScheme> {
        match self {
            DrmScheme::Css => ctx
                .css
                .as_mut()
                .and_then(css::resolve)
                .map(ResolvedScheme::Css),
            DrmScheme::Aacs10 => ctx
                .aacs
                .as_ref()
                .and_then(aacs::resolve_keys_v1)
                .map(ResolvedScheme::Aacs),
            DrmScheme::Aacs20 => ctx
                .aacs
                .as_ref()
                .and_then(aacs::resolve_keys_v2)
                .map(ResolvedScheme::Aacs),
            // AACS 2.1 derivation is wired but disabled. KCD validation
            // against a Variant-scheme disc is pending. To enable,
            // uncomment the line below.
            // DrmScheme::Aacs21 => ctx
            //     .aacs
            //     .as_ref()
            //     .and_then(aacs::resolve_keys_v21)
            //     .map(ResolvedScheme::Aacs),
            DrmScheme::Aacs21 => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a minimal cert: type byte + bus-encryption byte + 6 zero
    // cc_id bytes.
    fn cert(type_byte: u8) -> Vec<u8> {
        let mut v = vec![0u8; 8];
        v[0] = type_byte;
        v
    }

    // Synthetic AACS 2.x MKB with no Variant records.
    fn mkb_classical() -> Vec<u8> {
        vec![
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ]
    }

    // Synthetic AACS 2.x MKB with a 0x82 + 0x83 record pair.
    fn mkb_with_variant() -> Vec<u8> {
        let mut m = mkb_classical();
        m.extend_from_slice(&[0x82, 0x00, 0x00, 0x14]);
        m.extend_from_slice(&[0xEE; 16]);
        m.extend_from_slice(&[0x83, 0x00, 0x00, 0x14]);
        m.extend_from_slice(&[0x55; 16]);
        m
    }

    // Synthetic scrambled DVD sector — byte 0x14 carries the CSS
    // scramble flag in bits 4-5.
    fn scrambled_dvd_sector() -> Vec<u8> {
        let mut s = vec![0u8; 2048];
        s[0x14] = 0x30;
        s
    }

    #[test]
    fn detect_returns_none_for_unencrypted() {
        let probe = DrmProbe {
            dvd_sample_sector: None,
            content_cert: None,
            mkb: None,
        };
        assert_eq!(DrmScheme::detect(&probe), None);
    }

    #[test]
    fn detect_returns_css_for_scrambled_dvd() {
        let sector = scrambled_dvd_sector();
        let probe = DrmProbe {
            dvd_sample_sector: Some(&sector),
            content_cert: None,
            mkb: None,
        };
        assert_eq!(DrmScheme::detect(&probe), Some(DrmScheme::Css));
    }

    #[test]
    fn detect_returns_aacs10_for_type0_cert() {
        let c = cert(0x00);
        let probe = DrmProbe {
            dvd_sample_sector: None,
            content_cert: Some(&c),
            mkb: None,
        };
        assert_eq!(DrmScheme::detect(&probe), Some(DrmScheme::Aacs10));
    }

    #[test]
    fn detect_returns_aacs20_for_type1_cert_no_variant() {
        let c = cert(0x01);
        let mkb = mkb_classical();
        let probe = DrmProbe {
            dvd_sample_sector: None,
            content_cert: Some(&c),
            mkb: Some(&mkb),
        };
        assert_eq!(DrmScheme::detect(&probe), Some(DrmScheme::Aacs20));
    }

    #[test]
    fn detect_returns_aacs21_for_type1_cert_with_variant() {
        let c = cert(0x01);
        let mkb = mkb_with_variant();
        let probe = DrmProbe {
            dvd_sample_sector: None,
            content_cert: Some(&c),
            mkb: Some(&mkb),
        };
        assert_eq!(DrmScheme::detect(&probe), Some(DrmScheme::Aacs21));
    }

    #[test]
    fn detect_returns_aacs20_when_mkb_absent() {
        // Type-1 cert but no MKB to upgrade with -> Aacs20.
        let c = cert(0x01);
        let probe = DrmProbe {
            dvd_sample_sector: None,
            content_cert: Some(&c),
            mkb: None,
        };
        assert_eq!(DrmScheme::detect(&probe), Some(DrmScheme::Aacs20));
    }

    #[test]
    fn load_aacs21_returns_none() {
        // The Aacs21 dispatch arm is commented out; load() must
        // return None until KCD validation lands.
        let uk_ro = vec![0u8; 256];
        let vid = [0u8; 16];
        let keydb = aacs::KeyDb::empty();
        let providers: &[&dyn aacs::KeyProvider] = &[&keydb];
        let ctx_aacs = aacs::ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None,
        };
        let mut ctx = DrmContext {
            aacs: Some(ctx_aacs),
            css: None,
        };
        assert!(DrmScheme::Aacs21.load(&mut ctx).is_none());
    }

    /// Exercises the V21 helper directly. Gated `#[ignore]` because
    /// the chain reaches `MediaKeyVariantError::VariantsTableUnavailable`
    /// without a real Variant-scheme disc to fix the per-uv table
    /// layout against — running it here would assert only the
    /// not-yet-wired error code. Kept as a wiring smoke-test for
    /// future enablement.
    #[test]
    #[ignore]
    fn resolve_keys_v21_helper_exists() {
        let uk_ro = vec![0u8; 256];
        let vid = [0xAAu8; 16];
        let keydb = aacs::KeyDb::empty();
        let providers: &[&dyn aacs::KeyProvider] = &[&keydb];
        let mkb = mkb_with_variant();
        let ctx = aacs::ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: Some(&mkb),
        };
        // Just confirm the symbol is callable; we don't assert on the
        // result.
        let _ = aacs::resolve_keys_v21(&ctx);
    }
}
