//! LK19 (KU §2.2): "within libfreemkv's public API, `KeyRing` is the only source of
//! AACS-decrypted data". Each door below compiled before KU-X2 and must not compile now.
//! Per spec; do not change without a spec citation proving otherwise.
//!
//! `DecryptKeys::Aacs` construction (§2.2: "`#[non_exhaustive]` on the variant"):
//! ```compile_fail
//! let _ = libfreemkv::decrypt::DecryptKeys::Aacs {
//!     unit_keys: Vec::new(),
//!     format: libfreemkv::disc::ContentFormat::BdTs,
//! };
//! ```
//! `AacsKeyMap::from_ranges` (§2.2: "`pub(crate)`"):
//! ```compile_fail
//! let _ = libfreemkv::decrypt::AacsKeyMap::from_ranges(vec![(0, 3, 0)]);
//! ```
//! `AacsKeyMap::from_ranges_phased` (§2.2: "`pub(crate)`"):
//! ```compile_fail
//! use libfreemkv::decrypt::{AacsKeyMap, Phase};
//! let _ = AacsKeyMap::from_ranges_phased(vec![(0, 3, 0, Phase::All)]);
//! ```
//! `DecryptingSectorSource::with_key_map` (§2.2: "`pub(crate)`"):
//! ```compile_fail
//! use libfreemkv::{DecryptingSectorSource, FileSectorSource, decrypt::AacsKeyMap};
//! fn f(s: DecryptingSectorSource<FileSectorSource>, m: std::sync::Arc<AacsKeyMap>) {
//!     let _ = s.with_key_map(m);
//! }
//! ```
//! `DecryptingSectorSource::set_key_map` (§2.2: "`pub(crate)`"):
//! ```compile_fail
//! use libfreemkv::{DecryptingSectorSource, FileSectorSource, decrypt::AacsKeyMap};
//! fn f(s: &mut DecryptingSectorSource<FileSectorSource>, m: std::sync::Arc<AacsKeyMap>) {
//!     s.set_key_map(m);
//! }
//! ```
//! `DiscStream::with_key_map`, the same install on the live-drive stream:
//! ```compile_fail
//! fn f(s: libfreemkv::DiscStream, m: std::sync::Arc<libfreemkv::decrypt::AacsKeyMap>) {
//!     let _ = s.with_key_map(m);
//! }
//! ```
//! `aacs::content::decrypt_unit` (§2.2: "`pub(crate)`"):
//! ```compile_fail
//! libfreemkv::aacs::content::decrypt_unit(&mut [0u8; 6144], &[0u8; 16]);
//! ```
//!
//! `aacs::content::unit_key_opens` is a `keyproof`-gated key→content oracle, so
//! the DEFAULT public API carries no "did this key open this unit" answer. The
//! door below is itself gated to `not(keyproof)` so it asserts absence only in a
//! build where the oracle is meant to be absent (and does not false-fail under
//! `--features keyproof`, where the symbol is intentionally present).

/// With `keyproof` OFF, `aacs::content::unit_key_opens` must not exist:
/// ```compile_fail
/// libfreemkv::aacs::content::unit_key_opens(
///     &[0u8; 6144], &[0u8; 16], libfreemkv::disc::ContentFormat::BdTs);
/// ```
#[cfg(not(feature = "keyproof"))]
pub(crate) mod keyproof_gated_off {}
