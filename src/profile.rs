//! Drive profile loading and matching.

use crate::error::{Error, Result};
use serde::Deserialize;

/// Top-level profiles file — keyed by chipset + variant.
#[derive(Debug, Deserialize)]
pub struct ProfilesFile {
    #[serde(default)]
    pub mt1959_a: Vec<DriveProfile>,
    #[serde(default)]
    pub mt1959_b: Vec<DriveProfile>,
    #[serde(default)]
    pub renesas: Vec<DriveProfile>,
}

/// Drive identity — matched against INQUIRY data.
#[derive(Debug, Clone, Deserialize)]
pub struct Identity {
    #[serde(default)]
    pub vendor_id: String,
    #[serde(default)]
    pub product_revision: String,
    #[serde(default)]
    pub vendor_specific: String,
    #[serde(default)]
    pub firmware_date: String,
}

/// Per-drive profile.
#[derive(Debug, Clone, Deserialize)]
pub struct DriveProfile {
    pub identity: Identity,
    /// Expected first 4 bytes of the drive's unlock response — the
    /// per-drive signature the platform checks before trusting the
    /// extended-access surface. JSON-encoded as 8 lowercase hex chars.
    #[serde(default, deserialize_with = "deserialize_hex4")]
    pub signature: [u8; 4],
    /// Runtime firmware image uploaded during unlock (variant A/B
    /// firmware-load step). JSON-encoded as standard base64; empty when
    /// the profile carries no firmware blob.
    #[serde(default, deserialize_with = "deserialize_base64")]
    pub firmware: Vec<u8>,

    // ── OEM-extended-access CDB templates ──────────────────────────────
    //
    // All optional — older profile blobs that pre-date the CDB capture
    // pipeline simply omit these fields and decode as `None`. Encoded
    // in the JSON as lowercase hex strings without separators
    // (e.g. `"3c014410e29100002400"` for a 10-byte CDB).
    #[serde(default)]
    pub unlock_init_value: u8,
    #[serde(default)]
    pub unlock_response_size: u8,

    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_10")]
    pub read_vid_cdb: Option<[u8; 10]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_10")]
    pub read_disc_keys_cdb: Option<[u8; 10]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_12")]
    pub drive_nominal_speed_cdb: Option<[u8; 12]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_12")]
    pub set_speed_max_cdb: Option<[u8; 12]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_10")]
    pub read10_raw_2sec_cdb: Option<[u8; 10]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_10")]
    pub read10_raw_1sec_cdb: Option<[u8; 10]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_10")]
    pub read_buffer_verify_cdb: Option<[u8; 10]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_10")]
    pub write_buffer_cdb: Option<[u8; 10]>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes_10")]
    pub read_buffer_unlock_cdb: Option<[u8; 10]>,

    // Per-drive identifier tables — variable-length hex strings.
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes")]
    pub speed_zone_table: Option<Vec<u8>>,
    #[serde(default, deserialize_with = "deserialize_opt_hex_bytes")]
    pub speed_calc_table: Option<Vec<u8>>,
}

/// Chipset + variant — determined by which section the profile was found in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Platform {
    Mt1959A,
    Mt1959B,
    Renesas,
}

impl Platform {
    /// Stable, language-neutral platform identifier. The two MT1959 variants
    /// share the chipset but differ in their firmware-upload / unlock
    /// sequence, so they get distinct suffixes — callers (and logs) that key
    /// off `name()` must be able to tell A from B.
    pub fn name(&self) -> &'static str {
        match self {
            Platform::Mt1959A => "MediaTek MT1959-A",
            Platform::Mt1959B => "MediaTek MT1959-B",
            Platform::Renesas => "Renesas",
        }
    }
}

/// Result of a profile lookup: the matched profile plus the platform
/// (chipset + variant) of the section it was found in. The platform
/// determines which unlock/firmware sequence the driver runs.
pub struct ProfileMatch {
    /// The matched profile, cloned out of the profiles file.
    pub profile: DriveProfile,
    /// Which platform section the profile came from.
    pub platform: Platform,
}

// ── Parsing ────────────────────────────────────────────────────────────

/// Decode an even-length ASCII hex string into bytes.
///
/// Operates on raw bytes rather than `&str` char-boundary slices: a
/// non-ASCII input (e.g. a hand-edited profile with a multi-byte char)
/// could otherwise have `&s[i..i+2]` land inside a UTF-8 char boundary and
/// panic. Hex is ASCII, so any non-ASCII or non-hex byte simply fails to
/// decode. The error is a stable, language-neutral token (`"hex"`), not a
/// translatable English message.
fn decode_hex(s: &str) -> std::result::Result<Vec<u8>, &'static str> {
    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("hex");
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16).ok_or("hex")?;
        let lo = (pair[1] as char).to_digit(16).ok_or("hex")?;
        out.push((hi * 16 + lo) as u8);
    }
    Ok(out)
}

fn parse_hex4(s: &str) -> Result<[u8; 4]> {
    let bytes = decode_hex(s).map_err(|_| Error::ProfileParse)?;
    let out: [u8; 4] = bytes.try_into().map_err(|_| Error::ProfileParse)?;
    Ok(out)
}

fn deserialize_hex4<'de, D>(deserializer: D) -> std::result::Result<[u8; 4], D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        return Ok([0; 4]);
    }
    parse_hex4(&s).map_err(serde::de::Error::custom)
}

fn deserialize_base64<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use base64::Engine;
    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        return Ok(Vec::new());
    }
    base64::engine::general_purpose::STANDARD
        .decode(&s)
        .map_err(serde::de::Error::custom)
}

// ── Fixed-length hex deserializers for CDB templates ────────────────────
//
// Profile JSON encodes CDBs as lowercase hex strings without separators.
// An empty string / null / missing field decodes as `None`.

fn parse_hex_bytes(s: &str) -> std::result::Result<Vec<u8>, &'static str> {
    decode_hex(s)
}

fn deserialize_opt_hex_bytes_10<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<[u8; 10]>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    let Some(s) = opt else { return Ok(None) };
    if s.is_empty() {
        return Ok(None);
    }
    let bytes = parse_hex_bytes(&s).map_err(serde::de::Error::custom)?;
    let out: [u8; 10] = bytes
        .try_into()
        .map_err(|_| serde::de::Error::custom("len"))?;
    Ok(Some(out))
}

fn deserialize_opt_hex_bytes_12<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<[u8; 12]>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    let Some(s) = opt else { return Ok(None) };
    if s.is_empty() {
        return Ok(None);
    }
    let bytes = parse_hex_bytes(&s).map_err(serde::de::Error::custom)?;
    let out: [u8; 12] = bytes
        .try_into()
        .map_err(|_| serde::de::Error::custom("len"))?;
    Ok(Some(out))
}

fn deserialize_opt_hex_bytes<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    let Some(s) = opt else { return Ok(None) };
    if s.is_empty() {
        return Ok(None);
    }
    let bytes = parse_hex_bytes(&s).map_err(serde::de::Error::custom)?;
    Ok(Some(bytes))
}

// ── Loading ────────────────────────────────────────────────────────────

const BUNDLED_PROFILES: &str = include_str!("../profiles.json");

/// Parse the bundled profiles fresh into an owned [`ProfilesFile`].
///
/// Re-parses the embedded JSON (~800 KB) on every call; prefer
/// [`bundled`] for the hot path, which parses once and caches. This
/// owned form is kept for callers that need a mutable / independent copy.
pub fn load_bundled() -> Result<ProfilesFile> {
    load_from_str(BUNDLED_PROFILES)
}

/// Borrow the process-wide cached bundled profiles, parsing once on first
/// use. Avoids re-parsing the ~800 KB JSON on every `Drive::open()`.
///
/// Returns `None` if the embedded JSON fails to parse (a build-time bug —
/// the bundled blob is fixed at compile time, so the first successful call
/// guarantees all later calls succeed too).
pub fn bundled() -> Option<&'static ProfilesFile> {
    use std::sync::OnceLock;
    static CACHE: OnceLock<Option<ProfilesFile>> = OnceLock::new();
    CACHE
        .get_or_init(|| load_from_str(BUNDLED_PROFILES).ok())
        .as_ref()
}

/// Find a profile for a drive against the cached bundled profiles.
///
/// Convenience wrapper over [`bundled`] + [`find_by_drive_id`] that skips
/// the per-call re-parse. Returns `None` if no profile matches (or, in the
/// build-bug case, if the bundled JSON failed to parse).
pub fn find_bundled(drive_id: &crate::identity::DriveId) -> Option<ProfileMatch> {
    find_by_drive_id(bundled()?, drive_id)
}

fn load_from_str(data: &str) -> Result<ProfilesFile> {
    serde_json::from_str(data).map_err(|_| Error::ProfileParse)
}

/// Find a profile matching a drive's INQUIRY fields.
///
/// Two-pass per platform section (MT1959-A, then MT1959-B, then Renesas):
/// first an exact match including `firmware_date`, then — if none — a
/// looser match on vendor / revision / vendor-specific only. The exact
/// pass wins so a drive with a known firmware date binds to its precise
/// profile; the looser pass lets a drive whose firmware date we don't have
/// on file still match a same-model profile. All comparisons are
/// whitespace-trimmed. Returns the first section that yields a match.
pub fn find_by_drive_id(
    profiles: &ProfilesFile,
    drive_id: &crate::identity::DriveId,
) -> Option<ProfileMatch> {
    let v = drive_id.vendor_id.trim();
    let r = drive_id.product_revision.trim();
    let vs = drive_id.vendor_specific.trim();
    let date = drive_id.firmware_date.trim();

    for (platform, list) in [
        (Platform::Mt1959A, &profiles.mt1959_a),
        (Platform::Mt1959B, &profiles.mt1959_b),
        (Platform::Renesas, &profiles.renesas),
    ] {
        if let Some(p) = list.iter().find(|p| {
            p.identity.vendor_id.trim() == v
                && p.identity.product_revision.trim() == r
                && p.identity.vendor_specific.trim() == vs
                && p.identity.firmware_date.trim() == date
        }) {
            return Some(ProfileMatch {
                profile: p.clone(),
                platform,
            });
        }

        if let Some(p) = list.iter().find(|p| {
            p.identity.vendor_id.trim() == v
                && p.identity.product_revision.trim() == r
                && p.identity.vendor_specific.trim() == vs
        }) {
            return Some(ProfileMatch {
                profile: p.clone(),
                platform,
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::DriveId;

    fn make_drive_id(vendor: &str, rev: &str, vs: &str, date: &str) -> DriveId {
        let mut inquiry = vec![0u8; 96];
        inquiry[8..8 + vendor.len().min(8)]
            .copy_from_slice(&vendor.as_bytes()[..vendor.len().min(8)]);
        inquiry[32..32 + rev.len().min(4)].copy_from_slice(&rev.as_bytes()[..rev.len().min(4)]);
        inquiry[36..36 + vs.len().min(7)].copy_from_slice(&vs.as_bytes()[..vs.len().min(7)]);
        DriveId::from_inquiry(&inquiry, date)
    }

    #[test]
    fn test_find_known_drive() {
        let profiles = load_bundled().unwrap();
        let id = make_drive_id("HL-DT-ST", "1.03", "NM00000", "211810241934");
        let m = find_by_drive_id(&profiles, &id).unwrap();
        assert_eq!(m.profile.identity.vendor_id.trim(), "HL-DT-ST");
        assert_eq!(m.platform, Platform::Mt1959A);
    }

    #[test]
    fn test_find_unknown_drive() {
        let profiles = load_bundled().unwrap();
        let id = make_drive_id("FAKE-VND", "9.99", "XX12345", "");
        assert!(find_by_drive_id(&profiles, &id).is_none());
    }

    #[test]
    fn decode_hex_rejects_non_ascii_without_panic() {
        // A multi-byte char of even byte-length must not slice inside a
        // char boundary; it must decode-fail gracefully.
        assert!(decode_hex("中中").is_err()); // 6 bytes, none hex
        assert!(parse_hex4("中中").is_err()); // 6 bytes != 8 anyway
        // An 8-byte non-ASCII string (two 4-byte chars) hits the exact-len
        // path of parse_hex4; must still error, not panic.
        assert!(parse_hex4("𝕏𝕏").is_err());
    }

    #[test]
    fn decode_hex_roundtrips_valid_hex() {
        assert_eq!(decode_hex("00ff10").unwrap(), vec![0x00, 0xff, 0x10]);
        assert_eq!(parse_hex4("deadbeef").unwrap(), [0xde, 0xad, 0xbe, 0xef]);
        assert!(decode_hex("abc").is_err()); // odd length
        assert!(decode_hex("zz").is_err()); // non-hex
    }

    #[test]
    fn bundled_is_cached_and_matches_fresh_parse() {
        let cached = bundled().expect("bundled profiles parse");
        let fresh = load_bundled().unwrap();
        // Same data either way (compare section sizes — ProfilesFile isn't Eq).
        assert_eq!(cached.mt1959_a.len(), fresh.mt1959_a.len());
        // Cached accessor returns a stable address across calls.
        let a = bundled().unwrap() as *const ProfilesFile;
        let b = bundled().unwrap() as *const ProfilesFile;
        assert_eq!(a, b);
    }

    #[test]
    fn find_bundled_matches_known_drive() {
        let id = make_drive_id("HL-DT-ST", "1.03", "NM00000", "211810241934");
        let m = find_bundled(&id).unwrap();
        assert_eq!(m.platform, Platform::Mt1959A);
    }
}
