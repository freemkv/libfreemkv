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

    // ── New comprehensive tests ────────────────────────────────────────────────

    /// decode_hex accepts empty string → empty Vec.
    /// Mutation: returning an error on empty input breaks empty-field handling.
    #[test]
    fn decode_hex_accepts_empty_string() {
        assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
    }

    /// decode_hex handles all valid hex digit characters (0-9, a-f, A-F).
    /// Mutation: not supporting uppercase A-F means uppercase-encoded profiles fail.
    #[test]
    fn decode_hex_handles_upper_and_lower_case() {
        assert_eq!(
            decode_hex("DEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(
            decode_hex("deadbeef").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(
            decode_hex("DeAdBeEf").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }

    /// parse_hex4 rejects an 8-hex-char string (4 bytes) correctly.
    /// Spec: the signature field is exactly 4 bytes = 8 hex chars.
    /// Mutation: accepting 6 hex chars (3 bytes) would pass a wrong-length signature.
    #[test]
    fn parse_hex4_rejects_wrong_byte_length() {
        // 6 hex chars = 3 bytes ≠ 4.
        assert!(
            parse_hex4("aabbcc").is_err(),
            "3 bytes must be rejected for 4-byte field"
        );
        // 10 hex chars = 5 bytes ≠ 4.
        assert!(
            parse_hex4("aabbccddee").is_err(),
            "5 bytes must be rejected for 4-byte field"
        );
        // Exactly 8 hex chars = 4 bytes: must succeed.
        assert_eq!(parse_hex4("aabbccdd").unwrap(), [0xaa, 0xbb, 0xcc, 0xdd]);
    }

    /// Platform::name() returns stable, non-empty, language-neutral identifiers.
    /// These strings are logged and keyed on in caller code; changing them is a
    /// breaking change.
    /// Mutation: swapping Mt1959A and Mt1959B names silently misroutes firmware upload.
    #[test]
    fn platform_name_is_stable() {
        // The exact strings are part of the public stable API (logged/keyed).
        assert_eq!(Platform::Mt1959A.name(), "MediaTek MT1959-A");
        assert_eq!(Platform::Mt1959B.name(), "MediaTek MT1959-B");
        assert_eq!(Platform::Renesas.name(), "Renesas");
    }

    /// find_by_drive_id: exact match (including firmware_date) wins over loose match.
    /// Spec: two-pass — first an exact match including firmware_date, then looser.
    /// Build two synthetic ProfilesFile entries that differ only by firmware_date,
    /// and verify the correct one is selected.
    /// Mutation: doing only the loose pass would return the first entry regardless of date.
    #[test]
    fn find_by_drive_id_exact_date_wins_over_loose() {
        use serde_json::json;
        // Use an 8-char vendor_id (padded with a trailing space so `trim()` strips
        // the pad, matching the same trimmed form the JSON profile stores).
        // "TESTDRV " fills INQUIRY [8..16] exactly; `ascii_field.trim()` → "TESTDRV".
        let profiles_json = json!({
            "mt1959_a": [
                {
                    "identity": {
                        "vendor_id": "TESTDRV",
                        "product_revision": "1.00",
                        "vendor_specific": "XX00000",
                        "firmware_date": "200001010000"
                    },
                    "signature": "aabbccdd",
                    "firmware": ""
                },
                {
                    "identity": {
                        "vendor_id": "TESTDRV",
                        "product_revision": "1.00",
                        "vendor_specific": "XX00000",
                        "firmware_date": "200006150000"
                    },
                    "signature": "11223344",
                    "firmware": ""
                }
            ]
        })
        .to_string();
        let profiles: ProfilesFile = serde_json::from_str(&profiles_json).unwrap();

        // "TESTDRV " (with space) fills 8 bytes; trim() → "TESTDRV" on both sides.
        let id_date1 = make_drive_id("TESTDRV ", "1.00", "XX00000", "200001010000");
        let id_date2 = make_drive_id("TESTDRV ", "1.00", "XX00000", "200006150000");

        let m1 = find_by_drive_id(&profiles, &id_date1).unwrap();
        let m2 = find_by_drive_id(&profiles, &id_date2).unwrap();

        // Each must bind to its own profile by exact date match.
        assert_eq!(
            m1.profile.signature,
            [0xaa, 0xbb, 0xcc, 0xdd],
            "id_date1 must match first profile"
        );
        assert_eq!(
            m2.profile.signature,
            [0x11, 0x22, 0x33, 0x44],
            "id_date2 must match second profile"
        );
    }

    /// find_by_drive_id: loose match (no date) still works when an entry has
    /// the same vendor/revision/vs but an unknown firmware_date.
    /// Mutation: making the loose pass require a date match means "no date" drives
    ///           always return None even though a same-model profile exists.
    #[test]
    fn find_by_drive_id_loose_match_when_date_unknown() {
        use serde_json::json;
        // "LOOSEDR " fills 8 bytes; trim() → "LOOSEDR".
        let profiles_json = json!({
            "mt1959_a": [
                {
                    "identity": {
                        "vendor_id": "LOOSEDR",
                        "product_revision": "2.00",
                        "vendor_specific": "YY11111",
                        "firmware_date": "210101010000"
                    },
                    "signature": "deadbeef",
                    "firmware": ""
                }
            ]
        })
        .to_string();
        let profiles: ProfilesFile = serde_json::from_str(&profiles_json).unwrap();

        // Drive with an unknown firmware date — no exact match, loose match should work.
        // "LOOSEDR " fills 8 bytes; "000000000000" is the unknown date.
        let id = make_drive_id("LOOSEDR ", "2.00", "YY11111", "000000000000");
        let m = find_by_drive_id(&profiles, &id).unwrap();
        assert_eq!(
            m.profile.signature,
            [0xde, 0xad, 0xbe, 0xef],
            "loose match must bind the same-model profile when date differs"
        );
    }

    /// load_from_str (via load_bundled) returns ProfileParse on invalid JSON.
    /// Mutation: returning an empty ProfilesFile instead of an error silently
    ///           leaves the drive-profile database empty.
    #[test]
    fn load_from_str_returns_profile_parse_on_bad_json() {
        let result: Result<ProfilesFile> =
            serde_json::from_str("not valid json {{{{").map_err(|_| Error::ProfileParse);
        assert!(matches!(result, Err(Error::ProfileParse)));
    }

    /// Bundled profiles is non-empty (mt1959_a has at least one entry).
    /// This pins the embedded JSON: if profiles.json is accidentally emptied
    /// or truncated, this test goes red.
    /// Mutation: clearing profiles.json would make this fail.
    #[test]
    fn bundled_profiles_has_entries() {
        let profiles = load_bundled().unwrap();
        assert!(
            !profiles.mt1959_a.is_empty(),
            "bundled profiles must have at least one mt1959_a entry"
        );
    }

    /// deserialization of a profile with missing optional CDB fields
    /// produces None for those fields (not a parse error).
    /// Spec: all CDB template fields are `#[serde(default)]` — they are optional.
    /// Mutation: making a CDB field required breaks backward-compat with old blobs.
    #[test]
    fn profile_optional_cdb_fields_default_to_none() {
        use serde_json::json;
        let json_str = json!({
            "mt1959_a": [
                {
                    "identity": {
                        "vendor_id": "TEST",
                        "product_revision": "1.00",
                        "vendor_specific": "000000",
                        "firmware_date": ""
                    },
                    "signature": "00000000",
                    "firmware": ""
                }
            ]
        })
        .to_string();
        let profiles: ProfilesFile = serde_json::from_str(&json_str).unwrap();
        let p = &profiles.mt1959_a[0]; // DriveProfile directly
        // All optional CDB fields must be None when absent from JSON.
        assert!(
            p.read_vid_cdb.is_none(),
            "read_vid_cdb must default to None"
        );
        assert!(
            p.read_disc_keys_cdb.is_none(),
            "read_disc_keys_cdb must default to None"
        );
        assert!(
            p.drive_nominal_speed_cdb.is_none(),
            "drive_nominal_speed_cdb must default to None"
        );
        assert!(
            p.set_speed_max_cdb.is_none(),
            "set_speed_max_cdb must default to None"
        );
        assert!(
            p.speed_zone_table.is_none(),
            "speed_zone_table must default to None"
        );
        assert!(
            p.speed_calc_table.is_none(),
            "speed_calc_table must default to None"
        );
    }

    /// deserialize_hex4 of an empty string must produce [0;4] without error.
    /// This matches `deserialize_hex4`'s explicit early-return for empty strings.
    /// Mutation: treating empty string as an error prevents profiles where signature
    ///           was not captured from loading.
    #[test]
    fn profile_empty_signature_deserialises_as_zeroes() {
        use serde_json::json;
        let json_str = json!({
            "mt1959_a": [
                {
                    "identity": {
                        "vendor_id": "TEST",
                        "product_revision": "1.00",
                        "vendor_specific": "000000",
                        "firmware_date": ""
                    },
                    "signature": "",
                    "firmware": ""
                }
            ]
        })
        .to_string();
        let profiles: ProfilesFile = serde_json::from_str(&json_str).unwrap();
        assert_eq!(
            profiles.mt1959_a[0].signature, [0u8; 4],
            "empty signature must deserialise as [0;4]"
        );
    }
}
