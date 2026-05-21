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
    #[serde(default, deserialize_with = "deserialize_hex4")]
    pub signature: [u8; 4],
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
    pub fn name(&self) -> &'static str {
        match self {
            Platform::Mt1959A => "MediaTek MT1959",
            Platform::Mt1959B => "MediaTek MT1959",
            Platform::Renesas => "Renesas",
        }
    }
}

/// Result of profile lookup.
pub struct ProfileMatch {
    pub profile: DriveProfile,
    pub platform: Platform,
}

// ── Parsing ────────────────────────────────────────────────────────────

fn parse_hex4(s: &str) -> Result<[u8; 4]> {
    if s.len() != 8 {
        return Err(Error::ProfileParse);
    }
    let mut out = [0u8; 4];
    for i in 0..4 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|_| Error::ProfileParse)?;
    }
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
    if s.len() % 2 != 0 {
        return Err("odd hex length");
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| "invalid hex digit")?;
        out.push(byte);
    }
    Ok(out)
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
    if bytes.len() != 10 {
        return Err(serde::de::Error::custom("expected 10 bytes"));
    }
    let mut out = [0u8; 10];
    out.copy_from_slice(&bytes);
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
    if bytes.len() != 12 {
        return Err(serde::de::Error::custom("expected 12 bytes"));
    }
    let mut out = [0u8; 12];
    out.copy_from_slice(&bytes);
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

pub fn load_bundled() -> Result<ProfilesFile> {
    load_from_str(BUNDLED_PROFILES)
}

fn load_from_str(data: &str) -> Result<ProfilesFile> {
    serde_json::from_str(data).map_err(|_| Error::ProfileParse)
}

/// Find a profile matching a drive's INQUIRY fields.
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
}
