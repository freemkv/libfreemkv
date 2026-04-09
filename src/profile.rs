//! Drive profile loading and matching.

use serde::Deserialize;
use crate::error::{Error, Result};

/// Top-level profiles file — keyed by chipset.
#[derive(Debug, Deserialize)]
pub struct ProfilesFile {
    #[serde(default)]
    pub mt1959: Vec<DriveProfile>,
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
    #[serde(default)]
    pub variant: String,
    #[serde(default, deserialize_with = "deserialize_hex4")]
    pub signature: [u8; 4],
    #[serde(default, deserialize_with = "deserialize_base64")]
    pub firmware: Vec<u8>,

    #[serde(default)]
    pub dvd_all_regions: bool,
    #[serde(default)]
    pub bd_raw_read: bool,
    #[serde(default)]
    pub bd_raw_metadata: bool,
    #[serde(default)]
    pub unrestricted_speed: bool,

    #[serde(default)]
    pub drive_id: String,
    #[serde(default)]
    pub drive_version: String,
}

/// Chipset — determined by which section the profile was found in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Chipset {
    MediaTek,
    Renesas,
}

impl Chipset {
    pub fn name(&self) -> &'static str {
        match self {
            Chipset::MediaTek => "MediaTek MT1959",
            Chipset::Renesas => "Renesas",
        }
    }
}

/// Result of profile lookup — includes chipset from the section it was found in.
pub struct ProfileMatch {
    pub profile: DriveProfile,
    pub chipset: Chipset,
}

// ── Hex/base64 parsing ─────────────────────────────────────────────────

fn parse_hex4(s: &str) -> Result<[u8; 4]> {
    if s.len() != 8 {
        return Err(Error::ProfileParse { detail: format!("expected 8 hex chars, got {}", s.len()) });
    }
    let mut out = [0u8; 4];
    for i in 0..4 {
        out[i] = u8::from_str_radix(&s[i*2..i*2+2], 16)
            .map_err(|e| Error::ProfileParse { detail: format!("bad hex: {e}") })?;
    }
    Ok(out)
}

fn deserialize_hex4<'de, D>(deserializer: D) -> std::result::Result<[u8; 4], D::Error>
where D: serde::Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    if s.is_empty() { return Ok([0; 4]); }
    parse_hex4(&s).map_err(serde::de::Error::custom)
}

fn deserialize_base64<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where D: serde::Deserializer<'de> {
    use base64::Engine;
    let s = String::deserialize(deserializer)?;
    if s.is_empty() { return Ok(Vec::new()); }
    base64::engine::general_purpose::STANDARD
        .decode(&s)
        .map_err(serde::de::Error::custom)
}

// ── Loading ────────────────────────────────────────────────────────────

const BUNDLED_PROFILES: &str = include_str!("../profiles.json");

pub fn load_bundled() -> Result<ProfilesFile> {
    load_from_str(BUNDLED_PROFILES)
}

fn load_from_str(data: &str) -> Result<ProfilesFile> {
    serde_json::from_str(data)
        .map_err(|e| Error::ProfileParse { detail: format!("JSON: {e}") })
}

/// Find a profile matching a drive's INQUIRY fields. Returns profile + chipset.
pub fn find_by_drive_id(
    profiles: &ProfilesFile,
    drive_id: &crate::identity::DriveId,
) -> Option<ProfileMatch> {
    let v = drive_id.vendor_id.trim();
    let r = drive_id.product_revision.trim();
    let vs = drive_id.vendor_specific.trim();
    let date = drive_id.firmware_date.trim();

    for (chipset, list) in [
        (Chipset::MediaTek, &profiles.mt1959),
        (Chipset::Renesas, &profiles.renesas),
    ] {
        if let Some(p) = list.iter().find(|p| {
            p.identity.vendor_id.trim() == v
                && p.identity.product_revision.trim() == r
                && p.identity.vendor_specific.trim() == vs
                && p.identity.firmware_date.trim() == date
        }) {
            return Some(ProfileMatch { profile: p.clone(), chipset });
        }

        if let Some(p) = list.iter().find(|p| {
            p.identity.vendor_id.trim() == v
                && p.identity.product_revision.trim() == r
                && p.identity.vendor_specific.trim() == vs
        }) {
            return Some(ProfileMatch { profile: p.clone(), chipset });
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
        inquiry[8..8+vendor.len().min(8)].copy_from_slice(&vendor.as_bytes()[..vendor.len().min(8)]);
        inquiry[32..32+rev.len().min(4)].copy_from_slice(&rev.as_bytes()[..rev.len().min(4)]);
        inquiry[36..36+vs.len().min(7)].copy_from_slice(&vs.as_bytes()[..vs.len().min(7)]);
        DriveId::from_inquiry(&inquiry, date)
    }

    #[test]
    fn test_find_known_drive() {
        let profiles = load_bundled().unwrap();
        let id = make_drive_id("HL-DT-ST", "1.03", "NM00000", "211810241934");
        let m = find_by_drive_id(&profiles, &id).unwrap();
        assert_eq!(m.profile.identity.vendor_id.trim(), "HL-DT-ST");
        assert_eq!(m.chipset, Chipset::MediaTek);
    }

    #[test]
    fn test_find_unknown_drive() {
        let profiles = load_bundled().unwrap();
        let id = make_drive_id("FAKE-VND", "9.99", "XX12345", "");
        assert!(find_by_drive_id(&profiles, &id).is_none());
    }
}
