//! Drive profile loading and matching.
//!
//! The profile contains all per-drive data needed by the MT1959 platform handlers.
//! Profiles are loaded from JSON so new drives can be added without rebuilding.

use serde::Deserialize;
use crate::error::{Error, Result};

///
#[derive(Debug, Clone, Deserialize)]
pub struct DriveProfile {
    // ── Drive identity (from INQUIRY + GET_CONFIG) ─────────────────────

    /// Drive vendor from INQUIRY[8:16] (e.g. "HL-DT-ST")
    #[serde(default)]
    pub vendor_id: String,

    /// Drive product from INQUIRY[16:32] (e.g. "BD-RE BU40N")
    #[serde(default)]
    pub product_id: String,

    /// Firmware revision from INQUIRY[32:36] (e.g. "1.03")
    #[serde(default)]
    pub product_revision: String,

    /// Firmware type from INQUIRY[36:43] (e.g. "NM00000")
    #[serde(default)]
    pub vendor_specific: String,

    /// Firmware build date from GET_CONFIG 010C (e.g. "211810241934")
    #[serde(default)]
    pub firmware_date: String,

    // ── Platform variant ───────────────────────────────────────────────

    /// Chipset family: "mediatek" or "renesas".
    #[serde(default)]
    pub chipset: Chipset,

    /// Program variant: "mt1959_a" or "mt1959_b".
    /// Determines unlock mode/buf_id and handler layout.
    #[serde(default)]
    pub program: String,

    /// READ_BUFFER mode byte (0x01 for mt1959_a, 0x02 for mt1959_b).
    #[serde(default = "default_unlock_mode")]
    pub unlock_mode: u8,

    /// READ_BUFFER buffer ID (0x44 for mt1959_a, 0x77 for mt1959_b).
    #[serde(default = "default_unlock_buf_id")]
    pub unlock_buf_id: u8,


    /// Used with unlock_response_size_minus_init to compute response size.
    #[serde(default = "default_init_value")]
    pub unlock_init_value: u8,

    /// Response size = unlock_init_value + this (e.g. 1 + 63 = 64).
    #[serde(default = "default_response_size_minus_init")]
    pub unlock_response_size_minus_init: u8,

    /// Per-drive signature checked against unlock response[0:4].
    #[serde(default, deserialize_with = "deserialize_hex4")]
    pub drive_signature: [u8; 4],


    /// Uploaded on cold boot when unlock fails. ~1888 bytes typically.
    /// Contains volatile RAM-only runtime code for the drive's MediaTek SOC.
    #[serde(default, deserialize_with = "deserialize_base64")]
    pub ld_microcode: Vec<u8>,

    // ── Handlers 2/3: register reads ──────────────────────────────────

    #[serde(default, deserialize_with = "deserialize_hex_vec")]
    pub hardware_register_a_cdb: Vec<u8>,

    #[serde(default, deserialize_with = "deserialize_hex_vec")]
    pub hardware_register_b_cdb: Vec<u8>,


    /// Pre-built SET_CD_SPEED CDB with drive's nominal speed (12 bytes).
    /// Used in calibration "triple play": max → this → max.
    #[serde(default, deserialize_with = "deserialize_hex_vec")]
    pub drive_nominal_speed_cdb: Vec<u8>,


    /// for per-zone speed decisions. Per-drive calibration constants.
    #[serde(default, deserialize_with = "deserialize_hex_vec")]
    pub speed_zone_table: Vec<u8>,

    /// raw sector reads for speed math.
    #[serde(default, deserialize_with = "deserialize_hex_vec")]
    pub speed_calc_table: Vec<u8>,


    /// Drive supports reading DVDs regardless of region code.
    #[serde(default)]
    pub dvd_all_regions: bool,

    /// Drive supports raw Blu-ray sector reads.
    #[serde(default)]
    pub bd_raw_read: bool,

    /// Drive supports raw Blu-ray metadata reads.
    #[serde(default)]
    pub bd_raw_metadata: bool,

    /// Drive supports unrestricted read speed.
    #[serde(default)]
    pub unrestricted_speed: bool,

    // ── Metadata ──────────────────────────────────────────────────────

    #[serde(default)]
    pub drive_id: String,

    #[serde(default)]
    pub drive_version: String,
}

fn default_unlock_mode() -> u8 { 0x01 }
fn default_unlock_buf_id() -> u8 { 0x44 }
fn default_init_value() -> u8 { 1 }
fn default_response_size_minus_init() -> u8 { 0x3F }

/// Drive chipset family.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub enum Chipset {
    #[serde(rename = "mediatek")]
    MediaTek,
    #[serde(rename = "renesas")]
    Renesas,
}

impl Default for Chipset {
    fn default() -> Self { Chipset::MediaTek }
}

impl Chipset {
    pub fn name(&self) -> &'static str {
        match self {
            Chipset::MediaTek => "MediaTek MT1959",
            Chipset::Renesas => "Renesas",
        }
    }
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

fn parse_hex(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(Error::ProfileParse { detail: "odd hex length".into() });
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i+2], 16)
            .map_err(|e| Error::ProfileParse { detail: format!("bad hex: {e}") })?);
    }
    Ok(out)
}

fn deserialize_hex4<'de, D>(deserializer: D) -> std::result::Result<[u8; 4], D::Error>
where D: serde::Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    if s.is_empty() { return Ok([0; 4]); }
    parse_hex4(&s).map_err(serde::de::Error::custom)
}

fn deserialize_hex_vec<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where D: serde::Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    if s.is_empty() { return Ok(Vec::new()); }
    parse_hex(&s).map_err(serde::de::Error::custom)
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

/// Bundled profiles — compiled into the binary.
const BUNDLED_PROFILES: &str = include_str!("../profiles.json");

/// Load profiles from the bundled database.
pub fn load_bundled() -> Result<Vec<DriveProfile>> {
    load_from_str(BUNDLED_PROFILES)
}

/// Load all profiles from a JSON array file.
pub fn load_all(path: &std::path::Path) -> Result<Vec<DriveProfile>> {
    let data = std::fs::read_to_string(path)?;
    load_from_str(&data)
}

fn load_from_str(data: &str) -> Result<Vec<DriveProfile>> {
    let arr: Vec<DriveProfile> = serde_json::from_str(data)
        .map_err(|e| Error::ProfileParse { detail: format!("JSON: {e}") })?;
    Ok(arr)
}

/// Find a profile matching a drive's INQUIRY fields.
pub fn find_by_drive_id<'a>(
    profiles: &'a [DriveProfile],
    drive_id: &crate::identity::DriveId,
) -> Option<&'a DriveProfile> {
    let v = drive_id.vendor_id.trim();
    let r = drive_id.product_revision.trim();
    let vs = drive_id.vendor_specific.trim();

    // Match all four INQUIRY fields
    profiles.iter().find(|p| {
        p.vendor_id.trim() == v
            && p.product_revision.trim() == r
            && p.vendor_specific.trim() == vs
            && p.firmware_date.trim() == drive_id.firmware_date.trim()
    })
    // Fallback: match without date
    .or_else(|| profiles.iter().find(|p| {
        p.vendor_id.trim() == v
            && p.product_revision.trim() == r
            && p.vendor_specific.trim() == vs
    }))
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
        let p = find_by_drive_id(&profiles, &id).unwrap();
        assert_eq!(p.vendor_id.trim(), "HL-DT-ST");
        assert_eq!(p.vendor_specific.trim(), "NM00000");
    }

    #[test]
    fn test_find_unknown_drive() {
        let profiles = load_bundled().unwrap();
        let id = make_drive_id("FAKE-VND", "9.99", "XX12345", "");
        assert!(find_by_drive_id(&profiles, &id).is_none());
    }
}
