//! Drive profile loading and matching.
//!
//! Each supported drive has a profile containing the SCSI command
//! parameters needed to enable raw disc access mode. Profiles are
//! loaded from JSON files so new drives can be added without rebuilding.

use serde::Deserialize;
use crate::error::{Error, Result};

/// Per-drive profile containing SCSI parameters for raw disc access.
#[derive(Debug, Clone, Deserialize)]
pub struct DriveProfile {
    /// Drive vendor from INQUIRY[8:16] (e.g. "HL-DT-ST")
    #[serde(default)]
    pub vendor_id: String,

    /// Drive product (devtype) from INQUIRY product field (e.g. "BD-RE")
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

    /// Chipset manufacturer determining unlock/read command structure.
    #[serde(default)]
    pub chipset: Chipset,

    /// READ BUFFER mode byte for unlock CDB (e.g. 0x01 for MT1959-A, 0x02 for MT1959-B).
    #[serde(default = "default_unlock_mode")]
    pub unlock_mode: u8,

    /// READ BUFFER buffer ID for unlock CDB (e.g. 0x44 for MT1959-A, 0x77 for MT1959-B).
    #[serde(default = "default_unlock_buf_id")]
    pub unlock_buf_id: u8,


    /// Drive identifier string from the profile database.
    #[serde(default)]
    pub drive_id: String,

    /// Profile version string.
    #[serde(default)]
    pub drive_version: String,

    /// Expected response signature bytes [0:4] from the enable command.
    #[serde(default, deserialize_with = "deserialize_hex4")]
    pub signature: [u8; 4],

    /// Expected verification bytes [12:16] from the enable response.
    #[serde(skip, default = "default_verify")]
    pub verify: [u8; 4],

    /// 10-byte READ BUFFER CDB used to enable raw disc access.
    #[serde(default, deserialize_with = "deserialize_hex_vec")]
    pub unlock_cdb: Vec<u8>,

    /// Register read offsets (bytes 3-5 of READ BUFFER CDB).
    #[serde(default)]
    pub register_offsets: Vec<u32>,

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
}

fn default_verify() -> [u8; 4] {
    *b"MMkv"
}

fn default_unlock_mode() -> u8 {
    0x01
}

fn default_unlock_buf_id() -> u8 {
    0x44
}

/// Drive chipset — determines CDB structure for unlock and raw read commands.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub enum Chipset {
    /// MediaTek MT1959 — LG, ASUS, hp drives.
    /// CDB: READ_BUFFER with mode and buf_id from profile.
    #[serde(rename = "mediatek")]
    MediaTek,
    /// Renesas RS8xxx/RS9xxx — Pioneer, some HL-DT-ST drives.
    /// Not yet implemented.
    #[serde(rename = "renesas")]
    Renesas,
}

impl Default for Chipset {
    fn default() -> Self {
        Chipset::MediaTek
    }
}

impl Chipset {
    /// Human-readable name for this chipset.
    pub fn name(&self) -> &'static str {
        match self {
            Chipset::MediaTek => "MediaTek MT1959",
            Chipset::Renesas => "Renesas",
        }
    }
}

/// Parse a hex string like "999ec375" into [u8; 4].
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

/// Parse a hex string into a byte vector.
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

/// Custom serde deserializer for 4-byte hex signature strings.
fn deserialize_hex4<'de, D>(deserializer: D) -> std::result::Result<[u8; 4], D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_hex4(&s).map_err(serde::de::Error::custom)
}

/// Custom serde deserializer for hex-encoded byte vectors.
fn deserialize_hex_vec<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_hex(&s).map_err(serde::de::Error::custom)
}

/// Load a profile from a parsed JSON value.
pub fn load_from_json(json: &serde_json::Value) -> Result<DriveProfile> {
    let vendor = json["vendor_id"].as_str().unwrap_or("").to_string();
    let product = json["product_id"].as_str().unwrap_or("").to_string();
    let revision = json["product_revision"].as_str().unwrap_or("").to_string();
    let firmware_type = json["vendor_specific"].as_str().unwrap_or("").to_string();
    let firmware_date = json["firmware_date"].as_str().unwrap_or("").to_string();
    let chipset_str = json["chipset"].as_str().unwrap_or("unknown");

    let chipset = match chipset_str {
        "mediatek" => Chipset::MediaTek,
        "renesas" => Chipset::Renesas,
        _ => Chipset::MediaTek,
    };

    let unlock_mode = json["unlock_mode"].as_u64().map(|v| v as u8).unwrap_or(0x01);
    let unlock_buf_id = json["unlock_buf_id"].as_u64().map(|v| v as u8).unwrap_or(0x44);

    let sig_str = json["signature"].as_str().unwrap_or("");

    let signature = if sig_str.len() == 8 {
        parse_hex4(sig_str)?
    } else {
        [0; 4]
    };

    let unlock_cdb = json["unlock_cdb"].as_str()
        .map(|s| parse_hex(s))
        .transpose()?
        .unwrap_or_default();

    let register_offsets = json["register_cdbs"].as_array()
        .map(|arr| {
            arr.iter().filter_map(|v| {
                let s = v.as_str()?;
                // CDB format: 3c 01 44 XX XX XX 00 00 24 00
                // Register offset is bytes 3-5 (chars 6-12 in hex)
                if s.len() >= 12 {
                    u32::from_str_radix(&s[6..12], 16).ok()
                } else {
                    None
                }
            }).collect()
        })
        .unwrap_or_default();

    Ok(DriveProfile {
        vendor_id: vendor,
        product_id: product,
        product_revision: revision,
        vendor_specific: firmware_type,
        firmware_date,
        chipset,
        unlock_mode,
        unlock_buf_id,
        drive_id: json["drive_id"].as_str().unwrap_or("").to_string(),
        drive_version: json["drive_version"].as_str().unwrap_or("").to_string(),
        signature,
        verify: *b"MMkv",
        unlock_cdb,
        register_offsets,
        dvd_all_regions: json["capabilities"]["dvd_all_regions"].as_bool().unwrap_or(false),
        bd_raw_read: json["capabilities"]["bd_raw_read"].as_bool().unwrap_or(false),
        bd_raw_metadata: json["capabilities"]["bd_raw_metadata"].as_bool().unwrap_or(false),
        unrestricted_speed: json["capabilities"]["unrestricted_speed"].as_bool().unwrap_or(false),
    })
}

/// Bundled profiles — compiled into the binary.
/// Override with load_all() to load from a file instead.
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

/// Parse profiles from a JSON string.
fn load_from_str(data: &str) -> Result<Vec<DriveProfile>> {
    let json: serde_json::Value = serde_json::from_str(data)
        .map_err(|e| Error::ProfileParse { detail: format!("JSON: {e}") })?;

    let arr = json.as_array()
        .ok_or_else(|| Error::ProfileParse { detail: "expected array".into() })?;

    let mut profiles = Vec::with_capacity(arr.len());
    for entry in arr {
        match load_from_json(entry) {
            Ok(p) => profiles.push(p),
            Err(_) => continue, // skip malformed entries
        }
    }
    Ok(profiles)
}

/// Find a profile matching a drive's INQUIRY fields.
///
/// Matches by vendor + product + revision + vendor_specific (firmware type).
/// All fields trimmed before comparison.
pub fn find_by_drive_id<'a>(
    profiles: &'a [DriveProfile],
    drive_id: &crate::identity::DriveId,
) -> Option<&'a DriveProfile> {
    let v = drive_id.vendor_id.trim();
    let r = drive_id.product_revision.trim();
    let vs = drive_id.vendor_specific.trim();

    // Match all four INQUIRY fields for precise identification
    profiles.iter().find(|p| {
        p.vendor_id.trim() == v
            && p.product_revision.trim() == r
            && p.vendor_specific.trim() == vs
            && p.firmware_date.trim() == drive_id.firmware_date.trim()
    })
    // Fallback: match without date (for drives where 010C isn't available)
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
