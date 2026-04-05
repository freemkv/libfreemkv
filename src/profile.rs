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

    /// Chipset platform type determining the READ BUFFER variant.
    #[serde(default)]
    pub platform: PlatformType,

    /// Whether this drive supports raw disc access mode.
    #[serde(default)]
    pub supported: bool,

    /// Current readiness status of this drive.
    #[serde(default)]
    pub status: ReadinessStatus,

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

/// Chipset platform type. Determines the READ BUFFER mode and buffer ID.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub enum PlatformType {
    /// MediaTek MT1959 variant A: mode=0x01, buffer_id=0x44.
    #[serde(rename = "mt1959_a")]
    Mt1959A,
    /// MediaTek MT1959 variant B: mode=0x02, buffer_id=0x77.
    #[serde(rename = "mt1959_b")]
    Mt1959B,
    /// Pioneer chipset (not yet implemented).
    #[serde(rename = "pioneer")]
    Pioneer,
}

impl Default for PlatformType {
    fn default() -> Self {
        PlatformType::Mt1959A
    }
}

/// Readiness status of a drive for raw disc access.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub enum ReadinessStatus {
    /// Drive is ready — raw disc access can be enabled.
    Ready,
    /// Drive firmware needs an update before raw access is possible.
    NeedsFirmwareUpdate,
    /// Drive uses encrypted commands (not yet supported).
    Encrypted,
    /// Status unknown.
    Unknown,
}

impl Default for ReadinessStatus {
    fn default() -> Self {
        ReadinessStatus::Unknown
    }
}

impl PlatformType {
    /// Human-readable name for this platform.
    pub fn name(&self) -> &'static str {
        match self {
            PlatformType::Mt1959A => "MT1959-A",
            PlatformType::Mt1959B => "MT1959-B",
            PlatformType::Pioneer => "Pioneer",
        }
    }

    /// READ BUFFER mode byte for this chipset platform.
    pub fn mode(&self) -> u8 {
        match self {
            PlatformType::Mt1959A => 0x01,
            PlatformType::Mt1959B => 0x02,
            PlatformType::Pioneer => 0x01, // TBD
        }
    }

    /// READ BUFFER buffer ID for this chipset platform.
    pub fn buffer_id(&self) -> u8 {
        match self {
            PlatformType::Mt1959A => 0x44,
            PlatformType::Mt1959B => 0x77,
            PlatformType::Pioneer => 0x44, // TBD
        }
    }
}

/// Parse a hex string like "999ec375" into [u8; 4].
fn parse_hex4(s: &str) -> Result<[u8; 4]> {
    if s.len() != 8 {
        return Err(Error::ProfileParse(format!("expected 8 hex chars, got {}", s.len())));
    }
    let mut out = [0u8; 4];
    for i in 0..4 {
        out[i] = u8::from_str_radix(&s[i*2..i*2+2], 16)
            .map_err(|e| Error::ProfileParse(format!("bad hex: {e}")))?;
    }
    Ok(out)
}

/// Parse a hex string into a byte vector.
fn parse_hex(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(Error::ProfileParse("odd hex length".into()));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i+2], 16)
            .map_err(|e| Error::ProfileParse(format!("bad hex: {e}")))?);
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
    let program = json["program"].as_str().unwrap_or("unknown");

    let platform = match program {
        "mt1959_a" => PlatformType::Mt1959A,
        "mt1959_b" => PlatformType::Mt1959B,
        _ => PlatformType::Mt1959A, // default
    };

    let sig_str = json["signature"].as_str().unwrap_or("");

    // Drive is supported if it has a known program and valid signature
    let has_program = matches!(program, "mt1959_a" | "mt1959_b");
    let has_signature = sig_str.len() == 8;
    let supported = has_program && has_signature;

    let status = if supported {
        ReadinessStatus::Ready
    } else if json["status"].as_str() == Some("needs_flash") || program == "none" {
        ReadinessStatus::NeedsFirmwareUpdate
    } else {
        ReadinessStatus::Unknown
    };
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
        platform,
        supported,
        status,
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
        .map_err(|e| Error::ProfileParse(format!("JSON: {e}")))?;

    let arr = json.as_array()
        .ok_or_else(|| Error::ProfileParse("expected array".into()))?;

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
