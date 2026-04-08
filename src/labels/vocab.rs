//! Shared label vocabulary — values we are 100% confident about.
//!
//! Labels come from BD-J authoring tool files (bluray_project.bin,
//! playlists.xml, menu_base.prop, etc.) — NOT from BD spec fields.
//! This is NOT for MPLS/CLPI/STN data. Those follow the BD spec directly.
//!
//! Rules:
//! - Only map values we are 100% certain about (published codec names).
//! - Unknown codes (csp, eda, cf, etc.) pass through raw from disc.
//! - The app/CLI handles display text, not the lib.

/// Map a codec identifier found in label data to its display name.
///
/// These are well-known codec identifiers used across multiple BD-J
/// authoring tools. NOT BD spec STN codec IDs — those are decoded
/// separately in mpls.rs.
pub fn codec(code: &str) -> &str {
    match code {
        "MLP" => "TrueHD",
        "AC3" | "AC" => "Dolby Digital",
        "DTS" => "DTS",
        "DDL" => "Dolby Digital Plus",
        "WAV" => "PCM",
        "atmos" => "Dolby Atmos",
        _ => code,
    }
}
