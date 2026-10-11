//! Minimal, read-only parser for `/BDMV/BDJO/*.bdjo` — the BD-J Object files.
//!
//! Only the Application Management Table (AMT) is decoded, and only the four
//! fields the issue #45 Tier-2 menu-walk needs from each application record:
//! its `application_control_code` (1 = AUTOSTART), `base_directory`,
//! `classpath_extension`, and `initial_class` (the Xlet's fully-qualified class
//! name). Everything else in the file is skipped by width.
//!
//! Layout follows the published BD-J Object (BDJO) file format: a fixed 48-byte
//! header (8-byte magic+version, then a 40-byte section-address table that real
//! players skip), followed by the terminal-info, app-cache-info,
//! accessible-playlists, and application-management-table sections in sequence.
//!
//! This is a DOCUMENTED BINARY FORMAT read as data, never executed. Every field
//! is bounds-checked and any malformed input yields `None` — panic-free like
//! [`crate::bdnav::index`]. Tier-2 falls back to scanning every jar when this
//! returns `None`.

/// One application record from a BDJO Application Management Table — only the
/// fields the menu-walk consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BdjoApp {
    /// `application_control_code`: 1 = AUTOSTART, 2 = PRESENT.
    pub control_code: u8,
    /// `base_directory` — the primary jar id (e.g. "00000"), naming
    /// `/BDMV/JAR/<base_directory>.jar`.
    pub base_directory: String,
    /// `classpath_extension` — additional `;`-separated jar ids on the
    /// classpath, or empty.
    pub classpath_extension: String,
    /// `initial_class` — the autostart Xlet's fully-qualified class name
    /// (e.g. "com.foxbd.StandardMenuXlet").
    pub initial_class: String,
}

impl BdjoApp {
    /// Whether this application autostarts (the one whose Xlet drives the disc).
    pub fn is_autostart(&self) -> bool {
        self.control_code == AUTOSTART
    }

    /// The jar ids that make up this app's classpath: `base_directory` first,
    /// then each `;`-separated `classpath_extension` entry. Empty tokens are
    /// dropped. These name `/BDMV/JAR/<id>.jar`.
    pub fn jar_ids(&self) -> Vec<String> {
        let mut out = Vec::new();
        let base = self.base_directory.trim();
        if !base.is_empty() {
            out.push(base.to_string());
        }
        for part in self.classpath_extension.split(';') {
            let part = part.trim();
            if !part.is_empty() && !out.iter().any(|e| e == part) {
                out.push(part.to_string());
            }
        }
        out
    }
}

const AUTOSTART: u8 = 1;

// "BDJO" magic. The 4-byte version that follows ("0100"/"0200"/"0240"/"0300")
// is read but not validated — a future revision must still let Tier-2 fall back
// gracefully rather than mis-reject a readable table.
const MAGIC: u32 = u32::from_be_bytes(*b"BDJO");

/// Parse a `.bdjo` file's Application Management Table. Returns the list of
/// application records (in file order), or `None` on any structural problem.
pub(crate) fn parse(data: &[u8]) -> Option<Vec<BdjoApp>> {
    let mut r = BitReader::new(data);

    // ── Header ──────────────────────────────────────────────────────────────
    if r.read(32)? as u32 != MAGIC {
        return None;
    }
    r.skip(32)?; // version_number (not load-bearing for resolution)
    r.skip(40 * 8)?; // section-address table (players read sections sequentially)

    // ── TerminalInfo ────────────────────────────────────────────────────────
    r.skip(32)?; // length
    r.skip(5 * 8)?; // default_font (5 bytes)
    r.skip(4 + 1 + 1)?; // initial_havi_config_id + menu_call_mask + title_search_mask
    r.skip(34)?; // padding

    // ── AppCacheInfo ──────────────────────────────────────────────────────────
    r.skip(32)?; // length
    let num_item = r.read(8)? as usize;
    r.skip(8)?; // padding
    for _ in 0..num_item {
        r.skip(12 * 8)?; // each item: type(1) + ref_to_name(5) + lang_code(3) + pad(3)
    }

    // ── AccessiblePlaylists ───────────────────────────────────────────────────
    r.skip(32)?; // length
    let num_pl = r.read(11)? as usize;
    r.skip(1 + 1)?; // access_to_all_flag + autostart_first_playlist_flag
    r.skip(19)?; // padding
    for _ in 0..num_pl {
        r.skip(6 * 8)?; // each: name(5) + pad(1)
    }

    // ── ApplicationManagementTable ────────────────────────────────────────────
    r.skip(32)?; // length
    let num_app = r.read(8)? as usize;
    r.skip(8)?; // padding
    // num_app is a u8 read, so it is bounded at 255 by construction.
    let mut apps = Vec::with_capacity(num_app);
    for _ in 0..num_app {
        apps.push(parse_app(&mut r)?);
    }
    Some(apps)
}

// Parse one Application() record from the Application Management Table.
fn parse_app(r: &mut BitReader<'_>) -> Option<BdjoApp> {
    let control_code = r.read(8)? as u8;
    r.skip(4)?; // application_type
    r.skip(4)?; // reserved
    r.skip(32)?; // organization_id
    r.skip(16)?; // application_id
    r.skip(80)?; // descriptor tag + length (10 bytes)

    // application descriptor
    let num_profile = r.read(4)? as usize;
    r.skip(12)?; // padding
    for _ in 0..num_profile {
        r.skip(6 * 8)?; // profile_number(2) + major/minor/micro(3) + pad(1)
    }

    r.skip(8)?; // application_priority
    r.skip(2 + 2 + 4)?; // binding + visibility + reserved

    // application_name section: u16 data_length, then that many bytes, word-aligned.
    let names_len = r.read(16)? as usize;
    r.skip(names_len * 8)?;
    if !names_len.is_multiple_of(2) {
        r.skip(8)?;
    }

    // icon_locator (word-aligned string), then icon_flags(16).
    read_app_string(r)?;
    r.skip(16)?;

    let base_directory = read_app_string(r)?;
    let classpath_extension = read_app_string(r)?;
    let initial_class = read_app_string(r)?;

    // application_parameters: u8 data_length, that many bytes, word-aligned.
    let params_len = r.read(8)? as usize;
    r.skip(params_len * 8)?;
    if params_len.is_multiple_of(2) {
        r.skip(8)?;
    }

    Some(BdjoApp {
        control_code,
        base_directory,
        classpath_extension,
        initial_class,
    })
}

// A word-aligned length-prefixed string (`_read_app_string`): u8 length, then
// `length` bytes, then one pad byte when `length` is EVEN. Bytes are Latin-1
// decoded (FQCNs/jar ids are ASCII); trailing NULs are trimmed.
fn read_app_string(r: &mut BitReader<'_>) -> Option<String> {
    let len = r.read(8)? as usize;
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        s.push(r.read(8)? as u8 as char);
    }
    if len.is_multiple_of(2) {
        r.skip(8)?;
    }
    Some(s.trim_end_matches('\0').to_string())
}

// ── MSB-first bit reader ─────────────────────────────────────────────────────
// BDJO integers are bit-packed and consumed most-significant-first.
// Every read/skip is bounds-checked and returns `None` past the end.
struct BitReader<'a> {
    data: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        BitReader { data, bit_pos: 0 }
    }

    fn total_bits(&self) -> usize {
        self.data.len() * 8
    }

    // Read up to 64 bits, MSB-first. `None` if fewer than `n` bits remain.
    fn read(&mut self, n: u32) -> Option<u64> {
        if n == 0 {
            return Some(0);
        }
        if n > 64 {
            return None;
        }
        let end = self.bit_pos.checked_add(n as usize)?;
        if end > self.total_bits() {
            return None;
        }
        let mut v: u64 = 0;
        for _ in 0..n {
            let byte = self.data[self.bit_pos >> 3];
            let bit = (byte >> (7 - (self.bit_pos & 7))) & 1;
            v = (v << 1) | bit as u64;
            self.bit_pos += 1;
        }
        Some(v)
    }

    // Advance `n` bits. `None` if that would run past the end.
    fn skip(&mut self, n: usize) -> Option<()> {
        let end = self.bit_pos.checked_add(n)?;
        if end > self.total_bits() {
            return None;
        }
        self.bit_pos = end;
        Some(())
    }
}

#[cfg(test)]
#[path = "bdjo_tests.rs"]
mod tests;
