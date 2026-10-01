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
mod tests {
    use super::*;

    // ── Bit reader unit tests ──────────────────────────────────────────────
    #[test]
    fn bit_reader_reads_msb_first_and_bounds_checks() {
        let data = [0b1011_0010u8, 0b0100_0001];
        let mut r = BitReader::new(&data);
        assert_eq!(r.read(1), Some(1));
        assert_eq!(r.read(3), Some(0b011));
        assert_eq!(r.read(4), Some(0b0010));
        assert_eq!(r.read(8), Some(0b0100_0001));
        // Nothing left.
        assert_eq!(r.read(1), None);
        assert_eq!(r.read(0), Some(0));
    }

    #[test]
    fn bit_reader_skip_past_end_is_none() {
        let data = [0u8; 2];
        let mut r = BitReader::new(&data);
        assert_eq!(r.skip(16), Some(()));
        assert_eq!(r.skip(1), None);
    }

    // ── Fixture builder ─────────────────────────────────────────────────────
    // Byte-aligned BDJO builder for a single-app AMT. All bit fields here happen
    // to fall on byte boundaries, so the fixture can be assembled from bytes.
    struct AppSpec {
        control_code: u8,
        base_dir: &'static str,
        classpath_extension: &'static str,
        initial_class: &'static str,
    }

    // Optional content the minimal fixture leaves empty, laid out per the BD-J
    // Object (BDJO) file format: cache items, accessible playlists, and
    // per-app profiles / application_name bytes / parameter bytes.
    #[derive(Default)]
    struct Extras {
        cache_items: usize,
        playlists: usize,
        profiles: usize,
        name_bytes: usize,
        param_bytes: usize,
    }

    fn push_app_string(buf: &mut Vec<u8>, s: &str) {
        buf.push(s.len() as u8);
        buf.extend_from_slice(s.as_bytes());
        if s.len().is_multiple_of(2) {
            buf.push(0); // word-align pad on even length
        }
    }

    fn build_bdjo(apps: &[AppSpec]) -> Vec<u8> {
        build_bdjo_with(apps, &Extras::default())
    }

    fn build_bdjo_with(apps: &[AppSpec], x: &Extras) -> Vec<u8> {
        let mut b = Vec::new();
        // Header
        b.extend_from_slice(b"BDJO");
        b.extend_from_slice(b"0200");
        b.extend_from_slice(&[0u8; 40]); // section-address table

        // TerminalInfo: length(4) + default_font(5) + 1 byte (havi+masks) + pad(34 bits)
        b.extend_from_slice(&[0u8; 4]);
        b.extend_from_slice(&[0u8; 5]);
        // 4+1+1 = 6 bits then 34 bits padding = 40 bits = 5 bytes total.
        b.extend_from_slice(&[0u8; 5]);

        // AppCacheInfo: length(4) + num_item(1) + pad(1), then 12-byte items:
        // type(1) + ref_to_name(5) + lang_code(3) + pad(3).
        b.extend_from_slice(&[0u8; 4]);
        b.push(x.cache_items as u8);
        b.push(0);
        for _ in 0..x.cache_items {
            b.push(1);
            b.extend_from_slice(b"00007eng");
            b.extend_from_slice(&[0u8; 3]);
        }

        // AccessiblePlaylists: length(4) + [num_pl(11)+flags(2)+pad(19)], then
        // 6-byte entries: name(5) + pad(1).
        b.extend_from_slice(&[0u8; 4]);
        b.extend_from_slice(&(((x.playlists as u32) << 21) | (1 << 20)).to_be_bytes());
        for _ in 0..x.playlists {
            b.extend_from_slice(b"00800\0");
        }

        // AppManagementTable: length(4) + num_app(1) + pad(1)
        b.extend_from_slice(&[0u8; 4]);
        b.push(apps.len() as u8);
        b.push(0);

        for a in apps {
            b.push(a.control_code); // control_code(8)
            b.push(0); // type(4)+reserved(4)
            b.extend_from_slice(&[0u8; 4]); // org_id(32)
            b.extend_from_slice(&[0u8; 2]); // app_id(16)
            b.extend_from_slice(&[0u8; 10]); // descriptor tag+length(80)
            // num_profile(4) + pad(12), then 6-byte profiles:
            // profile(2) + major(1) + minor(1) + micro(1) + pad(1).
            b.push((x.profiles as u8) << 4);
            b.push(0);
            for _ in 0..x.profiles {
                b.extend_from_slice(&[0, 1, 1, 0, 0, 0]);
            }
            b.push(0); // priority(8)
            b.push(0); // binding(2)+visibility(2)+reserved(4)
            // application_name: data_length(16) + bytes, word-aligned (pad when odd).
            b.extend_from_slice(&(x.name_bytes as u16).to_be_bytes());
            b.extend(std::iter::repeat_n(b'n', x.name_bytes));
            if !x.name_bytes.is_multiple_of(2) {
                b.push(0);
            }
            // icon_locator (empty word-aligned string): len=0 + pad
            push_app_string(&mut b, "");
            b.extend_from_slice(&[0u8; 2]); // icon_flags(16)
            push_app_string(&mut b, a.base_dir);
            push_app_string(&mut b, a.classpath_extension);
            push_app_string(&mut b, a.initial_class);
            // application_parameters: data_length(8) + bytes, word-aligned (pad when even).
            b.push(x.param_bytes as u8);
            b.extend(std::iter::repeat_n(b'p', x.param_bytes));
            if x.param_bytes.is_multiple_of(2) {
                b.push(0);
            }
        }
        b
    }

    // Every skipped-by-width region populated (odd and even name/param lengths):
    // a wrong item/profile/padding width misaligns the strings that follow.
    #[test]
    fn parses_apps_with_cache_items_playlists_profiles_names_and_params() {
        for (name_bytes, param_bytes) in [(5, 3), (6, 4)] {
            let x = Extras {
                cache_items: 2,
                playlists: 3,
                profiles: 2,
                name_bytes,
                param_bytes,
            };
            let bytes = build_bdjo_with(
                &[
                    AppSpec {
                        control_code: 2,
                        base_dir: "00009",
                        classpath_extension: "",
                        initial_class: "com.studio.Helper",
                    },
                    AppSpec {
                        control_code: 1,
                        base_dir: "00000",
                        classpath_extension: "00001",
                        initial_class: "com.studio.MainXlet",
                    },
                ],
                &x,
            );
            let apps = parse(&bytes).expect("parses");
            assert_eq!(apps.len(), 2);
            assert_eq!(apps[0].initial_class, "com.studio.Helper");
            assert_eq!(apps[1].jar_ids(), vec!["00000", "00001"]);
            assert_eq!(apps[1].initial_class, "com.studio.MainXlet");
        }
    }

    #[test]
    fn app_strings_drop_trailing_nul_padding() {
        let bytes = build_bdjo(&[AppSpec {
            control_code: 1,
            base_dir: "0000\0",
            classpath_extension: "",
            initial_class: "a.B\0\0",
        }]);
        let apps = parse(&bytes).expect("parses");
        assert_eq!(apps[0].base_directory, "0000");
        assert_eq!(apps[0].initial_class, "a.B");
        assert_eq!(apps[0].jar_ids(), vec!["0000"]);
    }

    #[test]
    fn parses_autostart_app_fqcn_and_jar_ids() {
        let bytes = build_bdjo(&[AppSpec {
            control_code: 1,
            base_dir: "00000",
            classpath_extension: "00001;00002",
            initial_class: "com.foxbd.StandardMenuXlet",
        }]);
        let apps = parse(&bytes).expect("parses");
        assert_eq!(apps.len(), 1);
        let a = &apps[0];
        assert!(a.is_autostart());
        assert_eq!(a.initial_class, "com.foxbd.StandardMenuXlet");
        assert_eq!(a.base_directory, "00000");
        assert_eq!(a.jar_ids(), vec!["00000", "00001", "00002"]);
    }

    #[test]
    fn parses_multiple_apps_and_finds_the_autostart_one() {
        let bytes = build_bdjo(&[
            AppSpec {
                control_code: 2, // PRESENT, not autostart
                base_dir: "00009",
                classpath_extension: "",
                initial_class: "com.studio.Helper",
            },
            AppSpec {
                control_code: 1, // AUTOSTART
                base_dir: "00000",
                classpath_extension: "",
                initial_class: "com.studio.MainXlet",
            },
        ]);
        let apps = parse(&bytes).expect("parses");
        assert_eq!(apps.len(), 2);
        let auto: Vec<&BdjoApp> = apps.iter().filter(|a| a.is_autostart()).collect();
        assert_eq!(auto.len(), 1);
        assert_eq!(auto[0].initial_class, "com.studio.MainXlet");
        assert_eq!(auto[0].jar_ids(), vec!["00000"]);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = build_bdjo(&[AppSpec {
            control_code: 1,
            base_dir: "00000",
            classpath_extension: "",
            initial_class: "X",
        }]);
        bytes[0..4].copy_from_slice(b"NOPE");
        assert!(parse(&bytes).is_none());
    }

    #[test]
    fn truncation_yields_none_never_panics() {
        let bytes = build_bdjo(&[AppSpec {
            control_code: 1,
            base_dir: "00000",
            classpath_extension: "",
            initial_class: "com.studio.MainXlet",
        }]);
        // Every truncation length must return None, never panic or a partial Some.
        for cut in 0..bytes.len() {
            assert_eq!(parse(&bytes[..cut]), None, "cut={cut}");
        }
    }

    #[test]
    fn parse_never_panics_on_random_bytes() {
        let mut state: u64 = 0xB0D0_DEAD_BEEF_1234u64;
        for _ in 0..500 {
            // xorshift64*
            let mut x = state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            state = x;
            let len = (x % 300) as usize;
            let mut buf = vec![0u8; len];
            for (i, b) in buf.iter_mut().enumerate() {
                *b = ((x >> (i % 57)) & 0xFF) as u8;
            }
            // Give some of them a valid magic to exercise deeper paths.
            if len >= 4 && x & 1 == 0 {
                buf[0..4].copy_from_slice(b"BDJO");
            }
            let _ = parse(&buf);
        }
    }

    #[test]
    fn jar_ids_dedups_and_skips_empties() {
        let a = BdjoApp {
            control_code: 1,
            base_directory: "00000".into(),
            classpath_extension: "00000;;00003; ".into(),
            initial_class: "X".into(),
        };
        assert_eq!(a.jar_ids(), vec!["00000", "00003"]);
    }
}
