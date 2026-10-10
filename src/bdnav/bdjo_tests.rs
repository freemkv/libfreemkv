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
        assert_eq!(apps[0].parameters, vec![b'p'; param_bytes]);
        assert_eq!(apps[1].parameters, vec![b'p'; param_bytes]);
    }
}

#[test]
fn preserves_parameter_bytes_and_rejects_a_truncated_parameter_block() {
    let mut bytes = build_bdjo_with(
        &[AppSpec {
            control_code: 1,
            base_dir: "00002",
            classpath_extension: "",
            initial_class: "Main",
        }],
        &Extras {
            param_bytes: 4,
            ..Default::default()
        },
    );
    let end = bytes.len() - 1; // final alignment byte
    bytes[end - 4..end].copy_from_slice(&[0, 0xff, 0x80, b'=']);
    assert_eq!(parse(&bytes).unwrap()[0].parameters, [0, 0xff, 0x80, b'=']);
    for cut in 0..bytes.len() {
        assert!(
            parse(&bytes[..cut]).is_none(),
            "accepted truncation at {cut}"
        );
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
        parameters: Vec::new(),
    };
    assert_eq!(a.jar_ids(), vec!["00000", "00003"]);
}
