use super::*;
use std::sync::{Arc, Mutex};

// Instrumented SectorSource: records reads, capacity, and set_speed
// calls so the forwarding-impl tests can prove each trait method is
// delegated, not stubbed.
struct Spy {
    capacity: u32,
    reads: Arc<Mutex<Vec<(u32, u16, bool)>>>,
    speeds: Arc<Mutex<Vec<u16>>>,
    unit_bases: Arc<Mutex<Vec<u32>>>,
}

/// A `Spy` under test plus the handles recording its reads, speed sets,
/// and unit-base sets.
type SpyHarness = (
    Spy,
    Arc<Mutex<Vec<(u32, u16, bool)>>>,
    Arc<Mutex<Vec<u16>>>,
    Arc<Mutex<Vec<u32>>>,
);

impl Spy {
    fn new(capacity: u32) -> SpyHarness {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let speeds = Arc::new(Mutex::new(Vec::new()));
        let unit_bases = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                capacity,
                reads: reads.clone(),
                speeds: speeds.clone(),
                unit_bases: unit_bases.clone(),
            },
            reads,
            speeds,
            unit_bases,
        )
    }
}

impl SectorSource for Spy {
    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.reads.lock().unwrap().push((lba, count, recovery));
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0xa5);
        Ok(bytes)
    }
    fn set_speed(&mut self, kbs: u16) {
        self.speeds.lock().unwrap().push(kbs);
    }
    fn set_unit_base(&mut self, lba: u32) {
        self.unit_bases.lock().unwrap().push(lba);
    }
}

// Routes through a generic `S: SectorSource` bound so the forwarding impls (not the vtable)
// are exercised.
fn set_unit_base_generic<S: SectorSource>(mut s: S, base: u32) {
    s.set_unit_base(base);
}

// FUA and random-access answers must cross the forwarding impls, or Pass-N recovery
// reads lose Force Unit Access and a prefetcher claims random access.
struct FuaSpy(Arc<Mutex<Vec<bool>>>);
impl SectorSource for FuaSpy {
    fn capacity_sectors(&self) -> u32 {
        0
    }
    fn read_sectors(&mut self, _: u32, _: u16, _: &mut [u8], _: bool) -> Result<usize> {
        self.0.lock().unwrap().push(false);
        Ok(0)
    }
    fn read_sectors_fua(
        &mut self,
        _: u32,
        _: u16,
        _: &mut [u8],
        _: bool,
        fua: bool,
    ) -> Result<usize> {
        self.0.lock().unwrap().push(fua);
        Ok(0)
    }
    fn random_access(&self) -> bool {
        false
    }
}

fn fua_through<S: SectorSource>(mut s: S) -> bool {
    s.read_sectors_fua(0, 1, &mut [0u8; 2048], false, true)
        .unwrap();
    s.random_access()
}

#[test]
fn forwarding_impls_keep_fua_and_random_access() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let boxed: Box<dyn SectorSource> = Box::new(FuaSpy(seen.clone()));
    assert!(!fua_through(boxed));
    let mut spy = FuaSpy(seen.clone());
    let by_ref: &mut dyn SectorSource = &mut spy;
    assert!(!fua_through(by_ref));
    assert_eq!(*seen.lock().unwrap(), vec![true, true]);
}

// Leaf sources with no bus stage: image bytes on file are already bus-clear.
const UNMAPPED_LEAVES: &[&str] = &["FileSectorSource", "DirImage"];

// Byte offset where `s`'s first inline `#[cfg(test)] mod x {` starts (tests follow).
fn test_module_start(s: &str) -> usize {
    let mut from = 0;
    while let Some(i) = s[from..].find("#[cfg(test)]") {
        let at = from + i;
        let rest = s[at + 12..]
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty() && !l.starts_with("#["))
            .unwrap_or("");
        let rest = rest.strip_prefix("pub(crate) ").unwrap_or(rest);
        if rest.starts_with("mod ") && rest.ends_with('{') {
            return at;
        }
        from = at + 12;
    }
    s.len()
}

// Every production `impl SectorSource for X`, with whether its body forwards the list.
fn production_impls(s: &str) -> Vec<(String, bool)> {
    let s = &s[..test_module_start(s)];
    let mut out = Vec::new();
    for (at, _) in s.match_indices("SectorSource for ") {
        let line_start = s[..at].rfind('\n').map_or(0, |i| i + 1);
        if !s[line_start..at].trim_start().starts_with("impl") {
            continue;
        }
        let open = at + s[at..].find('{').expect("impl body");
        let name = s[at + 17..open].trim().to_string();
        let (mut depth, mut end) = (0i32, open);
        for (i, c) in s[open..].char_indices() {
            depth += match c {
                '{' => 1,
                '}' => -1,
                _ => 0,
            };
            if depth == 0 {
                end = open + i;
                break;
            }
        }
        out.push((name, s[open..end].contains("fn unmapped_stream_files")));
    }
    out
}

fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs")
                // Test code: `*tests*.rs` and the `test-util` fixtures (never in a release).
                && !p.file_name().unwrap().to_string_lossy().contains("tests")
                && p.file_name().is_some_and(|n| n != "test_util.rs")
        {
            out.push(p);
        }
    }
}

// A wrapper that keeps the trait default (`&[]`) silently drops the drive's unmapped
// stream files, reopening iso:// to bus-encrypted bytes. New source: forward, or list
// it in UNMAPPED_LEAVES with the reason.
#[test]
fn every_production_sector_source_forwards_unmapped_stream_files() {
    let mut files = Vec::new();
    rust_files(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let mut seen = Vec::new();
    let mut missing = Vec::new();
    for f in &files {
        for (name, forwards) in production_impls(&std::fs::read_to_string(f).unwrap()) {
            let base = name.split(['<', ' ']).next().unwrap_or("").to_string();
            if !forwards && !UNMAPPED_LEAVES.contains(&base.as_str()) {
                missing.push(format!("{} ({})", name, f.display()));
            }
            seen.push(base);
        }
    }
    assert!(
        missing.is_empty(),
        "do not forward unmapped_stream_files: {missing:?}"
    );
    for must in [
        "Drive",
        "DecryptingSectorSource",
        "UnitAligned",
        "PrefetchedSectorSource",
    ] {
        assert!(
            seen.iter().any(|n| n == must),
            "scanner lost {must}: {seen:?}"
        );
    }
}

// The scanner itself: a non-forwarding impl is caught, a test-module impl is not.
#[test]
fn production_impl_scanner_flags_a_wrapper_without_forwarding() {
    let src = "impl<S: SectorSource> SectorSource for Wrap<S> {\n    fn read_sectors() {}\n}\n\
                   #[cfg(test)]\n#[allow(x)]\nmod tests {\n    impl SectorSource for Mock {}\n}\n";
    assert_eq!(production_impls(src), [("Wrap<S>".to_string(), false)]);
    let ok =
        "impl SectorSource for &mut (dyn SectorSource + '_) {\n fn unmapped_stream_files() {}\n}";
    assert_eq!(
        production_impls(ok),
        [("&mut (dyn SectorSource + '_)".to_string(), true)]
    );
}

/// The default `capacity_sectors` is 0 (unknown). Grounding: trait
/// default body `fn capacity_sectors(&self) -> u32 { 0 }`.
#[test]
fn default_capacity_is_zero() {
    struct Minimal;
    impl SectorSource for Minimal {
        fn read_sectors(
            &mut self,
            _lba: u32,
            _count: u16,
            _buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            Ok(0)
        }
    }
    assert_eq!(Minimal.capacity_sectors(), 0);
}

/// The default `set_speed` is a no-op that must not panic.
/// Grounding: trait default body `fn set_speed(&mut self, _kbs) {}`.
#[test]
fn default_set_speed_is_noop() {
    struct Minimal;
    impl SectorSource for Minimal {
        fn read_sectors(
            &mut self,
            _lba: u32,
            _count: u16,
            _buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            Ok(0)
        }
    }
    let mut m = Minimal;
    m.set_speed(12345); // must not panic
}

// `Box<dyn SectorSource>` must forward capacity, read_sectors, and
// set_speed to the inner source, so boxed sources satisfy generic
// decorator bounds.
#[test]
fn boxed_dyn_forwards_all_methods() {
    let (spy, reads, speeds, unit_bases) = Spy::new(777);
    let mut boxed: Box<dyn SectorSource> = Box::new(spy);

    assert_eq!(boxed.capacity_sectors(), 777, "capacity must forward");

    let mut buf = vec![0u8; 3 * 2048];
    let n = boxed.read_sectors(99, 3, &mut buf, true).unwrap();
    assert_eq!(n, 3 * 2048, "read return must forward");
    assert!(buf.iter().all(|b| *b == 0xa5), "inner must have filled buf");

    boxed.set_speed(5400);

    assert_eq!(
        *reads.lock().unwrap(),
        vec![(99, 3, true)],
        "read args (lba/count/recovery) must forward unchanged"
    );
    assert_eq!(
        *speeds.lock().unwrap(),
        vec![5400],
        "set_speed must forward"
    );

    // set_unit_base through the generic bound exercises the forwarding impl
    // (a direct `boxed.set_unit_base()` would vtable-dispatch instead). A
    // missing forwarding body would silently no-op and record nothing.
    set_unit_base_generic(boxed, 64);
    assert_eq!(
        *unit_bases.lock().unwrap(),
        vec![64],
        "set_unit_base must forward through Box<dyn>"
    );
}

/// `&mut dyn SectorSource` must likewise forward every method.
/// Grounding: `impl SectorSource for &mut (dyn SectorSource + '_)`.
#[test]
fn mut_ref_dyn_forwards_all_methods() {
    let (mut spy, reads, speeds, unit_bases) = Spy::new(123);

    {
        let r: &mut dyn SectorSource = &mut spy;
        assert_eq!(r.capacity_sectors(), 123);

        let mut buf = vec![0u8; 2 * 2048];
        let n = r.read_sectors(7, 2, &mut buf, false).unwrap();
        assert_eq!(n, 2 * 2048);

        r.set_speed(8800);
    }

    // Pass `&mut dyn` as a generic S so the forwarding impl's set_unit_base
    // is the one under test, not the vtable path.
    let r2: &mut dyn SectorSource = &mut spy;
    set_unit_base_generic(r2, 128);

    assert_eq!(*reads.lock().unwrap(), vec![(7, 2, false)]);
    assert_eq!(*speeds.lock().unwrap(), vec![8800]);
    assert_eq!(
        *unit_bases.lock().unwrap(),
        vec![128],
        "set_unit_base must forward through &mut dyn"
    );
}

// Records every read, incl. whether it arrived via the FUA entry point
// (`Spy` leaves `read_sectors_fua` to the trait default, so it can't
// tell). `fua` is `None` when the plain `read_sectors` path was hit.
type ReadLog = Arc<Mutex<Vec<(u32, u16, bool, Option<bool>)>>>;

#[derive(Default)]
struct ReadSpy {
    calls: ReadLog,
    fill: u8,
}

impl SectorSource for ReadSpy {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.calls
            .lock()
            .unwrap()
            .push((lba, count, recovery, None));
        let bytes = count as usize * 2048;
        buf[..bytes].fill(self.fill);
        Ok(bytes)
    }
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        self.calls
            .lock()
            .unwrap()
            .push((lba, count, recovery, Some(fua)));
        let bytes = count as usize * 2048;
        buf[..bytes].fill(self.fill);
        Ok(bytes)
    }
}

// Routes through a generic `S: SectorSource` bound so the forwarding impl (not the vtable)
// runs.
fn read_generic<S: SectorSource>(
    mut s: S,
    lba: u32,
    count: u16,
    buf: &mut [u8],
    recovery: bool,
) -> Result<usize> {
    s.read_sectors(lba, count, buf, recovery)
}

/// Same, for the speed lever. The trait's own `set_speed` default is a
/// no-op, so a forwarding body that also did nothing is indistinguishable
/// from the default unless the call is routed through the generic bound.
fn set_speed_generic<S: SectorSource>(mut s: S, kbs: u16) {
    s.set_speed(kbs);
}

/// Same, for the FUA entry point.
fn read_fua_generic<S: SectorSource>(
    mut s: S,
    lba: u32,
    count: u16,
    buf: &mut [u8],
    recovery: bool,
    fua: bool,
) -> Result<usize> {
    s.read_sectors_fua(lba, count, buf, recovery, fua)
}

// Proves the forwarding impl actually delegates `read_sectors`, unlike
// `mut_ref_dyn_forwards_all_methods` (vtable dispatch).
#[test]
fn mut_ref_dyn_forwards_read_sectors_to_the_inner_source() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut inner = ReadSpy {
        calls: calls.clone(),
        fill: 0x5C,
    };

    let mut buf = vec![0u8; 4 * 2048];
    let r: &mut dyn SectorSource = &mut inner;
    let n = read_generic(r, 0x1234, 4, &mut buf, true).expect("delegated read succeeds");

    assert_eq!(
        n,
        4 * 2048,
        "the forwarding impl must return the INNER source's byte count"
    );
    assert!(
        buf.iter().all(|b| *b == 0x5C),
        "the inner source's bytes must land in the caller's buffer; an \
             undelegated read leaves it untouched and the caller muxes zeroes"
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![(0x1234, 4, true, None)],
        "lba/count/recovery must reach the inner source unchanged, on the \
             non-FUA entry point"
    );
}

// Same, for `read_sectors_fua`: the forwarder must reach the inner source's FUA entry
// point, carrying the `fua` bit through.
#[test]
fn mut_ref_dyn_forwards_read_sectors_fua_to_the_inner_source() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut inner = ReadSpy {
        calls: calls.clone(),
        fill: 0x3B,
    };

    let mut buf = vec![0u8; 2 * 2048];
    let r: &mut dyn SectorSource = &mut inner;
    let n = read_fua_generic(r, 42, 2, &mut buf, false, true).expect("delegated FUA read");

    assert_eq!(n, 2 * 2048, "byte count must come from the inner source");
    assert!(
        buf.iter().all(|b| *b == 0x3B),
        "the inner source's bytes must land in the caller's buffer"
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![(42, 2, false, Some(true))],
        "the FUA entry point must be the one reached, with fua=true intact"
    );
}

// The forwarding impl must delegate `set_speed`, which hides worse than the read methods
// because the trait default is also a no-op.
#[test]
fn mut_ref_dyn_forwards_set_speed_to_the_inner_source() {
    let (mut spy, _reads, speeds, _bases) = Spy::new(0);
    let r: &mut dyn SectorSource = &mut spy;
    set_speed_generic(r, 5540);

    assert_eq!(
        *speeds.lock().unwrap(),
        vec![5540],
        "the forwarding impl must pass set_speed through to the inner \
             source; swallowing it is indistinguishable from the trait default \
             and silently disables recovery-path throttling"
    );
}

/// The same for `Box<dyn SectorSource>`, which is the receiver the mux read
/// paths actually hold.
#[test]
fn boxed_dyn_forwards_set_speed_to_the_inner_source() {
    let (spy, _reads, speeds, _bases) = Spy::new(0);
    let b: Box<dyn SectorSource> = Box::new(spy);
    set_speed_generic(b, 11080);

    assert_eq!(
        *speeds.lock().unwrap(),
        vec![11080],
        "the boxed forwarding impl must pass set_speed through unchanged"
    );
}
