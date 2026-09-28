//! KU-L2 tests (KU design §7.1, §7.8). Spec-governed tests quote their KS rows (§7.0.5).

use super::resolve::Clock;
use super::*;
use crate::aacs::content::{ALIGNED_UNIT_LEN, encrypt_unit};
use crate::aacs::mkb::AacsVersion;
use crate::aacs::types::UnitKey;
use crate::disc::{DiscRegion, DiscTitle, Extent};
use crate::keysource::{KeySource, ResolveCtx};
use crate::test_util::{
    BdFile, CountingSource, EncryptedBdImage, MemSource, ReadLog, aacs_state, encrypted_bd_image,
    unit_key_ro,
};
use std::cell::Cell;
use std::sync::Mutex;
use std::time::Duration;

const K1: [u8; 16] = *b"\xA1KU-L2 key one!!";
const K2: [u8; 16] = *b"\xB2KU-L2 key two!!";
const K3: [u8; 16] = *b"\xC3KU-L2 key 3333!";
const WRONG: [u8; 16] = *b"\xD4KU-L2 wrong key";
const F1: [u8; 16] = *b"\xE5KU-L2 forensic1";
const F2: [u8; 16] = *b"\xF6KU-L2 forensic2";
const ALT: [u8; 16] = *b"\x07KU-L2 alternate";
const VID: [u8; 16] = *b"\x5AKU-L2 volumeID!";
const HASH: &str = "0x00112233445566778899aabbccddeeff00112233";

// ── Fixtures ────────────────────────────────────────────────────────────────

struct Fx {
    img: EncryptedBdImage,
    disc: Disc,
}

impl Fx {
    // `(start, sectors)` of file `i`.
    fn file(&self, i: usize) -> (u32, u32) {
        self.img.files[i]
    }
    // LBA of unit `u` of file `i`.
    fn unit(&self, i: usize, u: u32) -> u32 {
        self.file(i).0 + u * 3
    }
    fn plain(&self, lba: u32, units: u32) -> Vec<u8> {
        let at = lba as usize * 2048;
        masked(&self.img.plain[at..at + units as usize * ALIGNED_UNIT_LEN])
    }
    fn raw(&self, lba: u32, units: u32) -> Vec<u8> {
        let at = lba as usize * 2048;
        self.img.image[at..at + units as usize * ALIGNED_UNIT_LEN].to_vec()
    }
    // Re-encrypt unit `u` of file `i` under `key` (plain unit, CPI 11₂).
    fn reencrypt(&mut self, i: usize, u: u32, key: &[u8; 16]) {
        let at = self.unit(i, u) as usize * 2048;
        let mut unit = self.img.plain[at..at + ALIGNED_UNIT_LEN].to_vec();
        unit.chunks_mut(192).for_each(|p| p[0] |= 0xC0);
        assert!(encrypt_unit(&mut unit, key));
        self.img.image[at..at + ALIGNED_UNIT_LEN].copy_from_slice(&unit);
    }
    fn source(&self) -> Faulty {
        Faulty::new(MemSource::new(self.img.image.clone()))
    }
}

// Byte 0 of each source packet masked to its ATS bits (CPI cleared on decrypt, KS-5).
fn masked(b: &[u8]) -> Vec<u8> {
    let mut v = b.to_vec();
    v.chunks_mut(192).for_each(|p| p[0] &= 0x3F);
    v
}

fn stream(n: usize, units: u32, key: Option<[u8; 16]>) -> BdFile {
    BdFile::new(format!("BDMV/STREAM/{n:05}.m2ts"), units * 3, key)
}

// A BD disc over `files` (stream files first), declaring `declared` CPS units, whose
// title `t` plays the files `titles[t]` in order.
fn fixture(files: &[BdFile], declared: usize, titles: &[&[usize]]) -> Fx {
    let uk_ro = unit_key_ro(
        AacsVersion::V10,
        &vec![[0xEE; 16]; declared],
        &vec![1u16; titles.len().max(1)],
    );
    let img = encrypted_bd_image(files, &uk_ro);
    let disc = disc_over(&img, &uk_ro, titles, DiscFormat::BluRay);
    Fx { img, disc }
}

fn disc_over(
    img: &EncryptedBdImage,
    uk_ro: &[u8],
    titles: &[&[usize]],
    format: DiscFormat,
) -> Disc {
    let titles = titles
        .iter()
        .enumerate()
        .map(|(t, files)| {
            let extents: Vec<Extent> = files
                .iter()
                .map(|&f| Extent {
                    start_lba: img.files[f].0,
                    sector_count: img.files[f].1,
                })
                .collect();
            DiscTitle {
                playlist: format!("{t:05}.mpls"),
                size_bytes: extents.iter().map(|e| e.sector_count as u64 * 2048).sum(),
                extents,
                ..DiscTitle::empty()
            }
        })
        .collect();
    let capacity = (img.image.len() / 2048) as u32;
    Disc {
        volume_id: "KU_L2".into(),
        meta_title: None,
        format,
        capacity_sectors: capacity,
        capacity_bytes: capacity as u64 * 2048,
        layers: 1,
        titles,
        region: DiscRegion::Free,
        aacs: Some(
            aacs_state()
                .disc_hash(HASH)
                .volume_id(VID)
                .uk_ro(uk_ro.to_vec())
                .build(),
        ),
        css: None,
        encrypted: true,
        aacs_error: None,
        css_error: None,
        content_format: if format == DiscFormat::HdDvd {
            ContentFormat::MpegPs
        } else {
            ContentFormat::BdTs
        },
    }
}

/// A random-access source whose reads fail over any `dead` range (media damage). The
/// ranges can change between passes (a recovered sector).
#[derive(Clone)]
struct Faulty {
    inner: MemSource,
    dead: Arc<Mutex<Vec<(u32, u32)>>>,
    halt_after: Option<(Halt, Arc<Mutex<u32>>)>,
}

impl Faulty {
    fn new(inner: MemSource) -> Self {
        Faulty {
            inner,
            dead: Arc::default(),
            halt_after: None,
        }
    }
    fn kill(&self, start: u32, end: u32) {
        self.dead.lock().unwrap().push((start, end));
    }
    fn heal(&self) {
        self.dead.lock().unwrap().clear();
    }
}

impl SectorSource for Faulty {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], r: bool) -> Result<usize> {
        if let Some((h, left)) = &self.halt_after {
            let mut n = left.lock().unwrap();
            if *n == 0 {
                h.cancel();
            } else {
                *n -= 1;
            }
        }
        let end = lba + count as u32;
        if self
            .dead
            .lock()
            .unwrap()
            .iter()
            .any(|&(s, e)| s < end && lba < e)
        {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        self.inner.read_sectors(lba, count, buf, r)
    }
}

/// A source that is not random access (a prefetcher's contract).
struct NoSeek(MemSource);

impl SectorSource for NoSeek {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], r: bool) -> Result<usize> {
        self.0.read_sectors(lba, count, buf, r)
    }
    fn random_access(&self) -> bool {
        false
    }
}

// ── Fake key sources ────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Call {
    who: &'static str,
    samples: usize,
    vid: Option<[u8; 16]>,
    forensic: bool,
    at: Duration,
}

#[derive(Clone, Default)]
struct Calls(Arc<Mutex<Vec<Call>>>);

impl Calls {
    fn of(&self, who: &str) -> Vec<Call> {
        let calls = self.0.lock().unwrap();
        calls.iter().filter(|c| c.who == who).cloned().collect()
    }
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

/// A fake clock (Stop §5.0: time injected, no real sleeps). `cancel_at` cancels a halt
/// once the clock reaches it (a Stop during a backoff wait).
#[derive(Default)]
struct FakeClock {
    now: Mutex<Duration>,
    cancel_at: Option<(Duration, Halt)>,
}

impl Clock for FakeClock {
    fn now(&self) -> Duration {
        *self.now.lock().unwrap()
    }
    fn sleep(&self, d: Duration, halt: Option<&Halt>) -> Result<()> {
        let mut now = self.now.lock().unwrap();
        *now += d;
        if let Some((at, h)) = &self.cancel_at
            && *now >= *at
        {
            h.cancel();
        }
        match halt {
            Some(h) => h.check(),
            None => Ok(()),
        }
    }
}

// How a fake source answers.
#[derive(Clone)]
enum Answer {
    /// Every held key (a keydb).
    All,
    /// Only the held keys that open one of the samples (an online service).
    Matching,
}

#[derive(Clone)]
struct Spec {
    who: &'static str,
    dependent: bool,
    keys: Vec<[u8; 16]>,
    fmts: Vec<[u8; 16]>,
    answer: Answer,
    calls: Calls,
    clock: Option<Arc<FakeClock>>,
    // Transport failures (no answer) while the fake clock is before this.
    down_until: Option<Duration>,
    // Answered with this failure (e.g. a 5xx): not transport class.
    fails: Option<fn() -> Error>,
    cancel: Option<Halt>,
}

impl Spec {
    fn keydb(keys: &[[u8; 16]], calls: &Calls) -> Self {
        Spec {
            who: "keydb",
            dependent: false,
            keys: keys.to_vec(),
            fmts: Vec::new(),
            answer: Answer::All,
            calls: calls.clone(),
            clock: None,
            down_until: None,
            fails: None,
            cancel: None,
        }
    }
    fn online(keys: &[[u8; 16]], calls: &Calls) -> Self {
        Spec {
            who: "online",
            dependent: true,
            answer: Answer::Matching,
            ..Self::keydb(keys, calls)
        }
    }
}

struct Fake {
    spec: Spec,
    transport: Cell<bool>,
}

impl Fake {
    fn record(&self, ctx: &dyn ResolveCtx, forensic: bool) -> Result<Vec<[u8; 16]>> {
        let at = self.spec.clock.as_ref().map_or(Duration::ZERO, |c| c.now());
        let samples = ctx.samples(usize::MAX).unwrap_or_default();
        self.spec.calls.0.lock().unwrap().push(Call {
            who: self.spec.who,
            samples: samples.len(),
            vid: ctx.vid().map(|v| v.0),
            forensic,
            at,
        });
        if let Some(h) = &self.spec.cancel {
            h.cancel();
        }
        self.transport.set(false);
        if self.spec.down_until.is_some_and(|d| at < d) {
            self.transport.set(true);
            return Err(Error::KeyServiceUnavailable);
        }
        if let Some(f) = self.spec.fails {
            return Err(f());
        }
        let held = if forensic {
            &self.spec.fmts
        } else {
            &self.spec.keys
        };
        Ok(match self.spec.answer {
            // KS-26 (evidence): an index-1 anchor gets the whole forensic set.
            _ if forensic => held.clone(),
            Answer::All => held.clone(),
            Answer::Matching => held
                .iter()
                .filter(|k| samples.iter().any(|s| opens(s, k)))
                .copied()
                .collect(),
        })
    }
}

fn opens(unit: &[u8], key: &[u8; 16]) -> bool {
    let mut u = unit.to_vec();
    crate::aacs::content::decrypt_unit(&mut u, key);
    crate::aacs::content::is_clean(&u, ContentFormat::BdTs)
}

impl KeySource for Fake {
    fn get_unit_keys(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>> {
        let keys = self.record(ctx, false)?;
        Ok(keys
            .iter()
            .enumerate()
            .map(|(i, k)| UnitKey::new(i as u32, *k))
            .collect())
    }
    fn get_fmts_indexes(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>> {
        let keys = self.record(ctx, true)?;
        Ok(keys
            .iter()
            .enumerate()
            .map(|(i, k)| UnitKey::new(i as u32, *k))
            .collect())
    }
    fn label(&self) -> &'static str {
        self.spec.who
    }
    fn answer_depends_on_samples(&self) -> bool {
        self.spec.dependent
    }
    fn last_failure_was_transport(&self) -> bool {
        self.transport.get()
    }
}

fn factory(specs: &[Spec]) -> KeySourceFactory {
    let specs = specs.to_vec();
    Arc::new(move || {
        specs
            .iter()
            .map(|s| {
                Box::new(Fake {
                    spec: s.clone(),
                    transport: Cell::new(false),
                }) as Box<dyn KeySource>
            })
            .collect()
    })
}

fn resolve_with(
    fx: &Fx,
    reader: &mut dyn SectorSource,
    scope: KeyScope,
    specs: &[Spec],
    opts: ResolveKeysOptions,
    clock: &dyn Clock,
) -> Result<ResolvedKeySet> {
    let f = factory(specs);
    super::resolve::resolve(&fx.disc, reader, scope, &f, opts, clock).map(|r| r.keys)
}

fn resolve(fx: &Fx, scope: KeyScope, specs: &[Spec]) -> Result<ResolvedKeySet> {
    resolve_with(
        fx,
        &mut fx.source(),
        scope,
        specs,
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    )
}

fn code<T>(r: Result<T>) -> u16 {
    match r {
        Ok(_) => panic!("expected a refusal, got Ok"),
        Err(e) => e.code(),
    }
}

// Read `units` units of file `i` from unit `u` through `r` (anchored at the file start).
fn read<S: SectorSource>(
    r: &mut DecryptingSectorSource<S>,
    fx: &Fx,
    i: usize,
    u: u32,
    units: u32,
) -> Result<Vec<u8>> {
    r.set_unit_base(fx.file(i).0);
    let mut buf = vec![0u8; units as usize * ALIGNED_UNIT_LEN];
    let n = r.read_sectors(fx.unit(i, u), (units * 3) as u16, &mut buf, true)?;
    assert_eq!(
        n,
        buf.len(),
        "a satisfied read returns Ok(n) for the whole request"
    );
    Ok(masked(&buf))
}

// A caller bug refuses with E7013 plus a debug assertion (KU §6): a panic in a debug
// build, `Err(E7013)` in a release build.
fn caller_bug<T>(f: impl FnOnce() -> Result<T>) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Err(_) => {
            #[cfg(not(debug_assertions))]
            panic!("only a debug build asserts");
        }
        Ok(r) => assert_eq!(code(r), crate::error::E_DECRYPT_FAILED),
    }
}

const E7013: u16 = crate::error::E_DECRYPT_FAILED;
const E7022: u16 = crate::error::E_NO_DISC_KEY;
const E7026: u16 = crate::error::E_FMTS_KEY_MISSING;
const E7028: u16 = crate::error::E_KEY_SERVICE_UNAVAILABLE;
const E7032: u16 = crate::error::E_WHOLE_DISC_KEY_MISSING;

// Two stream files, A under K1 and B under K2, two declared CPS units, title i = file i.
fn two_units() -> Fx {
    fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K2))],
        2,
        &[&[0], &[1]],
    )
}

// ── §7.1: verdicts, n_decl, sources ─────────────────────────────────────────

/// LK1 (K-1, the garbled-MKV case). KS-14 [BD] §3.9.3: "Num_of_CPS_Unit field (16 bits)
/// indicates the number of CPS Units on the disc"; KS-10 §3.9.2: "All AV stream files that
/// are referred to by one Title are included in the same CPS Unit". Two declared, only K1
/// held: title B's file is unopened, so B refuses up front (E7022), never keyed with K1.
#[test]
fn one_of_two_declared_keys_refuses_the_other_units_title() {
    let fx = two_units();
    let calls = Calls::default();
    let r = resolve(
        &fx,
        KeyScope::Titles(vec![1]),
        &[Spec::keydb(&[K1], &calls)],
    );
    assert_eq!(code(r), E7022);
}

/// LK2 (guard). A missing key for a title outside the scope never refuses; one request.
#[test]
fn missing_key_for_an_unselected_title_does_not_block() {
    let fx = two_units();
    let calls = Calls::default();
    let set = resolve(
        &fx,
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[K1], &calls)],
    )
    .expect("title A is keyed");
    assert_eq!(set.source_requests(), 1);
    assert_eq!(calls.len(), 1);
    let mut r = set.title_reader(&fx.disc, 0, fx.source()).unwrap();
    assert_eq!(
        read(&mut r, &fx, 0, 0, 10).unwrap(),
        fx.plain(fx.unit(0, 0), 10)
    );
}

/// LK3 (K-1). KS-14: one declared CPS unit and the only held key opens none of the
/// ciphertext → E7013, never garbage.
#[test]
fn single_declared_unit_wrong_key_refuses_e7013() {
    let fx = fixture(&[stream(1, 10, Some(K1))], 1, &[&[0]]);
    let calls = Calls::default();
    let r = resolve(
        &fx,
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[WRONG], &calls)],
    );
    assert_eq!(code(r), E7013);
}

/// LK4. KS-14: with one declared CPS unit, a key proven on one piece keys every Lazy piece
/// (KU §2.3 step 10).
#[test]
fn single_declared_unit_keys_lazy_pieces() {
    let fx = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K1))],
        1,
        &[&[0, 1]],
    );
    let src = fx.source();
    let (b, n) = fx.file(1);
    src.kill(b, b + n);
    let calls = Calls::default();
    let set = resolve_with(
        &fx,
        &mut src.clone(),
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[K1], &calls)],
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    )
    .unwrap();
    assert!(set.lazy().is_empty(), "B is keyed by the one proven key");
    assert_eq!(set.status().keyed, 2);
    src.heal();
    let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
    assert_eq!(read(&mut r, &fx, 1, 0, 10).unwrap(), fx.plain(b, 10));
}

/// LK5 (K-1). KS-14, KS-10: the count of held keys never decides coverage. Two declared,
/// one held: proven on every piece → Ok; a piece it does not open, with nothing from the
/// source → E7022.
#[test]
fn held_count_never_decides_coverage() {
    let both_k1 = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K1))],
        2,
        &[&[0, 1]],
    );
    let calls = Calls::default();
    let set = resolve(
        &both_k1,
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[K1], &calls)],
    );
    assert_eq!(set.expect("proven everywhere").status().keyed, 2);

    let fx = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K2))],
        2,
        &[&[0, 1]],
    );
    let r = resolve(
        &fx,
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[K1], &calls), Spec::online(&[], &calls)],
    );
    assert_eq!(code(r), E7022);
}

/// LK6 (K-11). KS-10, KS-1 [BD] §3.10.1: "encryption is applied to every Aligned Unit in
/// the file". A damaged file never inherits its neighbour's key: it is Lazy, proves K2 from
/// the pool on read, or stops.
#[test]
fn damaged_extent_never_inherits_neighbour_key() {
    let fx = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K2))],
        2,
        &[&[0, 1]],
    );
    let (b, n) = fx.file(1);
    let calls = Calls::default();
    for (pool, proves) in [(vec![K1, K2], true), (vec![K1], false)] {
        let src = fx.source();
        src.kill(b, b + n);
        let set = resolve_with(
            &fx,
            &mut src.clone(),
            KeyScope::Titles(vec![0]),
            &[Spec::keydb(&pool, &calls)],
            ResolveKeysOptions::default(),
            &FakeClock::default(),
        )
        .unwrap();
        assert_eq!(set.lazy(), &[(b, b + n)], "the damaged file is Lazy");
        src.heal();
        let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
        let got = read(&mut r, &fx, 1, 0, 10);
        if proves {
            assert_eq!(got.unwrap(), fx.plain(b, 10));
        } else {
            assert_eq!(code(got), E7022, "never K1 over K2's ciphertext");
        }
    }
}

/// LK8. KS-16 [PV] §3.3: "Kvu = AES-G(Km, IDv)"; KS-14. Two groups → two online requests,
/// each with ≥ 8 units and the scanned VID. A seeded resolve over an image with no VID
/// sends the seed's VID (coord 3).
#[test]
fn each_piece_request_has_eight_units_and_the_vid() {
    let fx = two_units();
    let calls = Calls::default();
    let set = resolve(
        &fx,
        KeyScope::Titles(vec![0, 1]),
        &[Spec::online(&[K1, K2], &calls)],
    )
    .unwrap();
    let online = calls.of("online");
    assert_eq!(online.len(), 2);
    for c in &online {
        assert!(c.samples >= crate::keysource::MIN_SAMPLE_UNITS, "{c:?}");
        assert_eq!(c.vid, Some(VID));
    }
    assert_eq!(set.status().keyed, 2);

    let seed = resolve(
        &fx,
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[K1], &calls)],
    )
    .unwrap();
    let mut image = two_units();
    image.disc.aacs.as_mut().unwrap().volume_id = [0u8; 16];
    let calls = Calls::default();
    let opts = ResolveKeysOptions {
        seed: Some(&seed),
        ..Default::default()
    };
    let set = resolve_with(
        &image,
        &mut image.source(),
        KeyScope::Titles(vec![1]),
        &[Spec::online(&[K2], &calls)],
        opts,
        &FakeClock::default(),
    )
    .unwrap();
    assert_eq!(
        calls.of("online")[0].vid,
        Some(VID),
        "the seed's in-memory VID"
    );
    assert_eq!(set.status().keyed, 1);
}

/// LK9 (guard). KS-5 [BD] §3.10.2: CPI "shall be set to 00₂ if the data is not encrypted";
/// KS-24 corroborates. Every probe clear → Ok, 0 requests, also with `n_decl = 1`.
#[test]
fn all_clear_pieces_are_never_refused() {
    for declared in [2, 1] {
        let fx = fixture(
            &[stream(1, 10, None), stream(2, 10, None)],
            declared,
            &[&[0, 1]],
        );
        let calls = Calls::default();
        let set = resolve(
            &fx,
            KeyScope::WholeDisc,
            &[Spec::keydb(&[K1], &calls), Spec::online(&[K1], &calls)],
        )
        .unwrap();
        assert_eq!(set.source_requests(), 0);
        assert_eq!(calls.len(), 0);
        assert_eq!(set.status().clear, 2);
    }
}

/// LK10. KS-10: a piece too small to ask with borrows units from its own playlist, and is
/// Lazy (never Missing) when it cannot.
#[test]
fn small_piece_borrows_or_goes_lazy_never_missing() {
    let fx = fixture(
        &[
            stream(1, 10, Some(K1)),
            stream(2, 4, Some(K2)),
            stream(3, 5, Some(K2)),
            stream(4, 3, Some(K3)),
        ],
        3,
        &[&[0, 1, 2], &[3]],
    );
    let calls = Calls::default();
    let set = resolve(
        &fx,
        KeyScope::Titles(vec![0, 1]),
        &[Spec::keydb(&[K1], &calls), Spec::online(&[K2, K3], &calls)],
    )
    .unwrap();
    assert_eq!(calls.of("online").len(), 1, "S and T ask once, together");
    assert_eq!(calls.of("online")[0].samples, 9);
    let (u, n) = fx.file(3);
    assert_eq!(
        set.lazy(),
        &[(u, u + n)],
        "U cannot borrow: Lazy, not Missing"
    );
    assert_eq!(set.status().keyed, 3);
}

/// SG23 ⚑ — per spec; do not change without a spec citation — KS-10 [BD] §3.9.2: "All AV
/// stream files that are referred to by one Title are included in the same CPS Unit".
/// Borrowing stays inside one playlist: pieces of two titles never lend each other units.
#[test]
fn playlist_pieces_share_a_cps_unit_for_borrowing() {
    assert!(
        crate::spec::keys::KS_10_TITLE_ONE_CPS_UNIT
            .text
            .contains("same CPS Unit")
    );
    let fx = fixture(
        &[stream(1, 4, Some(K2)), stream(2, 5, Some(K2))],
        2,
        &[&[0], &[1]],
    );
    let calls = Calls::default();
    let set = resolve(
        &fx,
        KeyScope::Titles(vec![0, 1]),
        &[Spec::online(&[K2], &calls)],
    );
    assert_eq!(calls.of("online").len(), 0, "no cross-playlist borrow");
    // Nothing held and every piece Lazy: today's keyless case (KU §2.7).
    assert_eq!(code(set), E7022);
}

/// J21 (OQ-L2-1) guard. A piece where exactly one probe read and opened is Lazy with that
/// key as its first candidate — never keyed from one probe — and is proven on read.
#[test]
fn one_opened_probe_is_lazy_never_keyed() {
    let fx = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K2))],
        2,
        &[&[0, 1]],
    );
    let (b, n) = fx.file(1);
    let src = fx.source();
    src.kill(b + 3, b + n); // only B's first unit reads
    let calls = Calls::default();
    let set = resolve_with(
        &fx,
        &mut src.clone(),
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[K1, K2], &calls)],
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    )
    .unwrap();
    assert_eq!(set.lazy(), &[(b, b + n)]);
    assert_eq!(set.status().keyed, 1);
    src.heal();
    let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
    assert_eq!(read(&mut r, &fx, 1, 0, 10).unwrap(), fx.plain(b, 10));
}

/// LK22. A sample-independent source (a keydb) is asked once per resolve; an online source
/// once per unopened piece (N-KU10).
#[test]
fn sample_independent_source_asked_once() {
    let files: Vec<BdFile> = (1..=5).map(|i| stream(i, 10, Some(K2))).collect();
    let fx = fixture(&files, 5, &[&[0], &[1], &[2], &[3], &[4]]);
    let calls = Calls::default();
    let r = resolve(
        &fx,
        KeyScope::WholeDisc,
        &[Spec::keydb(&[], &calls), Spec::online(&[], &calls)],
    );
    assert_eq!(code(r), E7032);
    assert_eq!(calls.of("keydb").len(), 1);
    assert_eq!(calls.of("online").len(), 5);
}

/// LK23a (J13, J15). A source that gives no answer (transport class) for 20 s of simulated
/// time is retried at 0, 1, 3, 7, 15 and 23 s, then answers: one request, the key proven.
#[test]
fn transient_source_failure_retried_until_it_answers() {
    let fx = fixture(&[stream(1, 10, Some(K1))], 1, &[&[0]]);
    let clock = Arc::new(FakeClock::default());
    let calls = Calls::default();
    let mut online = Spec::online(&[K1], &calls);
    online.clock = Some(clock.clone());
    online.down_until = Some(Duration::from_secs(20));
    let set = resolve_with(
        &fx,
        &mut fx.source(),
        KeyScope::Titles(vec![0]),
        &[online],
        ResolveKeysOptions::default(),
        clock.as_ref(),
    )
    .unwrap();
    let at: Vec<u64> = calls.of("online").iter().map(|c| c.at.as_secs()).collect();
    assert_eq!(at, [0, 1, 3, 7, 15, 23]);
    assert_eq!(set.source_requests(), 1);
    assert_eq!(set.status().keyed, 1);
}

/// LK23b (J13, J15; Stop T31). A source that never answers is given up once 60 s pass with
/// no answer (E7028), then the next source is asked. An answered failure (a 5xx) is never
/// retried. A Stop during a backoff wait → `Halted`, no further attempt (Stop KT11).
#[test]
fn transient_source_gives_up_after_60s_without_an_answer() {
    let fx = fixture(&[stream(1, 10, Some(K1))], 1, &[&[0]]);
    let run = |specs: &[Spec], clock: &FakeClock, halt: Option<&Halt>| {
        let opts = ResolveKeysOptions {
            halt,
            ..Default::default()
        };
        resolve_with(
            &fx,
            &mut fx.source(),
            KeyScope::Titles(vec![0]),
            specs,
            opts,
            clock,
        )
    };
    let calls = Calls::default();
    let clock = Arc::new(FakeClock::default());
    let mut dead = Spec::online(&[K1], &calls);
    dead.clock = Some(clock.clone());
    dead.down_until = Some(Duration::MAX);
    let r = run(std::slice::from_ref(&dead), &clock, None);
    assert_eq!(code(r), E7028);
    let at: Vec<u64> = calls.of("online").iter().map(|c| c.at.as_secs()).collect();
    assert_eq!(at, [0, 1, 3, 7, 15, 23, 31, 39, 47, 55, 60]);

    let calls = Calls::default();
    let clock = Arc::new(FakeClock::default());
    let mut dead = Spec::online(&[K1], &calls);
    dead.clock = Some(clock.clone());
    dead.down_until = Some(Duration::MAX);
    let mut next = Spec::online(&[K1], &calls);
    next.who = "next";
    let set = run(&[dead, next], &clock, None).expect("the next source answers");
    assert_eq!(calls.of("next").len(), 1);
    assert_eq!(set.status().keyed, 1);

    let calls = Calls::default();
    let mut five_xx = Spec::online(&[K1], &calls);
    five_xx.fails = Some(|| Error::KeyServiceUnavailable);
    assert_eq!(code(run(&[five_xx], &FakeClock::default(), None)), E7028);
    assert_eq!(calls.len(), 1, "an answered failure is never asked twice");

    let calls = Calls::default();
    let halt = Halt::new();
    let clock = Arc::new(FakeClock {
        cancel_at: Some((Duration::from_secs(2), halt.clone())),
        ..Default::default()
    });
    let mut dead = Spec::online(&[K1], &calls);
    dead.clock = Some(clock.clone());
    dead.down_until = Some(Duration::MAX);
    let r = run(&[dead], &clock, Some(&halt));
    assert!(matches!(r, Err(Error::Halted)));
    assert_eq!(calls.len(), 2, "no attempt after the Stop");
}

/// LK16. KS-27 (evidence: the HD DVD book is withdrawn; the flag model is UNVERIFIED): a
/// single declared key is applied best-effort; several refuse (E7022).
#[test]
fn hddvd_single_key_best_effort_multi_key_refuses() {
    // DirImage lays out a BDMV or VIDEO_TS tree; the EVO is found under /HVDVD_TS.
    let files = [
        BdFile::new("HVDVD_TS/FEATURE.EVO", 30, Some(K1)),
        BdFile::new("BDMV/index.bdmv", 1, None),
    ];
    for (declared, ok) in [(1, true), (2, false)] {
        let uk_ro = unit_key_ro(AacsVersion::V10, &vec![[0xEE; 16]; declared], &[1]);
        let img = encrypted_bd_image(&files, &uk_ro);
        let disc = disc_over(&img, &uk_ro, &[&[0]], DiscFormat::HdDvd);
        let fx = Fx { img, disc };
        let calls = Calls::default();
        let r = resolve(
            &fx,
            KeyScope::Titles(vec![0]),
            &[Spec::keydb(&[K1], &calls)],
        );
        if ok {
            let s = r.unwrap().status();
            assert!(s.best_effort);
            assert_eq!(s.keyed, 1);
        } else {
            assert_eq!(code(r), E7022);
        }
    }
}

/// LK17. A set is bound to its disc (hash, capacity, format, VID fingerprint), and its
/// `Debug` shows no key or VID bytes.
#[test]
fn key_set_bound_to_disc_and_debug_redacts_keys_and_vid() {
    let fx = two_units();
    let calls = Calls::default();
    let set = resolve(&fx, KeyScope::WholeDisc, &[Spec::keydb(&[K1, K2], &calls)]).unwrap();
    assert!(set.is_for(&fx.disc));
    let mut other = two_units();
    other.disc.aacs.as_mut().unwrap().disc_hash = "0xffff".into();
    assert!(!set.is_for(&other.disc));
    let mut other = two_units();
    other.disc.aacs.as_mut().unwrap().volume_id = [1; 16];
    assert!(!set.is_for(&other.disc));
    let mut other = two_units();
    other.disc.capacity_sectors += 3;
    assert!(!set.is_for(&other.disc));
    caller_bug(|| set.title_reader(&other.disc, 0, fx.source()));
    assert!(set.vid_fingerprint().is_some());
    assert_eq!(set.proven_key_fingerprints().len(), 2);

    let dbg = format!("{set:?}").to_ascii_lowercase();
    for secret in [K1, K2, VID] {
        let hex: String = secret.iter().map(|b| format!("{b:02x}")).collect();
        assert!(!dbg.contains(&hex), "{dbg}");
        let dec: Vec<String> = secret.iter().map(|b| b.to_string()).collect();
        assert!(!dbg.contains(&dec.join(", ")), "{dbg}");
    }
}

/// LK18. A Stop during resolve builds no set.
#[test]
fn stop_during_resolve_builds_no_set() {
    let fx = two_units();
    let halt = Halt::new();
    let mut src = fx.source();
    src.halt_after = Some((halt.clone(), Arc::new(Mutex::new(40))));
    let calls = Calls::default();
    let opts = ResolveKeysOptions {
        halt: Some(&halt),
        ..Default::default()
    };
    let r = resolve_with(
        &fx,
        &mut src,
        KeyScope::WholeDisc,
        &[Spec::keydb(&[K1, K2], &calls)],
        opts,
        &FakeClock::default(),
    );
    assert!(matches!(r, Err(Error::Halted)));

    let halt = Halt::new();
    let mut online = Spec::online(&[K1, K2], &calls);
    online.cancel = Some(halt.clone());
    let opts = ResolveKeysOptions {
        halt: Some(&halt),
        ..Default::default()
    };
    let r = resolve_with(
        &fx,
        &mut fx.source(),
        KeyScope::WholeDisc,
        &[online],
        opts,
        &FakeClock::default(),
    );
    assert!(matches!(r, Err(Error::Halted)));
}

// ── §2.4: proof on first read ───────────────────────────────────────────────

// File B (K2) Lazy: every probe of it faulted at resolve time. Pool K1 + K2.
fn lazy_b(pool: &[[u8; 16]]) -> (Fx, ResolvedKeySet, Faulty) {
    let fx = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K2))],
        2,
        &[&[0, 1]],
    );
    let (b, n) = fx.file(1);
    let src = fx.source();
    src.kill(b, b + n);
    let calls = Calls::default();
    let set = resolve_with(
        &fx,
        &mut src.clone(),
        KeyScope::WholeDisc,
        &[Spec::keydb(pool, &calls)],
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    )
    .unwrap();
    src.heal();
    (fx, set, src)
}

/// LK15 (a)–(f). KS-1 [BD] §3.10.1: "encryption is applied to every Aligned Unit in the
/// file"; KS-2 (a unit is 3 sectors); KS-7 (Informative): "Each physical sector in an
/// Aligned Unit shall be allocated contiguously". A Lazy piece is proven on first read.
#[test]
fn on_arrival_proof() {
    // (a) a pool key and an in-batch partner → proven.
    let (fx, set, src) = lazy_b(&[K1, K2]);
    let b = fx.file(1).0;
    let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
    assert_eq!(
        read(&mut r, &fx, 1, 2, 4).unwrap(),
        fx.plain(fx.unit(1, 2), 4)
    );
    assert_eq!(set.proof_cache().get(b), Some(Proof::Proven(1)));

    // (b) no pool key opens it → E7022 (title) / E7032 (image); nothing asked.
    let (fx, set, src) = lazy_b(&[K1]);
    let mut r = set.title_reader(&fx.disc, 0, src.clone()).unwrap();
    assert_eq!(code(read(&mut r, &fx, 1, 0, 4)), E7022);
    let mut w = set.whole_disc_reader(&fx.disc, src, None).unwrap();
    let mut buf = vec![0u8; ALIGNED_UNIT_LEN];
    assert_eq!(
        code(w.read_sectors(fx.unit(1, 0), 3, &mut buf, true)),
        E7032
    );

    // (c) the piece's LAST unit: no forward units; the backward side read finds a partner.
    let (fx, set, src) = lazy_b(&[K1, K2]);
    let counted = CountingSource::new(src);
    let log = counted.log();
    let mut r = set.title_reader(&fx.disc, 0, counted).unwrap();
    assert_eq!(
        read(&mut r, &fx, 1, 9, 1).unwrap(),
        fx.plain(fx.unit(1, 9), 1)
    );
    assert!(
        log.touched(fx.unit(1, 0), fx.unit(1, 9)),
        "a backward side read"
    );
    assert_eq!(set.proof_cache().get(fx.file(1).0), Some(Proof::Proven(1)));

    // (d) both side reads fault (dead neighbours) → provisional one-unit proof: decrypted,
    // `Ok(n)` for the whole request, never a read error.
    let (fx, set, src) = lazy_b(&[K1, K2]);
    let (b, n) = fx.file(1);
    src.kill(b, fx.unit(1, 5));
    src.kill(fx.unit(1, 6), b + n);
    let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
    assert_eq!(
        read(&mut r, &fx, 1, 5, 1).unwrap(),
        fx.plain(fx.unit(1, 5), 1)
    );
    assert_eq!(set.proof_cache().get(b), Some(Proof::Provisional(1)));

    // (e) across 3 passes: a tail unit with permanently dead neighbours is decrypted every
    // pass; passes 2 and 3 reuse the ProofCache (shared by the set's clones): 0 side reads.
    let (fx, set, src) = lazy_b(&[K1, K2]);
    let (b, n) = fx.file(1);
    src.kill(b, fx.unit(1, 9));
    for pass in 0..3 {
        let set = set.clone();
        let counted = CountingSource::new(src.clone());
        let log: ReadLog = counted.log();
        let mut r = set.title_reader(&fx.disc, 0, counted).unwrap();
        assert_eq!(
            read(&mut r, &fx, 1, 9, 1).unwrap(),
            fx.plain(fx.unit(1, 9), 1)
        );
        let side = log
            .reads()
            .iter()
            .filter(|&&(l, _)| l != fx.unit(1, 9))
            .count();
        assert_eq!(side, if pass == 0 { 1 } else { 0 }, "pass {pass}");
    }
    assert!(set.proof_cache().get(b).is_some() && n > 0);

    // (f) a provisional key contradicted by the piece's next readable unit → key conflict.
    let (mut fx, _, _) = lazy_b(&[K1, K2]);
    fx.reencrypt(1, 5, &K1);
    let (b, n) = fx.file(1);
    let src = fx.source();
    src.kill(b, b + n);
    let calls = Calls::default();
    let set = resolve_with(
        &fx,
        &mut src.clone(),
        KeyScope::WholeDisc,
        &[Spec::keydb(&[K1, K2], &calls)],
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    )
    .unwrap();
    src.heal();
    src.kill(b, fx.unit(1, 5));
    src.kill(fx.unit(1, 6), b + n);
    let mut r = set.title_reader(&fx.disc, 0, src.clone()).unwrap();
    read(&mut r, &fx, 1, 5, 1).expect("provisional under K1");
    assert_eq!(set.proof_cache().get(b), Some(Proof::Provisional(0)));
    src.heal();
    assert_eq!(
        code(read(&mut r, &fx, 1, 7, 1)),
        E7022,
        "K1 does not open unit 7"
    );
}

/// SG25 ⚑ — per spec; do not change without a spec citation — KS-1 [BD] §3.10.1:
/// "encryption is applied to every Aligned Unit in the file"; KS-2. The final unit of a Lazy
/// piece is decrypted (proven backward), never a read error.
#[test]
fn last_unit_of_piece_is_decrypted_not_withheld() {
    assert!(
        crate::spec::keys::KS_1_ENCRYPT_EVERY_UNIT
            .text
            .contains("every Aligned Unit")
    );
    let (fx, set, src) = lazy_b(&[K1, K2]);
    let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
    assert_eq!(
        read(&mut r, &fx, 1, 9, 1).unwrap(),
        fx.plain(fx.unit(1, 9), 1)
    );
}

/// SG26 ⚑ — per spec; do not change without a spec citation — KS-1, KS-2 [BD] §3.10.1: "An
/// Aligned Unit consists of 32 MPEG source packets". Every other unit of the piece
/// unreadable → the readable one is decrypted on a provisional proof, never withheld.
#[test]
fn unit_next_to_dead_sectors_is_decrypted_not_withheld() {
    assert!(
        crate::spec::keys::KS_2_ALIGNED_UNIT
            .text
            .contains("32 MPEG source packets")
    );
    let (fx, set, src) = lazy_b(&[K1, K2]);
    let (b, n) = fx.file(1);
    src.kill(b, fx.unit(1, 3));
    src.kill(fx.unit(1, 4), b + n);
    let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
    assert_eq!(
        read(&mut r, &fx, 1, 3, 1).unwrap(),
        fx.plain(fx.unit(1, 3), 1)
    );
}

/// SG27 ⚑ (LK15 (h)) — layering; `prefetched.rs` "lba/count are advisory". A decrypting
/// reader over a non-random-access source is refused at construction (E7013).
#[test]
fn decrypting_reader_sits_below_the_prefetcher() {
    let (fx, set, _) = lazy_b(&[K1, K2]);
    let nose = || NoSeek(MemSource::new(fx.img.image.clone()));
    caller_bug(|| set.title_reader(&fx.disc, 0, nose()));
    caller_bug(|| set.whole_disc_reader(&fx.disc, nose(), None).map(|_| ()));
    assert!(fx.source().random_access());
    let boxed: Box<dyn SectorSource> = Box::new(nose());
    assert!(!boxed.random_access(), "Box<dyn> forwards random_access");
}

// §5 FMTS. A UHD FMTS disc: file 0 (A, K1) and the forensic clip (file 1, 60 units, base key K2)
// with an index-1 segment over units 0..16 and an index-2 segment over units 20..36. Our
// phase is Even (F1 / F2); odd segment units are the alternate variant (ALT).
fn fmts_fixture() -> Fx {
    let files = [
        stream(1, 10, Some(K1)),
        BdFile::new("BDMV/STREAM/00002.fmts", 180, Some(K2)),
        BdFile::new("AACS/IndividualSegment.tbl", 1, None),
    ];
    let mut fx = fixture(&files, 2, &[&[0], &[1], &[0, 1]]);
    fx.disc.format = DiscFormat::Fmts;
    let segs = [(1u16, 0u32, 16u32), (2, 20, 36)];
    let mut tbl = Vec::new();
    tbl.extend_from_slice(&0x0100_0000u32.to_be_bytes());
    tbl.extend_from_slice(&(segs.len() as u16).to_be_bytes());
    tbl.extend_from_slice(&16u16.to_be_bytes());
    for &(index, a, b) in &segs {
        tbl.extend_from_slice(&0x0100_0000u32.to_be_bytes());
        tbl.extend_from_slice(&index.to_be_bytes());
        tbl.extend_from_slice(&1u16.to_be_bytes());
        tbl.extend_from_slice(&(a * 32).to_be_bytes());
        tbl.extend_from_slice(&(b * 32 - 1).to_be_bytes());
    }
    let at = fx.file(2).0 as usize * 2048;
    fx.img.image[at..at + tbl.len()].copy_from_slice(&tbl);
    for &(index, a, b) in &segs {
        let ours = if index == 1 { F1 } else { F2 };
        for u in a..b {
            fx.reencrypt(1, u, if (u - a) % 2 == 0 { &ours } else { &ALT });
        }
    }
    fx
}

fn fmts_online(calls: &Calls) -> Spec {
    let mut s = Spec::online(&[K2], calls);
    s.fmts = vec![F1, F2];
    s
}

/// LK12. KS-25, KS-26 (evidence, no public FMTS spec): the forensic set is fetched once for
/// all titles and passes; the clip's base content keeps its own proven key (K-3), and the
/// alternate phase stays ciphertext.
#[test]
fn fmts_set_fetched_once_for_all_titles_and_passes() {
    let fx = fmts_fixture();
    let calls = Calls::default();
    let set = resolve(
        &fx,
        KeyScope::Titles(vec![0, 1, 2]),
        &[Spec::keydb(&[K1], &calls), fmts_online(&calls)],
    )
    .unwrap();
    let anchors = calls.of("online").iter().filter(|c| c.forensic).count();
    assert_eq!(anchors, 1, "one anchor round");
    assert_eq!(set.status().forensic, ForensicState::Resolved);
    let before = calls.len();
    for title in [1, 2, 1] {
        let mut r = set.title_reader(&fx.disc, title, fx.source()).unwrap();
        let got = read(&mut r, &fx, 1, 0, 60).unwrap();
        let plain = fx.plain(fx.file(1).0, 60);
        for u in 0..60usize {
            let unit = &got[u * ALIGNED_UNIT_LEN..(u + 1) * ALIGNED_UNIT_LEN];
            let want = &plain[u * ALIGNED_UNIT_LEN..(u + 1) * ALIGNED_UNIT_LEN];
            let alternate = (u < 16 || (20..36).contains(&u)) && (u % 2 == 1);
            if alternate {
                assert_ne!(unit, want, "unit {u}: alternate phase stays ciphertext");
            } else {
                assert_eq!(unit, want, "unit {u}");
            }
        }
    }
    assert_eq!(calls.len(), before, "nothing asks after resolve");
}

/// LK13. KS-25, KS-26 (evidence): every index-1 anchor unreadable → forensic Pending; a
/// decrypting single-pass or disc→ISO reader refuses E7026 before any output.
#[test]
fn fmts_all_anchors_unreadable_is_pending() {
    let fx = fmts_fixture();
    let src = fx.source();
    src.kill(fx.unit(1, 0), fx.unit(1, 16));
    let calls = Calls::default();
    let set = resolve_with(
        &fx,
        &mut src.clone(),
        KeyScope::WholeDisc,
        &[Spec::keydb(&[K1], &calls), fmts_online(&calls)],
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    )
    .unwrap();
    assert!(set.forensic_pending());
    assert_eq!(code(set.title_reader(&fx.disc, 1, fx.source())), E7026);
    assert_eq!(
        code(
            set.whole_disc_reader(&fx.disc, fx.source(), None)
                .map(|_| ())
        ),
        E7026
    );
    assert_eq!(
        code(check_decryptable(
            &fx.disc,
            false,
            Some(&set),
            &KeyScope::WholeDisc
        )),
        E7026
    );
    assert!(matches!(
        decrypt_status(&fx.disc, Some(&set)),
        DecryptStatus::ForensicPending
    ));
    assert!(check_decryptable(&fx.disc, true, Some(&set), &KeyScope::WholeDisc).is_ok());
    // Title A never touches the forensic clip.
    assert!(set.title_reader(&fx.disc, 0, fx.source()).is_ok());
}

/// LK15 (i) (KU4-4). KS-25, KS-26 (evidence). Segment units are never part of the on-arrival
/// proof: a Lazy forensic clip whose first readable unit is an alternate-phase unit, and
/// index-key units while forensic keys are Pending, cause no stop and no key conflict.
#[test]
fn on_arrival_proof_skips_forensic_segment_units() {
    let base_dead = |fx: &Fx, src: &Faulty| {
        src.kill(fx.unit(1, 16), fx.unit(1, 20));
        src.kill(fx.unit(1, 36), fx.unit(1, 60));
    };
    let fx = fmts_fixture();
    let calls = Calls::default();
    for pending in [false, true] {
        let src = fx.source();
        base_dead(&fx, &src);
        if pending {
            src.kill(fx.unit(1, 0), fx.unit(1, 16));
        }
        let set = resolve_with(
            &fx,
            &mut src.clone(),
            KeyScope::WholeDisc,
            &[Spec::keydb(&[K1, K2], &calls), fmts_online(&calls)],
            ResolveKeysOptions::default(),
            &FakeClock::default(),
        )
        .unwrap();
        assert_eq!(set.forensic_pending(), pending);
        let (c, n) = fx.file(1);
        assert_eq!(set.lazy(), &[(c, c + n)]);
        src.heal();
        let ext = [(c, c + n)];
        let mut r = set
            .decrypting(
                src,
                Some(&ext),
                StopKind::Title {
                    disc_hash: HASH.into(),
                },
                true,
            )
            .unwrap();
        // Unit 1: alternate phase, first readable; units 0..24 include index-key units.
        let got = read(&mut r, &fx, 1, 1, 23).unwrap();
        let plain = fx.plain(fx.unit(1, 1), 23);
        let unit =
            |b: &[u8], u: usize| b[u * ALIGNED_UNIT_LEN..(u + 1) * ALIGNED_UNIT_LEN].to_vec();
        // Base units 16..20 (buffer 15..19) are proven on arrival and decrypted.
        for u in 15..19 {
            assert_eq!(unit(&got, u), unit(&plain, u), "base unit {}", u + 1);
        }
        assert_eq!(set.proof_cache().get(c), Some(Proof::Proven(1)));
        // Unit 1 is the alternate phase: left as ciphertext, CPI kept.
        assert_eq!(unit(&got, 0), masked(&fx.raw(fx.unit(1, 1), 1)));
        assert!(fx.raw(fx.unit(1, 1), 1)[0] & 0xC0 != 0);
    }
}

// ── §3.1 surfaces: extract, input, mux, session ─────────────────────────────

/// LK11 (K-2). KS-1 [BD] §3.10.1: "encryption is applied to every Aligned Unit in the
/// file"; KS-10. A decrypted folder never keys a file with a key that does not open it:
/// `resolve(WholeDisc)` refuses up front (E7032), and a file left Lazy whose readable unit
/// no held key opens stops the extract (E7032), never written.
#[test]
fn extract_tree_refuses_a_file_no_key_opens() {
    let fx = two_units();
    let calls = Calls::default();
    assert_eq!(
        code(resolve(
            &fx,
            KeyScope::WholeDisc,
            &[Spec::keydb(&[K1], &calls)]
        )),
        E7032
    );

    let (fx, set, mut src) = lazy_b(&[K1]);
    let dest = tempfile::tempdir().unwrap();
    let opts = crate::disc::ExtractOptions {
        keys: Some(&set),
        ..Default::default()
    };
    let r = fx.disc.extract_tree(&mut src, dest.path(), &opts);
    assert_eq!(code(r), E7032);
    assert!(
        !dest.path().join("BDMV/STREAM/00002.m2ts").exists(),
        "B is never written"
    );

    let (fx, set, mut src) = lazy_b(&[K1, K2]);
    let dest = tempfile::tempdir().unwrap();
    let opts = crate::disc::ExtractOptions {
        keys: Some(&set),
        ..Default::default()
    };
    let res = fx.disc.extract_tree(&mut src, dest.path(), &opts).unwrap();
    assert!(res.complete);
    for (i, name) in [(0, "00001"), (1, "00002")] {
        let got = std::fs::read(dest.path().join(format!("BDMV/STREAM/{name}.m2ts"))).unwrap();
        assert_eq!(masked(&got), fx.plain(fx.file(i).0, 10), "{name}");
    }
}

/// LK7. `resolve` retains no source: after it, the factory's `Arc` count is 1, and it is
/// unchanged, with no call made, across readers, three passes with on-arrival proofs and a
/// mux.
#[test]
fn resolve_retains_no_source_and_nothing_asks_after() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let fx = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K2))],
        2,
        &[&[0, 1]],
    );
    let (b, n) = fx.file(1);
    let src = fx.source();
    src.kill(b, b + n);
    let calls = Calls::default();
    let f = factory(&[Spec::keydb(&[K1, K2], &calls)]);
    let set = ResolvedKeySet::resolve(
        &fx.disc,
        &mut src.clone(),
        KeyScope::WholeDisc,
        &f,
        ResolveKeysOptions::default(),
    )
    .unwrap()
    .keys;
    assert_eq!(Arc::strong_count(&f), 1);
    let asked = calls.len();
    src.heal();
    for _ in 0..3 {
        let mut r = set.title_reader(&fx.disc, 0, src.clone()).unwrap();
        assert_eq!(read(&mut r, &fx, 1, 0, 10).unwrap(), fx.plain(b, 10));
        let mut w = set.whole_disc_reader(&fx.disc, src.clone(), None).unwrap();
        let mut buf = vec![0u8; 30 * 2048];
        w.read_sectors(b, 30, &mut buf, true).unwrap();
    }
    let _ = crate::mux::mux_with_keys(
        crate::mux::MuxSource::Live {
            reader: Box::new(src.clone()),
            title: fx.disc.titles[0].clone(),
            format: ContentFormat::BdTs,
        },
        Some(&set),
        "null://",
        &crate::mux::MuxOptions {
            batch_sectors: 30,
            ..Default::default()
        },
        &Halt::new(),
        Arc::new(crate::mux::driver::NoopEvents),
    );
    assert_eq!(Arc::strong_count(&f), 1);
    assert_eq!(calls.len(), asked, "nothing asks after resolve");
}

// A scannable encrypted BD image: one playlist over one K1 clip of 10 units.
fn scannable_image() -> (EncryptedBdImage, Disc) {
    use crate::dirimage::tests::{minimal_clpi, one_item_mpls};
    let uk_ro = unit_key_ro(AacsVersion::V10, &[[0xEE; 16]], &[1]);
    let files = [
        BdFile::new("BDMV/index.bdmv", 1, None),
        BdFile::new("BDMV/PLAYLIST/00000.mpls", 1, None),
        BdFile::new("BDMV/CLIPINF/00000.clpi", 1, None),
        BdFile::new("BDMV/STREAM/00000.m2ts", 30, Some(K1)),
    ];
    let mut img = encrypted_bd_image(&files, &uk_ro);
    for (i, bytes) in [(1, one_item_mpls(b"00000")), (2, minimal_clpi(320))] {
        let at = img.files[i].0 as usize * 2048;
        img.image[at..at + bytes.len()].copy_from_slice(&bytes);
    }
    let mut src = MemSource::new(img.image.clone());
    let cap = src.capacity_sectors();
    let disc = Disc::scan_image(&mut src, cap, &crate::disc::ScanOptions::default()).unwrap();
    assert_eq!(disc.titles.len(), 1, "the fixture scans to one title");
    (img, disc)
}

/// `InputOptions::keys` (KU §3.1, §3.5): an `iso://` AACS source opens through the rip's set,
/// gated by `check_decryptable`, where the legacy path (no banked key) refuses E7022.
#[test]
fn iso_input_reads_through_the_key_set() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let (img, disc) = scannable_image();
    let calls = Calls::default();
    let f = factory(&[Spec::keydb(&[K1], &calls)]);
    let set = ResolvedKeySet::resolve(
        &disc,
        &mut MemSource::new(img.image.clone()),
        KeyScope::Titles(vec![0]),
        &f,
        ResolveKeysOptions::default(),
    )
    .unwrap()
    .keys;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disc.iso");
    std::fs::write(&path, &img.image).unwrap();
    let url = format!("iso://{}", path.display());
    let legacy = crate::input(&url, &crate::InputOptions::default());
    assert_eq!(crate::error_code(&legacy.err().unwrap()), Some(E7022));
    let opts = crate::InputOptions {
        keys: Some(set),
        ..Default::default()
    };
    let stream = crate::input(&url, &opts).expect("the set keys the title");
    assert_eq!(stream.info().extents, disc.titles[0].extents);
}

/// `DiscSession::resolve_key_set` resolves through the session's staged reader and keeps
/// nothing: the set is the caller's.
#[test]
fn session_resolves_a_key_set_through_its_reader() {
    let fx = two_units();
    let calls = Calls::default();
    let f = factory(&[Spec::keydb(&[K1, K2], &calls)]);
    let mut session = crate::session::DiscSession::from_parts_for_test(
        Some(two_units().disc),
        Some(Box::new(fx.source())),
        None,
    );
    let r = session
        .resolve_key_set(KeyScope::WholeDisc, &f, ResolveKeysOptions::default())
        .unwrap();
    assert_eq!(r.keys.status().keyed, 2);
    assert_eq!(calls.len(), 1);
    assert_eq!(Arc::strong_count(&f), 1);
}

/// KU §2.5: a title scope — even every title (`-t all`) — covers only the files its titles
/// play. A file no title plays is a whole-disc concern (E7032 there), never a title refusal.
#[test]
fn titles_scope_never_keys_a_file_no_title_plays() {
    let fx = fixture(
        &[
            stream(1, 10, Some(K1)),
            stream(2, 10, Some(K2)),
            stream(3, 10, Some(K3)),
        ],
        3,
        &[&[0], &[1]],
    );
    let calls = Calls::default();
    let keydb = [Spec::keydb(&[K1, K2], &calls)];
    let set = resolve(&fx, KeyScope::Titles(vec![0, 1]), &keydb).unwrap();
    assert_eq!(set.status().keyed, 2);
    assert_eq!(code(resolve(&fx, KeyScope::WholeDisc, &keydb)), E7032);
}

/// KS-25, KS-26 (evidence). A forensic segment whose index has no held key refuses (E7026):
/// it is never decrypted with the base key.
#[test]
fn fmts_segment_without_its_index_key_refuses() {
    let fx = fmts_fixture();
    let calls = Calls::default();
    let mut online = fmts_online(&calls);
    online.fmts = vec![F1];
    let r = resolve(
        &fx,
        KeyScope::WholeDisc,
        &[Spec::keydb(&[K1], &calls), online],
    );
    assert_eq!(code(r), E7026);
}

// Counts full-recovery (ECC) reads through to the source.
#[derive(Clone)]
struct RecoveryCount(Faulty, Arc<Mutex<u32>>);

impl SectorSource for RecoveryCount {
    fn capacity_sectors(&self) -> u32 {
        self.0.capacity_sectors()
    }
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], r: bool) -> Result<usize> {
        if r {
            *self.1.lock().unwrap() += 1;
        }
        self.0.read_sectors(lba, count, buf, r)
    }
}

/// KU §2.4, §6 (review item 2): the on-arrival loud stop on the live inline reader is E7022
/// at once, in both skip modes: never shrunk and retried, never an ECC recovery read,
/// never zero-filled as a bad sector.
#[test]
fn live_stream_stops_on_an_unkeyed_piece_without_recovery() {
    use crate::pes::Stream;
    for skip in [true, false] {
        let (fx, set, src) = lazy_b(&[K1]);
        let recovery = Arc::new(Mutex::new(0u32));
        let reader = RecoveryCount(src, recovery.clone());
        let mut stream = crate::mux::DiscStream::new(
            Box::new(reader),
            fx.disc.titles[0].clone(),
            set.decrypt_keys(),
            30,
            ContentFormat::BdTs,
            false,
            None,
        )
        .unwrap()
        .with_key_map(set.key_map())
        .with_arrival(set.arrival(set.title_stop()).expect("B is Lazy"));
        stream.skip_errors = skip;
        let got = loop {
            match stream.read() {
                Ok(Some(_)) => continue,
                other => break other,
            }
        };
        let err = got.expect_err("an unkeyed readable unit stops the mux");
        assert_eq!(crate::error_code(&err), Some(E7022), "skip={skip}: {err}");
        assert_eq!(
            *recovery.lock().unwrap(),
            0,
            "skip={skip}: no ECC recovery read"
        );
        assert_eq!(
            (stream.errors(), stream.lost_bytes()),
            (0, 0),
            "skip={skip}"
        );
    }
}

/// Review item 1 (guard): the status counts keyed pieces and proven keys from the pieces
/// themselves, so a whole-disc resolve over a disc with no scanned titles (FK9's fixture)
/// still reports its keyed stream file: (keyed, proven) = (1, 1).
#[test]
fn whole_disc_status_counts_keyed_files_without_titles() {
    let fx = fixture(&[stream(1, 10, Some(K1))], 1, &[]);
    let calls = Calls::default();
    let set = resolve(&fx, KeyScope::WholeDisc, &[Spec::keydb(&[K1], &calls)]).unwrap();
    let s = set.status();
    assert_eq!((s.keyed, s.proven, s.lazy), (1, 1, 0), "{s:?}");
}

/// KU §2.3 step 10 (review item 4). KS-14 [BD] §3.9.3: "Num_of_CPS_Unit field (16 bits)
/// indicates the number of CPS Units on the disc". The n_decl == 1 rule keys only Lazy
/// pieces no other held key opened: a piece whose one readable probe another key opened
/// stays Lazy with that candidate, never keyed with the proven key over it.
#[test]
fn single_unit_rule_never_overrides_a_piece_another_key_opened() {
    let fx = fixture(
        &[stream(1, 10, Some(K1)), stream(2, 10, Some(K2))],
        1,
        &[&[0, 1]],
    );
    let (b, n) = fx.file(1);
    let src = fx.source();
    src.kill(b + 3, b + n); // only B's first unit reads, and K2 opens it
    let calls = Calls::default();
    let set = resolve_with(
        &fx,
        &mut src.clone(),
        KeyScope::Titles(vec![0]),
        &[Spec::keydb(&[K1, K2], &calls)],
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    )
    .unwrap();
    assert_eq!(set.lazy(), &[(b, b + n)], "B stays Lazy (candidate K2)");
    src.heal();
    let mut r = set.title_reader(&fx.disc, 0, src).unwrap();
    assert_eq!(read(&mut r, &fx, 1, 0, 10).unwrap(), fx.plain(b, 10));
}

/// KU §2.4 (review item 5): the held keys are tried on U before any side read. A readable
/// unit no held key opens stops at once, with no side read of a (possibly damaged) area.
#[test]
fn unopenable_unit_stops_before_any_side_read() {
    let (fx, set, src) = lazy_b(&[K1]);
    let counted = CountingSource::new(src);
    let log = counted.log();
    let mut r = set.title_reader(&fx.disc, 0, counted).unwrap();
    assert_eq!(code(read(&mut r, &fx, 1, 4, 1)), E7022);
    assert_eq!(log.reads(), [(fx.unit(1, 4), 3)], "only the requested read");
}
