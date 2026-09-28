//! [`ResolvedKeySet::resolve`] (KU §2.3): pieces, probes, sources, verdicts.

use super::{
    ArrivalPiece, ForensicState, Inner, KeyResolution, KeyScope, ProofCache, ResolveKeysOptions,
    ResolvedKeySet,
};
use crate::aacs::content::{ALIGNED_UNIT_LEN, aacs_unit_encrypted, decrypt_unit, is_clean};
use crate::aacs::trace::{KeyNode, KeyOutcome, KeyStep, ResolutionTrace};
use crate::decrypt::{AacsKeyMap, Phase};
use crate::disc::{ContentFormat, Disc, DiscFormat, Extent};
use crate::error::{Error, Result};
use crate::halt::{Halt, Progress};
use crate::keysource::{DiscInputs, DiscInputsCtx, KeySource, MIN_SAMPLE_UNITS};
use crate::sector::SectorSource;
use crate::session::KeySourceFactory;
use crate::whole_disc::{UNIT, UnitSpan, probe_units, subtract_ranges, unit_head};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// First wait before re-asking a source that gave no answer (J13).
const RETRY_FIRST: Duration = Duration::from_secs(1);
/// The retry wait doubles up to this (J13).
const RETRY_CAP: Duration = Duration::from_secs(8);
/// A source is given up after this long with no answer (J13; Stop T31). A stall window, not
/// a total: any answer ends the retry at once.
const NO_ANSWER_WINDOW: Duration = Duration::from_secs(60);
/// Encrypted units sent in one per-piece request, at most.
const SAMPLE_CAP: usize = 32;

/// Time for the J13 retry: injected so tests use a fake clock and never sleep.
pub(crate) trait Clock {
    fn now(&self) -> Duration;
    /// Wait `d`, returning `Err(Halted)` as soon as `halt` is cancelled.
    fn sleep(&self, d: Duration, halt: Option<&Halt>) -> Result<()>;
}

/// The wall clock; waits are halt-aware ([`Halt::wait`]).
pub(crate) struct RealClock(Instant);

impl RealClock {
    pub(crate) fn new() -> Self {
        RealClock(Instant::now())
    }
}

impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
    fn sleep(&self, d: Duration, halt: Option<&Halt>) -> Result<()> {
        match halt {
            Some(h) => h.wait(d),
            None => {
                std::thread::sleep(d);
                Ok(())
            }
        }
    }
}

/// A piece's verdict (KU §2.3 step 9). `Ask` is transient: resolved before `resolve` ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Keyed(usize),
    Clear,
    Lazy(Option<usize>),
    Missing,
    Ask,
}

// One Clip AV stream file (or an uncovered title extent) in scope, with its probes.
struct Piece {
    spans: Vec<UnitSpan>,
    titles: Vec<usize>,
    rank: u64,
    units: u64,
    enc: Vec<Vec<u8>>,
    faults: usize,
    verdict: Verdict,
}

impl Piece {
    fn id(&self) -> u32 {
        self.spans[0].0
    }

    fn ranges(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.spans.iter().map(|&(s, n, _)| (s, s.saturating_add(n)))
    }

    // Unit heads on the piece's grid, as (first head, unit count) per span. A unit that
    // would cross its span's end is not a unit of the piece (KS-7, Informative).
    fn grid(&self) -> Vec<(u64, u64)> {
        self.spans
            .iter()
            .map(|&(s, n, anchor)| {
                let head = unit_head(s, anchor);
                let end = s as u64 + n as u64;
                (head, end.saturating_sub(head) / UNIT)
            })
            .collect()
    }
}

// What one request to one source produced.
enum Asked {
    Keys(Vec<[u8; 16]>),
    Empty,
    /// The source failed; the error is the run's `first_failure` if it came first.
    Failed,
    Skipped,
}

// The state of one `resolve` run.
struct Run<'a> {
    halt: Option<&'a Halt>,
    // The op's progress: busy around each source call (stop design §2.1 item 2).
    progress: Option<&'a Progress>,
    clock: &'a dyn Clock,
    format: ContentFormat,
    sources: Vec<Box<dyn KeySource>>,
    dead: Vec<bool>,
    inputs: DiscInputs,
    pool: Vec<[u8; 16]>,
    origin: Vec<&'static str>,
    requests: u32,
    trace: ResolutionTrace,
    // A source matched the disc and held a Media Key but had no VID (KU J23).
    km_needs_vid: bool,
    // The run's first source failure: a Missing refusal is this failure when there was one,
    // whichever piece's request failed (review B-1, KU-E1b); never E7022, so never E7034.
    first_failure: Option<Error>,
}

impl Run<'_> {
    fn check_halt(&self) -> Result<()> {
        match self.halt {
            Some(h) => h.check(),
            None => Ok(()),
        }
    }

    // Add keys to the pool (deduplicated): the pool only grows during `resolve`.
    fn add_keys(&mut self, keys: &[[u8; 16]], who: &'static str) {
        for k in keys {
            if !self.pool.contains(k) {
                self.pool.push(*k);
                self.origin.push(who);
            }
        }
    }

    fn opens(&self, unit: &[u8], slot: usize) -> bool {
        let mut u = unit.to_vec();
        decrypt_unit(&mut u, &self.pool[slot]);
        is_clean(&u, self.format)
    }

    // One request to source `i` with `samples`, retried only while it gives no answer (J13,
    // J15): `last_failure_was_transport()`, backoff 1 s doubling to 8 s, given up after 60 s
    // with no answer. Any answer ends it; a given-up or answered-with-failure source is dead.
    fn ask(&mut self, i: usize, samples: &[Vec<u8>], forensic: bool) -> Result<Asked> {
        if self.dead[i] {
            return Ok(Asked::Skipped);
        }
        self.requests += 1;
        let mut inputs = self.inputs.clone();
        inputs.samples = samples.to_vec();
        let ctx = DiscInputsCtx::new(&inputs).with_stop(self.halt, self.progress);
        let start = self.clock.now();
        let mut wait = RETRY_FIRST;
        let mut attempted = false;
        loop {
            if let Err(stop) = self.check_halt() {
                if attempted {
                    self.unanswered(i);
                }
                return Err(stop);
            }
            attempted = true;
            let src = &self.sources[i];
            let who = src.label().to_string();
            // Stop §2.1 item 2 (ST4-2): a source call (a keydb parse, a key-service call)
            // moves no CDB, so it is busy for the idle-only T29 probe.
            let busy = self.progress.map(Progress::busy);
            let answer = if forensic {
                src.get_fmts_indexes(&ctx).map(|k| (k, None))
            } else {
                src.resolve_unit_keys(&ctx).map(|r| {
                    let info = (r.matched, r.matched_entry, r.store_entries, r.miss_path);
                    (r.keys, Some(info))
                })
            };
            drop(busy);
            match answer {
                Ok((keys, info)) => {
                    let (matched, entry, store, miss) =
                        info.unwrap_or((false, None, None, Vec::new()));
                    self.km_needs_vid |= miss.contains(&KeyNode::NoVid);
                    let (path, outcome) = match (keys.is_empty(), matched) {
                        (false, _) => (vec![KeyNode::FoundUnitKeys], KeyOutcome::Resolved),
                        // The source's own reason (e.g. `NoVid`: `MissingVid`), else a bare no-key.
                        (true, true) => {
                            let outcome = if miss.contains(&KeyNode::NoVid) {
                                KeyOutcome::MissingVid
                            } else {
                                KeyOutcome::NoKey
                            };
                            let why = if miss.is_empty() {
                                vec![KeyNode::NoDerivableKey]
                            } else {
                                miss
                            };
                            ([vec![KeyNode::MatchedDisc], why].concat(), outcome)
                        }
                        (true, false) => (vec![KeyNode::NoEntry], KeyOutcome::NoKey),
                    };
                    self.trace.keys.push(KeyStep {
                        who,
                        path,
                        outcome,
                        matched_entry: entry,
                        store_entries: store,
                    });
                    return Ok(if keys.is_empty() {
                        Asked::Empty
                    } else {
                        Asked::Keys(keys.into_iter().map(|u| u.key).collect())
                    });
                }
                Err(Error::Halted) => {
                    // A source stopped mid-request: the request went out (minor 1).
                    self.unanswered(i);
                    return Err(Error::Halted);
                }
                Err(e) => {
                    let waited = self.clock.now().saturating_sub(start);
                    if src.last_failure_was_transport() && waited < NO_ANSWER_WINDOW {
                        tracing::info!(
                            target: "freemkv::keys",
                            who,
                            waited_secs = waited.as_secs(),
                            "key source gave no answer; retrying"
                        );
                        let pause = wait.min(NO_ANSWER_WINDOW - waited);
                        if let Err(stop) = self.clock.sleep(pause, self.halt) {
                            self.unanswered(i);
                            return Err(stop);
                        }
                        wait = (wait * 2).min(RETRY_CAP);
                        continue;
                    }
                    self.dead[i] = true;
                    self.trace.keys.push(KeyStep {
                        who,
                        path: Vec::new(),
                        outcome: KeyOutcome::NoKey,
                        matched_entry: None,
                        store_entries: None,
                    });
                    self.first_failure.get_or_insert(e);
                    return Ok(Asked::Failed);
                }
            }
        }
    }

    // A request to source `i` went out and a Stop ended its retries with no answer: the
    // trace keeps it, so a caller counting requests sees one was made.
    fn unanswered(&mut self, i: usize) {
        self.trace.keys.push(KeyStep {
            who: self.sources[i].label().to_string(),
            path: Vec::new(),
            outcome: KeyOutcome::NoKey,
            matched_entry: None,
            store_entries: None,
        });
    }

    // KU §2.3 step 9, rules 1 and 3–5, against the current pool. `Ask` means rule 2 applies:
    // an `Enc` probe no held key opens.
    fn judge(&self, p: &Piece) -> Verdict {
        if p.units == 0 {
            return Verdict::Clear;
        }
        let mut count = vec![0usize; self.pool.len()];
        let mut first_opened = None;
        let mut unopened = false;
        for u in &p.enc {
            match (0..self.pool.len()).find(|&s| self.opens(u, s)) {
                Some(s) => {
                    count[s] += 1;
                    first_opened.get_or_insert(s);
                }
                None => unopened = true,
            }
        }
        let best = (0..count.len()).max_by_key(|&s| (count[s], std::cmp::Reverse(s)));
        match best {
            // Rule 1: two probes, or the one probe of a one-unit piece.
            Some(s) if count[s] >= 2 || (p.units == 1 && count[s] == 1) => Verdict::Keyed(s),
            _ if unopened => Verdict::Ask,
            // Rule 3 (J21): one probe opened, never keyed from it.
            _ if p.enc.len() == 1 => Verdict::Lazy(first_opened),
            // Rule 4: every probe read clear.
            _ if p.enc.is_empty() && p.faults == 0 => Verdict::Clear,
            _ => Verdict::Lazy(first_opened),
        }
    }
}

fn sorted_ranges(mut v: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    v.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(v.len());
    for (s, e) in v {
        match out.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

fn overlaps(a: &[(u32, u32)], s: u32, e: u32) -> bool {
    a.iter().any(|&(x, y)| x < e && s < y)
}

// The pieces of `scope`: each content file the scope touches (on its own unit grid, KS-1),
// minus sectors an earlier file claimed (an SSIF re-lists m2ts sectors), then title extents
// no file covers (anchored at the extent start).
fn pieces(disc: &Disc, files: &[Vec<(u32, u32)>], sel: &[usize], whole: bool) -> Vec<Piece> {
    let title_ranges: Vec<(usize, u32, u32)> = sel
        .iter()
        .flat_map(|&t| {
            disc.titles[t]
                .extents
                .iter()
                .filter(|e| e.sector_count > 0)
                .map(move |e| (t, e.start_lba, e.start_lba.saturating_add(e.sector_count)))
        })
        .collect();
    let touches = |ranges: &[(u32, u32)]| -> Vec<usize> {
        let mut t: Vec<usize> = title_ranges
            .iter()
            .filter(|&&(_, s, e)| overlaps(ranges, s, e))
            .map(|&(t, _, _)| t)
            .collect();
        t.sort_unstable();
        t.dedup();
        t
    };
    let mut claimed: Vec<(u32, u32)> = Vec::new();
    let mut out = Vec::new();
    let push = |spans: Vec<UnitSpan>, out: &mut Vec<Piece>| {
        let ranges: Vec<(u32, u32)> = spans.iter().map(|&(s, n, _)| (s, s + n)).collect();
        let titles = touches(&ranges);
        let rank = titles
            .iter()
            .map(|&t| disc.titles[t].size_bytes)
            .max()
            .unwrap_or(0);
        out.push(Piece {
            spans,
            titles,
            rank,
            units: 0,
            enc: Vec::new(),
            faults: 0,
            verdict: Verdict::Ask,
        });
    };
    for file in files {
        let ends: Vec<(u32, u32)> = file.iter().map(|&(s, n)| (s, s + n)).collect();
        if !whole && touches(&ends).is_empty() {
            continue;
        }
        let mut spans = Vec::new();
        let mut off = 0u64;
        for &(lba, n) in file {
            let anchor = (lba as u64).saturating_sub(off % UNIT);
            for (s, e) in subtract_ranges(&[(lba, n)], &claimed) {
                spans.push((s, e - s, anchor));
            }
            off += n as u64;
        }
        claimed = sorted_ranges(claimed.into_iter().chain(ends).collect());
        if !spans.is_empty() {
            push(spans, &mut out);
        }
    }
    for &(_, s, e) in &title_ranges {
        let left = subtract_ranges(&[(s, e - s)], &claimed);
        if left.is_empty() {
            continue;
        }
        let spans = left.iter().map(|&(a, b)| (a, b - a, s as u64)).collect();
        claimed = sorted_ranges(claimed.into_iter().chain(left).collect());
        push(spans, &mut out);
    }
    out.sort_by_key(|p| p.id());
    out
}

fn in_segment(segments: &[(u32, u32)], lba: u32) -> bool {
    let i = segments.partition_point(|s| s.0 <= lba);
    i > 0 && lba < segments[i - 1].1
}

// KU §2.3 step 7: up to 32 units per piece on its grid, each `Enc`, `Clr` or `Fault`.
// FMTS segment units are not base probes (KU §2.3 step 6).
fn probe(
    reader: &mut dyn SectorSource,
    p: &mut Piece,
    segments: &[(u32, u32)],
    format: ContentFormat,
    halt: Option<&Halt>,
) -> Result<()> {
    let grid = p.grid();
    p.units = grid.iter().map(|g| g.1).sum();
    let mut buf = vec![0u8; ALIGNED_UNIT_LEN];
    for idx in probe_units(p.units) {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        let mut k = idx;
        let Some(&(head, _)) = grid.iter().find(|g| {
            let hit = k < g.1;
            if !hit {
                k -= g.1;
            }
            hit
        }) else {
            continue;
        };
        let Ok(lba) = u32::try_from(head + k * UNIT) else {
            continue;
        };
        if in_segment(segments, lba) {
            continue;
        }
        match reader.read_sectors(lba, UNIT as u16, &mut buf, false) {
            Ok(n) if n == buf.len() => {
                if aacs_unit_encrypted(&buf, format) {
                    p.enc.push(buf.clone());
                }
            }
            _ => p.faults += 1,
        }
    }
    Ok(())
}

fn missing_error(scope: &KeyScope, disc: &Disc) -> Error {
    match scope {
        KeyScope::WholeDisc => Error::WholeDiscKeyMissing,
        _ => Error::NoDiscKey {
            disc_hash: disc.aacs_disc_hash(),
        },
    }
}

/// The body of [`ResolvedKeySet::resolve`] with an injected clock.
pub(crate) fn resolve(
    disc: &Disc,
    reader: &mut dyn SectorSource,
    scope: KeyScope,
    sources: &KeySourceFactory,
    opts: ResolveKeysOptions,
    clock: &dyn Clock,
) -> Result<KeyResolution> {
    resolve_observed(disc, reader, scope, sources, opts, clock, None)
}

/// [`resolve`] reporting to the op's `progress` (stop design §2.1, T29).
pub(crate) fn resolve_observed(
    disc: &Disc,
    reader: &mut dyn SectorSource,
    scope: KeyScope,
    sources: &KeySourceFactory,
    opts: ResolveKeysOptions,
    clock: &dyn Clock,
    progress: Option<&Progress>,
) -> Result<KeyResolution> {
    let halt = opts.halt;
    if let Some(h) = halt {
        h.check()?;
    }
    // Step 1: not AACS, or nothing decrypted: no source call.
    let (Some(aacs), false) = (disc.aacs.as_ref(), scope == KeyScope::None) else {
        return Ok(KeyResolution {
            keys: ResolvedKeySet::none(),
            trace: ResolutionTrace::new(),
        });
    };
    let sel: Vec<usize> = match &scope {
        KeyScope::Titles(v) => {
            if let Some(&bad) = v.iter().find(|&&t| t >= disc.titles.len()) {
                return Err(Error::DiscTitleRange {
                    index: bad,
                    count: disc.titles.len(),
                });
            }
            v.clone()
        }
        _ => (0..disc.titles.len()).collect(),
    };
    // Step 3: KS-14 [BD] §3.9.3 "Num_of_CPS_Unit field (16 bits) indicates the number of CPS
    // Units on the disc" — the declared count; `None` (unparseable) fails closed.
    let n_decl = disc.declared_cps_units();
    let seed = opts.seed.filter(|s| s.is_aacs() && s.is_for(disc));
    // Step 4: the VID, in memory only.
    let vid = Some(aacs.volume_id)
        .filter(|v| *v != [0u8; 16])
        .or(opts.vid)
        .or(seed.and_then(|s| s.0.vid));
    let mut inputs = disc.inputs().unwrap_or_else(|| DiscInputs {
        disc_hash: aacs.disc_hash.clone(),
        volume_id: [0u8; 16],
        version: aacs.version,
        mkb: Vec::new(),
        unit_key_ro: Vec::new(),
        samples: Vec::new(),
        volume_label: None,
    });
    inputs.volume_id = vid.unwrap_or([0u8; 16]);
    let built = sources();
    let n_src = built.len();
    let vid_consumer = built.iter().any(|s| s.uses_vid());
    let mut run = Run {
        halt,
        progress,
        clock,
        format: disc.content_format,
        sources: built,
        dead: vec![false; n_src],
        inputs,
        pool: Vec::new(),
        origin: Vec::new(),
        requests: 0,
        trace: ResolutionTrace::new(),
        km_needs_vid: false,
        first_failure: None,
    };
    if let Some(s) = seed {
        run.add_keys(&s.0.pool, "seed");
    }
    let result = if disc.format == DiscFormat::HdDvd {
        resolve_hddvd(disc, reader, &scope, &sel, n_decl, &mut run)
    } else {
        resolve_bd(disc, reader, &scope, &sel, n_decl, seed, &mut run)
    };
    // Every source is dropped here: nothing can ask after `resolve` (LK7).
    let trace = std::mem::take(&mut run.trace);
    drop(run.sources);
    if let Some(out) = opts.trace {
        *out.lock().unwrap_or_else(|e| e.into_inner()) = trace.clone();
    }
    // KU J23: the VID would help only through a Km path or a source that consumes it.
    if let Some(flag) = opts.vid_would_help
        && vid.is_none()
        && (run.km_needs_vid || vid_consumer)
    {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    // The final check (Stop §6 ST-L3): a Stop during the last source call ends `Halted`,
    // never a refusal or a set.
    if let Some(h) = halt {
        h.check()?;
    }
    let mut inner = result?;
    inner.disc_hash = aacs.disc_hash.clone();
    inner.capacity = disc.capacity_sectors;
    inner.format = disc.format;
    inner.content_format = disc.content_format;
    inner.vid = vid;
    inner.n_decl = n_decl;
    inner.scope = scope;
    inner.requests = run.requests;
    tracing::info!(
        target: "freemkv::keys",
        requests = inner.requests,
        keyed = inner.keyed,
        clear = inner.clear,
        lazy = inner.arrival.len().saturating_sub(inner.clear),
        forensic = ?inner.forensic,
        declared = ?n_decl,
        "keys: resolved up front"
    );
    Ok(KeyResolution {
        keys: ResolvedKeySet(Arc::new(inner)),
        trace,
    })
}

// A fresh `Inner` for an AACS set, filled by the caller with the disc identity.
fn aacs_inner(run: &Run) -> Inner {
    let mut inner = Inner::empty();
    inner.aacs = true;
    inner.pool = run.pool.clone();
    inner.proofs = ProofCache::default();
    inner
}

// KU §2.6: HD DVD is best effort. Not probed (its encrypted flag is unverified, KS-27):
// a single declared key is applied to every piece; several, or none parseable, refuse.
fn resolve_hddvd(
    disc: &Disc,
    reader: &mut dyn SectorSource,
    scope: &KeyScope,
    sel: &[usize],
    n_decl: Option<usize>,
    run: &mut Run,
) -> Result<Inner> {
    if n_decl != Some(1) {
        tracing::error!(target: "freemkv::keys", declared = ?n_decl, "multi-key HD DVD cannot be matched reliably");
        return Err(Error::NoDiscKey {
            disc_hash: disc.aacs_disc_hash(),
        });
    }
    let files = crate::whole_disc::content_files(reader).unwrap_or_default();
    let ps = pieces(disc, &files, sel, *scope == KeyScope::WholeDisc);
    if run.pool.is_empty() {
        let samples = disc.content_samples(reader, MIN_SAMPLE_UNITS);
        for i in 0..run.sources.len() {
            if let Asked::Keys(k) = run.ask(i, &samples, false)? {
                let who = run.sources[i].label();
                run.add_keys(&k[..1], who);
                break;
            }
        }
        if run.pool.is_empty() {
            let failure = run.first_failure.take();
            return Err(failure.unwrap_or_else(|| missing_error(scope, disc)));
        }
    }
    let mut inner = aacs_inner(run);
    inner.best_effort = true;
    inner.origin = run.origin.first().copied();
    inner.proven = vec![0];
    inner.keyed = ps.len();
    let mut ranges = Vec::new();
    for p in &ps {
        ranges.extend(p.ranges().map(|(s, e)| (s, e, 0usize)));
        inner.spans.extend(p.spans.iter().copied());
    }
    inner.map = Arc::new(AacsKeyMap::from_ranges(ranges));
    Ok(inner)
}

// KU §2.3 steps 5–14 for BD/UHD content.
fn resolve_bd(
    disc: &Disc,
    reader: &mut dyn SectorSource,
    scope: &KeyScope,
    sel: &[usize],
    n_decl: Option<usize>,
    seed: Option<&ResolvedKeySet>,
    run: &mut Run,
) -> Result<Inner> {
    let format = disc.content_format;
    // Step 5: pieces. An unreadable filesystem fails a whole-disc copy (it would otherwise
    // ship ciphertext); a title rip keys its title extents instead.
    let fs = match crate::udf::read_filesystem(reader) {
        Ok(fs) => Some(fs),
        Err(e) if *scope == KeyScope::WholeDisc => return Err(e),
        Err(e) => {
            tracing::warn!(target: "freemkv::keys", error = %e, "no filesystem: keying title extents");
            None
        }
    };
    let files = match &fs {
        Some(fs) => match crate::whole_disc::content_files_in(fs, reader) {
            Ok(f) => f,
            Err(e) if *scope == KeyScope::WholeDisc => return Err(e),
            Err(_) => Vec::new(),
        },
        None => Vec::new(),
    };
    let mut ps = pieces(disc, &files, sel, *scope == KeyScope::WholeDisc);
    // Step 6: FMTS, when the forensic clip is in scope.
    let layout = match &fs {
        Some(fs) => super::fmts::layout(fs, reader)?,
        None => None,
    };
    let clip: Vec<(u32, u32)> = layout
        .as_ref()
        .map(|l| {
            l.clip
                .iter()
                .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
                .collect()
        })
        .unwrap_or_default();
    let layout = layout.filter(|_| {
        ps.iter()
            .any(|p| p.ranges().any(|(s, e)| overlaps(&clip, s, e)))
    });
    let segments: Vec<(u32, u32)> = layout
        .as_ref()
        .map(|l| sorted_ranges(l.ranges.iter().map(|&(s, e, _)| (s, e)).collect()))
        .unwrap_or_default();
    // Step 7: probes.
    for p in &mut ps {
        probe(reader, p, &segments, format, run.halt)?;
    }
    // Step 8: sample-independent sources, once, with the main title's samples.
    let needs_keys = ps
        .iter()
        .any(|p| p.units > 0 && (!p.enc.is_empty() || p.faults > 0));
    if needs_keys {
        let main = disc.main_title().map(|t| {
            t.extents
                .iter()
                .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
                .collect::<Vec<_>>()
        });
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for p in ps.iter().filter(|p| {
            main.as_ref()
                .is_none_or(|m| p.ranges().any(|(s, e)| overlaps(m, s, e)))
        }) {
            samples.extend(p.enc.iter().take(MIN_SAMPLE_UNITS).cloned());
        }
        samples.truncate(SAMPLE_CAP);
        for i in 0..run.sources.len() {
            if run.sources[i].answer_depends_on_samples() {
                continue;
            }
            if let Asked::Keys(k) = run.ask(i, &samples, false)? {
                let who = run.sources[i].label();
                run.add_keys(&k, who);
            }
        }
    }
    // Step 9: verdicts from the pool, then rule 2 for the rest, largest title first.
    for p in &mut ps {
        p.verdict = run.judge(p);
    }
    let mut order: Vec<usize> = (0..ps.len())
        .filter(|&i| ps[i].verdict == Verdict::Ask)
        .collect();
    order.sort_by_key(|&i| (std::cmp::Reverse(ps[i].rank), ps[i].id()));
    // Step 11: sample-dependent requests ≤ n_decl (or the number of pieces).
    let mut budget = n_decl.unwrap_or(ps.len());
    for &i in &order {
        run.check_halt()?;
        let v = run.judge(&ps[i]);
        if v != Verdict::Ask {
            ps[i].verdict = v;
            continue;
        }
        let samples = borrow_samples(run, &ps, i);
        if samples.len() < MIN_SAMPLE_UNITS {
            // Step 9.2: too little ciphertext to ask with: Lazy, proven on arrival.
            ps[i].verdict = Verdict::Lazy(None);
            continue;
        }
        let dependent: Vec<usize> = (0..run.sources.len())
            .filter(|&s| run.sources[s].answer_depends_on_samples())
            .collect();
        if budget == 0 || dependent.is_empty() {
            ps[i].verdict = Verdict::Missing;
            continue;
        }
        budget -= 1;
        for s in dependent {
            if let Asked::Keys(k) = run.ask(s, &samples, false)? {
                let who = run.sources[s].label();
                run.add_keys(&k, who);
                if run.judge(&ps[i]) != Verdict::Ask {
                    break;
                }
            }
        }
        // Proven on this piece's own units: ≥ 2 → Keyed; an unopened unit → Missing. A piece
        // with a single own `Enc` unit that the answer opens stays Lazy (J21).
        ps[i].verdict = match run.judge(&ps[i]) {
            Verdict::Ask => Verdict::Missing,
            v => v,
        };
    }
    apply_single_unit_rule(run, &mut ps, n_decl)?;
    if let Some(i) = ps.iter().position(|p| p.verdict == Verdict::Missing) {
        let lba = ps[i].id();
        let failure = run.first_failure.take();
        let err = failure.unwrap_or_else(|| missing_error(scope, disc));
        tracing::error!(target: "freemkv::keys", lba, code = err.code(), "a stream file in scope has no key; refusing before any output");
        return Err(err);
    }
    // KU §2.7: an empty pool on an encrypted scope is today's keyless case.
    if run.pool.is_empty() && ps.iter().any(|p| matches!(p.verdict, Verdict::Lazy(_))) {
        let failure = run.first_failure.take();
        return Err(failure.unwrap_or_else(|| missing_error(scope, disc)));
    }
    let mut inner = aacs_inner(run);
    // Step 6 (continued): forensic keys, reused from the seed or anchored once.
    let forensic = match &layout {
        None => None,
        Some(l) => Some(resolve_forensic(reader, l, seed, run, format)?),
    };
    build(&mut inner, &ps, layout.as_ref(), forensic, run, clip);
    Ok(inner)
}

// Step 9.2: this piece's unopened `Enc` units first, topped up from other unopened pieces
// of the same playlist, never across playlists (SG23).
fn borrow_samples(run: &Run, ps: &[Piece], i: usize) -> Vec<Vec<u8>> {
    // KS-10 [BD] §3.9.2: "All AV stream files that are referred to by one Title are included
    // in the same CPS Unit" — so only pieces sharing a title may lend units.
    let unopened = |p: &Piece| -> Vec<Vec<u8>> {
        p.enc
            .iter()
            .filter(|u| !(0..run.pool.len()).any(|s| run.opens(u, s)))
            .cloned()
            .collect()
    };
    let mut out = unopened(&ps[i]);
    for (j, q) in ps.iter().enumerate() {
        if out.len() >= SAMPLE_CAP {
            break;
        }
        if j == i || q.verdict != Verdict::Ask || !q.titles.iter().any(|t| ps[i].titles.contains(t))
        {
            continue;
        }
        out.extend(unopened(q));
    }
    out.truncate(SAMPLE_CAP);
    out
}

// Step 10: the `n_decl == 1` rule.
fn apply_single_unit_rule(run: &Run, ps: &mut [Piece], n_decl: Option<usize>) -> Result<()> {
    // KS-14 [BD] §3.9.3: "Num_of_CPS_Unit field (16 bits) indicates the number of CPS Units
    // on the disc" — with one declared, a key proven on one piece keys them all.
    if n_decl != Some(1) {
        return Ok(());
    }
    if let Some(slot) = ps.iter().find_map(|p| match p.verdict {
        Verdict::Keyed(s) => Some(s),
        _ => None,
    }) {
        for p in ps.iter_mut() {
            // J22 ("never guessed"; J21 parity): keyed only on step 9's evidence, the key
            // opening every Enc probe, two or more (or one of a one-unit piece); else Lazy.
            let opens_all = !p.enc.is_empty() && p.enc.iter().all(|u| run.opens(u, slot));
            let enough = p.enc.len() >= 2 || p.units == 1;
            match p.verdict {
                Verdict::Lazy(_) if opens_all && enough => p.verdict = Verdict::Keyed(slot),
                Verdict::Lazy(_) if opens_all || p.enc.is_empty() => {
                    p.verdict = Verdict::Lazy(Some(slot));
                }
                _ => {}
            }
        }
        return Ok(());
    }
    let any_enc = ps.iter().any(|p| !p.enc.is_empty());
    let any_opened = ps
        .iter()
        .flat_map(|p| &p.enc)
        .any(|u| (0..run.pool.len()).any(|s| run.opens(u, s)));
    if any_enc && !run.pool.is_empty() && !any_opened {
        tracing::error!(
            target: "freemkv::keys",
            code = crate::error::E_DECRYPT_FAILED,
            "one CPS unit declared and the held key opens none of the ciphertext: wrong key"
        );
        return Err(Error::DecryptFailed);
    }
    Ok(())
}

// The forensic outcome: resolved keys and phases, or Pending.
enum Forensic {
    Resolved(Vec<[u8; 16]>, HashMap<u16, Phase>),
    Pending,
}

fn resolve_forensic(
    reader: &mut dyn SectorSource,
    layout: &super::fmts::Layout,
    seed: Option<&ResolvedKeySet>,
    run: &mut Run,
    format: ContentFormat,
) -> Result<Forensic> {
    if layout.unresolved {
        return Err(Error::FmtsKeyMissing);
    }
    if let Some(s) = seed.filter(|s| s.0.forensic == ForensicState::Resolved) {
        return Ok(Forensic::Resolved(
            s.0.fmts_keys.clone(),
            s.0.fmts_phases.clone(),
        ));
    }
    let halt = run.halt;
    // A failed request is kept as the run's first failure, not returned per batch.
    let mut ask = |batch: &[Vec<u8>]| -> Result<Option<Vec<[u8; 16]>>> {
        for i in 0..run.sources.len() {
            if let Asked::Keys(k) = run.ask(i, batch, true)? {
                return Ok(Some(k));
            }
        }
        Ok(None)
    };
    let keys = match super::fmts::anchor(reader, layout, halt, &mut ask)? {
        super::fmts::Anchor::Keys(k) => k,
        super::fmts::Anchor::Pending => {
            tracing::warn!(target: "freemkv::keys", "fmts: every index-1 anchor read faulted: forensic keys pending");
            return Ok(Forensic::Pending);
        }
        super::fmts::Anchor::Missing(failure) => {
            let failure = failure.or(run.first_failure.take());
            return Err(failure.unwrap_or(Error::FmtsKeyMissing));
        }
    };
    // Every segment must map to a held index key; a hole would fall to the base key.
    if layout
        .ranges
        .iter()
        .any(|&(_, _, idx)| idx == 0 || idx as usize > keys.len())
    {
        return Err(Error::FmtsKeyMissing);
    }
    match super::fmts::phases(reader, layout, &keys, format, halt)? {
        Some(p) => Ok(Forensic::Resolved(keys, p)),
        None => Err(Error::FmtsKeyMissing),
    }
}

// Step 14: maps from Keyed pieces (and the forensic ranges); arrival pieces for the rest.
fn build(
    inner: &mut Inner,
    ps: &[Piece],
    layout: Option<&super::fmts::Layout>,
    forensic: Option<Forensic>,
    run: &Run,
    clip: Vec<(u32, u32)>,
) {
    let base = run.pool.len();
    let mut ranges: Vec<(u32, u32, usize, Phase)> = Vec::new();
    let mut seg_ranges: Vec<(u32, u32, usize, Phase)> = Vec::new();
    if let (Some(l), Some(f)) = (layout, &forensic) {
        inner.forensic_clip = clip.clone();
        inner.segments = sorted_ranges(l.ranges.iter().map(|&(s, e, _)| (s, e)).collect());
        match f {
            Forensic::Pending => inner.forensic = ForensicState::Pending,
            Forensic::Resolved(keys, phases) => {
                inner.forensic = ForensicState::Resolved;
                inner.fmts_keys = keys.clone();
                inner.fmts_phases = phases.clone();
                for &(s, e, idx) in &l.ranges {
                    let slot = base + (idx as usize).saturating_sub(1);
                    if (idx as usize) <= keys.len() && idx > 0 {
                        let phase = phases.get(&idx).copied().unwrap_or(Phase::Verify);
                        seg_ranges.push((s, e, slot, phase));
                    }
                }
            }
        }
    }
    let mut proven = Vec::new();
    for p in ps {
        inner.spans.extend(p.spans.iter().copied());
        match p.verdict {
            Verdict::Keyed(slot) => {
                inner.keyed += 1;
                if !proven.contains(&slot) {
                    proven.push(slot);
                }
                inner.origin = inner.origin.or(run.origin.get(slot).copied());
                let extents: Vec<Extent> = p
                    .ranges()
                    .map(|(s, e)| Extent {
                        start_lba: s,
                        sector_count: e - s,
                    })
                    .collect();
                // Base content around the forensic segments keeps the piece's own key (K-3).
                ranges.extend(crate::mux::resolve::fill_base_key_gaps(
                    &extents,
                    &seg_ranges,
                    slot,
                ));
            }
            Verdict::Clear | Verdict::Lazy(_) => {
                if p.verdict == Verdict::Clear {
                    inner.clear += 1;
                } else {
                    inner.lazy.extend(p.ranges());
                }
                inner.arrival.push(ArrivalPiece {
                    id: p.id(),
                    spans: p.spans.clone(),
                    candidate: match p.verdict {
                        Verdict::Lazy(c) => c,
                        _ => None,
                    },
                });
            }
            Verdict::Missing | Verdict::Ask => {}
        }
    }
    inner.lazy = sorted_ranges(std::mem::take(&mut inner.lazy));
    inner.spans.sort_unstable_by_key(|s| s.0);
    inner.proven = proven;
    ranges.extend(seg_ranges);
    ranges.sort_unstable_by_key(|r| r.0);
    inner.map = Arc::new(AacsKeyMap::from_ranges_phased(ranges));
}
