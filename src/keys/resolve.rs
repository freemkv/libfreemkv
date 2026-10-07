//! [`KeyRing::acquire`] (KU §2.3): pieces, probes, sources, verdicts, over [`KeyEvidence`].

use super::evidence::{Detector, KeyEvidence, Piece, Sample, Sampler, sorted_ranges};
use super::{
    AcquireOptions, ArrivalPiece, ForensicState, Inner, KeyResolution, KeyRing, KeyScope,
    ProofCache, overlaps,
};
use crate::aacs::content::{decrypt_unit, is_clean};
use crate::aacs::trace::{KeyNode, KeyOutcome, KeyStep, ResolutionTrace};
use crate::ctx::Ctx;
use crate::decrypt::{AacsKeyMap, Phase};
use crate::disc::{ContentFormat, Extent};
use crate::error::{Error, Result};
use crate::halt::{Halt, Liveness};
use crate::keysource::{DiscInputs, DiscInputsCtx, KeySource, MIN_SAMPLE_UNITS};
use crate::session::KeySourceFactory;
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
// Most keys taken from one source answer: real discs declare few CPS units; a source cannot
// flood the pool or starve a later source.
const MAX_POOL_KEYS: usize = 256;

/// Time for the J13 retry: injected so tests use a fake clock and never sleep.
pub(crate) trait Clock {
    fn now(&self) -> Duration;
    /// Wait `d`, returning `Err(Halted)` as soon as `halt` is cancelled.
    fn sleep(&self, d: Duration, halt: &Halt) -> Result<()>;
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
    fn sleep(&self, d: Duration, halt: &Halt) -> Result<()> {
        halt.wait(d)
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

// One piece in scope with its probes.
struct Probed {
    piece: Piece,
    units: u64,
    enc: Vec<Vec<u8>>,
    faults: usize,
    verdict: Verdict,
}

impl Probed {
    fn new(piece: Piece) -> Self {
        Probed {
            piece,
            units: 0,
            enc: Vec::new(),
            faults: 0,
            verdict: Verdict::Ask,
        }
    }

    fn id(&self) -> u32 {
        self.piece.id()
    }

    fn ranges(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.piece.ranges()
    }
}

// What one request to one source produced.
enum Asked {
    /// The keys, and each one's number (CPS unit / HD DVD Title Key entry).
    Keys(Vec<[u8; 16]>, Vec<u32>),
    Empty,
    /// The source failed; the error is the run's `first_failure` if it came first.
    Failed,
    Skipped,
}

// The state of one `resolve` run.
struct Run<'a> {
    halt: &'a Halt,
    // The op's progress: busy around each source call (stop design §2.1 item 2).
    progress: Option<&'a Liveness>,
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
        self.halt.check()
    }

    // Add keys to the pool (deduplicated): the pool only grows during `resolve`.
    fn add_keys(&mut self, keys: &[[u8; 16]], who: &'static str) {
        if keys.len() > MAX_POOL_KEYS {
            tracing::warn!(target: "freemkv::keys", who, "source answer truncated: extra keys ignored");
        }
        for k in keys.iter().take(MAX_POOL_KEYS) {
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
        let ctx = DiscInputsCtx::new(&inputs).with_stop(Some(self.halt), self.progress);
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
            let busy = self.progress.map(Liveness::busy);
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
                        Asked::Keys(
                            keys.iter().map(|u| u.key).collect(),
                            keys.iter().map(|u| u.idx).collect(),
                        )
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
    fn judge(&self, p: &Probed) -> Verdict {
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

// A loose clip file's one piece, proven on arrival.
pub(crate) fn loose_file_piece(capacity: u32) -> ArrivalPiece {
    let p = Piece::loose_file(capacity);
    ArrivalPiece {
        id: p.id(),
        spans: p.spans,
        candidate: None,
        clear: false,
    }
}

// Each whole-disc piece's unit spans, for the unit-grid guards in `whole_disc_tests`.
#[cfg(test)]
pub(crate) fn whole_disc_pieces(
    disc: &crate::disc::Disc,
    files: &[Vec<(u32, u32)>],
) -> Vec<Vec<crate::whole_disc::UnitSpan>> {
    let sel: Vec<usize> = (0..disc.titles.len()).collect();
    super::evidence::pieces(disc, files, &sel, true)
        .into_iter()
        .map(|p| p.spans)
        .collect()
}

// KU §2.3 step 7: up to 32 units per piece on its grid, each `Enc`, `Clr` or `Fault`.
// FMTS segment units are not base probes (KU §2.3 step 6).
fn probe(
    sampler: &mut Sampler,
    p: &mut Probed,
    segments: &[(u32, u32)],
    format: ContentFormat,
    detector: Detector,
    halt: &Halt,
) -> Result<()> {
    let (units, samples) = sampler.probe(&p.piece, segments, format, detector, halt)?;
    p.units = units;
    for s in samples {
        match s {
            Sample::Enc(u) => p.enc.push(u),
            Sample::Clear => {}
            Sample::Fault => p.faults += 1,
        }
    }
    Ok(())
}

fn missing_error(scope: &KeyScope, disc_hash: &str) -> Error {
    match scope {
        KeyScope::WholeDisc => Error::WholeDiscKeyMissing,
        _ => Error::NoDiscKey {
            disc_hash: crate::hex::strip_hex_prefix(disc_hash).to_string(),
        },
    }
}

/// The body of [`KeyRing::acquire`] with an injected clock.
pub(crate) fn acquire(
    ev: &KeyEvidence,
    sampler: &mut Sampler,
    sources: &KeySourceFactory,
    opts: AcquireOptions,
    ctx: &Ctx,
    clock: &dyn Clock,
) -> Result<KeyResolution> {
    let halt = &ctx.halt;
    halt.check()?;
    // Step 1: not AACS, or nothing decrypted: no source call.
    let (Some(aacs), false) = (ev.aacs.as_ref(), ev.scope == KeyScope::None) else {
        return Ok(KeyResolution {
            keys: KeyRing::none(),
            trace: ResolutionTrace::new(),
        });
    };
    // Step 3: KS-14 [BD] §3.9.3 "Num_of_CPS_Unit field (16 bits) indicates the number of CPS
    // Units on the disc" — the declared count; `None` (unparseable) fails closed.
    let n_decl = aacs.n_declared;
    // KA9: evidence with no captured hash is still looked up by its title-key file.
    let disc_hash = if ev.media.disc_hash.is_empty() && !aacs.unit_key_ro.is_empty() {
        crate::aacs::inf::disc_hash_hex(&crate::aacs::inf::disc_hash(&aacs.unit_key_ro))
    } else {
        ev.media.disc_hash.clone()
    };
    let media = super::MediaId {
        disc_hash: disc_hash.clone(),
        ..ev.media.clone()
    };
    let seed = opts.seed.filter(|s| s.is_aacs() && s.is_for(&media));
    // Step 4: the VID, in memory only.
    let vid = aacs.vid.or(opts.vid).or(seed.and_then(|s| s.0.vid));
    let inputs = DiscInputs {
        disc_hash: disc_hash.clone(),
        volume_id: vid.unwrap_or([0u8; 16]),
        version: aacs.version,
        mkb: aacs.mkb.clone(),
        unit_key_ro: aacs.unit_key_ro.clone(),
        samples: Vec::new(),
        volume_label: aacs.volume_label.clone(),
    };
    let built = sources();
    let n_src = built.len();
    let vid_consumer = built.iter().any(|s| s.uses_vid());
    let mut run = Run {
        halt,
        progress: opts.liveness,
        clock,
        format: ev.container,
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
    let result = decide(ev, sampler, &disc_hash, n_decl, seed, &mut run);
    // Every source is dropped here: nothing can ask after `acquire` (LK7).
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
    halt.check()?;
    let mut inner = result?;
    inner.disc_hash = disc_hash;
    inner.capacity = media.capacity;
    inner.format = media.format;
    inner.content_format = ev.container;
    inner.vid = vid;
    inner.n_decl = n_decl;
    inner.scope = ev.scope.clone();
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
        keys: KeyRing(Arc::new(inner)),
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

// The pieces in scope with their probes, and the forensic layout when its clip is in scope.
struct Scoped<'e> {
    ps: Vec<Probed>,
    layout: Option<&'e super::fmts::Layout>,
    clip: Vec<(u32, u32)>,
}

// KU §2.3 steps 5–7, every AACS format: the evidence's pieces, the forensic layout when its
// clip is in scope, and each piece probed by the format's own detector.
fn probe_scope<'e>(ev: &'e KeyEvidence, sampler: &mut Sampler, halt: &Halt) -> Result<Scoped<'e>> {
    let mut ps: Vec<Probed> = ev.pieces.iter().cloned().map(Probed::new).collect();
    let (layout, clip) = scope_layout(ev);
    let segments: Vec<(u32, u32)> = layout
        .map(|l| sorted_ranges(l.ranges.iter().map(|&(s, e, _)| (s, e)).collect()))
        .unwrap_or_default();
    let t0 = std::time::Instant::now();
    let before = sampler.stats();
    for p in &mut ps {
        let tp = std::time::Instant::now();
        let reads_before = sampler.stats().reads;
        probe(sampler, p, &segments, ev.container, ev.detector, halt)?;
        tracing::debug!(
            target: "freemkv::keys",
            phase = "probe_piece",
            piece = p.id(),
            units = p.units,
            reads = sampler.stats().reads - reads_before,
            enc = p.enc.len(),
            faults = p.faults,
            elapsed_ms = tp.elapsed().as_millis() as u64,
            "piece probed"
        );
    }
    let after = sampler.stats();
    let reads = after.reads - before.reads;
    let read_ms = (after.total - before.total).as_millis() as u64;
    tracing::info!(
        target: "freemkv::keys",
        phase = "probe_scope",
        pieces = ps.len(),
        reads,
        faults = after.faults - before.faults,
        read_ms,
        avg_read_ms = read_ms.checked_div(u64::from(reads)).unwrap_or(0),
        slowest_read_ms = after.slowest.as_millis() as u64,
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "sampled every piece before asking a key source"
    );
    Ok(Scoped { ps, layout, clip })
}

// The forensic layout when its clip is in scope (KU §2.3 step 6), and that clip. Reads nothing.
fn scope_layout(ev: &KeyEvidence) -> (Option<&super::fmts::Layout>, Vec<(u32, u32)>) {
    let layout = ev.fmts.as_ref();
    let clip: Vec<(u32, u32)> = layout
        .map(|l| {
            l.clip
                .iter()
                .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
                .collect()
        })
        .unwrap_or_default();
    let layout = layout.filter(|_| {
        ev.pieces
            .iter()
            .any(|p| p.ranges().any(|(s, e)| overlaps(&clip, s, e)))
    });
    (layout, clip)
}

// KS-14 [BD] §3.9.3: one declared CPS unit is one Unit Key for every stream file, so the disc
// is trusted as declared: the main title's samples ask the sources once, and the key that
// opens them keys every piece in scope. No per-piece probing (32 reads a piece: minutes of
// seeks on a many-clip disc). `None` (no ciphertext found, or no key opens it) falls back to
// the probed path, which decides clear content and refusals.
fn resolve_one_unit(
    ev: &KeyEvidence,
    sampler: &mut Sampler,
    disc_hash: &str,
    seed: Option<&KeyRing>,
    run: &mut Run,
) -> Result<Option<Inner>> {
    let samples = sampler.main_samples(ev.main.as_ref(), MIN_SAMPLE_UNITS);
    if samples.is_empty() {
        return Ok(None);
    }
    // Two opened samples prove a key (rule 1), or the one when only one was found.
    let need = samples.len().min(2);
    let opener = |run: &Run| {
        (0..run.pool.len())
            .find(|&slot| samples.iter().filter(|u| run.opens(u, slot)).count() >= need)
    };
    let mut slot = opener(run);
    // Sample-independent sources (a local keydb) before those that need the samples.
    let mut order: Vec<usize> = (0..run.sources.len()).collect();
    order.sort_by_key(|&i| run.sources[i].answer_depends_on_samples());
    let mut tried: Vec<usize> = Vec::new();
    run.check_halt()?;
    for i in order {
        if slot.is_some() {
            break;
        }
        // Too little ciphertext to ask with unambiguously (KU step 9.2): the probed path decides.
        if run.sources[i].answer_depends_on_samples() && samples.len() < MIN_SAMPLE_UNITS {
            continue;
        }
        run.check_halt()?;
        if let Asked::Keys(k, _) = run.ask(i, &samples, false)? {
            let who = run.sources[i].label();
            run.add_keys(&k, who);
            slot = opener(run);
        }
        tried.push(i);
    }
    let Some(slot) = slot else {
        // Encrypted, sources asked, no key held at all: one declared unit means nothing in
        // scope can be keyed, so refuse now rather than probe every piece to the same end.
        if !tried.is_empty() && run.pool.is_empty() {
            let failure = run.first_failure.take();
            return Err(failure.unwrap_or_else(|| missing_error(&ev.scope, disc_hash)));
        }
        // Asked already: the probed path never asks these sources again.
        for i in tried {
            run.dead[i] = true;
        }
        return Ok(None);
    };
    let (layout, clip) = scope_layout(ev);
    let ps: Vec<Probed> = ev
        .pieces
        .iter()
        .cloned()
        .map(|piece| {
            let mut p = Probed::new(piece);
            p.units = p.piece.grid().iter().map(|g| g.1).sum();
            p.verdict = if p.units == 0 {
                Verdict::Clear
            } else {
                Verdict::Keyed(slot)
            };
            p
        })
        .collect();
    tracing::info!(
        target: "freemkv::keys",
        phase = "one_cps_unit",
        pieces = ps.len(),
        samples = samples.len(),
        "one CPS unit declared: every piece keyed by the key that opens the main title"
    );
    let mut inner = aacs_inner(run);
    inner.no_stream_files = ev.no_stream_files;
    let forensic = match layout {
        None => None,
        Some(l) => Some(resolve_forensic(sampler, l, seed, run, ev.container)?),
    };
    build(&mut inner, &ps, layout, forensic, run, clip);
    Ok(Some(inner))
}

// Several declared CPS units: each stream file sits in exactly one (KS-10), so one encrypted
// unit per piece (the first its probe grid finds) names its key. The main title's samples ask
// first; pieces no held key opens then ask with their own units, at most `n_decl` requests.
// `None` (a piece unreadable, or one no key opens) falls back to the probed path.
fn resolve_per_unit(
    ev: &KeyEvidence,
    sampler: &mut Sampler,
    disc_hash: &str,
    seed: Option<&KeyRing>,
    run: &mut Run,
    n_decl: usize,
) -> Result<Option<Inner>> {
    let (layout, clip) = scope_layout(ev);
    let segments: Vec<(u32, u32)> = layout
        .map(|l| sorted_ranges(l.ranges.iter().map(|&(s, e, _)| (s, e)).collect()))
        .unwrap_or_default();
    let mut ps: Vec<Probed> = Vec::with_capacity(ev.pieces.len());
    for piece in &ev.pieces {
        let mut p = Probed::new(piece.clone());
        let (units, unit, faults) =
            sampler.first_encrypted(&p.piece, &segments, ev.container, ev.detector, run.halt)?;
        p.units = units;
        p.faults = faults;
        match unit {
            Some(u) => p.enc.push(u),
            // Faults with no ciphertext found: not trusted, the probed path decides.
            None if faults > 0 => return Ok(None),
            None => p.verdict = Verdict::Clear,
        }
        ps.push(p);
    }
    let opened = |run: &Run, p: &Probed| -> Option<usize> {
        p.enc
            .first()
            .and_then(|u| (0..run.pool.len()).find(|&s| run.opens(u, s)))
    };
    let unopened = |run: &Run, ps: &[Probed]| -> Vec<usize> {
        (0..ps.len())
            .filter(|&i| ps[i].verdict == Verdict::Ask && opened(run, &ps[i]).is_none())
            .collect()
    };
    let mut tried: Vec<usize> = Vec::new();
    // Sample-independent sources (a local keydb) give their whole answer once.
    if !unopened(run, &ps).is_empty() {
        let main = sampler.main_samples(ev.main.as_ref(), MIN_SAMPLE_UNITS);
        for i in 0..run.sources.len() {
            if run.sources[i].answer_depends_on_samples() {
                continue;
            }
            run.check_halt()?;
            let asked = run.ask(i, &main, false)?;
            if !matches!(asked, Asked::Skipped) {
                tried.push(i);
            }
            if let Asked::Keys(k, _) = asked {
                let who = run.sources[i].label();
                run.add_keys(&k, who);
            }
        }
    }
    // Then, largest piece first, each still-unopened unit asks the sample-dependent sources
    // with enough of its own ciphertext (topped up from pieces of the same title, KS-10), at
    // most `n_decl` requests. One answer may open many pieces. A source that answers with no
    // key has none for this disc: it is not asked again.
    let mut budget = n_decl;
    let mut thin: Vec<usize> = Vec::new();
    let mut gave_up: Vec<usize> = Vec::new();
    loop {
        let mut todo: Vec<usize> = unopened(run, &ps)
            .into_iter()
            .filter(|i| !thin.contains(i) && !gave_up.contains(i))
            .collect();
        let live: Vec<usize> = (0..run.sources.len())
            .filter(|&i| run.sources[i].answer_depends_on_samples() && !run.dead[i])
            .collect();
        if todo.is_empty() || budget == 0 || live.is_empty() {
            break;
        }
        todo.sort_by_key(|&i| (std::cmp::Reverse(ps[i].piece.rank), ps[i].id()));
        let lead = todo[0];
        let mut samples = sampler.encrypted_units(
            &ps[lead].piece,
            &segments,
            ev.container,
            ev.detector,
            run.halt,
            MIN_SAMPLE_UNITS,
        )?;
        for &j in &todo[1..] {
            if samples.len() >= MIN_SAMPLE_UNITS {
                break;
            }
            if ps[j]
                .piece
                .titles
                .iter()
                .any(|t| ps[lead].piece.titles.contains(t))
            {
                let more = sampler.encrypted_units(
                    &ps[j].piece,
                    &segments,
                    ev.container,
                    ev.detector,
                    run.halt,
                    MIN_SAMPLE_UNITS - samples.len(),
                )?;
                samples.extend(more);
            }
        }
        if samples.len() < MIN_SAMPLE_UNITS {
            // Too little ciphertext to ask with (KU step 9.2): proven on arrival instead.
            thin.push(lead);
            continue;
        }
        budget -= 1;
        for &i in &live {
            run.check_halt()?;
            let asked = run.ask(i, &samples, false)?;
            if !matches!(asked, Asked::Skipped) && !tried.contains(&i) {
                tried.push(i);
            }
            match asked {
                Asked::Keys(k, _) => {
                    let who = run.sources[i].label();
                    run.add_keys(&k, who);
                    if opened(run, &ps[lead]).is_some() {
                        break;
                    }
                }
                Asked::Empty => run.dead[i] = true,
                Asked::Failed | Asked::Skipped => {}
            }
        }
        if opened(run, &ps[lead]).is_none() {
            gave_up.push(lead);
        }
    }
    // Every unit's sample was sent: a piece no held key opens has no key to find.
    let whole_with_keys =
        ev.scope == KeyScope::WholeDisc && !run.pool.is_empty() && run.first_failure.is_none();
    for (idx, p) in ps.iter_mut().enumerate() {
        if p.verdict == Verdict::Ask {
            match opened(run, p) {
                Some(slot) => p.verdict = Verdict::Keyed(slot),
                None if thin.contains(&idx) => p.verdict = Verdict::Lazy(None),
                // Not asked at all (too little ciphertext): the probed path decides.
                None if tried.is_empty() => {
                    for &i in &tried {
                        run.dead[i] = true;
                    }
                    return Ok(None);
                }
                // A whole-disc copy blanks it while other pieces are usable (as the probed path).
                None if whole_with_keys => {
                    tracing::warn!(target: "freemkv::keys", lba = p.id(), "no held key opens this stream file: it will be blanked in the image");
                    p.verdict = Verdict::Lazy(None);
                }
                None => {
                    let lba = p.id();
                    let failure = run.first_failure.take();
                    let err = failure.unwrap_or_else(|| missing_error(&ev.scope, disc_hash));
                    tracing::error!(target: "freemkv::keys", lba, code = err.code(), "a stream file in scope has no key; refusing before any output");
                    return Err(err);
                }
            }
        }
    }
    // KU §2.7: an empty pool on an encrypted scope is the keyless case.
    if run.pool.is_empty() && ps.iter().any(|p| matches!(p.verdict, Verdict::Lazy(_))) {
        let failure = run.first_failure.take();
        return Err(failure.unwrap_or_else(|| missing_error(&ev.scope, disc_hash)));
    }
    if whole_with_keys && !ps.iter().any(|p| matches!(p.verdict, Verdict::Keyed(_))) {
        let failure = run.first_failure.take();
        return Err(failure.unwrap_or_else(|| missing_error(&ev.scope, disc_hash)));
    }
    tracing::info!(
        target: "freemkv::keys",
        phase = "per_cps_unit",
        declared = n_decl,
        pieces = ps.len(),
        "each piece keyed by the key that opens its one sampled unit"
    );
    let mut inner = aacs_inner(run);
    inner.no_stream_files = ev.no_stream_files;
    let forensic = match layout {
        None => None,
        Some(l) => Some(resolve_forensic(sampler, l, seed, run, ev.container)?),
    };
    build(&mut inner, &ps, layout, forensic, run, clip);
    Ok(Some(inner))
}

// The one encryption decision, every AACS format and source (CSS makes it on its scramble
// flag): content read in the clear asks no source, whatever key files the disc declares;
// encrypted content is keyed by the format's rules, refused when no key is found.
fn decide(
    ev: &KeyEvidence,
    sampler: &mut Sampler,
    disc_hash: &str,
    n_decl: Option<usize>,
    seed: Option<&KeyRing>,
    run: &mut Run,
) -> Result<Inner> {
    // A forensic layout in scope keeps the probed path: its segments follow their own rules.
    if ev.detector == Detector::Verified && scope_layout(ev).0.is_none() {
        let trusted = match n_decl {
            Some(1) => resolve_one_unit(ev, sampler, disc_hash, seed, run)?,
            Some(n) if n > 1 => resolve_per_unit(ev, sampler, disc_hash, seed, run, n)?,
            _ => None,
        };
        if let Some(inner) = trusted {
            return Ok(inner);
        }
    }
    let scoped = probe_scope(ev, sampler, run.halt)?;
    if in_clear(&scoped, sampler, ev.container, run.halt)? {
        tracing::info!(target: "freemkv::keys", pieces = scoped.ps.len(), "content in the clear: no key source asked");
        let ps: Vec<Probed> = scoped
            .ps
            .into_iter()
            .map(|p| Probed {
                verdict: Verdict::Clear,
                ..p
            })
            .collect();
        let mut inner = aacs_inner(run);
        inner.no_stream_files = ev.no_stream_files;
        build(&mut inner, &ps, None, None, run, Vec::new());
        return Ok(inner);
    }
    match ev.detector {
        Detector::PerPack => resolve_hddvd(ev, sampler, disc_hash, seed, run),
        Detector::Verified => resolve_bd(ev, scoped, sampler, disc_hash, n_decl, seed, run),
    }
}

// Whether every piece's probes read clear (a piece with no whole unit holds no AACS unit)
// and so do the forensic segments in scope.
fn in_clear(
    scoped: &Scoped,
    sampler: &mut Sampler,
    format: ContentFormat,
    halt: &Halt,
) -> Result<bool> {
    let pieces_clear = scoped
        .ps
        .iter()
        .all(|p| p.units == 0 || (p.enc.is_empty() && p.faults == 0));
    if !pieces_clear {
        return Ok(false);
    }
    match scoped.layout {
        None => Ok(true),
        Some(l) => super::fmts::segments_clear(sampler.source(), l, format, halt),
    }
}

// `[HD]` §4.3: an HD DVD EVOBU is keyed by its CPI's `TITLE_KEY_PTR`, not by a CPS-unit map,
// so every Title Key a source gives is held under its entry number and proven on decrypted
// packs of the main title. Unprovable (no structural test applied) is best effort.
fn resolve_hddvd(
    ev: &KeyEvidence,
    sampler: &mut Sampler,
    disc_hash: &str,
    seed: Option<&KeyRing>,
    run: &mut Run,
) -> Result<Inner> {
    let mut keys: Vec<(u32, [u8; 16])> = seed
        .map(|s| {
            s.0.key_nums
                .iter()
                .copied()
                .zip(s.0.pool.iter().copied())
                .collect()
        })
        .unwrap_or_default();
    let mut origin = (!keys.is_empty()).then_some("seed");
    if keys.is_empty() {
        let samples = sampler.main_samples(ev.main.as_ref(), MIN_SAMPLE_UNITS);
        for i in 0..run.sources.len() {
            if let Asked::Keys(k, idx) = run.ask(i, &samples, false)? {
                let nums = idx.iter().map(|&i| title_key_number(ev, i));
                keys = nums.zip(k).take(MAX_POOL_KEYS).collect();
                origin = Some(run.sources[i].label());
                break;
            }
        }
    }
    if keys.is_empty() {
        let failure = run.first_failure.take();
        return Err(failure.unwrap_or_else(|| missing_error(&ev.scope, disc_hash)));
    }
    let proof = prove_hddvd(sampler.source(), ev.main.as_ref(), &keys, run.halt)?;
    let proven = match proof {
        HdProof::Wrong {
            opened,
            failed,
            missing,
        } => {
            tracing::error!(target: "freemkv::keys", opened, failed, missing, code = crate::error::E_DECRYPT_FAILED, "the held Title Keys do not open the HD DVD's packs: wrong key");
            return Err(Error::DecryptFailed);
        }
        HdProof::Proven(slots) => slots,
        HdProof::Unproven => {
            tracing::warn!(target: "freemkv::keys", "no decrypted HD DVD pack could be checked: keys applied unproven");
            Vec::new()
        }
    };
    let mut inner = aacs_inner(run);
    inner.no_stream_files = ev.no_stream_files;
    inner.best_effort = proven.is_empty();
    inner.pool = keys.iter().map(|k| k.1).collect();
    inner.key_nums = keys.iter().map(|k| k.0).collect();
    inner.origin = origin;
    inner.proven = proven;
    inner.keyed = ev.pieces.len();
    let mut ranges = Vec::new();
    for p in &ev.pieces {
        ranges.extend(p.ranges().map(|(s, e)| (s, e, 0usize)));
        inner.spans.extend(p.spans.iter().copied());
    }
    // `span_at` bisects: a fragmented file or interleaved pieces arrive out of LBA order.
    inner.spans.sort_unstable_by_key(|s| s.0);
    inner.map = Arc::new(AacsKeyMap::from_ranges(ranges));
    Ok(inner)
}

// A source's key `idx` is its position among the title-key file's present entries
// (`UnitKey::idx`); the VTKF entry number (`TITLE_KEY_PTR`) is that entry's slot.
fn title_key_number(ev: &KeyEvidence, idx: u32) -> u32 {
    ev.aacs
        .as_ref()
        .and_then(|a| crate::aacs::inf::parse_vtkf(&a.unit_key_ro))
        .and_then(|f| f.encrypted_keys.get(idx as usize).map(|k| k.0))
        .unwrap_or(idx.saturating_add(1))
}

// Sectors read per HD DVD proof window, and the windows spread over the main title.
const HD_PROOF_SECTORS: u32 = 2048;
const HD_PROOF_WINDOWS: u64 = 3;

// The verdict of decrypting sampled HD DVD packs under the held Title Keys.
enum HdProof {
    /// Most checked packs verified; the pool slots that opened them.
    Proven(Vec<usize>),
    /// No decrypted pack could be checked (no structural test applied, or nothing encrypted).
    Unproven,
    /// Checked packs did not verify, or named a Title Key not held.
    Wrong {
        opened: usize,
        failed: usize,
        missing: usize,
    },
}

// Decrypt the encrypted packs of a few windows of the main title under the CPI each EVOBU's
// NV_PCK gives (`[HD]` §4.3.4) and judge them by `hddvd::payload_check`. A wrong key leaves
// bytes 128.. random, so a structural test passes about once in 2^16.
fn prove_hddvd(
    reader: &mut dyn crate::sector::SectorSource,
    main: Option<&super::evidence::MainTitle>,
    keys: &[(u32, [u8; 16])],
    halt: &Halt,
) -> Result<HdProof> {
    use crate::aacs::hddvd::{
        PACK_LEN, PackKind, classify, decrypt_pack, needs_key, payload_check,
    };
    let Some(main) = main else {
        return Ok(HdProof::Unproven);
    };
    let total: u64 = main.extents.iter().map(|e| u64::from(e.sector_count)).sum();
    let (mut opened, mut failed, mut missing) = (0usize, 0usize, 0usize);
    let mut slots = Vec::new();
    for w in 0..HD_PROOF_WINDOWS {
        halt.check()?;
        let Some((lba, left)) =
            sector_at(&main.extents, total * (2 * w + 1) / (2 * HD_PROOF_WINDOWS))
        else {
            continue;
        };
        let n = left.min(HD_PROOF_SECTORS);
        let mut buf = vec![0u8; n as usize * PACK_LEN];
        let got = match reader.read_sectors(lba, n as u16, &mut buf, false) {
            Ok(got) => got.min(buf.len()),
            Err(e) if super::evidence::fatal_read(&e) => return Err(e),
            Err(_) => continue,
        };
        let mut cpi = None;
        for pack in buf[..got].as_chunks::<PACK_LEN>().0 {
            let kind = classify(pack);
            if let PackKind::Nav(c) = kind {
                cpi = c;
                continue;
            }
            let Some(c) = cpi.filter(|c| needs_key(kind, Some(c)) && c.key_vf() == 0b10) else {
                continue;
            };
            let Some(slot) = keys.iter().position(|k| k.0 == c.title_key_ptr()) else {
                missing += 1;
                continue;
            };
            let mut plain = pack.to_vec();
            decrypt_pack(&mut plain, &keys[slot].1, &c);
            match payload_check(&plain) {
                Some(true) => {
                    opened += 1;
                    if !slots.contains(&slot) {
                        slots.push(slot);
                    }
                }
                Some(false) => failed += 1,
                None => {}
            }
        }
    }
    Ok(match (opened, failed + missing) {
        (0, 0) => HdProof::Unproven,
        (o, f) if o > f => HdProof::Proven(slots),
        _ => HdProof::Wrong {
            opened,
            failed,
            missing,
        },
    })
}

// The sector `off` sectors into `extents`, and how many sectors of its extent follow it.
fn sector_at(extents: &[Extent], mut off: u64) -> Option<(u32, u32)> {
    for e in extents {
        let n = u64::from(e.sector_count);
        if off < n {
            let lba = u64::from(e.start_lba) + off;
            return Some((u32::try_from(lba).ok()?, (n - off) as u32));
        }
        off -= n;
    }
    None
}

// KU §2.3 steps 8–14 for BD/UHD content not in the clear, over `probe_scope`'s probes.
fn resolve_bd(
    ev: &KeyEvidence,
    scoped: Scoped,
    sampler: &mut Sampler,
    disc_hash: &str,
    n_decl: Option<usize>,
    seed: Option<&KeyRing>,
    run: &mut Run,
) -> Result<Inner> {
    let format = ev.container;
    let scope = &ev.scope;
    let Scoped {
        mut ps,
        layout,
        clip,
    } = scoped;
    // Step 8: sample-independent sources, once, with the main title's samples.
    let needs_keys = ps
        .iter()
        .any(|p| p.units > 0 && (!p.enc.is_empty() || p.faults > 0));
    if needs_keys {
        let main = ev.main.as_ref().map(|t| {
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
            if let Asked::Keys(k, _) = run.ask(i, &samples, false)? {
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
    order.sort_by_key(|&i| (std::cmp::Reverse(ps[i].piece.rank), ps[i].id()));
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
            if let Asked::Keys(k, _) = run.ask(s, &samples, false)? {
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
    // A whole-disc copy blanks a piece no held key opens (counted on arrival) while any
    // other piece is usable; a source fault, an empty pool or no usable piece still refuses.
    if *scope == KeyScope::WholeDisc
        && !run.pool.is_empty()
        && run.first_failure.is_none()
        && ps.iter().any(|p| p.verdict != Verdict::Missing)
    {
        for p in ps.iter_mut().filter(|p| p.verdict == Verdict::Missing) {
            tracing::warn!(target: "freemkv::keys", lba = p.id(), "no held key opens this stream file: it will be blanked in the image");
            p.verdict = Verdict::Lazy(None);
        }
    }
    if let Some(i) = ps.iter().position(|p| p.verdict == Verdict::Missing) {
        let lba = ps[i].id();
        let failure = run.first_failure.take();
        let err = failure.unwrap_or_else(|| missing_error(scope, disc_hash));
        tracing::error!(target: "freemkv::keys", lba, code = err.code(), "a stream file in scope has no key; refusing before any output");
        return Err(err);
    }
    // KU §2.7: an empty pool on an encrypted scope is today's keyless case.
    if run.pool.is_empty() && ps.iter().any(|p| matches!(p.verdict, Verdict::Lazy(_))) {
        let failure = run.first_failure.take();
        return Err(failure.unwrap_or_else(|| missing_error(scope, disc_hash)));
    }
    let mut inner = aacs_inner(run);
    inner.no_stream_files = ev.no_stream_files;
    // Step 6 (continued): forensic keys, reused from the seed or anchored once.
    let forensic = match layout {
        None => None,
        Some(l) => Some(resolve_forensic(sampler, l, seed, run, format)?),
    };
    build(&mut inner, &ps, layout, forensic, run, clip);
    Ok(inner)
}

// Step 9.2: this piece's unopened `Enc` units first, topped up from other unopened pieces
// of the same playlist, never across playlists (SG23).
fn borrow_samples(run: &Run, ps: &[Probed], i: usize) -> Vec<Vec<u8>> {
    // KS-10 [BD] §3.9.2: "All AV stream files that are referred to by one Title are included
    // in the same CPS Unit" — so only pieces sharing a title may lend units.
    let unopened = |p: &Probed| -> Vec<Vec<u8>> {
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
        if j == i
            || q.verdict != Verdict::Ask
            || !q
                .piece
                .titles
                .iter()
                .any(|t| ps[i].piece.titles.contains(t))
        {
            continue;
        }
        out.extend(unopened(q));
    }
    out.truncate(SAMPLE_CAP);
    out
}

// Step 10: the `n_decl == 1` rule.
fn apply_single_unit_rule(run: &Run, ps: &mut [Probed], n_decl: Option<usize>) -> Result<()> {
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
    sampler: &mut Sampler,
    layout: &super::fmts::Layout,
    seed: Option<&KeyRing>,
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
            if let Asked::Keys(k, _) = run.ask(i, batch, true)? {
                return Ok(Some(k));
            }
        }
        Ok(None)
    };
    let keys = match super::fmts::anchor(sampler.source(), layout, Some(halt), &mut ask)? {
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
    match super::fmts::phases(sampler.source(), layout, &keys, format, Some(halt))? {
        Some(p) => Ok(Forensic::Resolved(keys, p)),
        None => Err(Error::FmtsKeyMissing),
    }
}

// Step 14: maps from Keyed pieces (and the forensic ranges); arrival pieces for the rest.
fn build(
    inner: &mut Inner,
    ps: &[Probed],
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
        inner.spans.extend(p.piece.spans.iter().copied());
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
                ranges.extend(crate::keys::fmts::fill_base_key_gaps(
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
                    spans: p.piece.spans.clone(),
                    candidate: match p.verdict {
                        Verdict::Lazy(c) => c,
                        _ => None,
                    },
                    clear: p.verdict == Verdict::Clear,
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
