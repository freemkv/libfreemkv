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
    Keys(Vec<[u8; 16]>),
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
    halt: &Halt,
) -> Result<()> {
    let (units, samples) = sampler.probe(&p.piece, segments, format, halt)?;
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
    let result = match ev.detector {
        Detector::Unverified => resolve_hddvd(ev, sampler, &disc_hash, n_decl, &mut run),
        Detector::Verified => resolve_bd(ev, sampler, &disc_hash, n_decl, seed, &mut run),
    };
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

// KU §2.6: HD DVD is best effort. Not probed (its encrypted flag is unverified, KS-27):
// a single declared key is applied to every piece; several, or none parseable, refuse.
fn resolve_hddvd(
    ev: &KeyEvidence,
    sampler: &mut Sampler,
    disc_hash: &str,
    n_decl: Option<usize>,
    run: &mut Run,
) -> Result<Inner> {
    if n_decl != Some(1) {
        tracing::error!(target: "freemkv::keys", declared = ?n_decl, "multi-key HD DVD cannot be matched reliably");
        return Err(Error::NoDiscKey {
            disc_hash: crate::hex::strip_hex_prefix(disc_hash).to_string(),
        });
    }
    if run.pool.is_empty() {
        let samples = sampler.main_samples(ev.main.as_ref(), MIN_SAMPLE_UNITS);
        for i in 0..run.sources.len() {
            if let Asked::Keys(k) = run.ask(i, &samples, false)? {
                let who = run.sources[i].label();
                run.add_keys(&k[..1], who);
                break;
            }
        }
        if run.pool.is_empty() {
            let failure = run.first_failure.take();
            return Err(failure.unwrap_or_else(|| missing_error(&ev.scope, disc_hash)));
        }
    }
    let mut inner = aacs_inner(run);
    inner.no_stream_files = ev.no_stream_files;
    inner.best_effort = true;
    inner.origin = run.origin.first().copied();
    inner.proven = vec![0];
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

// KU §2.3 steps 5–14 for BD/UHD content.
fn resolve_bd(
    ev: &KeyEvidence,
    sampler: &mut Sampler,
    disc_hash: &str,
    n_decl: Option<usize>,
    seed: Option<&KeyRing>,
    run: &mut Run,
) -> Result<Inner> {
    let format = ev.container;
    let scope = &ev.scope;
    // Step 5: the evidence's pieces.
    let mut ps: Vec<Probed> = ev.pieces.iter().cloned().map(Probed::new).collect();
    // Step 6: FMTS, when the forensic clip is in scope.
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
        ps.iter()
            .any(|p| p.ranges().any(|(s, e)| overlaps(&clip, s, e)))
    });
    let segments: Vec<(u32, u32)> = layout
        .map(|l| sorted_ranges(l.ranges.iter().map(|&(s, e, _)| (s, e)).collect()))
        .unwrap_or_default();
    // Step 7: probes.
    for p in &mut ps {
        probe(sampler, p, &segments, format, run.halt)?;
    }
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
            if let Asked::Keys(k) = run.ask(i, batch, true)? {
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
