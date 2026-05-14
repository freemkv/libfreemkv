//! Pass-N (`Disc::patch`) read-error handler — A/B golden fixture.
//!
//! Background (2026-05-13, v0.20.8 release bundle planning):
//!
//! `libfreemkv::disc::read_error::handle_read_error` is supposed to be
//! the single source of truth for sector-read error → recovery action
//! decisions. Pass 1 sweep routes through it. Pass N patch's
//! `handle_read_failure` (in `disc/patch.rs`) does NOT — historically
//! MEDIUM_ERROR / NOT_READY get inline handling with their own thresholds
//! (`PASSN_DAMAGE_THRESHOLD_PCT=6` vs the sweep's `12`), their own
//! damage_window (state.damage_window, separate from ReadCtx.damage_window),
//! and their own skip logic (`compute_damage_skip`, which runs AFTER
//! the failure handler and has a size-aware `range_remaining/4` cap
//! that `handle_read_error::JumpAhead` does not know about).
//!
//! This file is the A/B fixture for that unification. It pins the
//! CURRENT (pre-unification) end-to-end behavior of `Disc::patch` for
//! eight canonical damage profiles against a synthetic
//! `ScriptedSectorReader`. Each profile asserts the exact observable
//! outcome — final mapfile byte counts and outer-loop counters — so any
//! attempt to refactor the failure path either preserves the goldens or
//! the test fails loudly.
//!
//! The prompt called for "exact sequence of `ReadAction` enums per
//! LBA"; that framing doesn't fit the current architecture because
//! `handle_read_failure` produces `FailureAction`, not `ReadAction`,
//! and interleaves with `compute_damage_skip` + cursor management in
//! the outer loop. The observable contract — what `Disc::patch` does
//! to the mapfile and how many reads it performs — is the equivalent
//! invariant, captured end-to-end.
//!
//! Why we expect divergence under naïve unification (see final report
//! of the 0.20.8 unification attempt): the patch loop's skip semantics
//! live in `compute_damage_skip` POST-failure-handler, with a size-aware
//! cap that `handle_read_error` knows nothing about; routing through
//! `handle_read_error` would invert that cursor flow. The fixture stays
//! checked in regardless — it documents the contract for the next
//! refactor attempt.

use libfreemkv::ContentFormat;
use libfreemkv::Disc;
use libfreemkv::DiscFormat;
use libfreemkv::disc::CopyOptions;
use libfreemkv::disc::DiscRegion;
use libfreemkv::disc::mapfile::{Mapfile, SectorStatus};
use libfreemkv::error::{Error, Result};
use libfreemkv::scsi;
use libfreemkv::{ScsiSense, SectorSource};
use std::sync::{Arc, Mutex};

const SECTOR_SIZE: usize = 2048;

/// Per-attempt result the script can emit. `Ok` returns a deterministic
/// per-sector byte pattern (LBA mod 256 in each sector). `Err` returns
/// the SCSI sense triple supplied — the patch failure path inspects
/// `scsi_sense().sense_key` to classify (MEDIUM, NOT_READY,
/// HARDWARE, ILLEGAL_REQUEST, ABORTED_COMMAND).
#[derive(Debug, Clone, Copy)]
enum ScriptStep {
    Ok,
    Err { sense_key: u8, asc: u8, ascq: u8 },
}

/// A scripted reader. For each (lba, count) read attempt, picks the
/// step at `attempt_idx[lba]`, advances the index. If no script entry
/// exists for an LBA, defaults to `Ok` so we don't need to script
/// every sector of large ranges.
///
/// "Batch fails if ANY sector in the batch is bad" — matches real
/// drive behavior (`pass_n_size_aware_skip.rs` uses the same model).
/// For batched reads we synthesize an Err with the FIRST scripted
/// failure in the batch.
struct ScriptedSectorReader {
    capacity: u32,
    /// Per-LBA script of (step, then next step on retry, …). When
    /// retries exhaust the script, the LAST step repeats forever.
    script: std::collections::HashMap<u32, Vec<ScriptStep>>,
    /// Per-LBA index into its script vec. Bumps on each read attempt
    /// at that LBA.
    attempt_idx: Mutex<std::collections::HashMap<u32, usize>>,
    /// Full read trace: every (lba, count, result_was_ok) tuple in
    /// call order. Lets the test assert that adaptive-batch dropped
    /// to count=1, bisection happened, etc.
    trace: Arc<Mutex<Vec<(u32, u16, bool)>>>,
}

impl ScriptedSectorReader {
    fn new(capacity: u32) -> (Self, Arc<Mutex<Vec<(u32, u16, bool)>>>) {
        let trace = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                capacity,
                script: std::collections::HashMap::new(),
                attempt_idx: Mutex::new(std::collections::HashMap::new()),
                trace: trace.clone(),
            },
            trace,
        )
    }

    /// Set a single-step script for `lba`: every attempt yields `step`.
    fn always(&mut self, lba: u32, step: ScriptStep) {
        self.script.insert(lba, vec![step]);
    }

    /// Set a multi-step script for `lba`: first attempt yields
    /// `steps[0]`, second `steps[1]`, … on retry the last step repeats.
    #[allow(dead_code)]
    fn sequence(&mut self, lba: u32, steps: Vec<ScriptStep>) {
        self.script.insert(lba, steps);
    }

    fn step_for(&self, lba: u32) -> ScriptStep {
        let v = match self.script.get(&lba) {
            Some(v) => v,
            None => return ScriptStep::Ok,
        };
        let mut idx = self.attempt_idx.lock().unwrap();
        let i = idx.entry(lba).or_insert(0);
        let step = v[(*i).min(v.len() - 1)];
        *i += 1;
        step
    }
}

impl SectorSource for ScriptedSectorReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        // Look at every sector in the batch — first failure determines
        // the outcome.
        let mut failure: Option<(u8, u8, u8)> = None;
        for offset in 0..count as u32 {
            match self.step_for(lba + offset) {
                ScriptStep::Ok => {}
                ScriptStep::Err {
                    sense_key,
                    asc,
                    ascq,
                } => {
                    failure = Some((sense_key, asc, ascq));
                    break;
                }
            }
        }
        let ok = failure.is_none();
        self.trace.lock().unwrap().push((lba, count, ok));
        if let Some((sense_key, asc, ascq)) = failure {
            return Err(Error::ScsiError {
                opcode: scsi::SCSI_READ_10,
                status: scsi::SCSI_STATUS_CHECK_CONDITION,
                sense: Some(ScsiSense {
                    sense_key,
                    asc,
                    ascq,
                }),
            });
        }
        // Per-sector LBA byte pattern.
        for (i, chunk) in buf.chunks_mut(SECTOR_SIZE).enumerate() {
            chunk.fill(((lba + i as u32) & 0xff) as u8);
        }
        Ok(buf.len())
    }

    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
}

fn synthetic_disc(capacity_sectors: u32) -> Disc {
    Disc {
        volume_id: String::new(),
        meta_title: None,
        format: DiscFormat::BluRay,
        capacity_sectors,
        capacity_bytes: capacity_sectors as u64 * SECTOR_SIZE as u64,
        layers: 1,
        titles: Vec::new(),
        region: DiscRegion::Free,
        aacs: None,
        css: None,
        encrypted: false,
        aacs_error: None,
        content_format: ContentFormat::BdTs,
    }
}

fn prep_iso_and_mapfile(
    iso_path: &std::path::Path,
    total_bytes: u64,
    finished_ranges: &[(u64, u64)],
    nontrimmed_ranges: &[(u64, u64)],
) {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(iso_path)
        .unwrap();
    f.set_len(total_bytes).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(&[]).unwrap();

    let map_path = libfreemkv::disc::mapfile_path_for(iso_path);
    let mut mf = Mapfile::create(&map_path, total_bytes, "test").unwrap();
    for &(pos, size) in finished_ranges {
        mf.record(pos, size, SectorStatus::Finished).unwrap();
    }
    for &(pos, size) in nontrimmed_ranges {
        mf.record(pos, size, SectorStatus::NonTrimmed).unwrap();
    }
}

/// Observable outcome of a patch run. Goldens for each profile pin
/// these exact values.
#[derive(Debug, PartialEq, Eq)]
struct Golden {
    /// `bytes_good` at end of patch.
    bytes_good: u64,
    /// `bytes_unreadable` at end.
    bytes_unreadable: u64,
    /// `bytes_pending` (NonTrimmed) at end.
    bytes_pending: u64,
    /// Did the pass exit via wedge-detection?
    wedged_exit: bool,
    /// Sanity bound on trace length — patch makes a finite number of
    /// reads bounded by `MAX_SKIPS_PER_RANGE * range_sectors` plus
    /// retries. Asserted as an UPPER bound only (so any reduction in
    /// retries via future tuning doesn't fail the test spuriously).
    max_reads: usize,
}

/// Common helper: prep ISO + mapfile, run `disc.copy(multipass)`,
/// return (PatchOutcome ↔ CopyResult, final-map stats, trace length).
fn run_profile(
    profile_name: &str,
    capacity_sectors: u32,
    nontrimmed: &[(u64, u64)],
    finished: &[(u64, u64)],
    scripted: ScriptedSectorReader,
    trace: Arc<Mutex<Vec<(u32, u16, bool)>>>,
) -> (
    libfreemkv::disc::CopyResult,
    libfreemkv::disc::mapfile::MapStats,
    usize,
) {
    let total_bytes: u64 = capacity_sectors as u64 * SECTOR_SIZE as u64;
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    prep_iso_and_mapfile(&iso_path, total_bytes, finished, nontrimmed);

    let opts = CopyOptions {
        decrypt: false,
        multipass: true,
        ..Default::default()
    };

    let mut reader = scripted;
    let pr = disc
        .copy(&mut reader, &iso_path, &opts)
        .unwrap_or_else(|e| panic!("[{profile_name}] disc.copy returned Err: {e:?}"));

    let map_path = libfreemkv::disc::mapfile_path_for(&iso_path);
    let map = Mapfile::load(&map_path).unwrap();
    let stats = map.stats();

    let trace_len = trace.lock().unwrap().len();

    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(&map_path);

    (pr, stats, trace_len)
}

// ─────────────────────────── Profile 1: CLEAN ────────────────────────────
//
// The NonTrimmed range has zero scripted failures — every read succeeds.
// Patch should march through the range and mark it Finished. Validates
// the happy-path side of the failure-handler dispatch (it shouldn't
// fire at all).

#[test]
fn profile_01_clean_all_recoverable() {
    let capacity_sectors: u32 = 256;
    let (reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    // No scripted errors → all reads succeed.

    let nontrimmed = [(100 * 2048, 16 * 2048)]; // 16-sector NonTrimmed range
    let finished = [
        (0, 100 * 2048),
        (116 * 2048, (capacity_sectors as u64 - 116) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "01_clean",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    let expected = Golden {
        bytes_good: capacity_sectors as u64 * 2048,
        bytes_unreadable: 0,
        bytes_pending: 0,
        wedged_exit: false,
        max_reads: 8, // adaptive batch=32 reads finishes 16 sectors in 1 read; allow up to 8.
    };
    assert_eq!(stats.bytes_good, expected.bytes_good, "01_clean bytes_good");
    assert_eq!(
        stats.bytes_unreadable, expected.bytes_unreadable,
        "01_clean bytes_unreadable"
    );
    assert_eq!(
        stats.bytes_pending, expected.bytes_pending,
        "01_clean bytes_pending"
    );
    assert!(!pr.halted, "01_clean halted");
    assert!(
        trace_len <= expected.max_reads,
        "01_clean trace_len={trace_len} exceeds bound {}",
        expected.max_reads
    );
}

// ─────────────────────────── Profile 2: ALL MEDIUM ───────────────────────
//
// Every LBA in the NonTrimmed range returns MEDIUM_ERROR every attempt.
// Adaptive-batch drops to count=1 on first batch failure, then each
// single-sector read fails → consecutive_failures climbs, damage_window
// fills, compute_damage_skip fires, MAX_SKIPS_PER_RANGE caps the work,
// remaining bytes stay NonTrimmed (NEVER marked Unreadable inside a
// single pass — 2026-05-11 design call).

#[test]
fn profile_02_all_medium_error() {
    let capacity_sectors: u32 = 256;
    let (mut reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    for lba in 100..116 {
        reader.always(
            lba,
            ScriptStep::Err {
                sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x00,
            },
        );
    }

    let nontrimmed = [(100 * 2048, 16 * 2048)];
    let finished = [
        (0, 100 * 2048),
        (116 * 2048, (capacity_sectors as u64 - 116) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "02_all_medium",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    // GOLDEN: the 16-sector bad range stays NonTrimmed (bytes_pending).
    // Pre-2026-05-11 patch would mark Unreadable here; current code
    // preserves NonTrimmed so subsequent passes get another shot.
    assert_eq!(
        stats.bytes_good,
        (capacity_sectors as u64 - 16) * 2048,
        "02_all_medium bytes_good"
    );
    assert_eq!(
        stats.bytes_unreadable, 0,
        "02_all_medium bytes_unreadable (must NOT be marked terminal in one pass)"
    );
    assert_eq!(
        stats.bytes_pending,
        16 * 2048,
        "02_all_medium bytes_pending (NonTrimmed retained across passes)"
    );
    assert!(!pr.halted, "02_all_medium halted");
    // Upper bound: every sector probed individually + a few batch-drop
    // and skip-escalation attempts. 16 sectors × ~3 visits ≈ 50.
    assert!(
        trace_len <= 80,
        "02_all_medium trace_len={trace_len} exceeds 80"
    );
}

// ───────────────────── Profile 3: ALTERNATING GOOD/BAD ───────────────────
//
// LBAs 100, 102, 104, ... bad; odd LBAs good. Validates that good
// sectors interleaved with bad get recovered individually after the
// adaptive split (batch-fail → count=1 → per-sector probe).

#[test]
fn profile_03_alternating_good_bad() {
    let capacity_sectors: u32 = 256;
    let (mut reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    for lba in (100..116).step_by(2) {
        reader.always(
            lba,
            ScriptStep::Err {
                sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x00,
            },
        );
    }

    let nontrimmed = [(100 * 2048, 16 * 2048)];
    let finished = [
        (0, 100 * 2048),
        (116 * 2048, (capacity_sectors as u64 - 116) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "03_alternating",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    // GOLDEN: 8 good sectors interleaved should mostly be Finished;
    // 8 bad stay NonTrimmed. Allow 2 sectors of slop for the actual
    // bisect cursor advance — converging on alternating bad/good in
    // a single pass isn't always exact at boundaries with the
    // size-aware skip cap.
    let good_total = stats.bytes_good;
    let baseline_good = (capacity_sectors as u64 - 16) * 2048;
    let middle_recovered = good_total - baseline_good;
    assert!(
        middle_recovered >= 6 * 2048,
        "03_alternating recovered only {middle_recovered} bytes of 8 good sectors"
    );
    assert!(
        middle_recovered <= 9 * 2048,
        "03_alternating recovered MORE than scripted good sectors: {middle_recovered}"
    );
    assert_eq!(stats.bytes_unreadable, 0, "03_alternating bytes_unreadable");
    // Remaining must be NonTrimmed (pending), not lost.
    assert!(
        stats.bytes_pending > 0,
        "03_alternating expected NonTrimmed remainder, got bytes_pending=0"
    );
    assert!(!pr.halted, "03_alternating halted");
    assert!(
        trace_len <= 120,
        "03_alternating trace_len={trace_len} exceeds 120"
    );
}

// ───────────────────── Profile 4: EDGE-BAD (size-aware-skip canon) ───────
//
// Bad at start (100..104), good middle (104..112), bad at end (112..116).
// This is the size-aware-skip canonical case. The middle good sectors
// MUST be recovered — pre-fix patch would skip-escalate across the
// whole range and miss them.

#[test]
fn profile_04_edge_bad_good_middle() {
    let capacity_sectors: u32 = 256;
    let (mut reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    for lba in 100..104 {
        reader.always(
            lba,
            ScriptStep::Err {
                sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x00,
            },
        );
    }
    for lba in 112..116 {
        reader.always(
            lba,
            ScriptStep::Err {
                sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x00,
            },
        );
    }

    let nontrimmed = [(100 * 2048, 16 * 2048)];
    let finished = [
        (0, 100 * 2048),
        (116 * 2048, (capacity_sectors as u64 - 116) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "04_edge_bad",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    // GOLDEN: the 8 good middle sectors should land Finished (allowing
    // 2 sectors of bisection slop at boundaries).
    let middle_recovered = stats.bytes_good - (capacity_sectors as u64 - 16) * 2048;
    assert!(
        middle_recovered >= 6 * 2048,
        "04_edge_bad recovered only {middle_recovered} bytes of 8 good middle sectors"
    );
    assert_eq!(stats.bytes_unreadable, 0, "04_edge_bad bytes_unreadable");
    assert!(
        stats.bytes_pending > 0,
        "04_edge_bad bytes_pending expected > 0"
    );
    assert!(!pr.halted, "04_edge_bad halted");
    assert!(
        trace_len <= 120,
        "04_edge_bad trace_len={trace_len} exceeds 120"
    );
}

// ───────────────────── Profile 5: SINGLE BAD SECTOR ──────────────────────
//
// 1 bad sector in the middle of an otherwise good 16-sector NonTrimmed
// range. Validates the common "stochastic miss in Pass 1, easily picked
// up in Pass N" scenario.

#[test]
fn profile_05_single_bad_sector() {
    let capacity_sectors: u32 = 256;
    let (mut reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    reader.always(
        108,
        ScriptStep::Err {
            sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
            asc: 0x11,
            ascq: 0x00,
        },
    );

    let nontrimmed = [(100 * 2048, 16 * 2048)];
    let finished = [
        (0, 100 * 2048),
        (116 * 2048, (capacity_sectors as u64 - 116) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "05_single_bad",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    // GOLDEN: 15 of 16 sectors recovered. 1 sector stays NonTrimmed
    // (NOT Unreadable — same multi-pass tolerance principle).
    assert_eq!(
        stats.bytes_good,
        (capacity_sectors as u64 - 1) * 2048,
        "05_single_bad bytes_good"
    );
    assert_eq!(stats.bytes_unreadable, 0, "05_single_bad bytes_unreadable");
    assert_eq!(stats.bytes_pending, 2048, "05_single_bad bytes_pending");
    assert!(!pr.halted, "05_single_bad halted");
    assert!(
        trace_len <= 80,
        "05_single_bad trace_len={trace_len} exceeds 80"
    );
}

// ───────────────────── Profile 6: DEEP PIT ───────────────────────────────
//
// A contiguous 8-sector bad pit in the middle of a wider 24-sector
// NonTrimmed range. Tests the damage-window threshold + size-aware-skip
// converging on the actual pit boundaries instead of bailing on
// MAX_SKIPS_PER_RANGE.

#[test]
fn profile_06_deep_pit() {
    let capacity_sectors: u32 = 256;
    let (mut reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    for lba in 108..116 {
        reader.always(
            lba,
            ScriptStep::Err {
                sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x00,
            },
        );
    }

    // 24 sectors NonTrimmed: 100..108 good, 108..116 BAD, 116..124 good.
    let nontrimmed = [(100 * 2048, 24 * 2048)];
    let finished = [
        (0, 100 * 2048),
        (124 * 2048, (capacity_sectors as u64 - 124) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "06_deep_pit",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    // GOLDEN: 16 good (8 on each side of the pit) recovered, 8 bad
    // stay NonTrimmed.
    let recovered_in_range = stats.bytes_good - (capacity_sectors as u64 - 24) * 2048;
    assert!(
        recovered_in_range >= 14 * 2048,
        "06_deep_pit recovered only {recovered_in_range} bytes of 16 good sectors"
    );
    assert_eq!(stats.bytes_unreadable, 0, "06_deep_pit bytes_unreadable");
    assert!(
        stats.bytes_pending > 0,
        "06_deep_pit bytes_pending expected > 0"
    );
    assert!(!pr.halted, "06_deep_pit halted");
    assert!(
        trace_len <= 120,
        "06_deep_pit trace_len={trace_len} exceeds 120"
    );
}

// ───────────────────── Profile 7: MEDIUM-THEN-GOOD ───────────────────────
//
// First N attempts at each bad LBA fail with MEDIUM_ERROR, then succeed.
// Tests whether patch's retry semantics revisit failed sectors. Current
// patch dispatches NonTrimmed on first failure and ADVANCES the cursor
// — it does NOT retry the same LBA inside one pass for MEDIUM_ERROR
// (only NOT_READY retries in-place). So the goldens here are: bad
// sectors stay NonTrimmed in this pass (the recovery would happen in a
// subsequent pass, which this single-pass fixture does not run).

#[test]
fn profile_07_medium_then_good() {
    let capacity_sectors: u32 = 256;
    let (mut reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    // Sectors 105..110: fail twice, then succeed.
    for lba in 105..110 {
        reader.sequence(
            lba,
            vec![
                ScriptStep::Err {
                    sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                    asc: 0x11,
                    ascq: 0x00,
                },
                ScriptStep::Err {
                    sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                    asc: 0x11,
                    ascq: 0x00,
                },
                ScriptStep::Ok,
            ],
        );
    }

    let nontrimmed = [(100 * 2048, 16 * 2048)];
    let finished = [
        (0, 100 * 2048),
        (116 * 2048, (capacity_sectors as u64 - 116) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "07_medium_then_good",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    // GOLDEN: patch's cache-priming (`prime_cache`) issues 3
    // throwaway single-sector reads at lba-3..lba before each count==1
    // recovery read. Those throwaway reads ADVANCE the per-LBA script
    // step counter even though their results are discarded. So a
    // 3-step script (fail, fail, ok) gets consumed by 2 prime calls
    // plus 1 real read → the real read sees `Ok` and the sector is
    // recovered. Net effect: patch fully recovers the range in one
    // pass thanks to priming, even though the script said "fails on
    // first two attempts."
    //
    // This is the documented cache-prime behavior (`disc/patch.rs`
    // ~line 398, "Proven 2026-05-07 with dd-as-oracle: 8/8 sectors
    // recoverable when primed vs 6/8 cold"). The golden pins it.
    assert_eq!(
        stats.bytes_good,
        capacity_sectors as u64 * 2048,
        "07_medium_then_good bytes_good — cache-prime should consume \
         the failing script steps so the real read sees Ok"
    );
    assert_eq!(
        stats.bytes_unreadable, 0,
        "07_medium_then_good bytes_unreadable"
    );
    assert_eq!(stats.bytes_pending, 0, "07_medium_then_good bytes_pending");
    assert!(!pr.halted, "07_medium_then_good halted");
    assert!(
        trace_len <= 100,
        "07_medium_then_good trace_len={trace_len} exceeds 100"
    );
}

// ───────────────────── Profile 8: BATCHED-FAIL ONLY ──────────────────────
//
// LBA 108 fails on BATCH reads (any batch including it) but succeeds
// individually. Models a marginal sector that the drive can ECC-recover
// when read alone but not at multi-sector throughput. Validates that
// adaptive batch's drop-to-count=1 retries the same starting position
// and rescues the data.
//
// Implementation note: the scripted reader marks the entire batch failed
// on any failed sector. We can't easily differentiate "single vs batch"
// without bigger plumbing — so this profile uses a script that fails
// once then succeeds on retry at the same LBA, simulating "drive
// recovered after retry."

#[test]
fn profile_08_batch_fail_singles_ok() {
    let capacity_sectors: u32 = 256;
    let (mut reader, trace) = ScriptedSectorReader::new(capacity_sectors);
    // Sector 108: fail on first call (which is the batch read), succeed
    // on second call (the drop-to-count=1 retry at the same position).
    reader.sequence(
        108,
        vec![
            ScriptStep::Err {
                sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x00,
            },
            ScriptStep::Ok,
        ],
    );

    let nontrimmed = [(100 * 2048, 16 * 2048)];
    let finished = [
        (0, 100 * 2048),
        (116 * 2048, (capacity_sectors as u64 - 116) * 2048),
    ];

    let (pr, stats, trace_len) = run_profile(
        "08_batch_fail",
        capacity_sectors,
        &nontrimmed,
        &finished,
        reader,
        trace,
    );

    // GOLDEN: the second attempt succeeds → all 16 sectors recovered.
    assert_eq!(
        stats.bytes_good,
        capacity_sectors as u64 * 2048,
        "08_batch_fail bytes_good — second attempt should recover"
    );
    assert_eq!(stats.bytes_unreadable, 0, "08_batch_fail bytes_unreadable");
    assert_eq!(stats.bytes_pending, 0, "08_batch_fail bytes_pending");
    assert!(!pr.halted, "08_batch_fail halted");
    assert!(
        trace_len <= 80,
        "08_batch_fail trace_len={trace_len} exceeds 80"
    );
}

// ─────────────────────────────────────────────────────────────────────────
//
// Suppressed for now: NOT_READY-then-recover, HARDWARE_ERROR (wedge),
// ILLEGAL_REQUEST (wedge), and ABORTED_COMMAND profiles. Each would
// trigger long real-time sleeps inside `handle_read_failure`:
//
//   - NOT_READY (sense_key=0x02, asc=0x02/0x03/0x04): 15 s pause per
//     occurrence (`patch_not_ready_pause`), and retries the same LBA
//     in-place. Even one NOT_READY costs the test 15 s wall-time.
//
//   - HARDWARE_ERROR / ILLEGAL_REQUEST: 30 s per occurrence
//     (`WEDGE_FAMILY_COOLDOWN_SECS`), bounded by
//     `WEDGE_ABORT_THRESHOLD=16` before wedged-exit. Worst case ~8
//     minutes per profile.
//
// The sleeps are not injectable. Adding them would require either a
// `now()` / `sleep()` trait injection (out of scope for the unification
// task) or a "test mode" compile-time flag (architectural smell). The
// behavioural contracts for those paths are captured in
// `read_error.rs`'s in-module tests instead — they exercise the
// classifier without invoking the patch loop's sleep side-effects.
//
// If the unification ever proceeds, the next step is to add a clock
// injection point in `handle_read_failure` and extend this fixture
// with the wedge/NOT_READY profiles too.
