# Rip recovery — what libfreemkv owns

**Recovery strategy moved OUT of this crate in 1.6.0.** The forward sweep, the
targeted retry pass, the ddrescue mapfile, damage classification and the
multipass loop now live in the **`freemkv-engine`** crate as
`freemkv_engine::recovery::{copy, sweep, patch}`. The dependency runs
engine → libfreemkv, so this crate cannot call into the engine; front-ends
(`freemkv` CLI, autorip) get recovery from the engine directly.

What stayed here are the two layers underneath the strategy: the single-shot
read primitive, and the in-stream request-size adaptation that sits in front of
it. This document covers those, plus the design constraints they encode — the
constraints are the reason the strategy above them looks the way it does, so
they belong with the code that enforces them.

For the strategy itself — damage-jump thresholds, pass ordering, mapfile status
state machine, wedge detection — read `freemkv-engine/src/recovery/`.

| Layer | Where it lives | What it does |
|-------|---------------|--------------|
| 1 — Bad-range retry | **`freemkv-engine`** (`recovery::patch`) | Re-reads non-`+` ranges with the long timeout. Idempotent; caller invokes N times. |
| 2 — Single-shot primitive | `Drive::read` in `src/drive/mod.rs` | One CDB, one timeout, one result. No inline retries, no SCSI reset. |
| 3 — In-loop request adaptation | `DiscStream::fill_extents` in `src/mux/disc.rs` | Halves the batch on failure, retries at the same LBA, walks back up on a clean-read streak. |

Layer 2 also translates drive facts: [`SenseFamily`](../src/scsi/mod.rs)
classifies SCSI sense data into the categories the engine's strategy routes on
(marginal vs. hardware vs. not-ready). Getting that classification wrong
silently misroutes recovery, which is why it lives next to the transport rather
than in the strategy.

Layer 3 runs inside any consumer of `DiscStream` — direct PES pipeline, ISO
playback — without caller involvement, and applies whether or not the engine's
recovery is in play.

## In-stream — adaptive batch halving (`DiscStream::fill_extents`)

When a consumer reads a `DiscStream` directly (no ISO intermediate),
`fill_extents` runs an adaptive sizer in front of `Drive::read`:

1. Try the current preferred batch size (e.g. 32 sectors, one BD ECC block).
2. On failure: halve the batch and retry at the same LBA. Emit
   `EventKind::BatchSizeChanged { reason: Shrunk }`.
3. On a clean-read streak: probe back up toward the preferred size. Emit
   `EventKind::BatchSizeChanged { reason: Probed }`.
4. If a single-sector read fails: skip (zero-fill, emit
   `EventKind::SectorSkipped`) when `skip_errors` is set, otherwise return
   `Err(DiscRead)`.

This exists so a transient single-sector glitch inside a 32-sector batch can be
isolated and read individually without the caller implementing retry logic. See
[`src/event.rs`](../src/event.rs) for the emitted events.

## Design choices

These are constraints on the read path, enforced here and relied on by the
engine's strategy.

**`Drive::read` is single-shot.** No inline retry phases, no SCSI reset, no
eject cycle. The `recovery` flag controls only the per-CDB timeout (10 s vs.
60 s); on any failure it returns `Err(DiscRead)` immediately. Inline recovery
(5× gentle retry → close + SCSI reset + reopen → 5× more) was removed in
0.13.6. See the stop-wedge postmortem (2026-04-25) for rationale: the inline
reset on the LG BU40N (Initio USB-SATA bridge) wedged drive firmware below the
bridge without ever recovering a sector, and the gentle-retry phase produced
long stretches of 0 KB/s with nothing to show for it. Recovery responsibility
is layered instead: layer 1 handles ranges, layer 3 handles request size,
neither touches the wedge-prone reset path.

**No `MODE SELECT` to disable drive retries.** Neither ddrescue nor any
consumer ripper does this. Drive firmware has access to raw analog signal,
laser power control and drive-specific ECC tuning that userspace cannot
replicate — disabling it throws away recovery headroom on marginal sectors. The
fast pass fails quickly via short SG_IO timeouts and lets the firmware work the
long timeout during retry.

**No SCSI reset from any read path.** There is no reset escape hatch on
`Drive` at all: the `SG_SCSI_RESET` ioctl and STOP/START UNIT escalation went in
0.13.6, the macOS reset (always a no-op) was removed entirely, and the
top-level `scsi::reset()` wrappers went with their last callers. The only
remaining reset is a Windows-specific device-level helper in
[`src/scsi/windows.rs`](../src/scsi/windows.rs), never reached from a read.

**ISO intermediate, even for single-pass.** The engine's Pass 1 always writes
an ISO, and the mux stage reads it back via `FileSectorSource`. For a
no-retry rip this costs a few minutes but buys resumability across crashes,
re-muxability without re-ripping, and a persistent forensic artifact. Callers
who need pure speed can bypass it with `DiscStream::new(Box::new(drive), …)` —
nothing forbids it, and layer 3 still applies there.

## References

- [ddrescue manual, Algorithm chapter](https://www.gnu.org/software/ddrescue/manual/ddrescue_manual.html)
- [ddrescue optical media notes](https://www.electric-spoon.com/doc/gddrescue/html/Optical-media.html)
- Recovery strategy and mapfile: `freemkv-engine/src/recovery/`
- In this crate: [`src/drive/mod.rs`](../src/drive/mod.rs) (`Drive::read`),
  [`src/scsi/mod.rs`](../src/scsi/mod.rs) (`SenseFamily`),
  [`src/mux/disc.rs`](../src/mux/disc.rs) (`DiscStream::fill_extents`),
  [`src/event.rs`](../src/event.rs) (progress events).
