# Rip recovery — three-layer architecture

`libfreemkv` supports a multi-stage rip model for damaged or protection-bearing
discs: a fast forward sweep that tolerates read failures, in-loop request-size
adaptation that survives transient drive trouble without bailing, and targeted
retry passes against a persistent bad-range map. The stream pipeline
(`DiscStream` + `input`/`output`) operates against the resulting ISO image, so
the mux stage never touches the drive.

Recovery is layered cleanly. Each layer has one responsibility and does not
reach into the others.

| Layer | Where it lives | What it does |
|-------|---------------|--------------|
| 1 — Bad-range retry | `Disc::patch` (one pass over the mapfile per call) | Re-reads non-`+` ranges with the long timeout. Idempotent; caller invokes N times. |
| 2 — Single-shot primitive | `Drive::read` in `src/drive/mod.rs` | One CDB, one timeout, one result. No inline retries, no SCSI reset. |
| 3 — In-loop request adaptation | `DiscStream::fill_extents` adaptive batch sizer | Halves the batch on failure, retries at the same LBA, walks back up on a clean-read streak. |

The library exposes flat verbs; the caller drives the multipass loop. Autorip
runs `Disc::sweep` once, then loops `Disc::patch` until either the mapfile is
clean or the configured retry budget is exhausted, then hands the ISO off to
the mux pipeline. The `freemkv` CLI does the same shape with a
terminal-output progress sink. Layer 3 runs inside any consumer of
`DiscStream` (direct PES pipeline, ISO playback, etc.) without caller
involvement.

Three primitives compose the disc-side flow:

| Primitive                 | What it does                                                          |
|---------------------------|-----------------------------------------------------------------------|
| `Disc::sweep`             | disc → ISO, one forward pass. Writes a sidecar `.mapfile`. Opt-in skip-on-error. |
| `Disc::patch`             | Re-reads bad ranges from the drive. One pass per call; caller invokes N times. |
| `DiscStream` (ISO source) | Reads sectors from the ISO, feeds decrypt → demux → codec → mux.      |

## Data model

### Mapfile

Format: [ddrescue](https://www.gnu.org/software/ddrescue/manual/ddrescue_manual.html)-compatible
plain text, greppable, tool-interoperable. Flushed to disk on every `record()`
so a crashed rip loses at most one block.

```
# Rescue Logfile. Created by libfreemkv v0.13.6
# Current pos / status / pass / pass_time
0x000000000  ?  1  0
#      pos        size  status
0x000000000  0x12a35d000    +
0x12a35d000  0x000003000    -
0x12a360000  0x009c4a000    +
0x12d00a000  0x000064000    *
```

Status characters match ddrescue:

| Char | Meaning                                            |
|------|----------------------------------------------------|
| `?`  | Not yet attempted                                  |
| `*`  | Fast-pass failed; needs edge-trim                  |
| `/`  | Trimmed; interior needs sector scrape              |
| `-`  | Unreadable this session                            |
| `+`  | Finished (good)                                    |

Position and size are hex byte offsets into the ISO.

### `SweepOptions` and `PatchOptions`

The library no longer dispatches between sweep and patch internally — the
caller picks the verb explicitly per pass. The two option structs are flat
and have no overlap:

```rust
SweepOptions {
    decrypt: true,
    resume: false,
    batch_sectors: None,
    skip_on_error: true,           // damage-jump + zero-fill on read failure
    progress: Some(&reporter),
    halt: Some(flag),
}

PatchOptions {
    decrypt: true,
    block_sectors: None,
    full_recovery: true,
    reverse: true,                 // walk bad ranges high → low LBA
    wedged_threshold: 50,
    progress: Some(&reporter),
    halt: Some(flag),
}
```

Caller-orchestrated dispatch (the policy `Disc::copy` used to embed):

- No mapfile → `sweep` (fresh Pass 1).
- Mapfile with `?` ranges → `sweep` with `resume: true`.
- Mapfile covers full disc, only `*` / `/` / `-` ranges → `patch`.
- Mapfile clean → done; no further pass needed.

Each consumer (autorip, `freemkv` CLI) implements the loop in roughly five
lines of `Mapfile::stats()` checks.

## Algorithm

### Pass 1 — fast sweep (`Disc::sweep`)

1. Read one ECC block (32 sectors for UHD, 16 for BD/DVD) at the current LBA.
2. On success: write data to ISO, mark `+`, advance.
3. On failure (with `multipass`): zero-fill, mark `*`, advance.
4. Track a sliding window of the last 16 ECC block results. When ≥12% are failures
   → **damage-jump**: skip ahead by `256×batch×multiplier` sectors (8 MB base for
   UHD). Double the multiplier on each jump (8→16→32→64 MB...). Zero-fill the gap as `*`.
5. On 16 consecutive good reads: reset jump multiplier to 1, restore max read speed.
6. Speed control: damage zone entry → minimum speed, exit → maximum speed.
7. Only transport failures (USB bridge crash) abort the pass.

Pass 1 completes when every byte has been visited (either `+` or `*`).

### Pass 2+ — patch (`Disc::patch`)

`Disc::patch` reads the mapfile and iterates every non-`+` range. Default: **reverse** mode
(walks ranges from highest LBA to lowest, within each range from end to start).

1. Issue a single-sector read with 60 s timeout (`recovery=true`). Drive firmware
   does its own ECC recovery inside that window.
2. On success: write the good bytes into the ISO, mark `+`.
3. On failure with non-marginal SCSI sense: bail immediately (drive won't produce data).
4. On failure with marginal sense: mark `-`, continue.
5. Update the mapfile after every block — crash-safe resume.
6. Wedged-drive exit: 50 consecutive failures with zero recovery → bail this pass.

### In-stream — adaptive batch halving (`DiscStream::fill_extents`)

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

This is layer 3. It exists so a transient single-sector glitch in a 32-sector
batch can be isolated and read individually without the caller needing to
implement retry logic.

## Design choices

**`Drive::read` is single-shot.** No inline retry phases, no SCSI reset,
no eject cycle. The `recovery` flag controls only the per-CDB timeout
(1.5 s vs. 30 s); on any failure it returns `Err(DiscRead)` immediately.
Inline recovery (5× gentle retry → close + SCSI reset + reopen → 5× more)
was removed in 0.13.6. See the stop-wedge postmortem (2026-04-25) for rationale:
the inline reset on the LG BU40N (Initio USB-SATA bridge)
wedged drive firmware below the bridge without ever recovering a sector,
and the gentle-retry phase produced long stretches of 0 KB/s with no
recoveries to show for it. Recovery responsibility is now layered: layer 1
handles ranges, layer 3 handles request size, neither touches the
wedge-prone reset path.

**No `MODE SELECT` to disable drive retries.** Research showed neither ddrescue
nor MakeMKV does this. Drive firmware has access to raw analog signal, laser
power control, and drive-specific ECC tuning that userspace can't replicate —
disabling it throws away recovery headroom on marginal sectors. We fail fast
via short SG_IO timeouts in pass 1 and let the firmware work the long timeout
in pass 2 / patch.

**No SCSI reset from any retry path.** `SgIoTransport::reset` (Linux) is
trimmed to a kernel SG_IO state flush plus ALLOW MEDIUM REMOVAL — the
`SG_SCSI_RESET` ioctl and STOP/START UNIT escalation were removed in 0.13.6.
The macOS reset (which had been a no-op) was removed entirely. The top-level
`scsi::reset()` / `reset_with_timeout()` / `reset_blocking()` wrappers were
also removed (no callers). The remaining `Drive::reset()` is only invoked
explicitly by callers that need an eject-cycle escape hatch — it is never
reached from a read path.

**ISO intermediate, even for single-pass.** Pass 1 always writes an ISO. The
mux stage reads the ISO via `IsoSectorReader`. For single-pass (no retries),
this adds ~2-3 min (local disk mux) but gains resumability across crashes,
re-muxability without re-ripping, and a persistent forensic artifact. Callers
who need pure speed can bypass and use `DiscStream::new(Box::new(drive), …)`
directly — the lib doesn't forbid it, and layer 3 (adaptive batch halving)
still applies there.

**Mapfile in ddrescue format.** Plain text so users can `less` it, `diff` it,
or feed it to ddrescue's own tooling. Crash-safe (flush-per-record). Entries
coalesce on adjacent same-status ranges so files stay small.

**Patches target `-`, `*`, `/`, and `?` alike.** The status state machine is
ddrescue's but `patch` collapses the distinction — it just tries every
non-finished range with the long timeout. Future work can specialize (trim vs.
scrape vs. retry with direction reversal) if there's measured benefit.

## References

- [ddrescue manual, Algorithm chapter](https://www.gnu.org/software/ddrescue/manual/ddrescue_manual.html)
- [ddrescue optical media notes](https://www.electric-spoon.com/doc/gddrescue/html/Optical-media.html)
- Source: [`src/disc/mapfile.rs`](../src/disc/mapfile.rs), [`src/disc/sweep.rs`](../src/disc/sweep.rs) (`Disc::sweep`), [`src/disc/patch.rs`](../src/disc/patch.rs) (`Disc::patch`), [`src/drive/mod.rs`](../src/drive/mod.rs) (`Drive::read`), [`src/mux/disc.rs`](../src/mux/disc.rs) (`DiscStream::fill_extents`).
