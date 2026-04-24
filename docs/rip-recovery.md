# Rip recovery — multi-pass architecture

`libfreemkv` supports a two-stage rip model for damaged or protection-bearing
discs: a fast forward sweep that tolerates read failures, followed by targeted
retry passes against a persistent bad-range map. The stream pipeline
(`DiscStream` + `input`/`output`) operates against the resulting ISO image, so
the mux stage never touches the drive.

Three primitives compose the flow:

| Primitive                 | What it does                                                          |
|---------------------------|-----------------------------------------------------------------------|
| `Disc::copy`              | disc → ISO. Writes a sidecar `.mapfile`. Opt-in skip-forward on failure. |
| `Disc::patch`             | Re-reads bad ranges from the drive. Idempotent; call N times.         |
| `DiscStream` (ISO source) | Reads sectors from the ISO, feeds decrypt → demux → codec → mux.      |

The caller orchestrates. Autorip's `rip_disc` loops `copy` then N × `patch` per
the `MAX_RETRIES` config, then hands the ISO off to the existing mux pipeline.

## Data model

### Mapfile

Format: [ddrescue](https://www.gnu.org/software/ddrescue/manual/ddrescue_manual.html)-compatible
plain text, greppable, tool-interoperable. Flushed to disk on every `record()`
so a crashed rip loses at most one block.

```
# Rescue Logfile. Created by libfreemkv v0.11.22
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

### `CopyOptions` and `PatchOptions`

Defaults preserve pre-`0.11.21` behavior — full drive recovery on every read,
abort on the first unreadable sector. Opt in to the recovery-friendly path:

```rust
CopyOptions {
    skip_on_error: true,   // zero-fill bad blocks, continue
    skip_forward: true,    // exponential skip-forward after a failure
    resume: true,          // pick up from an existing ISO + mapfile
    decrypt: false,        // keep the ISO a raw disc image
    ..Default::default()
}
```

## Algorithm

### Pass 1 — fast sweep

1. Read 64 KB (32 sectors, one BD ECC block) at the current LBA.
2. On success: mark the range `+`, advance by one block.
3. On failure (with `skip_on_error`): zero-fill the block in the ISO, mark
   it `*`, advance.
4. If `skip_forward` is set: after a failure, jump ahead by an exponentially
   growing amount (256 KB initial, doubling on consecutive failures, capped at
   1% of disc). The skipped bytes are also marked `*` — `patch` will visit
   them later.
5. Reset the skip size to 256 KB on the first success after a failure.

Pass 1 completes when every byte has terminal status (`+`, `-`, or the caller
bails via the halt flag).

### Pass 2+ — patch

`Disc::patch` reads the mapfile and iterates every non-`+` range. For each:

1. Issue a drive read with full recovery enabled (SCSI-level retries,
   ECC recovery, the lot).
2. On success: write the good bytes into the ISO at the exact byte offset,
   mark `+`.
3. On failure: mark `-`.
4. Update the mapfile after every block — crash-safe resume.

Idempotent. Call `patch` N times for N retry attempts; typically the caller
stops early if a pass recovers zero bytes (structure-protected sectors will
never yield).

## Design choices

**No `MODE SELECT` to disable drive retries.** Research showed neither ddrescue
nor MakeMKV does this. Drive firmware has access to raw analog signal, laser
power control, and drive-specific ECC tuning that userspace can't replicate —
disabling it throws away recovery headroom on marginal sectors. We fail fast
via short SG_IO timeouts instead, and we avoid per-sector probing on first
contact by using large blocks + skip-forward.

**ISO intermediate, even for single-pass.** Pass 1 always writes an ISO. The
mux stage reads the ISO via `IsoSectorReader`. For single-pass (no retries),
this adds ~2-3 min (local disk mux) but gains resumability across crashes,
re-muxability without re-ripping, and a persistent forensic artifact. Callers
who need pure speed can bypass and use `DiscStream::new(Box::new(drive), …)`
directly — the lib doesn't forbid it.

**Mapfile in ddrescue format.** Plain text so users can `less` it, `diff` it,
or feed it to ddrescue's own tooling. Crash-safe (flush-per-record). Entries
coalesce on adjacent same-status ranges so files stay small.

**Patches target `-`, `*`, `/`, and `?` alike.** The status state machine is
ddrescue's but `patch` collapses the distinction — it just tries every
non-finished range with full recovery. Future work can specialize (trim vs.
scrape vs. retry with direction reversal) if there's measured benefit.

## References

- [ddrescue manual, Algorithm chapter](https://www.gnu.org/software/ddrescue/manual/ddrescue_manual.html)
- [ddrescue optical media notes](https://www.electric-spoon.com/doc/gddrescue/html/Optical-media.html)
- Source: [`src/disc/mapfile.rs`](../src/disc/mapfile.rs), [`src/disc/mod.rs`](../src/disc/mod.rs) (`Disc::copy`, `Disc::patch`)
