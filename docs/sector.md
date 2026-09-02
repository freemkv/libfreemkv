# src/sector/mod.rs — rationale notes

## `read_sectors_fua` FUA rationale

`fua = true` asks the drive to bypass its readahead cache and physically
re-fetch the medium — a Pass-N marginal-sector lever: a cached hit can mask a
*stochastic* sector that would land differently off the platter on each
physical read, so `FuaRetry` re-reads it FUA. It is never blanket-applied to
the bulk path (forcing every sequential read past the cache collapses
streaming throughput ~10x).

## Test rationale: forwarding-impl coverage (`mod tests`)

- `set_unit_base_generic` / `read_generic` / `read_fua_generic` /
  `set_speed_generic`: a direct call on a `&mut dyn SectorSource` or
  `Box<dyn SectorSource>` receiver auto-derefs straight to the vtable and
  never touches the `impl SectorSource for Box<dyn SectorSource>` /
  `impl SectorSource for &mut (dyn SectorSource + '_)` forwarding bodies.
  Routing the call through a generic `S: SectorSource` bound instead forces
  monomorphization to go through the forwarding body, so these tests are the
  only ones that actually exercise it.

- `mut_ref_dyn_forwards_read_sectors_to_the_inner_source`: proves the
  forwarding impl actually delegates `read_sectors` — passes args through
  unchanged, returns the inner source's byte count, and leaves the inner
  source's bytes in the caller's buffer. A body that returned a bare `Ok(n)`
  without calling through would be a delegating reader that reads nothing and
  reports success: the caller sees `Ok` and consumes an untouched buffer.

- `mut_ref_dyn_forwards_read_sectors_fua_to_the_inner_source`: same, for the
  FUA entry point — the forwarder must reach the inner source's FUA method
  (not silently downgrade to the plain read, and not fabricate a count),
  carrying the `fua` bit through. FUA is the Pass-N lever that re-fetches a
  stochastic sector past the drive cache (MMC-6 READ(10) FUA bit); a
  forwarder that dropped it would make every FUA retry re-read the same
  cached bytes and "confirm" the bad sector.

- `mut_ref_dyn_forwards_set_speed_to_the_inner_source` /
  `boxed_dyn_forwards_set_speed_to_the_inner_source`: `set_speed` hides worse
  than the read methods because the trait's own default body is also a
  no-op (`fn set_speed(&mut self, _kbs: u16) {}`), so a forwarding impl that
  drops the call on the floor compiles, type-checks, and looks exactly like a
  source that legitimately has no speed control. The consequence is not a
  wrong value but a silently absent one: the recovery path throttles a
  struggling drive by lowering its read speed, and a forwarder that swallowed
  the call would leave the drive at full speed through the damaged region
  while the caller believed it had slowed down.
