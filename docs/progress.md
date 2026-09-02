# progress.rs — design notes

## Why one progress signal type

Pre-0.13.16 the API leaked `pos`, `bytes_good`, `work_done`, `bytes_pending`,
`Finished`/`NonTrimmed` mapfile semantics — and consumers reinvented the math
each time they wanted a percentage. UIs ended up reading one source while
server-side computed from another, producing wrong percentages without
anyone noticing. `PassProgress` + the `Progress` trait exist so there is
exactly one shape every pass emits and every consumer renders from.

## `PassProgress` field mapping for `PassKind::Verify`

- `work_done` = sectors read so far
- `work_total` = total sectors in title
- `bytes_good_total` = good + slow + recovered sectors × 2048
- `bytes_unreadable_total` = bad sectors × 2048
- `bytes_pending_total` = 0 (verify processes sequentially, nothing pending)

## Why `PassProgress` is not `Copy`

`located` carries a `Vec`. Constructed once per (throttled) emission and
passed by reference to `Progress::report`, so this costs one small heap
alloc per UI tick — cheap, and it makes `PassProgress` the single complete
contract a client renders from.

## `Heartbeat` hot-path cost

`tick` is cheap on the hot path: it reads one `Instant` and compares. For
pure-CPU inner loops where even that is too much, use `tick_cpu`, which
only consults the clock every 256 calls.
