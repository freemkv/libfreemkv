# `DecryptingSectorSource`

## Why this decorator is the single source of truth for decrypt-on-read

Every decrypt-on-read caller (e.g. `DiscStream`) wraps its source in this
decorator rather than calling the cipher helpers directly. The actual cipher
code lives in `crate::aacs` and `crate::css`; the decorator just calls the
existing `crate::decrypt::decrypt_sectors` helper that drives both of them
in-place after each read. Centralizing the call site means callers can wire
the decorator unconditionally and keep their pipeline shape uniform
regardless of encryption state — for `DecryptKeys::None` discs it is simply
a pass-through, so there is no branch in caller code for "is this disc
encrypted?".

## `KeyFetch`: why two explicit operations

`KeyFetch` exposes `unit_keys` and `fmts_indexes` as two separate operations
rather than one generic callback, so the "one base key vs. a whole forensic
set" contract lives in the type instead of a caller guessing at the return
length:

* `unit_keys` resolves the base Unit Key(s) for a CPS unit from real
  encrypted samples drawn from it. The non-forensic path: one key per CPS
  unit (the pool grows by whatever it returns). Used by the mux's base /
  multi-CPS map resolution and by the sweep/patch recovery decorator.
* `fmts_indexes` resolves the disc's AACS 2.1 forensic index keys from an
  index-1 single-phase anchor batch. The source hands back the COMPLETE set
  (ordered index 1..N); the caller sizes the forensic map to `len()` and
  never assumes a fixed N (32 is all we've seen, but the contract is
  "whatever the source returns, >= 1, is all of them").

`KeyFetch` is a stateless, shared pair of `Arc<Fn>`, so one instance is built
once and cloned cheaply (two `Arc` bumps) into every read path; it is
`Send + Sync` so it can ride the mux highway's producer thread.
