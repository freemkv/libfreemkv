# dvdnav::nav — First-Play navigation executor

The DVD arm of "mimic a real player" title selection (issue #40).

A player enters a disc at the First-Play PGC (FP_PGC): a short program of VM
commands (parsed by `super::vmcmd`) that runs before any menu draws and
typically dispatches straight to the feature title. This module reads that
program out of `VIDEO_TS.IFO` (VMGI), executes its pre-command list on a
minimal register machine, and — when the program deterministically reaches a
title dispatch (`JumpTT`) — maps the selected VMG title through the title
search pointer table (`TT_SRPT`) to its `(VTS number, title-in-set)`
coordinates. That is the title a player would land on, so DVD title selection
can prefer it over the structural leading-cell heuristic.

Every table count and offset is attacker-controlled, so all arithmetic is
checked and all reads are bounds-guarded — the entry point `resolve_from_vmg`
never panics on any input (exercised by the never-panic harness).

## The `Vm` register machine

`Vm` models the subset of the DVD-Video VM register file the First-Play
resolver needs: 16 general parameters (GPRMs) and the system parameters
(SPRMs). Execution starts from a cold machine (all zero) — the same starting
point a player has before any user interaction. The SPRMs a First-Play
routine might branch on (region, parental level, language) are
player-/session-specific and unknown at scan time; leaving them zero means
the resolver only commits to a title when the program reaches one *without*
depending on them (an unconditional dispatch, or a conditional whose
zero-register path a player also takes). When a branch genuinely depends on
an unknown SPRM the routine is treated as undecidable and the caller falls
back — see the module contract above.

`gprm_tainted` tracks per-GPRM taint: set when the register holds a value
derived from a system parameter (SPRM). An SPRM read taints a compare
directly (`is_sprm`); this bit tracks an SPRM *laundered through* a GPRM
store so a later GPRM-only compare that reads it is still recognised as
undecidable.

## `Vm::eval` — compare taint

`eval` evaluates a compare predicate and returns the predicate result
together with a *taint* flag set when either operand read a system parameter
(SPRM). An SPRM's value is player-/session-specific (region, parental level,
language) and unknown at scan time, so a branch decision that depends on one
is undecidable — the caller abstains rather than commit to the
cold-power-on arm. A GPRM read taints only when that register was itself
loaded from an SPRM (tracked by `gprm_tainted`); a GPRM holding a
program-established value does not taint.

## Test fixture: `build_vmgi`

`build_vmgi` assembles a VMGI (`VIDEO_TS.IFO`) image with a First-Play PGC
whose pre-command list is `pre`, and a TT_SRPT at sector `tt_srpt_sector`
mapping each title to `(vtsn, vts_ttn)`.

Layout: the fixed VMGI header (magic + the FP_PGC and TT_SRPT pointers), then
the FP_PGC (with a command table holding `pre`), then the TT_SRPT at its
sector. Kept minimal but spec-shaped so the same code path a real disc takes
is exercised.

## Regression test rationale

- `sprm_gated_dispatch_abstains`: a dispatch gated on an SPRM is undecidable
  at scan time. `if SPRM0 == g0 -> JumpTT 1` is *true* on the cold machine
  (both read 0), so without SPRM taint-tracking the resolver would wrongly
  return title 1; with it, the SPRM read taints the branch and it abstains.

- `sprm_guarded_store_then_unconditional_dispatch_resolves` (Defect-1
  regression, over-abstention): a store guarded by an SPRM-tainted compare
  does NOT gate a control transfer, so it must not abstain. `if SPRM0 == g0
  -> g0 = 5` (a guarded store) is followed by an UNCONDITIONAL `JumpTT 1`; the
  tainted predicate only decides whether the store runs, so the resolver must
  resolve to title 1.

- `sprm_laundered_through_gprm_abstains` (Defect-2 regression,
  under-abstention): an SPRM laundered through a GPRM must still taint a
  later branch. `SetGPRM g0 = SPRM20` followed by `if g0 == g1 -> JumpTT 1` —
  the compare reads only GPRMs, but g0 now holds an SPRM-derived value, so
  the decision is undecidable and the resolver must abstain.
