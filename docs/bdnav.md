# bdnav — HDMV navigation resolver

Notes relocated from `src/bdnav/mod.rs` doc comments (kept out of the source
to respect the comment-guard's prose caps).

## No per-disc special-casing

There is no per-disc special-casing: one correctly-booted VM resolves
densely-branched dispatchers as well as simple ones. The honest boundary is
BD-J (the feature is chosen by a Java Xlet, not by the HDMV VM); there — and
on any malformed data or non-convergence — the resolver ABSTAINS (`None`)
and selection falls back to the structural/heuristic order.

## `resolve_feature`

Resolves the playlist id the disc's First-Play navigation plays as the
feature, restricted to ids the caller marks as feature candidates
(`is_feature_candidate` — typically video-bearing, non-trivial titles, so a
short logo/pre-roll `PlayPlayList` is skipped). Returns `None` for BD-J
discs, missing/malformed nav data, or when navigation does not converge on
a candidate.
