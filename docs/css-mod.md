# `src/css/mod.rs` — extended notes

Overflow detail relocated from doc comments in `src/css/mod.rs` (kept short
there per the comment-guard caps). Function/item names below match the code.

## `CSS_LOCKED_BAIL`

The bus-auth read gate is global (all-or-nothing), so a run this long of
`05/6F/03` (CSS-locked) reads means the gate is shut and nothing here is
crackable — bail instead of grinding the full 50_000-sector budget. That
grind is what made rc5 appear to hang on a wedged USB bridge.

## `crack_key`

Recovers the CSS title key with no keys, by scanning scrambled sectors and
running the known-plaintext attack (see the `keyless` module). The crib comes
from the periodic-run detector: a scrambled sector's cleartext region
(bytes 0x00..0x80) often ends in a short-period repeating run (stuffing /
constant fill); the attack assumes that run continues across the 0x80
boundary into the encrypted region, giving the known plaintext the 2^16 LFSR
recovery needs. Scans up to 50000 sectors across the extents and returns the
first sector that yields a key — no player keys, no disc-key crack. Works on
a live drive (after bus-auth unlocks reads) and on disc images alike.

This convenience form runs to completion (no cancellation) and returns just
the key; callers needing an operator-Stop / watchdog cancel, or the
three-way `CrackOutcome` (to distinguish "unencrypted" from
"encrypted-but-uncracked"), use `crack_key_outcome`, which takes a `halt`
token.

## `CrackOutcome`

Distinguishes the three cases the bare `Option<CssState>` conflated (and
which caused a silent-failure bug: scrambled-but-uncracked content was
treated as "unencrypted" and muxed as plaintext garbage at exit 0):

- `Cracked` — a scrambled sector yielded a title key.
- `Unencrypted` — NO scrambled sector was seen across the scanned extents
  (`is_scrambled_pack` never true): the content is genuinely plaintext, so
  proceeding without a key is correct.
- `ScrambledUncracked` — scrambled sectors WERE seen but no key could be
  recovered (the recovery found no crackable crib, or the scrambled region
  was unreadable). The content is encrypted; muxing it as plaintext would
  emit garbage, so callers MUST surface a hard error instead of falling
  through to "unencrypted" — the per-title `crate::error::Error::CssKeyMissing`
  when it is ONE title's own re-crack that failed (skippable: a sibling VTS
  may still crack), or the disc-level `crate::error::Error::CssNoDiscKey`
  when it is the disc-wide scan (`Disc::css_error`, every title fails
  identically).

## `resolve_dvd_title_key`

The SINGLE place every DVD read path obtains a title key, so the file-backed
mux highway (`crate::build_iso_pipeline`) and the live-drive single-pass
`crate::DiscStream` descramble a DVD identically ("reading is reading"). CSS
keys are per-VTS and crackable from the scrambled data itself, so a
`None`/MPEG-PS title cracks its own key here, in playback order over
`extents`. Everything else is left untouched:

- AACS keys (HD-DVD `.evo` is also MPEG-PS but arrives as `Aacs`) — no CSS.
- a title that already carries a key — nothing to resolve.
- a genuinely clear DVD (no scrambled sector) — stays `None`, a mux no-op.

A scrambled-but-uncrackable title is a hard `crate::error::Error::CssKeyMissing`,
never a silent scrambled-passthrough mux. That code is the PER-TITLE one
(`error::is_skippable_title_stub`), which is correct here: this function
cracks ONE title's own extents, and another VTS on the same disc may still
yield its key, so an all-titles rip skips this title and finishes the rest.
The whole-disc failure is `crate::error::Error::CssNoDiscKey`, raised by
`Disc::ensure_decryptable_keys` from the scan's `css_error`.

## `crack_key_scan` (private)

Tracks a `saw_scrambled` flag so a scrambled-but-uncracked disc is
distinguished from a genuinely-unencrypted one (the `crack_key` `Option`
wrapper collapses both to `None` via `CrackOutcome::into_state`).

## `descramble_sector`

A no-op unless the sector is a scrambled MPEG-2 PS PACK. The pack start code
is checked, not just the byte 0x14 flag bits, for the same reason
`descramble_region` checks it: 0x14 only means "scrambling control" inside a
pack, and in an IFO it is whatever that format stores there. A real
`VIDEO_TS.IFO` sector holds 0x15 there while starting `00 26 00 00`;
descrambling it destroyed 1912 of its 2048 bytes, and because that sector
carries TT_SRPT the disc enumerated 38 titles while an image decrypted from
it enumerated 10, silently, at exit 0.

This function has no callers inside the crate, but the module-level example
prescribes it — so the crate's own documented guidance led straight into
that defect. Making the guard part of the function, rather than something
each caller must remember, is what keeps the safe path the easy one.

## `descramble_region`

Descrambles a whole CSS buffer in place, re-cracking the title key on a VOB
region boundary. `title_key` is a CACHE of the last crack, not a fixed disc
key: it changes per VTS/VOB region, so it is validated on every scrambled
sector and re-cracked on a miss (the standard on-demand per-region rekey).

This CSS key acquisition is intrinsic to the cipher — CSS has no external key
source, the ONLY way to a title key is cracking the data — so it lives with
the CSS primitives and runs inside `decrypt::decrypt_sectors` (a public,
self-contained CSS decrypt), NOT at the post-decrypt recovery seam that AACS
key-fetch and FMTS segment-skip use (those consume external inputs).

The clear header (`<0x80`) is never scrambled, so its periodic crib predicts
the plaintext at `0x80`. Descramble with the cached key; if the crib fails to
reappear the key region changed (or the primed key was wrong) — restore the
ciphertext, re-crack from this very sector, and descramble again. A crib-less
sector (no periodic run) can be neither validated nor cracked, so it rides
the cached key — correct, because it lives in the same region as the nearby
crib sector that set the cache.

This section used to document an `Error::DecryptFailed` for the case where a
sector's crib rejects the cached key and the re-crack from that sector also
fails. That behaviour was tried and REVERTED: crib mismatch plus crack
failure is the signature of a crib FALSE POSITIVE, not of a stale key, and
failing there made real discs unrippable. The code now descrambles with the
cached key and returns `Ok`.

## `has_scramble_flag_bits`

Deliberately NOT called `is_scrambled`. Byte 0x14 only means "scrambling
control" inside an MPEG-2 Program Stream pack; in an IFO, UDF or ISO 9660
sector it is whatever that format stores there. Treating the flag alone as
proof of scrambling is what destroyed 1912 bytes of a real disc's
`VIDEO_TS.IFO` — the sector carrying TT_SRPT — so the disc enumerated 38
titles and an image decrypted from it enumerated 10, silently, at exit 0.

Callers want `is_scrambled_pack`. It asks the same question and also
requires the pack start code, which every genuinely scrambled VOB sector
carries and no IFO sector does.

It stays public only because an integration test asserts the flag
extraction directly. It has no production callers, and its previous doc
comment claimed one (`decrypt::decrypt_sectors`) that did not exist — so the
name was an invitation and the documentation was an argument for accepting
it.

## `is_scrambled_pack`

The HARDENED test the crack scan uses to set its `saw_scrambled` evidence
flag (Fix 3).

`has_scramble_flag_bits` keys solely on bits 4-5 of byte 0x14. That byte is
only meaningful inside a real DVD sector — an MPEG-2 Program Stream pack,
which ALWAYS begins with the 32-bit pack-start code `00 00 01 BA` at offset
0x00. A tiny clear / nav-only stub (a 0.5 s menu loop, an FBI-warning title)
can carry arbitrary bytes that happen to set bits 4-5 of byte 0x14; trusting
byte 0x14 alone there would flip the scan's `saw_scrambled` gate and make a
genuinely-UNENCRYPTED title report `ScrambledUncracked` — a false E7023.

Requiring the pack-start signature FIRST means only a sector that is
structurally a DVD video pack can be counted as scramble evidence. This does
NOT weaken the genuine "encrypted but uncrackable" hard-fail: a real
scrambled feature is made of valid PS packs, so its scrambled sectors still
pass this check and still drive `ScrambledUncracked` when no key cracks.

The pack-start check alone is not enough, and the sector's PES `stream_id`
(offset 0x11, the byte after the 14-byte pack header's `00 00 01` PES
prefix) is the second load-bearing gate. CSS scrambles ONLY elementary
streams — video (`0xE0..=0xEF`) and private_stream_1 audio/subpicture
(`0xBD`) — and it never touches the clear header, so byte 0x11 is the TRUE
stream_id even on a scrambled sector. The MPEG-PS structural packets that
are never CSS-scrambled — system_header (`0xBB`), padding (`0xBE`) and
private_stream_2 (`0xBF`, DVD PCI/DSI navigation) — must be excluded,
because on those the byte at 0x14 is NOT a PES scrambling-control field but
raw payload/structure whose bits 4-5 land set by chance.

This is the DETECTION defect a decrypted HD-DVD tripped: an HD-DVD `.evo` is
MPEG-PS exactly like a DVD `.vob`, and its RDI navigation packs are
private_stream_2 (`0xBF`) whose payload byte at 0x14 routinely has bits 4-5
set. With no `0x11` gate those nav packs flipped the crack scan's
`saw_scrambled` flag on a disc that carries no CSS at all; the crack then
found no key (there is none) and the scan returned `ScrambledUncracked`,
hard-failing a perfectly good HD-DVD with `CssKeyMissing` — E7023. Excluding
`0xBB/0xBE/0xBF` at 0x11 makes the evidence gate match what the crack itself
can act on (the recovery only recovers a key from a scrambled ES pack), so a
decrypted HD-DVD now scans to `Unencrypted` and muxes cleanly.

This does NOT weaken the genuine "encrypted but uncrackable" hard-fail on a
real DVD: a scrambled DVD feature is made of video (`0xE0..`) and
private_stream_1 (`0xBD`) packs, none of which are excluded, so its
scrambled sectors still set `saw_scrambled` and still drive
`ScrambledUncracked` when no key cracks. Byte 0x11 is in the clear header,
so a scrambled DVD pack can never masquerade as `0xBB/0xBE/0xBF`.

The DESCRAMBLE path gates on this same function — `descramble_sector` and
`descramble_region` both call it, not the raw flag test — because the raw
test does not merely mis-skip a sector there: it descrambles one that was
never scrambled and destroys it. The measured case is written up above under
`descramble_region`: a `VIDEO_TS.IFO` sector holding 0x15 at offset 0x14 lost
1912 of its 2048 bytes, taking TT_SRPT with it, and the disc's 38 titles
became 10 — silently, at exit 0. One gate, both paths.

## Tests (`mod tests`)

Detail relocated from test doc comments:

- `a_crib_false_positive_keeps_the_cached_key_rather_than_failing`:
  `attack_crib` is a heuristic — it finds a periodic run in the unscrambled
  header and predicts the run continues past 0x80. When that prediction does
  not hold, the crib reports a mismatch even though the cached key is
  correct, and the re-crack then fails BECAUSE the crib was never valid. So
  this combination is the signature of a crib false positive, not of a stale
  key, and the cached key remains the best available evidence. This test
  exists because a prior round read the same code as "descrambling with a
  key we just proved stale" and made it `DecryptFailed` to match the AACS
  path; real DVDs hit this constantly, and the change made a real disc
  unrippable (no unit test caught it — the real-media acceptance gate did).
  CSS is not AACS: an AACS unit key either opens a unit or does not, whereas
  a CSS title key is recovered from data whose recoverability varies sector
  by sector.

- `has_scramble_flag_bits_short_buffer_is_false_no_panic`: grounding —
  `sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0` short-circuits so
  a short buffer never reads index 0x14. Mutation: swapping operand order
  would panic indexing a 20-byte slice; this test catches it.

- `has_scramble_flag_bits_uses_bits_4_5_only`: grounding —
  `(sector[0x14] >> 4) & 0x03`. Mutation: widening the mask to `& 0x0F` makes
  0x40 report scrambled; the 0x40 assert catches it.

- `has_scramble_flag_bits_exact_sector_length_accepted`: grounding —
  `sector.len() >= 2048`. Mutation: `>= 2048` → `> 2048` makes an exact
  2048-byte scrambled sector report false; this catches it.

- `is_scrambled_pack_requires_pack_start_signature`: Fix 3 hardening —
  `is_scrambled_pack` requires BOTH the pack-start code and the 0x14 bits, so
  a clear/nav-only stub with stray 0x14 bits isn't scramble evidence (else a
  genuinely unencrypted title reports the false E7023).

- `is_scrambled_pack_excludes_nav_and_structural_stream_ids`: detection fix —
  `is_scrambled_pack` must exclude system_header/padding/private_stream_2 by
  `stream_id` at 0x11, since CSS never scrambles those and byte 0x14 there is
  raw payload, not a scrambling-control field. This is the exact
  decrypted-HD-DVD defect (an `.evo` RDI pack is 0xBF with pack-start and
  0x14 bits set); the video/private_stream_1 cases prove the exclusion
  doesn't weaken real-DVD detection.

- `MockSource` fields `lock_all`, `crackable`, `short_read`, `stream_id`: model
  a CSS-locked drive (`05/6F/03` on every read), a designated crackable LBA
  serving a full synthetic scrambled sector, a short-read count (the
  `recovery: true` short-read contract, `Some(0)` being the degenerate
  spin-guard case), and the PES `stream_id` written at 0x11 (used to model an
  HD-DVD `.evo` RDI nav pack via `PRIVATE_STREAM_2`).

- `crackable_sector`: builds a crackable scrambled sector whose cleartext
  header (0x59..0x80) carries a periodic run continuing across the 0x80
  boundary, mirroring the `synth_periodic_sector` fixture in the keyless
  tests but built from the crate-internal `scramble_sector`.

- `a_source_that_returns_zero_sectors_still_obeys_the_scan_budget`: `tried`
  increments only per inspected sector inside `for s in 0..usable`; an
  `Ok(0)` never runs that loop, so `advance` is forced to 1 to keep the
  cursor moving and the extent's full declared length is walked. Mutation:
  deleting the `tried` charge in the `usable == 0` arm goes red at 60,000
  reads for a misbehaving/adversarial source.

- `crack_key_caps_total_tries_at_50000`: grounding — `max_tries = 50_000`,
  `tried += 1` before the read, loop guard `tried < max_tries`. Mutation:
  widening the cap, or removing the increment, both go red.

- `crack_key_budget_is_shared_across_extents`: grounding — `tried` is
  declared outside the extent loop with a shared `if tried >= max_tries`
  break. Mutation: moving the declaration inside the loop would give each
  extent its own budget (80,000 total instead of 50,000).

- `crack_key_scans_from_extent_start_lba`: grounding —
  `reader.read_sectors(ext.start_lba + i, ...)`. Mutation: dropping
  `ext.start_lba +` would start LBAs at 0 instead of the extent's start.

- `crack_key_continues_past_read_errors`: grounding — a read `Err` falls
  through to the next sector rather than aborting. Mutation: propagating the
  error with `?` would stop the scan after the first failure.

- `crack_key_empty_extent_reads_nothing` / `crack_key_no_extents_is_none`:
  grounding — `while i < ext.sector_count` / `for ext in extents` are no-ops
  on empty input; an off-by-one (`<=`) or a pre-loop read would break these.

- `crack_outcome_reaches_cracked_with_span`: audit gap "MockSource never
  yields a crackable sector" — drives the full `crack_key_scan` over a
  synthetic ISO to exercise the `Cracked` branch and `crack_span` recording
  end-to-end, previously untested.

- `is_scrambled_uncracked_is_true_for_that_case_and_false_for_the_other_two`:
  a prior round introduced the three-way split precisely because conflating
  the cases made an uncrackable disc exit 0 with garbage output; earlier
  tests only asserted the TRUE direction, so a predicate answering "yes"
  unconditionally was indistinguishable from a correct one. All three
  outcomes here come from real `crack_key_outcome` scans, not hand-built enum
  values.

- `resolve_dvd_title_key_*` tests: `resolve_dvd_title_key` is the SINGLE
  shared per-title CSS step both read paths (`build_iso_pipeline` multi-pass
  and `DiscStream::new` single-pass) call, so these pin its full contract at
  the shared boundary — crack success, hard-fail on scrambled-uncrackable
  (never leaving `keys` as `None`, which would mux scrambled bytes as
  plaintext), the decrypted-HD-DVD RDI-pack regression (end to end), `--raw`
  passthrough, the AACS-keys-untouched gate, the clear-DVD no-op, and the
  halt-cancellation-surfaces-as-Halted fix (not a truncated-scan verdict).

- `all_locked_synthetic_iso_yields_css_key_missing_signal`: audit §2/§5 #7 —
  pins that an all-locked synthetic ISO across MULTIPLE extents produces
  `ScrambledUncracked`, the exact signal `disc/mod.rs` converts into
  `disc.css_error = Some(Error::CssKeyMissing)`.

- `recrack_succeeds_on_other_vts_extents`: audit gap "success path missing"
  — the prior re-crack test only covered the locked→None path; this proves a
  key cracked for one VTS is genuinely re-derived (not reused) for another.
