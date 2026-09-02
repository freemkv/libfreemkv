# BD-TS demux (`src/mux/ts.rs`) — extended rationale

Pointer target for `// See docs/ts-demux.md` comments in `ts.rs`. Each
section below expands a comment that was trimmed in the source for the
comment-guard's prose cap.

## `MAX_PES_BUFFER_TOTAL` — aggregate PES-buffer ceiling

`MAX_PES_BUFFER` bounds each PID's buffer independently and never sees the
total, while the tracked-PID count comes straight off the disc:
`TsDemuxer::new` makes one `PesAssembler` per SELECTED stream, and the
selection derives from the MPLS STN, whose per-category counts are `u8`
(up to 255 each across 8 categories) bounded only by the MPLS file's own
bytes. A crafted MPLS declaring 100 streams on 100 distinct PIDs, plus a
clip feeding each PID continuation packets (no PUSI) until just under the
per-PID cap, held 100 x 64 MiB = 6.4 GiB of PES buffers at once; 1000
distinct PIDs — well inside the 8192-entry `pid_index` table — is 64 GiB.

So the per-PID cap is derived from this total instead: `pes_cap` is
`MAX_PES_BUFFER_TOTAL / tracked_pids`, clamped to `MAX_PES_BUFFER`. A real
title selects a handful of streams and keeps the full 64 MiB each; only a
stream count far past anything an authored disc carries is squeezed, and
even then a complete HEVC/UHD access unit (1-3 MiB) still fits at ~170
PIDs. Overflow is graceful in any case — the partial PES is dropped and
the assembler resyncs on the next PUSI, flagging a discontinuity.

## `cc_is_gap` — continuity-counter gap test

Canonical continuity-counter gap test (ISO/IEC 13818-1 §2.4.3.3). The
4-bit `continuity_counter` increments by one for each TS packet of a PID
that CARRIES PAYLOAD — a packet with adaptation field only does not
increment it, so such packets must be excluded by the caller rather than
diffed here. A packet MAY legally repeat the previous counter (the spec's
duplicate packet, whose payload is identical); that is not a loss.
Anything else means one or more packets for the PID were dropped.

Single source of truth for BOTH users in this file: the PES assembler
(`process_packet`) and the PSI section reassembler (`collect_psi_section`).
`collect_psi_section` used to reimplement it as a strict `cc != expected`,
which rejected legal duplicates and legal AF-only packets — a
spec-conformant PMT continuation was then reported as desync and the
title's stream list came back empty.

## `collect_psi_section` — PSI section reassembly across TS packets

Reassemble a single PSI section (PAT / PMT) for `target_pid` with the
expected `table_id`, respecting TS-packet boundaries. The section pointed
at by `pointer_field` in the PUSI packet may be longer than the 184-byte
TS payload (PSI sections can reach 1021 bytes; a PMT with many ES entries
spans 2+ packets). Reading a flat slice of the input would walk straight
through the next packet's TP_extra_header + TS header as if it were table
content, yielding a wrong PID / garbage stream_type. This walks the PUSI
packet, applies `pointer_field` bounded to within that packet's payload,
then appends the payload of each subsequent continuation packet (same
PID, no PUSI) until `3 + section_length` bytes have been collected.
