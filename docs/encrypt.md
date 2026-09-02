# encrypt.rs — relocated rationale

Overflow prose that would not fit under `ci/comment-guard.py`'s caps,
moved here per file with a `// See docs/encrypt.md — <topic>` pointer
left at the original site.

## HandshakeResult::drive_unlocked

True when the VID came from an unlocker (in `freemkv-unlock`) that
unlocked the drive. Such a drive serves CLEAR content, so AACS bus
encryption is already removed AT THE DRIVE — the same end state a
successful cert handshake's `read_data_key` provides, just via
firmware instead of the AKE. The bus-key gate MUST credit this as a
valid bus-removal: bus encryption is unremovable only when NEITHER the
firmware unlocked the drive NOR the cert handshake yielded a bus key.
Without this, a SUCCESSFUL unlock (VID present, `read_data_key: None`)
paradoxically trips the gate and blocks ALL key resolution (incl. the
online source).

## bus_encryption_removed

Single source of truth for "is AACS bus encryption gone for this
scan?". The gate asks ONLY this — `if !removed { error }` — never
enumerating cases. Bus encryption is gone when ANY of these holds:
the disc never had it; file/ISO reads (content already clear at read
time); an unlocker unlocked the drive (it serves clear content); or
the cert handshake produced the bus key. Add a NEW removal mechanism
HERE, never in the gate.

## AacsCertUnlocker

libfreemkv-side driver for the AACS cert route. It owns the host-cert
collection (a keysource concern that stays in libfreemkv) and then
dispatches the actual mutual-auth to the `freemkv-unlock` AACS
unlocker via the `crate::unlock_bridge`. The firmware (drive-prep) and
CSS routes dispatch the same way at their own call sites; this one
carries the host certs.

## AacsCertUnlocker::authenticate

Runs the host-certificate mutual-auth handshake: collect
non-compiled-in host certs from the key sources + credentials, then
hand them to the AACS unlocker (via the `freemkv-unlock` dispatch),
which tries each cert (wedge-guarded) and on success yields the
Volume ID + `read_data_key` (the AACS 2.0 bus key). Returns a
`CertUnlockFailure` on every no-VID outcome.

## unlock_error_to_error

Maps a `CertUnlockFailure` back to the `Error` variant
`do_handshake_cert` has always surfaced, so `scan_with`'s rendering
and the path-1 disc-hash → VUK fallback are byte-for-byte unchanged.
(`NoHostCert` keeps the `<no host cert>` sentinel.)

## handshake_has_volume_id

Did the cert handshake actually carry a Volume ID? Extracted so it can
be tested as a VALUE. It only ever reaches an operator as the
`has_volume_id` field of the `bus_key_unavailable` warn, and asserting
on a `tracing` field means installing a capturing subscriber — which
is thread-local, while `tracing`'s callsite-interest cache is global.
Those two facts race: the test failed roughly one run in ten under the
full parallel suite while passing every time in isolation, and
serialising the captures was not enough because the cache can be
re-evaluated against the process default rather than the thread-local
dispatch.

A predicate this small does not need a subscriber to verify. The
polarity is the whole point: an `==` here would tell an operator a VID
was absent on exactly the discs where one was present.

## Disc::do_handshake

SCSI handshake — drives the VID-acquisition flow and returns a
structured `HandshakeResult` for downstream key resolution.

VID acquisition runs through `do_handshake_cert`, which first uses the
OEM VID a firmware unlocker may have stashed at drive `init()` (a
drive-functionality capability decoupled from the host cert + HRL) and
falls back to the cert-based mutual-auth handshake (dispatched to the
`freemkv-unlock` AACS unlocker) when none is present. The cert path
also yields `read_data_key`, required for AACS 2.0 bus decryption.

Returns `(handshake, error)`:
- `(Some(_), None)` — VID acquired
- `(None, Some(_))` — specific failure mode (`AacsHostCertRejected` or
  `AacsVidUnavailable`)
- `(None, None)` — handshake not attempted (no keydb; resolution will
  proceed with VID=zero and rely on path 1 disc-hash → VUK lookup)

## Disc::do_handshake_cert

Cert-based AACS handshake — the cert route for VID acquisition.

Before running the cert mutual-auth, this checks for an OEM Volume ID
a firmware unlocker stashed at drive `init()`. Such an unlocker
unlocks *drive functionality*, not just the disc: VID retrieval via
the drive's OEM CDB is a capability separate from `unlock`. When one
served a VID, we use it and SKIP the cert handshake entirely — the
OEM path gets the VID *without* the host certificate + HRL, decoupling
VID from the cert chain. The OEM path yields no `read_data_key` (no
bus-key is derived); AACS 2.0 content needing read_data_key for bus
decryption must still use the cert path, so an unlocker with no OEM
VID capability returns `None` and we fall through to cert auth
unchanged.

## Disc::collect_host_certs

Collect every AACS host cert the caller carries, from BOTH the
explicit `DriveCredentials` and the key-source layer
(`crate::KeySource::host_certs` across each source), unioned. Host
certs are keysource-served, never compiled in; this is the one place
the OEM cert route gathers them. An empty result is the graceful
no-cert signal (the caller turns it into `Error::AacsNoHostCert`).
`mkb` is the disc's MKB generation when known, forwarded to each
source's `crate::KeySource::host_certs` so a source MAY return only
generation-appropriate certs (the default ignores it).

## Disc::resolve_vid_only

Build a keys-free AACS state that carries only the Volume ID (+
version metadata), for callers that resolve Unit Keys out-of-band and
have disabled the local keydb. The VID is on-disc content read during
the handshake; preserving it here lets the out-of-band path use it. No
keys are present (`unit_keys` empty, `vuk` None), so the disc reports
as "encrypted, no keys" until the caller re-scans with a resolved Unit
Key.

## build_mkb (test helper)

An MKB with one Type-and-Version record (type 0x10) carrying the
version as BE u32 at record offset 8, followed by a recorded EOF
record then trailing zero padding. mkb_content_len walks records and
stops at the first padding (type 0) byte (aacs/inf.rs).

## resolve_vid_only_no_cert_defaults_version_uhd (test)

No content cert at all → version defaults to UHD (major 2), matching
`read_aacs_version` so the scanned `AacsState.version` and the
out-of-band fetch agree on the Unit_Key_RO stride (audit #4: a wrong
BD-vs-UHD guess mis-parses unit keys). bus_encryption false
(unreadable → off).

## Skipped/deferred test coverage

Tests for `read_vid_oem` (response parsing) are skipped in this file
since `Drive` construction requires a live transport; parsing branches
are exercised through `read_vid_oem`'s callers in integration. The
`collect_host_certs` tests union `DriveCredentials` with the
key-source layer; empty means the route fails gracefully
(`AacsNoHostCert`).
