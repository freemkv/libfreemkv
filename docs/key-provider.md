# KeyProvider (src/aacs/provider.rs)

Long-form notes for `src/aacs/provider.rs`, relocated from doc comments to
stay under the comment-guard caps. See the module's own (trimmed) `//!` doc
for the short version.

## Method flavors

`KeyProvider` methods come in two flavors:

- **Bulk material** (`device_keys`, `processing_keys`, `media_keys`) — the
  resolver unions (and dedups) results across all providers and tries each
  candidate.
- **Disc-keyed lookup** (`lookup_disc_by_hash`, `lookup_disc_by_vid`) — the
  resolver short-circuits on the first hit, so providers are queried in
  array order with fastest/closest first.

`host_certs` is a sixth method but is NOT consumed by the resolver chain:
the SCSI handshake reads host certs directly from the caller-supplied
credentials, not from the provider array. A provider that overrides
`host_certs` today has no effect on the handshake; the method is retained
as a forward-looking extension point only. Correspondingly,
`Providers::host_certs` (the union helper) is `#[allow(dead_code)]` and
unused by the resolver chain today.

Default impls return empty / `None` so backends only override the methods
they actually support — an external key service might implement only
`lookup_disc_by_hash`, while a local file might implement all six.

Calls may block (disk I/O, network round-trips). The resolver invokes each
method at most a handful of times per scan; for per-disc memoization,
implementations should cache internally.

## `Providers` (resolver-side aggregation helper)

The resolver wraps `ctx.providers` (`&[&dyn KeyProvider]`) in `Providers`;
its methods apply the union-vs-short-circuit policy described above per
method. The bulk unions dedup so overlapping providers don't make the
resolver re-walk/re-validate identical material.

## `SuppliedKey` (bridge for `Disc::decrypt_with`)

A `KeyProvider` backed by a single caller-supplied key's raw material — the
bridge for `crate::disc::Disc::decrypt_with`.

The application's key source did the lookup and handed in material at one
level (DK / PK / MK / VUK). This exposes exactly that material to the
version-dispatched resolver, which owns ALL derivation — so a source never
derives, and the lib remains the single home for the AACS chain across
1.0 / 2.0 / 2.1 / 2.x.

Each level fills only its own field; the rest stay empty, so the resolver
naturally runs the matching path (DK→…, PK→…, MK-pool brute, or a
disc-keyed VUK hit). `decrypt_with` already knows the disc, so the
`lookup_disc_by_*` hash/VID arguments are irrelevant — a present
`disc_entry` is returned for any query.

## Test: `providers_host_certs_unions_every_providers_certs_in_array_order`

`Providers::host_certs` is the union across the provider array. It is not
wired into the handshake today (see above), so nothing else in the crate
would notice a body that dropped every cert on the floor — and the day it
IS wired in, a silently-empty cert list means the drive AACS authentication
finds no host certificate to present and every disc fails to open, with no
indication that the caller's certs were discarded.

Unlike the bulk key unions this one does NOT dedup (`HostCert` is not
Ord/Hash), so the assertion is on the full concatenation in array order.
