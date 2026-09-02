# `src/io/image_writer.rs`

## Why this is not `freemkv_engine::copy`

The engine's `copy` is the RECOVERY path — mapfile sidecar, `--multipass`
sweep/patch, damage-jump, ECC-aware batching, auto-resume. Every one of those
exists because an optical drive returns read errors on marginal media. A
file-backed or synthesized source has no marginal media: a read either
succeeds or the underlying file is broken, and retrying it is pointless.

Routing a non-drive source through the recovery path is not merely wasteful,
it is wrong. Its mapfile identity check compares AACS unit keys and the VID,
both of which are empty for an already-decrypted source, so identity passes
for ANY such source: a second run with a different input to the same output
path would resume over the previous image and produce wrong content at exit
zero. Keeping the two paths separate makes that unrepresentable.

So: drive sources get `freemkv_engine::copy`. Everything else gets this.

## `write_image` rationale

The output is a faithful image of whatever the source presents — decrypted
if the caller wrapped the source in a
[`DecryptingSectorSource`](../src/sector/decrypting.rs), ciphertext if it did
not. `write_image` performs no decryption itself and makes no decryption
decision; that belongs to the caller, which knows whether the run is
`--raw`.

On cancellation the partial file is left in place — the caller decides
whether a partial image is worth keeping, and deleting a multi-gigabyte file
the user may want to inspect is not this function's call to make.

A short read from the source is treated as an error rather than zero-filled:
silently padding a truncated source would produce an image that looks
complete and is not.
