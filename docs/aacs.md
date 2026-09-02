# AACS Encryption Support

## Overview

AACS (Advanced Access Content System) is the encryption layer used by Blu-ray
and UHD 4K discs to protect content. libfreemkv implements AACS decryption so
disc access is transparent to the application.

There are two major versions:

- **AACS 1.0** -- Used by standard Blu-ray discs.
- **AACS 2.0 / 2.1** -- Used by UHD 4K Blu-ray discs. Adds a per-sector bus
  encryption layer on top of the standard content encryption. UHD drives accept
  AACS 1.0 host credentials for backward compatibility.

All versions use AES-128 for content decryption. The library reads the keys it
needs from `keydb.cfg`, walks the disc's Media Key Block (MKB) to resolve the
disc's key, and decrypts the content stream. AACS-encrypted discs therefore
require a `keydb.cfg`; CSS-protected DVDs do not (see the CSS notes in the
library docs).

## How it works (feature level)

When a disc is scanned, the library:

1. Reads the disc's AACS key-input files from the `/AACS/` directory.
2. Resolves the disc's key from `keydb.cfg` — either directly from a per-disc
   entry, or by walking the MKB with the keys present in the keydb.
3. Performs the drive-level SCSI authentication handshake needed to obtain the
   Volume ID and, for UHD, the bus-decryption key.
4. Decrypts the content stream as titles are read.

A resolved key is verified against actual disc content before it is applied, so
a stale or wrong key fails loudly rather than producing silent garbage. If no
usable key is available for an AACS-encrypted disc, the library surfaces a
specific error (the E70xx family) describing which part of the chain was
missing, and a missing `keydb.cfg` surfaces as `Error::KeydbLoad` with the
sentinel path `<no keydb in search paths>`.

## API Usage

AACS decryption is transparent to the application. `Disc::scan()` handles
everything automatically:

```rust
use libfreemkv::{Drive, Disc};
use libfreemkv::disc::ScanOptions;
use std::path::Path;

let mut drive = Drive::open(Path::new("/dev/sg4")).unwrap();
drive.wait_ready().unwrap();
drive.init().unwrap();
let disc = Disc::scan(&mut drive, &ScanOptions::default()).unwrap();

// Check encryption state
if disc.encrypted {
    if let Some(ref aacs) = disc.aacs {
        println!("AACS {}.0", aacs.version);
        println!("Key source: {}", aacs.key_source.name());
        if let Some(mkb_ver) = aacs.mkb_version {
            println!("MKB version: {}", mkb_ver);
        }
    } else {
        println!("Encrypted but keys not available");
    }
}

// Read content -- decryption is applied on read by the DiscStream decorator.
// Live disc does NOT go through the URL resolver: `input("disc://...")` returns
// Error::DiscUrlNotDirect by design.
let keys = disc.decrypt_keys();
let mut stream = DiscStream::new(
    Box::new(drive),
    disc.titles[0].clone(),
    keys,
    batch_sectors,
    disc.titles[0].content_format,
    false, // raw: false → decrypt on read
    None,  // halt
)?;
while let Ok(Some(frame)) = stream.read() {
    // decrypted PES frames
}
```

The application never calls decryption functions and never manages the
drive-level handshake. It DOES own key resolution — see below.

### Key resolution is the caller's job

`libfreemkv` is **lookup-free: it resolves no keys and reads no keydb.** There is
no `ScanOptions::with_keydb`, and `ScanOptions` has no keydb field — its only
scan input is the optional drive credentials for the live-drive authenticated
handshake.

The caller resolves a key out-of-band through a key source and applies it with
[`Disc::decrypt_with`]. `freemkv-keysources` is the crate that implements the
keydb and key-server sources; `ScanOptions::key_sources` takes them as
`Box<dyn KeySource>`.

### AacsState

After a successful scan, `disc.aacs` contains an `AacsState`:

| Field | Type | Description |
|-------|------|-------------|
| `version` | `u8` | AACS version (1 or 2) |
| `bus_encryption` | `bool` | Whether bus encryption is active |
| `mkb_version` | `Option<u32>` | MKB version from disc |
| `disc_hash` | `String` | Identifier for the disc's key-input files |
| `key_source` | `KeyOrigin` | How the disc's key was resolved |

## keydb.cfg

`keydb.cfg` is the single source of AACS key material. It is a text file (lines
starting with `;` or `#` are comments) holding the host credentials and per-disc
entries the library uses to resolve a disc. autorip can auto-download and
refresh it from a configured URL. The library does not ship any AACS keys
compiled into the binary.

## `UnitKey::is_default_index` test rationale

`is_default_index` is the public predicate that separates ordinary (index-0)
content keys from FMTS forensic index keys (see `UnitKey` docs; AACS 2.1
`IndividualSegment.tbl` tagging). A body answering `true` for everything would
present a forensic index key as an ordinary content key -- the caller would
decrypt the bulk of the title with a key that only opens 1/32nd of it;
answering `false` for everything would hide every ordinary key.

`is_default_index_separates_the_two_constructors` is pinned against the two
named constructors, which are the contract: `UnitKey::new` builds the ordinary
key, `UnitKey::forensic` builds an index key for `1..=32`.

`is_default_index_agrees_with_the_forensic_index_resolver` checks the
predicate agrees with the one consumer of `index_number` in the crate:
`crate::aacs::index_select::resolve_disc_index` resolves the disc's forensic
index from exactly the keys that are NOT default. If the two disagree, a disc
resolves an index whose key the rest of the pipeline treats as ordinary (or
vice versa).

## `src/aacs/mod.rs` module notes

### KEYDB.cfg line format

```
| DK | DEVICE_KEY 0x... | DEVICE_NODE 0x... | KEY_UV 0x... | KEY_U_MASK_SHIFT 0x...
| PK | 0x...
| HC | HOST_PRIV_KEY 0x... | HOST_CERT 0x...
| HC2 | HOST_PRIV_KEY 0x... | HOST_CERT 0x...
0x<disc_hash> = <title> | D | <date> | M | 0x<media_key> | I | 0x<disc_id> | V | 0x<vuk> | U | <unit_keys>
```

### Spec provenance tags

The crypto in `src/aacs/` carries `[TAG] §x.y` citations back to the published
AACS specification (Final Rev 0.953), so each primitive links to the section it
implements:

- `[C]` -- AACS Introduction and Common Cryptographic Elements Book (primitives,
  MKB/key-management).
- `[PR]` -- AACS Pre-recorded Video Book (Volume/Title Key layer).
- `[BD]` -- AACS Blu-ray Disc Pre-recorded Book (CPS Unit Key, Aligned Unit,
  Block Key).
- `[RE]` -- reverse-engineered from real discs, cited only where the public
  spec is silent (the `0x86` verify record and the Category-C MKB type values).

### AACS key-input path discovery (`AacsRole`, `role_paths`, `find_hddvd_aacs_dir`)

BD and UHD keep their key material under a fixed `/AACS/...` tree, so those
paths are constants. HD DVD keeps the equivalents in a reserved root directory
whose NAME is authoring-house-specific -- observed `ANY!` (Dukes of Hazzard)
and `AAC!` (Freedom / Memory-Tech), each with a `<name>!_BAK` mirror -- and
whose title-key file is NOT always `VTKF000.AACS` (Freedom ships
`VTKF090.AACS` + `VTKF100.AACS`). So the HD DVD files are DISCOVERED from the
parsed UDF tree (`find_hddvd_aacs_dir` + `role_paths`), never hardcoded.

Each key role (`AacsRole`) resolves to an ordered candidate list -- the BD/UHD
constants first, then whatever the HD DVD directory actually holds -- which
every reader walks with `read_first`, first-that-reads. No reader ever
branches on disc type: a BD/UHD disc has the `/AACS/` files so those win; an
HD DVD has none of them, so it falls through to the discovered entries.
Centralised so `resolve_vid_only`, `read_aacs_inputs`, `read_mkb_content`, and
`read_aacs_version` can never silently diverge the disc_hash / MKB / VID that
another reader feeds a key service.

`find_hddvd_aacs_dir` identifies the HD DVD AACS directory structurally, NOT by
a hardcoded name: the root child directory whose name ends in `!` (so the
`<name>!_BAK` backup mirror, which also ends in a non-`!` char, is not
mistaken for it) and which contains `MKBROM.AACS`. Observed real names:
`ANY!` (Dukes of Hazzard), `AAC!` (Freedom). A BD/UHD disc has no such
directory -> `None`.

`role_paths` builds the ordered candidate list: fixed BD/UHD paths first, then
the discovered HD DVD files. For `AacsRole::UnitKey` every `VTKF*.AACS` in the
directory is appended in sorted name order -- a disc may carry more than one
variant (Freedom: `VTKF090` + `VTKF100`), not just `VTKF000`.

`read_first` walks an AACS role's candidate paths and returns the first that
reads; `read` performs the actual per-path read (full file or bounded prefix),
so callers share the same first-present walk regardless of read style. Returns
`Error::AacsNoKeys` if no candidate is present. Generic over the path element
(`&str` or owned `String`) so it accepts the `Vec<String>` that `role_paths`
builds from the discovered HD DVD directory.

### Test: `a_bang_suffixed_directory_without_mkbrom_is_not_the_aacs_directory`

The `!`-suffix and the `MKBROM.AACS` presence test are BOTH required -- the
discovery is a conjunction, not a disjunction. The existing fixtures only ever
present a directory that satisfies both (`AAC!` with `MKBROM.AACS`) alongside
one that satisfies neither (`AAC!_BAK` -- which contains `MKBROM.AACS` but is
ALSO reached only after the real dir), so either half of the conjunction could
be dropped and the same directory would still be found. This test's fixture
has a directory that satisfies the name half and NOT the contents half: it
must not be picked. If it were, the HD DVD path would resolve `MKBROM.AACS`,
`CONTENT_CERT.AACS` and the title-key file under a directory that holds none
of them -- the disc reports "no AACS key files" and never rips.
