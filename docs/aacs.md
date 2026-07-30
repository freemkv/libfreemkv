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
