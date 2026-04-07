# AACS Encryption Support

## Overview

AACS (Advanced Access Content System) is the encryption layer used by Blu-ray and UHD 4K discs to protect content. libfreemkv implements AACS decryption to enable transparent disc access.

There are two major versions:

- **AACS 1.0** -- Used by standard Blu-ray discs. Relies on a custom 160-bit elliptic curve for bus authentication and AES-128 for content encryption. Processing keys and device keys can derive the media key from the disc's Media Key Block (MKB).

- **AACS 2.0** -- Used by UHD 4K Blu-ray discs. Adds a per-sector bus encryption layer (read_data_key) on top of the standard content encryption. Uses P-256/SHA-256 for its native handshake, though drives accept AACS 1.0 host certificates for backward compatibility.

Both versions use AES-128-CBC for content decryption with a fixed initialization vector. The fundamental key hierarchy is the same: a Volume Unique Key (VUK) decrypts per-title unit keys, which in turn decrypt the content stream.


## Architecture

AACS support is split across two modules:

### `aacs.rs` -- Keys and Decryption

Handles everything related to key resolution and content decryption:

- KEYDB.cfg parsing (device keys, processing keys, host certificates, per-disc entries)
- Disc hash computation (SHA-1 of `Unit_Key_RO.inf`)
- VUK resolution chain (4 paths, described below)
- MKB record parsing and media key derivation
- Subset-difference tree traversal (AACS-G3 key derivation)
- Unit_Key_RO.inf parsing and unit key decryption
- Content Certificate parsing (AACS version detection)
- Aligned unit decryption (AES-128-CBC)
- Bus decryption (AACS 2.0 read_data_key layer)

### `aacs_handshake.rs` -- SCSI Authentication

Handles the drive-level SCSI authentication protocol:

- ECDH key agreement on the AACS 160-bit curve
- ECDSA signing and verification
- Bus key derivation
- AGID management (allocate/invalidate)
- Volume ID retrieval (encrypted with bus key, verified by AES-CMAC)
- Read Data Key retrieval (for AACS 2.0 bus decryption)
- AACS LA public key certificate verification


## Key Resolution Chain

When a disc is scanned, `resolve_keys()` attempts four paths in priority order. The first path that succeeds is used.

### Path 1: KEYDB VUK Lookup (fastest)

```
Unit_Key_RO.inf --> SHA-1 --> disc_hash --> KEYDB lookup --> VUK
```

The disc hash is computed as the SHA-1 digest of the raw `Unit_Key_RO.inf` file from the disc's `/AACS/` directory. This hash is used as the lookup key in `KEYDB.cfg`. If a matching entry contains a VUK (`V` field), it is used directly.

This is the fast path and resolves the vast majority of discs in a well-maintained KEYDB.

### Path 2: KEYDB Media Key + Volume ID

```
KEYDB media_key + Volume ID (from SCSI handshake) --> VUK derivation
```

If the disc hash is not in the KEYDB but a KEYDB entry has a matching Volume ID (`I` field) and a media key (`M` field), the VUK is derived:

```
VUK = AES-128-ECB-DECRYPT(media_key, volume_id) XOR volume_id
```

Requires a successful SCSI handshake to obtain the Volume ID.

### Path 3: MKB + Processing Keys

```
MKB (from disc) + processing_keys (from KEYDB) --> media_key --> VUK
```

Processing keys are pre-computed keys that work against specific MKB versions. For each processing key, the library:

1. Parses the MKB to extract the Verify Media Key Record (`mk_dv`), subset-difference index, and conditional values (cvalues).
2. Tries each processing key against each UV/cvalue pair: `mk = AES-DEC(pk, cvalue) XOR cvalue`.
3. Validates the derived media key: `AES-ECB(mk, mk_dv)` must produce 12 leading zero bytes.
4. Derives VUK from the validated media key and Volume ID.

### Path 4: MKB + Device Keys (Subset-Difference Tree)

```
MKB + device_keys --> subset-difference tree traversal --> processing_key --> media_key --> VUK
```

The most complex path. Each device key has an associated node number, UV value, and mask parameters that position it in the AACS subset-difference tree. The library:

1. Finds the subset-difference entry in the MKB that applies to the device key's node.
2. Traverses the tree using AACS-G3 key derivation: `aesg3(key, inc) = AES-DEC(key, seed) XOR seed`, where `seed[15]` is incremented by `inc`. Each tree node produces a left child (inc=0), a processing key (inc=1), and a right child (inc=2).
3. At each level, selects left or right based on the UV bit at the current position.
4. The resulting processing key is validated against the MKB cvalue to derive the media key.
5. VUK is derived from the media key and Volume ID.


## Content Decryption

### Aligned Units

AACS encrypts content in aligned units of 6144 bytes (3 sectors of 2048 bytes each). The encryption flag is signaled by the copy_permission_indicator bits in byte 0 of the unit (`unit[0] & 0xC0 != 0`).

### Per-Unit Key Derivation

Each aligned unit has its own decryption key derived from the CPS unit key:

1. **Derive**: AES-128-ECB encrypt the first 16 bytes of the unit (plaintext TP_extra_header) with the unit key.
2. **XOR**: XOR the encrypted result with the original 16 bytes to produce the per-unit decryption key.
3. **Decrypt**: AES-128-CBC decrypt bytes 16 through 6143 using the per-unit key and the fixed AACS IV.
4. **Clear flag**: Clear the encryption indicator bits (`unit[0] &= !0xC0`).

### Fixed IV

All AES-CBC operations in AACS use the same fixed initialization vector, defined in the AACS specification.

### Verification

After decryption, the library verifies correctness by checking for MPEG-TS sync bytes (0x47) at the expected 192-byte packet boundaries within the unit. Blu-ray transport stream packets are 192 bytes: 4-byte TP_extra_header followed by a 188-byte TS packet.


## Bus Encryption

### AACS 1.0

Standard Blu-ray discs do not use bus encryption. Content is read directly from the disc and decrypted using the unit key.

### AACS 2.0

UHD 4K discs add a per-sector bus encryption layer. The drive encrypts data as it is read from the disc, and the host must decrypt it before applying AACS content decryption.

Bus encryption uses a **read_data_key** obtained during the SCSI handshake. For each 2048-byte sector within an aligned unit, bytes 16 through 2047 are AES-128-CBC encrypted with the read_data_key and the fixed AACS IV. The first 16 bytes of each sector remain plaintext.

The full decryption pipeline for AACS 2.0:

1. **Bus decrypt**: For each sector, AES-128-CBC decrypt bytes 16..2047 with the read_data_key.
2. **Content decrypt**: Standard per-unit key derivation and AES-128-CBC decryption as described above.


## SCSI Handshake

The AACS SCSI authentication handshake establishes a shared bus key between host and drive, then uses it to securely transfer the Volume ID and read data keys.

### Protocol Flow

1. **Invalidate AGIDs**: Send REPORT KEY with format 0x3F for AGIDs 0-3 to clear stale sessions.
2. **Allocate AGID**: REPORT KEY format 0x00 returns a fresh Authentication Grant ID.
3. **Send host credentials**: SEND KEY format 0x01 transmits the host nonce (20 random bytes) and host certificate (92 bytes).
4. **Receive drive credentials**: REPORT KEY format 0x01 returns the drive nonce and drive certificate.
5. **Receive drive key**: REPORT KEY format 0x02 returns the drive's ephemeral EC key point and ECDSA signature over `host_nonce || drive_key_point`.
6. **Verify drive key**: The signature is verified against the drive's public key (extracted from its certificate). AACS 1.0 certificates are verified against the AACS LA public key.
7. **Send host key**: The host generates an ephemeral key pair, signs `drive_nonce || host_key_point` with the host private key, and sends via SEND KEY format 0x02.
8. **Compute bus key**: ECDH shared secret = `host_private_key * drive_key_point`. The bus key is the low 128 bits of the shared point's x-coordinate.

### Post-Authentication Reads

- **Volume ID**: REPORT DISC STRUCTURE format 0x80. Returns 16-byte VID encrypted with the bus key, plus an AES-CMAC MAC for integrity verification.
- **Read Data Keys**: REPORT DISC STRUCTURE format 0x84. Returns the read_data_key and write_data_key, each AES-ECB encrypted with the bus key.

### Elliptic Curve

AACS 1.0 uses a custom 160-bit Weierstrass curve (`y^2 = x^3 + ax + b mod p`) with 20-byte field elements. The library implements full EC arithmetic: point addition, doubling, scalar multiplication, modular inverse, ECDSA sign/verify, and ECDH key agreement.


## AACS 2.0 Status

AACS 2.0 discs are detected via the Content Certificate file (`Content000.cer` or `Content001.cer`). A certificate type byte of 0x01 indicates AACS 2.0.

AACS 2.0 drives are identified by their drive certificate type (0x11). These drives natively use P-256/SHA-256, but accept AACS 1.0 host certificates for backward compatibility.

Current implementation status:

- AACS 2.0 detection: **implemented** (Content Certificate parsing, drive cert type check)
- AACS 1.0 handshake with AACS 2.0 drives: **implemented** (backward compatibility mode)
- Full P-256 AACS 2.0 handshake: **not yet implemented** (prepared but rarely needed since drives accept AACS 1.0 host certs)
- Bus decryption with read_data_key: **implemented**
- Content decryption: **implemented** (same as AACS 1.0)

In practice, AACS 2.0 UHD discs work through the backward-compatible AACS 1.0 handshake path, with the addition of read_data_key bus decryption.


## API Usage

AACS decryption is transparent to the application. The `Disc::scan()` method handles everything automatically:

```rust
use libfreemkv::{DriveSession, Disc};
use libfreemkv::disc::ScanOptions;
use std::path::Path;

let mut session = DriveSession::open(Path::new("/dev/sr0")).unwrap();
let disc = Disc::scan(&mut session, &ScanOptions::default()).unwrap();

// Check encryption state
if disc.encrypted {
    if let Some(ref aacs) = disc.aacs {
        println!("AACS {}.0", aacs.version);
        println!("Key source: {}", aacs.key_source.name());
        println!("Disc hash: {}", aacs.disc_hash);
        if let Some(mkb_ver) = aacs.mkb_version {
            println!("MKB version: {}", mkb_ver);
        }
    } else {
        println!("Encrypted but keys not available");
    }
}

// Read content -- decryption is automatic
let mut reader = disc.open_title(&mut session, 0).unwrap();
while let Some(unit) = reader.read_unit().unwrap() {
    // unit is 6144 bytes of decrypted content
}
```

The application never touches keys, never calls decryption functions, and never manages handshakes. All of that is internal to `Disc::scan()` and `ContentReader::read_unit()`.

### KEYDB Location

`ScanOptions` controls where the KEYDB is loaded from. If no explicit path is set, the library checks:

1. `~/.config/aacs/KEYDB.cfg`
2. `/etc/aacs/KEYDB.cfg`

To specify an explicit path:

```rust
let opts = ScanOptions::with_keydb("/path/to/KEYDB.cfg");
let disc = Disc::scan(&mut session, &opts).unwrap();
```

### AacsState

After a successful scan, `disc.aacs` contains an `AacsState` with:

| Field | Type | Description |
|-------|------|-------------|
| `version` | `u8` | AACS version (1 or 2) |
| `bus_encryption` | `bool` | Whether bus encryption is active |
| `mkb_version` | `Option<u32>` | MKB version from disc |
| `disc_hash` | `String` | SHA-1 of Unit_Key_RO.inf (hex with 0x prefix) |
| `key_source` | `KeySource` | How keys were resolved |
| `vuk` | `[u8; 16]` | Volume Unique Key |
| `unit_keys` | `Vec<(u32, [u8; 16])>` | Decrypted unit keys (CPS unit number, key) |
| `read_data_key` | `Option<[u8; 16]>` | AACS 2.0 bus decryption key |
| `volume_id` | `[u8; 16]` | Volume ID from SCSI handshake |

### KeySource

| Variant | Description |
|---------|-------------|
| `KeyDb` | VUK found directly in KEYDB by disc hash |
| `KeyDbDerived` | Media key + Volume ID from KEYDB, VUK derived |
| `ProcessingKey` | MKB + processing keys from KEYDB |
| `DeviceKey` | MKB + device keys, subset-difference tree traversal |


## KEYDB.cfg Format Reference

The KEYDB.cfg file contains all cryptographic material needed for AACS decryption. Lines starting with `;` or `#` are comments.

### Device Keys

```
| DK | DEVICE_KEY 0x<key> | DEVICE_NODE 0x<node> | KEY_UV 0x<uv> | KEY_U_MASK_SHIFT 0x<shift>
```

- `key`: 16-byte AES device key (hex)
- `node`: Device node number in the subset-difference tree (hex)
- `uv`: UV value for tree positioning (hex)
- `shift`: U mask shift value (hex)

### Processing Keys

```
| PK | 0x<key>
```

- `key`: 16-byte pre-computed processing key (hex)

### Host Certificate

```
| HC | HOST_PRIV_KEY 0x<privkey> | HOST_CERT 0x<cert>
```

- `privkey`: 20-byte ECDSA private key (hex)
- `cert`: 92-byte AACS host certificate (hex)

The host certificate is used for SCSI authentication. It contains the host's public key and is signed by the AACS Licensing Administrator.

### Disc Entries

```
0x<disc_hash> = <title> | D | <date> | M | 0x<media_key> | I | 0x<disc_id> | V | 0x<vuk> | U | <unit_keys>
```

- `disc_hash`: 20-byte SHA-1 of Unit_Key_RO.inf (hex)
- `title`: Human-readable disc title
- `D`: Date tag, followed by release/rip date
- `M`: Media key tag, followed by 16-byte media key (hex)
- `I`: Disc ID tag, followed by 16-byte Volume ID (hex)
- `V`: VUK tag, followed by 16-byte Volume Unique Key (hex)
- `U`: Unit keys tag, followed by space-separated `<unit_num>-0x<key>` pairs

All fields after the title are optional. A minimal entry needs only the disc hash and VUK:

```
0x<disc_hash> = <title> | V | 0x<vuk>
```

Inline comments are supported with `;`:

```
0x<disc_hash> = <title> | V | 0x<vuk> ; MKBv77
```
