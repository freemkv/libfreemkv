# Disc to Rip: End-to-End Flow

How libfreemkv goes from a disc in the drive to decrypted content ready for backup.
This is the starting point for understanding the library.

## The Pipeline

```
Insert disc
    │
    ▼
1. Open drive (drive.rs)
    │  INQUIRY → identify drive
    │  Match bundled profile → chipset, unlock parameters
    │
    ▼
2. AACS handshake (aacs_handshake.rs) — optional, separate transport
    │  Allocate AGID
    │  Exchange certificates + nonces (ECDH)
    │  Derive bus key
    │  Read Volume ID + read_data_key
    │  (fails gracefully if drive doesn't support AACS for this disc)
    │
    ▼
3. Unlock drive (drive.rs → platform/mt1959.rs)
    │  Vendor-specific command activates raw read mode
    │  Required — drive firmware blocks all reads without it
    │
    ▼
4. Read UDF filesystem (udf.rs)
    │  Sector 256: AVDP → find Volume Descriptor Sequence
    │  VDS: Partition Descriptor (physical start) + Logical Volume (metadata start)
    │  Metadata partition → File Set Descriptor → Root directory
    │  Walk directory tree: BDMV/, AACS/, CERTIFICATE/
    │  → docs/udf.md
    │
    ▼
5. Read AACS files from disc (aacs.rs)
    │  AACS/Unit_Key_RO.inf → SHA1 = disc hash
    │  AACS/Content000.cer → AACS version (1.0 or 2.0), bus encryption flag
    │  MKB via SCSI → for key derivation fallback
    │
    ▼
6. Resolve AACS keys (aacs.rs → resolve_keys)
    │  Path 1: disc hash → KEYDB.cfg → VUK (fast, 99% of discs)
    │  Path 2: KEYDB media key + Volume ID → VUK
    │  Path 3: MKB + processing keys → media key → VUK
    │  Path 4: MKB + device keys → subset-difference tree → VUK
    │  VUK → decrypt unit keys from Unit_Key_RO.inf
    │  → docs/aacs.md
    │
    ▼
7. Parse playlists (mpls.rs)
    │  BDMV/PLAYLIST/*.mpls → titles with play items
    │  Each play item: clip ID, in/out timestamps
    │  STN table: video, audio, subtitle streams with codec + language
    │  → docs/mpls.md
    │
    ▼
8. Parse clip info (clpi.rs)
    │  BDMV/CLIPINF/*.clpi → EP map (timestamp → sector mapping)
    │  Coarse + fine entries → full PTS and SPN
    │  SPN → byte offset → sector extents for reading
    │  → docs/clpi.md
    │
    ▼
9. Parse BD-J labels (jar.rs) — optional
    │  BDMV/JAR/*.jar → Java class constant pool strings
    │  Audio track labels: "English Descriptive Audio", "French 5.1", etc.
    │
    ▼
10. Read + decrypt content (disc.rs → ContentReader)
     │  For each aligned unit (6144 bytes = 3 sectors):
     │    Read 3 sectors from disc
     │    If AACS 2.0: bus decrypt (read_data_key, per-sector AES-CBC)
     │    If encrypted: unit decrypt (per-unit key derivation + AES-CBC)
     │    Output decrypted content
     │
     ▼
  Decrypted m2ts stream → ready for muxing/backup
```

## API Summary

```rust
// Steps 1 + 3 (open + unlock)
let mut session = DriveSession::open(Path::new("/dev/sr0"))?;

// Steps 2 + 4-9 (AACS + scan)
let disc = Disc::scan(&mut session, &ScanOptions::with_keydb("keydb.cfg"))?;

// Step 10 (read + decrypt)
let mut reader = disc.open_title(&mut session, 0)?;
while let Some(unit) = reader.read_unit()? {
    output.write_all(&unit)?;
}
```

Three lines. Everything else is internal.

## Module Reference

| Module | Doc | Purpose |
|--------|-----|---------|
| drive.rs | [drive-access.md](drive-access.md) | Open, identify, unlock, read |
| scsi.rs | [drive-access.md](drive-access.md) | Platform SCSI transport |
| udf.rs | [udf.md](udf.md) | UDF 2.50 filesystem |
| mpls.rs | [mpls.md](mpls.md) | MPLS playlists + STN streams |
| clpi.rs | [clpi.md](clpi.md) | CLPI clip info + EP map |
| aacs.rs | [aacs.md](aacs.md) | Key resolution + content decrypt |
| aacs_handshake.rs | [aacs.md](aacs.md) | SCSI bus authentication |
| disc.rs | -- | High-level scan + read API |
| jar.rs | -- | BD-J audio track labels |
| error.rs | -- | Error codes (E1xxx-E7xxx) |
