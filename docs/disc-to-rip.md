# Disc to Rip: End-to-End Flow

How libfreemkv goes from a disc in the drive to decrypted content ready for backup.
This is the starting point for understanding the library.

## The Pipeline

```
Insert disc
    │
    ▼
1. Open drive (drive/mod.rs)
    │  INQUIRY → identify drive
    │  Match bundled profile → chipset, unlock parameters
    │
    ▼
2. Init drive (drive/mod.rs → platform/mt1959)
    │  Firmware upload (if needed, 10s recovery wait)
    │  Unlock → vendor-specific command activates raw read mode
    │  Speed calibration → probe_disc()
    │
    ▼
3. AACS handshake (aacs/handshake.rs) — optional
    │  Allocate AGID
    │  Exchange certificates + nonces (ECDH)
    │  Derive bus key
    │  Read Volume ID + read_data_key
    │  (fails gracefully if drive doesn't support AACS for this disc)
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
5. Read AACS files from disc (aacs/mod.rs)
    │  AACS/Unit_Key_RO.inf → SHA1 = disc hash
    │  AACS/Content000.cer → AACS version (1.0 or 2.0), bus encryption flag
    │  MKB via SCSI → for key derivation fallback
    │
    ▼
6. Resolve encryption keys (decrypt.rs → resolve_encryption)
    │  BD AACS:
    │    Path 1: disc hash → KEYDB.cfg → VUK (fast, 99% of discs)
    │    Path 2: KEYDB media key + Volume ID → VUK
    │    Path 3: MKB + processing keys → media key → VUK
    │    Path 4: MKB + device keys → subset-difference tree → VUK
    │    VUK → decrypt unit keys from Unit_Key_RO.inf
    │  DVD CSS:
    │    Table-driven cipher — no KEYDB needed
    │  → docs/aacs.md
    │
    ▼
7. Parse playlists (mpls.rs) — BD/UHD only
    │  BDMV/PLAYLIST/*.mpls → titles with play items
    │  Each play item: clip ID, in/out timestamps
    │  STN table: video, audio, subtitle streams with codec + language
    │  → docs/mpls.md
    │
    ▼
8. Parse clip info (clpi.rs) — BD/UHD only
    │  BDMV/CLIPINF/*.clpi → EP map (timestamp → sector mapping)
    │  Coarse + fine entries → full PTS and SPN
    │  SPN → byte offset → sector extents for reading
    │  → docs/clpi.md
    │
    ▼
9. Parse BD-J labels (labels/) — optional
    │  BDMV/JAR/*.jar → Java class constant pool strings
    │  5 format parsers: Paramount, Criterion, Pixelogic, CTRM, Deluxe
    │  Audio track labels: "English Descriptive Audio", "French 5.1", etc.
    │
    ▼
10. Stream content (mux/disc.rs → DiscStream)
     │  Read sectors → decrypt → TS demux → PES frames
     │  Or: read sectors → decrypt → raw bytes (for ISO output)
     │  Drive::read() is single-shot. DiscStream::fill_extents adapts the
     │  batch size on failure (halve / probe-up). Bad-range retry is layer
     │  1 above this — Disc::patch re-runs against the mapfile.
     │
     ▼
  PES frames → output stream (MKV, M2TS, network, etc.)
```

## API Summary

```rust
// Open + init drive
let mut drive = Drive::open(Path::new("/dev/sg4"))?;
drive.wait_ready()?;
drive.init()?;
drive.probe_disc()?;

// Scan disc (UDF + playlists + AACS — all automatic)
let disc = Disc::scan(&mut drive, &ScanOptions::default())?;

// Stream pipeline — PES frames from any source to any output.
// 0.18: input() returns Box<dyn FrameSource>, output() returns Box<dyn FrameSink>;
// direction is type-checked, so calling .write() on an input is a compile error.
let opts = InputOptions::default();
let mut input = libfreemkv::input("disc:///dev/sg4", &opts)?;
let title = input.info().clone();
let mut output = libfreemkv::output("mkv://Movie.mkv", &title)?;
while let Ok(Some(frame)) = input.read() {
    output.write(&frame)?;
}
output.finish()?;
```

## Module Reference

| Module | Doc | Purpose |
|--------|-----|---------|
| drive/ | [drive-access.md](drive-access.md) | Open, identify, init, unlock, single-shot read |
| scsi/ | [drive-access.md](drive-access.md) | Platform SCSI transport (Linux, macOS, Windows) |
| udf.rs | [udf.md](udf.md) | UDF 2.50 filesystem |
| mpls.rs | [mpls.md](mpls.md) | MPLS playlists + STN streams |
| clpi.rs | [clpi.md](clpi.md) | CLPI clip info + EP map |
| ifo.rs | -- | DVD IFO parser |
| aacs/ | [aacs.md](aacs.md) | Key resolution + content decrypt + bus handshake |
| css/ | -- | DVD CSS cipher |
| decrypt.rs | -- | Unified decrypt dispatcher (AACS/CSS/None) |
| disc/ | [rip-recovery.md](rip-recovery.md) | Disc::scan + Disc::sweep + Disc::patch + mapfile |
| labels/ | -- | BD-J stream labels (5 format parsers) |
| mux/ | -- | Stream implementations (7 stream types) |
| pes.rs | -- | PES frame types + FrameSource / FrameSink traits |
| sector/ | -- | SectorSource / SectorSink + DecryptingSectorSource decorator |
| io/ | -- | Pipeline<I, R> + Sink trait + WritebackFile |
| halt.rs | -- | Halt cancellation token |
| keydb.rs | -- | KEYDB download, parse, save |
| error.rs | -- | Error codes (E1xxx-E8xxx) |
| event.rs | -- | Drive event system |
