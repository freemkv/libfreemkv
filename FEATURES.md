# libfreemkv — Feature List

## v0.7.1 (current)

### Done
- [x] Drive access: open, identify, unlock, firmware upload, speed calibration, eject
- [x] 206 bundled drive profiles (MediaTek MT1959 A + B variants)
- [x] SCSI transport: Linux SG_IO, macOS IOKit, Windows SPTI
- [x] UDF 2.50 filesystem parser (metadata partitions, Blu-ray profile)
- [x] MPLS playlist parser (play items, STN table, secondary streams)
- [x] CLPI clip info parser (EP map, sector extents)
- [x] AACS 1.0 decryption (4 VUK paths: KEYDB, media key, processing key, device key)
- [x] AACS 2.0 SCSI handshake (P-256/SHA-256 ECDH, bus decryption, read data key)
- [x] KEYDB.cfg download, verify, save (raw TCP, zero HTTP deps)
- [x] Content reading with adaptive batch sizing, error recovery, 12+ MB/s
- [x] Stream labels: 5 BD-J format parsers (Paramount, Criterion, Pixelogic, CTRM, Deluxe)
- [x] MKV muxer: 8 codec parsers (H.264, HEVC, AC-3, DTS, TrueHD, PGS, VC-1, LPCM)
- [x] SectorReader trait: decouples disc scanning from SCSI
- [x] 7 stream types: Disc, ISO, MKV, M2TS, Network, Stdio, Null
- [x] IOStream trait with URL-based resolver (scheme://path)
- [x] FMKV metadata header for M2TS and network streams
- [x] Numeric error codes only (no English text in library)
- [x] Event system for progress callbacks

### Planned
- [ ] Windows testing on real hardware
- [ ] Pioneer Renesas platform support (48 drives, need GET_CONFIG 010C)
- [ ] DVD CSS decryption
- [ ] TranscodeStream (ffmpeg integration)
- [ ] ISO write with BD-compliant UDF structure
