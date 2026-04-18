# libfreemkv — Feature List

## v0.10.10 (current)

### Done
- [x] Drive access: open, identify, unlock, firmware upload, speed calibration, eject
- [x] 206 bundled drive profiles (MediaTek MT1959 A + B variants)
- [x] SCSI transport: Linux SG_IO, macOS IOKit, Windows SPTI
- [x] UDF 2.50 filesystem parser (metadata partitions, Blu-ray profile)
- [x] MPLS playlist parser (play items, STN table, secondary streams)
- [x] CLPI clip info parser (EP map, sector extents)
- [x] AACS 1.0 decryption (4 VUK paths: KEYDB, media key, processing key, device key)
- [x] AACS 2.0 SCSI handshake (P-256/SHA-256 ECDH, bus decryption, read data key)
- [x] DVD IFO parser (VMG, VTS, PGC chains, cell addresses)
- [x] DVD CSS decryption (bus auth, disc key via player keys, title key, sector descramble)
- [x] CSS Stevenson plaintext attack for ISO key recovery
- [x] MPEG-2 PS demuxer (DVD Program Stream with PES extraction)
- [x] MPEG-2 video codec parser (sequence header, quantizer matrices, keyframe detection)
- [x] KEYDB.cfg download, verify, save (raw TCP, zero HTTP deps)
- [x] Content reading with adaptive batch sizing, error recovery, 12+ MB/s
- [x] Stream labels: 5 BD-J format parsers (Paramount, Criterion, Pixelogic, CTRM, Deluxe)
- [x] MKV muxer: 14 codec parsers (H.264, HEVC, MPEG-2, AC-3, DTS, TrueHD, PGS, DVD Sub, VC-1, LPCM, +4 more)
- [x] SectorReader trait: decouples disc scanning from SCSI
- [x] 7 stream types: Disc, ISO, MKV, M2TS, Network, Stdio, Null
- [x] PES pipeline with unified Stream trait (any source → any dest)
- [x] FMKV metadata header for M2TS and network streams
- [x] Numeric error codes only (no English text in library)
- [x] Event system for progress callbacks

### Planned
- [ ] Windows testing on real hardware
- [ ] Pioneer Renesas platform support (48 drives, need GET_CONFIG 010C)
- [ ] TranscodeStream (ffmpeg integration)
- [ ] ISO write with BD-compliant UDF structure
