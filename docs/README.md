# libfreemkv Documentation

Technical documentation for [libfreemkv](https://github.com/freemkv/libfreemkv), the open source optical drive library.

## Start Here

**[Disc to Rip: End-to-End Flow](disc-to-rip.md)** — How the library goes from a disc in the drive to decrypted content. Read this first.

## Reference

| Document | What it covers |
|----------|---------------|
| [Architecture](architecture.md) | Module map, design principles, error codes, platform support |
| [Drive Access](drive-access.md) | Drive, SCSI transport, profiles, unlock, why raw mode is needed |
| [Rip Recovery](rip-recovery.md) | Three-layer recovery model: Disc::patch, single-shot Drive::read, DiscStream batch halving |
| [AACS Encryption](aacs.md) | Key resolution (4 paths), content decryption, bus encryption, SCSI handshake |
| [UDF Filesystem](udf.md) | UDF 2.50 with metadata partitions, pointer chain, how files are read from disc |
| [MPLS Playlists](mpls.md) | Playlist format, play items, STN stream table, coding types |
| [CLPI Clip Info](clpi.md) | EP map (coarse + fine entries), timestamp-to-sector mapping, extent calculation |
| [API Design](api-design.md) | Stream API design, PES pipeline, input/output resolution |

## Reading Order

If you want to understand the whole library:

1. **[Disc to Rip](disc-to-rip.md)** — the big picture
2. **[Architecture](architecture.md)** — how modules fit together
3. **[Drive Access](drive-access.md)** — how we talk to hardware
4. **[UDF](udf.md)** → **[MPLS](mpls.md)** → **[CLPI](clpi.md)** — how disc content is structured
5. **[AACS](aacs.md)** — how encryption works and how we break it

## API Documentation

Generated API docs are on [docs.rs/libfreemkv](https://docs.rs/libfreemkv).
