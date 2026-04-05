# Contributing to libfreemkv

Thank you for your interest in helping make disc archival accessible to everyone.

## Contributing Drive Profiles

The most impactful contribution is adding support for new drives. If you have
an optical drive that isn't listed in [profiles/](profiles/), we'd love your help.

### How to submit drive data

1. Install the tool:
   ```bash
   cargo install libfreemkv
   ```

2. Run `freemkv-info` with your drive:
   ```bash
   freemkv-info /dev/sr0 --raw > my_drive.txt
   ```

3. Open a pull request or issue with the output file attached.

That's it. The raw SCSI response data lets us build a profile for your drive.

### What data is collected

`freemkv-info --raw` sends two standard SCSI commands to your drive:

- **INQUIRY** (opcode 0x12) — returns drive vendor, model, firmware version
- **GET CONFIGURATION** (opcode 0x46) — returns drive feature data

These are read-only, standard SCSI commands. They don't modify your drive
or access any disc data. Every operating system sends these commands
automatically when a drive is connected.

### Priority: Pioneer drives

We especially need data from **Pioneer** Blu-ray drives (BDR-S08, BDR-S09,
BDR-S12, BDR-S13, BDR-209, BDR-212, etc). If you have one, your contribution
would help unlock support for 130+ Pioneer drive firmware versions.

## Contributing Code

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Write tests for your changes
4. Ensure `cargo test` and `cargo clippy` pass
5. Submit a pull request

### Code Style

- Run `cargo fmt` before committing
- No `unsafe` without a comment explaining why
- Public APIs need doc comments
- Error handling via `Result<T, Error>`, no panics in library code

### Architecture

- `src/scsi.rs` — SCSI transport layer (SG_IO on Linux)
- `src/profile.rs` — Profile loading and matching
- `src/platform/` — Per-chipset command implementations
- `src/drive.rs` — High-level DriveSession API
- `profiles/` — JSON drive profile data

## License

By contributing, you agree that your contributions will be licensed under AGPL-3.0.
