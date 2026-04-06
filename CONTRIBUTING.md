# Contributing to libfreemkv

Thanks for your interest in contributing!

## Ways to help

- **Report a bug** — open an issue with steps to reproduce
- **Submit your drive profile** — run `freemkv info --share` to help expand hardware support
- **Fix a bug** — fork, branch, PR
- **Add a feature** — open an issue first to discuss

## Development

```bash
cargo build
cargo test
```

## Code style

- Follow Rust conventions (`cargo fmt`, `cargo clippy`)
- Field names follow SPC-4 and MMC-6 SCSI standards
- Error codes are structured — no user-facing text in the library

## License

By contributing, you agree your code will be licensed under AGPL-3.0.
