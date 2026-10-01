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

### Parity goldens

`parity_*` tests pin what every input-to-output path produces today (output hashes, loss
counters, refusal codes) in `tests/goldens/*.golden`. A change that moves one must say why.
Re-bless on purpose, then review the diff:

```bash
FREEMKV_BLESS_GOLDENS=1 cargo test --lib parity_
git diff tests/goldens
```

## Code style

- Follow Rust conventions (`cargo fmt`, `cargo clippy`)
- Field names follow SPC-4 and MMC-6 SCSI standards
- Error codes are structured — no user-facing text in the library

## License

By contributing, you agree your code will be licensed under MIT.
