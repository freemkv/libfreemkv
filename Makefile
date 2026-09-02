# libfreemkv — local dev helper.
# Mirrors the workspace-wide CI checks but scoped to this single crate.

.PHONY: test build check loom ci clean

test:
	cargo test --tests

build:
	cargo build --release

check:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

# Model-check the scsi::fd_handoff atomics. `cargo test` cannot fail on those
# orderings — they compile to the same instructions on x86_64 — so this is the
# only check on them. Mirrors the `loom` CI job.
loom:
	RUSTFLAGS="--cfg loom" cargo test --lib --no-default-features --features scsi fd_handoff

ci: check build test loom

clean:
	cargo clean
