//! Parity goldens: one small text file per cell under `<crate>/tests/goldens/`, recording
//! what a path produced today (output hashes, loss counters, gate verdicts). A later change
//! must reproduce every cell exactly, or re-bless it on purpose.
//!
//! Re-bless every cell: `FREEMKV_BLESS_GOLDENS=1 cargo test --lib parity_` (libfreemkv) or
//! `FREEMKV_BLESS_GOLDENS=1 cargo test --test parity_goldens` (engine), then review the
//! `git diff` of `tests/goldens/` and name the reason in the commit message.

use std::fmt::{Display, Write as _};
use std::path::PathBuf;

/// The env var that rewrites goldens instead of comparing against them.
pub const BLESS_ENV: &str = "FREEMKV_BLESS_GOLDENS";

/// One cell's observations, built in order and compared (or blessed) by [`Golden::check`].
pub struct Golden {
    file: PathBuf,
    cell: String,
    text: String,
}

impl Golden {
    /// A cell named `cell` for the crate at `manifest_dir` (pass `env!("CARGO_MANIFEST_DIR")`).
    pub fn new(manifest_dir: &str, cell: &str) -> Self {
        Self {
            file: PathBuf::from(manifest_dir)
                .join("tests/goldens")
                .join(format!("{cell}.golden")),
            cell: cell.to_string(),
            text: String::new(),
        }
    }

    /// Record `key = value`.
    pub fn kv(&mut self, key: &str, value: impl Display) -> &mut Self {
        let _ = writeln!(self.text, "{key} = {value}");
        self
    }

    /// Record the length and SHA-256 of `bytes` under `key`.
    pub fn bytes(&mut self, key: &str, bytes: &[u8]) -> &mut Self {
        use sha2::{Digest, Sha256};
        let digest: String = Sha256::digest(bytes)
            .iter()
            .take(16)
            .map(|b| format!("{b:02x}"))
            .collect();
        self.kv(key, format_args!("{} bytes sha256:{digest}", bytes.len()))
    }

    /// The recorded text so far.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Compare against the committed file; under [`BLESS_ENV`] write it instead.
    pub fn check(&self) {
        if std::env::var_os(BLESS_ENV).is_some() {
            std::fs::create_dir_all(self.file.parent().expect("goldens dir")).expect("mkdir");
            std::fs::write(&self.file, &self.text).expect("write golden");
            return;
        }
        let want = std::fs::read_to_string(&self.file).unwrap_or_else(|_| {
            panic!(
                "golden `{}` is missing; run with {BLESS_ENV}=1 to create it",
                self.cell
            )
        });
        assert_eq!(
            self.text, want,
            "golden `{}` changed: if intended, re-bless with {BLESS_ENV}=1 and name the reason",
            self.cell
        );
    }
}
