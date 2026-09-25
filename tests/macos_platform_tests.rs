//! macOS platform-integration tests that do NOT require an optical drive.
//!
//! These exercise the Mac-only code paths under `platform/fs_type/macos.rs`,
//! `io/platform_macos.rs`, and `io/writeback_file` end-to-end against the real
//! kernel. They run under `cargo test` without `--ignored`, so CI on macOS
//! (and the local `cargo test` a developer would run before pushing) covers
//! them automatically.

#![cfg(target_os = "macos")]

use libfreemkv::WritebackFile;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

fn tmp_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("freemkv-macos-test-{pid}-{nanos}-{name}"));
    p
}

#[test]
fn writeback_file_roundtrips_write_read() {
    let path = tmp_path("roundtrip");
    let payload = b"freemkv writeback roundtrip on APFS";

    {
        let mut wf = WritebackFile::create(&path).expect("create WritebackFile");
        wf.write_all(payload).expect("write");
        wf.sync_all().expect("sync_all");
    }

    let mut f = std::fs::File::open(&path).expect("reopen for read");
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).expect("read");
    let _ = std::fs::remove_file(&path);

    assert_eq!(buf, payload);
}

#[test]
fn writeback_file_seek_and_patch() {
    let path = tmp_path("seek");

    let mut wf = WritebackFile::create(&path).expect("create WritebackFile");
    wf.write_all(b"AAAAAAAA").expect("initial write");
    wf.seek(SeekFrom::Start(3)).expect("seek back");
    wf.write_all(b"BB").expect("patch write");
    wf.sync_all().expect("sync_all");
    drop(wf);

    let contents = std::fs::read(&path).expect("read back");
    let _ = std::fs::remove_file(&path);
    assert_eq!(&contents, b"AAABBAAA");
}

#[test]
fn writeback_file_create_with_size_hint_preallocates() {
    let path = tmp_path("prealloc");
    let hint: u64 = 16 * 1024 * 1024;

    let mut wf = WritebackFile::create_with_size_hint(&path, hint).expect("create_with_size_hint");
    wf.write_all(&[0xAB; 4096]).expect("write header");
    wf.sync_all().expect("sync_all");
    drop(wf);

    // Reported file length reflects bytes written, not the preallocation.
    let meta = std::fs::metadata(&path).expect("stat");
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        meta.len(),
        4096,
        "reported length should equal bytes written"
    );
}

#[test]
fn list_drives_does_not_panic_without_hardware() {
    // Even on a Mac with no optical drive, enumeration must return cleanly.
    let _ = libfreemkv::list_drives();
}
