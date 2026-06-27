fn main() {
    emit_git_suffix();

    let target = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target == "macos" {
        println!("cargo:rustc-link-lib=framework=IOKit");
        println!("cargo:rustc-link-lib=framework=CoreFoundation");

        let out_dir = std::env::var("OUT_DIR").unwrap();
        let obj = format!("{out_dir}/macos_shim.o");
        let lib = format!("{out_dir}/libmacos_scsi.a");

        // Build the shim for the TARGET arch, not the host's. A bare `cc` on an
        // Apple-Silicon CI runner defaults to arm64, so cross-building to
        // x86_64-apple-darwin would link a host-arch object against x86_64 Rust
        // code → "Undefined symbols for architecture x86_64". (Still raw `cc`,
        // not the `cc` crate, which breaks IOKit exclusive access.)
        let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
        let clang_arch: &str = if target_arch == "aarch64" {
            "arm64"
        } else {
            &target_arch // x86_64 → x86_64
        };

        std::process::Command::new("cc")
            .args([
                "-arch",
                clang_arch,
                "-c",
                "src/scsi/macos_shim.c",
                "-o",
                &obj,
                "-framework",
                "IOKit",
                "-framework",
                "CoreFoundation",
                "-Wall",
                "-O2",
            ])
            .status()
            .expect("failed to compile macos_shim.c");

        std::process::Command::new("ar")
            .args(["rcs", &lib, &obj])
            .status()
            .expect("failed to create static lib");

        println!("cargo:rustc-link-search=native={out_dir}");
        println!("cargo:rustc-link-lib=static=macos_scsi");
        println!("cargo:rerun-if-changed=src/scsi/macos_shim.c");
    }
}

/// Bake the git short hash into the build as `GIT_SUFFIX` so any muxed MKV or
/// FVI index is traceable to the exact source revision (e.g. ` (g835cc99)`).
/// Empty when git or the repo is unavailable (e.g. a crates.io tarball build),
/// leaving just the package version. Always emitted so `env!("GIT_SUFFIX")`
/// resolves on every target.
fn emit_git_suffix() {
    let suffix = git_short_hash()
        .map(|h| format!(" (g{h})"))
        .unwrap_or_default();
    println!("cargo:rustc-env=GIT_SUFFIX={suffix}");

    // Re-run when HEAD (or the branch it points at) moves so the stamp stays
    // current without a clean rebuild.
    println!("cargo:rerun-if-changed=.git/HEAD");
    if let Ok(head) = std::fs::read_to_string(".git/HEAD") {
        if let Some(ref_path) = head.strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed=.git/{}", ref_path.trim());
        }
    }
}

fn git_short_hash() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let h = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if h.is_empty() { None } else { Some(h) }
}
