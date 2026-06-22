fn main() {
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
