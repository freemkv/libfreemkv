fn main() {
    let target = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target == "macos" {
        println!("cargo:rustc-link-lib=framework=IOKit");
        println!("cargo:rustc-link-lib=framework=CoreFoundation");

        let out_dir = std::env::var("OUT_DIR").unwrap();
        let obj = format!("{out_dir}/macos_shim.o");
        let lib = format!("{out_dir}/libmacos_scsi.a");

        std::process::Command::new("cc")
            .args([
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
