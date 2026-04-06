//! freemkv-info — Drive identification and compatibility checker.
//!
//! Sends standard SCSI INQUIRY and GET CONFIGURATION commands to an optical drive,
//! displays drive identity and compatibility status, and optionally outputs raw
//! response data for profile contribution.
//!
//! Usage:
//!   freemkv-info /dev/sr0
//!   freemkv-info /dev/sr0 --raw
//!   freemkv-info /dev/sr0 --json

use std::env;
use std::path::Path;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("freemkv-info — Drive identification and compatibility checker");
        eprintln!();
        eprintln!("Usage: freemkv-info <device> [options]");
        eprintln!();
        eprintln!("  <device>    Optical drive device (e.g. /dev/sr0)");
        eprintln!("  --raw       Output raw SCSI response hex (for profile contribution)");
        eprintln!("  --json      Output machine-readable JSON");
        eprintln!("  --profiles  Path to profiles directory (default: ./profiles)");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  freemkv-info /dev/sr0");
        eprintln!("  freemkv-info /dev/sr0 --raw > my_drive.txt");
        process::exit(1);
    }

    let device = Path::new(&args[1]);
    let raw_mode = args.iter().any(|a| a == "--raw");
    let json_mode = args.iter().any(|a| a == "--json");
    let profiles_dir = args.iter()
        .position(|a| a == "--profiles")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("profiles");

    // Open SCSI transport
    let mut transport = match libfreemkv::scsi::open(device) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error: Cannot open {}: {}", device.display(), e);
            process::exit(1);
        }
    };

    // INQUIRY
    let inquiry = match libfreemkv::scsi::inquiry(transport.as_mut()) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("Error: INQUIRY failed: {}", e);
            process::exit(1);
        }
    };

    // GET CONFIGURATION feature 0x010C
    let gc_010c = libfreemkv::scsi::get_config_010c(transport.as_mut()).ok();

    if json_mode {
        print_json(&inquiry, &gc_010c);
    } else if raw_mode {
        print_raw(&inquiry, &gc_010c);
    } else {
        print_human(&inquiry, &gc_010c, profiles_dir);
    }
}

fn print_human(
    inquiry: &libfreemkv::scsi::InquiryResult,
    gc_010c: &Option<Vec<u8>>,
    profiles_dir: &str,
) {
    println!("freemkv-info v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Drive:    {} {} {}", inquiry.vendor_id, inquiry.model, inquiry.firmware);
    println!("INQUIRY:  additional_length=0x{:02X} ({})",
        inquiry.raw.get(4).unwrap_or(&0),
        inquiry.raw.get(4).unwrap_or(&0));

    if let Some(gc) = gc_010c {
        let data_hex: String = gc.iter().map(|b| format!("{:02x}", b)).collect();
        println!("Feature 0x010C: {}", data_hex);
    } else {
        println!("Feature 0x010C: not available");
    }

    // Try to match profile
    if let Ok(profiles) = libfreemkv::profile::load_all(Path::new(profiles_dir)) {
        let matched = profiles.iter().find(|p| {
            p.drive_id.contains(&inquiry.vendor_id)
                && p.drive_id.contains(&inquiry.model)
        });

        println!();
        match matched {
            Some(p) => {
                println!("Profile:  FOUND ({})", p.chipset.name());
                println!("Raw Read: Supported");
            }
            None => {
                println!("Profile:  NOT FOUND");
                println!("Raw Read: Unknown — run with --raw and submit a profile request");
            }
        }
    } else {
        println!();
        println!("Profile:  No profiles directory found at '{}'", profiles_dir);
    }
}

fn print_raw(
    inquiry: &libfreemkv::scsi::InquiryResult,
    gc_010c: &Option<Vec<u8>>,
) {
    println!("# freemkv-info raw output");
    println!("# Submit this file to https://github.com/freemkv/libfreemkv/issues");
    println!();
    println!("vendor: {}", inquiry.vendor_id);
    println!("model: {}", inquiry.model);
    println!("firmware: {}", inquiry.firmware);
    println!();

    // Full INQUIRY hex
    println!("inquiry_hex: {}", hex_encode(&inquiry.raw));
    println!("inquiry_length: {}", inquiry.raw.len());

    // GET CONFIG 0x010C
    if let Some(gc) = gc_010c {
        println!("get_config_010c_hex: {}", hex_encode(gc));
        println!("get_config_010c_length: {}", gc.len());
    } else {
        println!("get_config_010c_hex: ERROR");
    }
}

fn print_json(
    inquiry: &libfreemkv::scsi::InquiryResult,
    gc_010c: &Option<Vec<u8>>,
) {
    let json = serde_json::json!({
        "vendor": inquiry.vendor_id,
        "model": inquiry.model,
        "firmware": inquiry.firmware,
        "inquiry_hex": hex_encode(&inquiry.raw),
        "inquiry_length": inquiry.raw.len(),
        "get_config_010c_hex": gc_010c.as_ref().map(|g| hex_encode(g)),
    });
    println!("{}", serde_json::to_string_pretty(&json).unwrap());
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}
