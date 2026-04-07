//! aacs-test — Test AACS handshake against a real drive.
//!
//! Usage: aacs-test /dev/sr0 /path/to/keydb.cfg

use std::env;
use std::path::Path;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: aacs-test <device> <keydb_path>");
        std::process::exit(1);
    }

    let device = Path::new(&args[1]);
    let keydb_path = Path::new(&args[2]);

    println!("aacs-test v{}", env!("CARGO_PKG_VERSION"));
    println!();

    // Open drive WITHOUT unlock — AACS auth must happen before raw mode
    print!("Opening {} (no unlock)... ", device.display());
    let mut session = match libfreemkv::DriveSession::open_no_unlock(device) {
        Ok(s) => { println!("OK"); s }
        Err(e) => { println!("FAILED: {}", e); std::process::exit(1); }
    };
    println!("  Drive: {} {}", session.profile.drive_id.trim(), session.profile.chipset.name());

    // Load KEYDB
    print!("Loading KEYDB... ");
    let keydb = match libfreemkv::aacs::KeyDb::load(keydb_path) {
        Ok(db) => {
            println!("OK ({} disc entries, {} DK, {} PK)",
                db.disc_entries.len(), db.device_keys.len(), db.processing_keys.len());
            db
        }
        Err(e) => { println!("FAILED: {}", e); std::process::exit(1); }
    };

    let host_cert = match &keydb.host_cert {
        Some(hc) => {
            println!("  Host cert: {} bytes, priv_key[0]=0x{:02x}",
                hc.certificate.len(), hc.private_key[0]);
            hc
        }
        None => { println!("  No host cert in KEYDB"); std::process::exit(1); }
    };

    // AACS handshake
    println!();
    print!("AACS authenticate... ");
    let mut auth = match libfreemkv::aacs::handshake::aacs_authenticate(
        &mut session,
        &host_cert.private_key,
        &host_cert.certificate,
    ) {
        Ok(a) => {
            println!("OK");
            println!("  Bus key: {:02x?}", &a.bus_key);
            println!("  AGID: {}", a.agid);
            println!("  Drive cert type: 0x{:02x}", a.drive_cert[0]);
            a
        }
        Err(e) => {
            println!("FAILED: {}", e);
            std::process::exit(1);
        }
    };

    // Read Volume ID
    print!("Reading Volume ID... ");
    match libfreemkv::aacs::handshake::read_volume_id(&mut session, &mut auth) {
        Ok(vid) => {
            println!("OK");
            println!("  VID: {:02x?}", vid);

            // Try to find matching disc in KEYDB
            let matched = keydb.disc_entries.values()
                .find(|e| e.disc_id == Some(vid));
            if let Some(entry) = matched {
                println!("  KEYDB match: {} (hash {})", entry.title, entry.disc_hash);
                if let Some(vuk) = entry.vuk {
                    println!("  VUK: {:02x?}", vuk);
                }
            } else {
                println!("  No exact VID match in KEYDB");
            }
        }
        Err(e) => println!("FAILED: {}", e),
    }

    // Read data keys (AACS 2.0)
    print!("Reading data keys... ");
    match libfreemkv::aacs::handshake::read_data_keys(&mut session, &mut auth) {
        Ok((rdk, wdk)) => {
            println!("OK (AACS 2.0 bus encryption)");
            println!("  Read data key:  {:02x?}", rdk);
            println!("  Write data key: {:02x?}", wdk);
        }
        Err(e) => println!("not available: {} (likely AACS 1.0)", e),
    }

    println!();
    println!("Done.");
}
