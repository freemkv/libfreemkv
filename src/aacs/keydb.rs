//! AACS Key Database parsing — KEYDB.cfg format.

use std::collections::HashMap;

/// Parsed AACS key database.
#[derive(Debug)]
pub struct KeyDb {
    /// Device keys for MKB processing
    pub device_keys: Vec<DeviceKey>,
    /// Processing keys (pre-computed media keys for specific MKB versions)
    pub processing_keys: Vec<[u8; 16]>,
    /// Host certificate + private key for SCSI authentication
    pub host_certs: Vec<HostCert>,
    /// Per-disc VUK entries indexed by disc hash (hex lowercase)
    pub disc_entries: HashMap<String, DiscEntry>,
}

/// A device key for MKB subset-difference tree processing.
#[derive(Debug, Clone)]
pub struct DeviceKey {
    pub key: [u8; 16],
    pub node: u16,
    pub uv: u32,
    pub u_mask_shift: u8,
}

/// Host certificate + private key for AACS SCSI authentication.
#[derive(Debug, Clone)]
pub struct HostCert {
    /// AACS 1.0: 20 bytes. AACS 2.0: 32 bytes.
    pub private_key: [u8; 20],
    /// AACS 1.0: 92 bytes. AACS 2.0: 132 bytes.
    pub certificate: Vec<u8>,
    /// AACS 2.0 host private key (P-256, 32 bytes). None for AACS 1.0 only.
    pub private_key_v2: Option<[u8; 32]>,
    /// AACS 2.0 host certificate (type 0x11). None for AACS 1.0 only.
    pub certificate_v2: Option<Vec<u8>>,
}

/// A per-disc entry from the key database.
#[derive(Debug, Clone)]
pub struct DiscEntry {
    /// Disc hash (20 bytes, hex)
    pub disc_hash: String,
    /// Disc title
    pub title: String,
    /// Media Key (16 bytes) — from MKB processing
    pub media_key: Option<[u8; 16]>,
    /// Disc ID (16 bytes)
    pub disc_id: Option<[u8; 16]>,
    /// Volume Unique Key (16 bytes) — decrypts title keys
    pub vuk: Option<[u8; 16]>,
    /// Unit keys (title keys) indexed by CPS unit number
    pub unit_keys: Vec<(u32, [u8; 16])>,
}

/// Path to the operator-managed local plugin file. Operators drop
/// additional AACS keys here (same on-disk format as keydb.cfg) and
/// they layer transparently on top of the built-ins and the main
/// keydb.cfg at load time. Returns `None` if `HOME` (or `USERPROFILE`
/// on Windows) is not set in the environment.
pub fn local_plugin_path() -> Option<std::path::PathBuf> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
    Some(std::path::PathBuf::from(home).join(".config/freemkv/local_keys.cfg"))
}

/// Parse a hex string like "0xABCD..." into bytes.
pub(crate) fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i + 2], 16).ok()?);
    }
    Some(out)
}

/// Parse hex into a fixed-size array.
pub(crate) fn parse_hex16(s: &str) -> Option<[u8; 16]> {
    let v = parse_hex(s)?;
    if v.len() != 16 {
        return None;
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&v);
    Some(out)
}

pub(crate) fn parse_hex20(s: &str) -> Option<[u8; 20]> {
    let v = parse_hex(s)?;
    if v.len() != 20 {
        return None;
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&v);
    Some(out)
}

impl KeyDb {
    /// Construct an empty KeyDb.
    pub fn empty() -> Self {
        KeyDb {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            host_certs: Vec::new(),
            disc_entries: HashMap::new(),
        }
    }

    /// Construct a KeyDb pre-populated with the compiled-in public AACS 1.0
    /// device keys and processing keys. Sufficient on its own to derive
    /// VUKs for any AACS 1.0 disc whose MKB version is covered.
    pub fn with_builtins() -> Self {
        let mut db = Self::empty();
        db.add_builtins();
        db
    }

    /// Push the built-in AACS 1.0 device keys and processing keys into
    /// this KeyDb. Duplicates (identical device-key triples or identical
    /// processing-key bytes) are silently skipped — first-seen wins.
    fn add_builtins(&mut self) {
        for entry in crate::aacs::builtin_keys::BUILTIN_DEVICE_KEYS {
            self.add_device_key_dedup(entry.to_device_key());
        }
        for pk in crate::aacs::builtin_keys::BUILTIN_PROCESSING_KEYS {
            self.add_processing_key_dedup(*pk);
        }
    }

    fn add_device_key_dedup(&mut self, dk: DeviceKey) {
        let dup = self
            .device_keys
            .iter()
            .any(|x| x.node == dk.node && x.uv == dk.uv && x.u_mask_shift == dk.u_mask_shift);
        if !dup {
            self.device_keys.push(dk);
        }
    }

    fn add_processing_key_dedup(&mut self, pk: [u8; 16]) {
        if !self.processing_keys.iter().any(|x| *x == pk) {
            self.processing_keys.push(pk);
        }
    }

    /// Merge another KeyDb's entries into this one, additively. Existing
    /// entries are kept; duplicates from `other` are silently dropped.
    /// Used to layer external keydb.cfg / local plugin contents on top of
    /// the built-ins.
    pub fn merge_from(&mut self, other: KeyDb) {
        for dk in other.device_keys {
            self.add_device_key_dedup(dk);
        }
        for pk in other.processing_keys {
            self.add_processing_key_dedup(pk);
        }
        // Host certs and disc entries do not have a stable "identity"
        // key suitable for dedup beyond byte-equality, so append host
        // certs as-is and insert disc entries with first-wins semantics
        // by hash.
        self.host_certs.extend(other.host_certs);
        for (hash, entry) in other.disc_entries {
            self.disc_entries.entry(hash).or_insert(entry);
        }
    }

    /// Parse a KEYDB.cfg file from a string.
    pub fn parse(data: &str) -> Self {
        let mut db = KeyDb {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            host_certs: Vec::new(),
            disc_entries: HashMap::new(),
        };

        for line in data.lines() {
            let line = line.trim();

            // Skip comments and empty lines
            if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
                continue;
            }

            // Device Key
            if line.starts_with("| DK") {
                if let Some(dk) = Self::parse_device_key(line) {
                    db.device_keys.push(dk);
                }
                continue;
            }

            // Processing Key
            if line.starts_with("| PK") {
                if let Some(pk) = Self::parse_processing_key(line) {
                    db.processing_keys.push(pk);
                }
                continue;
            }

            // Host Certificate (AACS 2.0)
            if line.starts_with("| HC2") {
                if let Some(hc) = db.host_certs.last_mut() {
                    if let Some((pk, cert)) = Self::parse_host_cert_v2(line) {
                        hc.private_key_v2 = Some(pk);
                        hc.certificate_v2 = Some(cert);
                    }
                }
                continue;
            }

            // Host Certificate (AACS 1.0)
            if line.starts_with("| HC") {
                if let Some(hc) = Self::parse_host_cert(line) {
                    db.host_certs.push(hc);
                }
                continue;
            }

            // Disc entry: starts with 0x
            if line.starts_with("0x") && line.contains(" = ") {
                if let Some(entry) = Self::parse_disc_entry(line) {
                    db.disc_entries.insert(entry.disc_hash.clone(), entry);
                }
            }
        }

        db
    }

    /// Load a KEYDB.cfg from disk, layered on top of the compiled-in
    /// built-in keys. If `path` does not exist, returns a KeyDb that
    /// contains only the built-ins (no error). If `path` exists but
    /// cannot be read, the underlying I/O error is returned.
    ///
    /// An operator plugin slot at `$HOME/.config/freemkv/local_keys.cfg`
    /// (same on-disk format) is also layered on top when present. This
    /// lets operators drop additional keys at runtime without editing
    /// the main keydb.cfg.
    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        let mut db = Self::with_builtins();
        if path.exists() {
            let data = std::fs::read_to_string(path)?;
            db.merge_from(Self::parse(&data));
        }
        db.merge_local_plugin();
        Ok(db)
    }

    /// Load only the built-in keys plus the operator plugin slot at
    /// `$HOME/.config/freemkv/local_keys.cfg` (if present). Used when
    /// no main keydb.cfg path is configured.
    pub fn load_or_builtins() -> Self {
        let mut db = Self::with_builtins();
        db.merge_local_plugin();
        db
    }

    /// If `$HOME/.config/freemkv/local_keys.cfg` exists, layer its
    /// contents on top of this KeyDb. Errors reading the plugin file
    /// are silently ignored — the plugin is a best-effort augmentation.
    fn merge_local_plugin(&mut self) {
        let Some(path) = local_plugin_path() else {
            return;
        };
        if !path.exists() {
            return;
        }
        if let Ok(data) = std::fs::read_to_string(&path) {
            self.merge_from(Self::parse(&data));
        }
    }

    /// Look up a disc by its hash. Returns the VUK if found.
    pub fn find_vuk(&self, disc_hash: &str) -> Option<[u8; 16]> {
        let hash = disc_hash
            .trim()
            .to_lowercase()
            .trim_start_matches("0x")
            .to_string();
        // Try with 0x prefix and without
        self.disc_entries
            .get(&format!("0x{hash}"))
            .or_else(|| self.disc_entries.get(&hash))
            .and_then(|e| e.vuk)
    }

    /// Look up a disc by its hash. Returns the full entry.
    pub fn find_disc(&self, disc_hash: &str) -> Option<&DiscEntry> {
        let hash = disc_hash
            .trim()
            .to_lowercase()
            .trim_start_matches("0x")
            .to_string();
        self.disc_entries
            .get(&format!("0x{hash}"))
            .or_else(|| self.disc_entries.get(&hash))
    }

    // ── Parsers ─────────────────────────────────────────────────────────────

    fn parse_device_key(line: &str) -> Option<DeviceKey> {
        // | DK | DEVICE_KEY 0x... | DEVICE_NODE 0x... | KEY_UV 0x... | KEY_U_MASK_SHIFT 0x...
        let key_str = line.split("DEVICE_KEY").nth(1)?.split('|').next()?.trim();
        let node_str = line.split("DEVICE_NODE").nth(1)?.split('|').next()?.trim();
        let uv_str = line.split("KEY_UV").nth(1)?.split('|').next()?.trim();
        let shift_str = line
            .split("KEY_U_MASK_SHIFT")
            .nth(1)?
            .split(';')
            .next()?
            .split('|')
            .next()?
            .trim();

        Some(DeviceKey {
            key: parse_hex16(key_str)?,
            node: u16::from_str_radix(node_str.trim_start_matches("0x"), 16).ok()?,
            uv: u32::from_str_radix(uv_str.trim_start_matches("0x"), 16).ok()?,
            u_mask_shift: u8::from_str_radix(shift_str.trim_start_matches("0x"), 16).ok()?,
        })
    }

    fn parse_processing_key(line: &str) -> Option<[u8; 16]> {
        // | PK | 0x...
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() >= 3 {
            let key_str = parts[2].split(';').next()?.trim();
            return parse_hex16(key_str);
        }
        None
    }

    fn parse_host_cert(line: &str) -> Option<HostCert> {
        // | HC | HOST_PRIV_KEY 0x... | HOST_CERT 0x...
        let priv_str = line
            .split("HOST_PRIV_KEY")
            .nth(1)?
            .split('|')
            .next()?
            .trim();
        let cert_str = line
            .split("HOST_CERT")
            .nth(1)?
            .split(';')
            .next()?
            .split('|')
            .next()?
            .trim();

        Some(HostCert {
            private_key: parse_hex20(priv_str)?,
            certificate: parse_hex(cert_str)?,
            private_key_v2: None,
            certificate_v2: None,
        })
    }

    /// Parse AACS 2.0 host cert: `| HC2 | HOST_PRIV_KEY 0x... | HOST_CERT 0x...`
    fn parse_host_cert_v2(line: &str) -> Option<([u8; 32], Vec<u8>)> {
        let priv_str = line
            .split("HOST_PRIV_KEY")
            .nth(1)?
            .split('|')
            .next()?
            .trim();
        let cert_str = line
            .split("HOST_CERT")
            .nth(1)?
            .split(';')
            .next()?
            .split('|')
            .next()?
            .trim();

        let priv_bytes = parse_hex(priv_str)?;
        if priv_bytes.len() != 32 {
            return None;
        }
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&priv_bytes);

        let cert = parse_hex(cert_str)?;
        if cert.len() < 132 {
            return None;
        }

        Some((pk, cert))
    }

    fn parse_disc_entry(line: &str) -> Option<DiscEntry> {
        // 0x<hash> = <title> | D | <date> | M | 0x<mk> | I | 0x<id> | V | 0x<vuk> | U | <unit_keys>
        let (hash_part, rest) = line.split_once(" = ")?;
        let disc_hash = hash_part.trim().to_lowercase();

        // Extract title (before first |)
        let title_part = rest.split(" | ").next().unwrap_or("").trim();
        // Clean title: "TITLE_NAME (Display Title)" → use display title if present
        let title = if let Some(start) = title_part.find('(') {
            if let Some(end) = title_part.rfind(')') {
                title_part[start + 1..end].to_string()
            } else {
                title_part.to_string()
            }
        } else {
            title_part.to_string()
        };

        // Parse fields by tag
        let mut media_key = None;
        let mut disc_id = None;
        let mut vuk = None;
        let mut unit_keys = Vec::new();

        let parts: Vec<&str> = rest.split(" | ").collect();
        let mut i = 0;
        while i < parts.len() {
            match parts[i].trim() {
                "M" => {
                    if i + 1 < parts.len() {
                        media_key = parse_hex16(parts[i + 1].trim());
                        i += 1;
                    }
                }
                "I" => {
                    if i + 1 < parts.len() {
                        disc_id = parse_hex16(parts[i + 1].trim());
                        i += 1;
                    }
                }
                "V" => {
                    if i + 1 < parts.len() {
                        vuk = parse_hex16(parts[i + 1].trim());
                        i += 1;
                    }
                }
                "U" => {
                    if i + 1 < parts.len() {
                        // Unit keys: "1-0xKEY" or "1-0xKEY ; comment"
                        let uk_str = parts[i + 1].split(';').next().unwrap_or("").trim();
                        for uk in uk_str.split(' ') {
                            let uk = uk.trim();
                            if let Some((num, key)) = uk.split_once('-') {
                                if let Ok(n) = num.parse::<u32>() {
                                    if let Some(k) = parse_hex16(key) {
                                        unit_keys.push((n, k));
                                    }
                                }
                            }
                        }
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        Some(DiscEntry {
            disc_hash,
            title,
            media_key,
            disc_id,
            vuk,
            unit_keys,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Get KEYDB path from KEYDB_PATH environment variable. Returns None if not set or not found.
    fn keydb_path() -> Option<std::path::PathBuf> {
        let path = std::path::PathBuf::from(std::env::var("KEYDB_PATH").ok()?);
        if path.exists() { Some(path) } else { None }
    }

    #[test]
    fn test_parse_disc_entry() {
        let line = r#"***REMOVED*** = DUNE_PART_TWO (Dune: Part Two) | D | 2024-04-02 | M | ***REMOVED*** | I | ***REMOVED*** | V | ***REMOVED*** | U | 1-***REMOVED*** ; MKBv77"#;
        let entry = KeyDb::parse_disc_entry(line).unwrap();
        assert_eq!(entry.title, "Dune: Part Two");
        assert!(entry.media_key.is_some());
        assert!(entry.vuk.is_some());
        assert_eq!(entry.unit_keys.len(), 1);
        assert_eq!(entry.unit_keys[0].0, 1);
    }

    #[test]
    fn test_parse_device_key() {
        let line = "| DK | DEVICE_KEY ***REMOVED*** | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17 ; MKBv01-MKBv48";
        let dk = KeyDb::parse_device_key(line).unwrap();
        assert_eq!(dk.node, 0x0800);
        assert_eq!(dk.u_mask_shift, 0x17);
    }

    #[test]
    fn test_parse_host_cert() {
        let line = "| HC | HOST_PRIV_KEY ***REMOVED*** | HOST_CERT ***REMOVED*** ; Revoked";
        let hc = KeyDb::parse_host_cert(line).unwrap();
        assert_eq!(hc.private_key[0], 0x90);
        assert_eq!(hc.certificate.len(), 92);
    }

    // Mutex serializes tests that mutate process-wide environment
    // variables (HOME). Tests in the same module run on threads by
    // default; a shared lock here prevents env-var bleed-through.
    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn keydb_default_has_builtins() {
        let db = KeyDb::with_builtins();
        assert_eq!(db.device_keys.len(), 4);
        assert_eq!(db.processing_keys.len(), 3);
        assert!(db.host_certs.is_empty());
        assert!(db.disc_entries.is_empty());

        let expected_first: [u8; 16] = [
            0x5F, 0xB8, 0x6E, 0xF1, 0x27, 0xC1, 0x9C, 0x17, 0x1E, 0x79, 0x9F, 0x61, 0xC2, 0x7B,
            0xDC, 0x2A,
        ];
        assert_eq!(db.device_keys[0].key, expected_first);
        assert_eq!(db.device_keys[0].node, 0x0800);
    }

    #[test]
    fn keydb_load_layers_on_builtins() {
        // Synthetic keydb.cfg with one extra device key not in built-ins.
        let extra_dk = "| DK | DEVICE_KEY 0xAABBCCDDEEFF00112233445566778899 | DEVICE_NODE 0x1234 | KEY_UV 0x00001234 | KEY_U_MASK_SHIFT 0x05 ; extra\n";

        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempdir_isolated_home();
        let cfg_path = tmp.path().join("keydb.cfg");
        std::fs::write(&cfg_path, extra_dk).unwrap();

        let db = KeyDb::load(&cfg_path).unwrap();
        // 4 built-ins + 1 extra = 5 DKs total
        assert_eq!(db.device_keys.len(), 5);
        // Built-ins are still present and come first
        assert_eq!(db.device_keys[0].node, 0x0800);
        // Extra entry is layered on top
        assert!(db.device_keys.iter().any(|d| d.node == 0x1234));
        assert_eq!(db.processing_keys.len(), 3);
    }

    #[test]
    fn keydb_load_missing_file_falls_back_to_builtins() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _tmp = tempdir_isolated_home();
        let db = KeyDb::load(std::path::Path::new("/nonexistent/keydb.cfg")).unwrap();
        assert_eq!(db.device_keys.len(), 4);
        assert_eq!(db.processing_keys.len(), 3);
        assert!(db.host_certs.is_empty());
        assert!(db.disc_entries.is_empty());
    }

    #[test]
    fn keydb_local_plugin_layered() {
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempdir_isolated_home();

        // Drop a local_keys.cfg into the synthetic HOME with one extra DK.
        let plugin_dir = tmp.path().join(".config/freemkv");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        let plugin_path = plugin_dir.join("local_keys.cfg");
        std::fs::write(
            &plugin_path,
            "| DK | DEVICE_KEY 0x112233445566778899AABBCCDDEEFF00 | DEVICE_NODE 0xABCD | KEY_UV 0x0000ABCD | KEY_U_MASK_SHIFT 0x07 ; plugin\n",
        )
        .unwrap();

        // load_or_builtins must include the plugin entry.
        let db = KeyDb::load_or_builtins();
        assert_eq!(db.device_keys.len(), 5);
        assert!(db.device_keys.iter().any(|d| d.node == 0xABCD));

        // load() of a non-existent main keydb must also include the plugin.
        let db2 = KeyDb::load(std::path::Path::new("/nonexistent/keydb.cfg")).unwrap();
        assert!(db2.device_keys.iter().any(|d| d.node == 0xABCD));
    }

    #[test]
    fn resolve_with_no_keydb_path() {
        // Higher-level resolution: when ScanOptions has no keydb_path
        // and no keydb.cfg is in standard search paths, the loader
        // returns a KeyDb populated with the built-ins (plus any
        // plugin entries). This is the "AACS 1.0 just works" path.
        let _guard = ENV_LOCK.lock().unwrap();
        let _tmp = tempdir_isolated_home();
        // Wipe XDG/system paths from view by pointing HOME at an empty
        // dir and confirming there's nothing in our plugin slot. Then
        // assert the no-path entry point returns built-ins only.
        let db = KeyDb::load_or_builtins();
        assert_eq!(db.device_keys.len(), 4);
        assert_eq!(db.processing_keys.len(), 3);
    }

    #[test]
    fn keydb_dedup_keeps_first() {
        // Loading the same DK twice (built-in + identical entry in cfg)
        // results in one entry, not two.
        let dup_dk = "| DK | DEVICE_KEY ***REMOVED*** | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17 ; duplicate of builtin\n";
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempdir_isolated_home();
        let cfg_path = tmp.path().join("keydb.cfg");
        std::fs::write(&cfg_path, dup_dk).unwrap();

        let db = KeyDb::load(&cfg_path).unwrap();
        // Still 4, not 5 — the duplicate was deduped
        assert_eq!(db.device_keys.len(), 4);
    }

    /// Build a temporary directory and point `HOME`/`USERPROFILE` at it
    /// so the local-plugin loader sees an isolated, empty environment by
    /// default. The returned [`TempDir`] auto-cleans on drop.
    fn tempdir_isolated_home() -> TempDir {
        let tmp = TempDir::new();
        // SAFETY: tests serialize via ENV_LOCK before calling this.
        unsafe {
            std::env::set_var("HOME", tmp.path());
            std::env::set_var("USERPROFILE", tmp.path());
        }
        tmp
    }

    /// Minimal scoped temp directory — auto-removes on drop.
    /// Avoids adding the `tempfile` crate as a dependency.
    struct TempDir {
        path: std::path::PathBuf,
    }
    impl TempDir {
        fn new() -> Self {
            use std::time::{SystemTime, UNIX_EPOCH};
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let tid = std::thread::current().id();
            let path = std::env::temp_dir().join(format!(
                "libfreemkv-keydb-test-{:?}-{}-{}",
                tid,
                std::process::id(),
                nanos
            ));
            std::fs::create_dir_all(&path).expect("create tempdir");
            TempDir { path }
        }
        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn test_parse_full_keydb() {
        let path = match keydb_path() {
            Some(p) => p,
            None => return,
        }; // skip if not available

        let db = KeyDb::load(&path).unwrap();

        assert_eq!(db.device_keys.len(), 4);
        assert_eq!(db.processing_keys.len(), 3);
        assert!(!db.host_certs.is_empty());
        assert!(db.disc_entries.len() > 170000);

        // Look up Dune: Part Two
        let dune = db
            .disc_entries
            .values()
            .find(|e| e.title.contains("Dune: Part Two") && e.vuk.is_some())
            .expect("Dune: Part Two not found");
        assert!(dune.media_key.is_some());
        assert!(dune.vuk.is_some());
        assert!(!dune.unit_keys.is_empty());

        eprintln!(
            "Parsed {} disc entries, {} DK, {} PK",
            db.disc_entries.len(),
            db.device_keys.len(),
            db.processing_keys.len()
        );
    }
}
