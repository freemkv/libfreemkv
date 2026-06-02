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
    /// Construct an empty KeyDb. Used by unit tests; production code
    /// reaches a populated KeyDb via [`KeyDb::load`] or [`KeyDb::parse`].
    pub fn empty() -> Self {
        KeyDb {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            host_certs: Vec::new(),
            disc_entries: HashMap::new(),
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

            // Device Key.
            // Two shapes are accepted:
            //   1. Positioned DK: `| DK | DEVICE_KEY 0x... | DEVICE_NODE 0x... | KEY_UV 0x... | KEY_U_MASK_SHIFT 0x...`
            //      → loaded into `device_keys` (deterministic tree walk via `calc_pk_from_dk`).
            //   2. Orphan DK: `| DK | DEVICE_KEY 0x...` with no position fields.
            //      → loaded into `processing_keys` (brute walker / terminal validation).
            // Per AACS spec a "PK" IS a DK at terminal position, so both row types
            // are DKs in the unified model; only the metadata differs.
            if line.starts_with("| DK") {
                if let Some(dk) = Self::parse_device_key(line) {
                    db.device_keys.push(dk);
                } else if let Some(key) = Self::parse_orphan_dk(line) {
                    db.processing_keys.push(key);
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

    /// Load a KEYDB.cfg from disk.
    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        let data = std::fs::read_to_string(path)?;
        Ok(Self::parse(&data))
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

    /// Iterate every disc entry. Used by Path 3 (scan for matching VID).
    pub fn iter_disc_entries(&self) -> impl Iterator<Item = &DiscEntry> {
        self.disc_entries.values()
    }
}

// ── KeyProvider impl ──────────────────────────────────────────────────────────
//
// Lets `KeyDb` plug into `resolve_keys` via the trait. Cloning happens in the
// bulk methods because the trait returns owned `Vec`s (so HTTP-backed providers
// don't need to retain state across calls).

impl super::provider::KeyProvider for KeyDb {
    fn device_keys(&self) -> Vec<DeviceKey> {
        self.device_keys.clone()
    }
    fn processing_keys(&self) -> Vec<[u8; 16]> {
        self.processing_keys.clone()
    }
    fn media_keys(&self) -> Vec<[u8; 16]> {
        // Every per-disc Media Key in the db. The resolver dedups; MKs are
        // MKB-scoped so the same value recurs across a pressing's discs.
        self.iter_disc_entries()
            .filter_map(|e| e.media_key)
            .collect()
    }
    fn host_certs(&self) -> Vec<HostCert> {
        self.host_certs.clone()
    }
    fn lookup_disc_by_hash(&self, disc_hash: &[u8; 20]) -> Option<DiscEntry> {
        let mut hex = String::with_capacity(42);
        hex.push_str("0x");
        for b in disc_hash {
            hex.push_str(&format!("{b:02X}"));
        }
        self.find_disc(&hex).cloned()
    }
    fn lookup_disc_by_vid(&self, volume_id: &[u8; 16]) -> Option<DiscEntry> {
        self.iter_disc_entries()
            .find(|e| matches!(e.disc_id, Some(id) if &id == volume_id))
            .cloned()
    }
}

// ── Private parsers (re-open the inherent impl) ─────────────────────────────

impl KeyDb {
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

    /// Parse an orphan DK row: a `| DK |` line carrying only the
    /// `DEVICE_KEY` field (no position metadata). The key is then
    /// treated like a terminal/unpositioned label by the resolver
    /// (Path 2's brute walker). Returns `None` if the line carries
    /// any position field — those are positioned DKs and parsed by
    /// [`Self::parse_device_key`] instead.
    fn parse_orphan_dk(line: &str) -> Option<[u8; 16]> {
        if line.contains("DEVICE_NODE")
            || line.contains("KEY_UV")
            || line.contains("KEY_U_MASK_SHIFT")
        {
            return None;
        }
        let key_str = line
            .split("DEVICE_KEY")
            .nth(1)?
            .split('|')
            .next()?
            .split(';')
            .next()?
            .trim();
        parse_hex16(key_str)
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
        let line = r#"0x000102030405060708090A0B0C0D0E0F10111213 = SAMPLE_FILM (Sample Film) | D | 2024-01-01 | M | 0x000102030405060708090A0B0C0D0E0F | I | 0x101112131415161718191A1B1C1D1E1F | V | 0x202122232425262728292A2B2C2D2E2F | U | 1-0x303132333435363738393A3B3C3D3E3F ; MKBv77"#;
        let entry = KeyDb::parse_disc_entry(line).unwrap();
        assert_eq!(entry.title, "Sample Film");
        assert!(entry.media_key.is_some());
        assert!(entry.vuk.is_some());
        assert_eq!(entry.unit_keys.len(), 1);
        assert_eq!(entry.unit_keys[0].0, 1);
    }

    #[test]
    fn test_parse_device_key() {
        let line = "| DK | DEVICE_KEY 0x000102030405060708090A0B0C0D0E0F | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17 ; MKBv01-MKBv48";
        let dk = KeyDb::parse_device_key(line).unwrap();
        assert_eq!(dk.node, 0x0800);
        assert_eq!(dk.u_mask_shift, 0x17);
    }

    #[test]
    fn test_orphan_dk_row_loads_into_processing_keys() {
        // `| DK |` row without position fields = an orphan DK. Per the
        // unified model the resolver treats it like a terminal/PK
        // candidate: it lands in `processing_keys` and the brute walker
        // handles it.
        let cfg = r#"
| DK | DEVICE_KEY 0xDEADBEEF0001020304050607080900AA ; orphan, no position fields
| DK | DEVICE_KEY 0x000102030405060708090A0B0C0D0E0F | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17 ; positioned MKBv01-MKBv48
| PK | 0xCAFEBABE0001020304050607080900BB ; legacy PK row still works
"#;
        let db = KeyDb::parse(cfg);
        assert_eq!(
            db.device_keys.len(),
            1,
            "positioned DK row should land in device_keys"
        );
        // Orphan DK + legacy PK row both end up in processing_keys.
        assert_eq!(
            db.processing_keys.len(),
            2,
            "orphan DK row + legacy PK row both belong in processing_keys"
        );
        assert_eq!(db.processing_keys[0][..4], [0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(db.processing_keys[1][..4], [0xCA, 0xFE, 0xBA, 0xBE]);
    }

    #[test]
    fn test_parse_orphan_dk_rejects_lines_with_position_fields() {
        // The parser must NOT pick up a positioned DK row as an orphan
        // (that would double-count). parse_orphan_dk explicitly checks.
        let positioned = "| DK | DEVICE_KEY 0x000102030405060708090A0B0C0D0E0F | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17";
        assert!(
            KeyDb::parse_orphan_dk(positioned).is_none(),
            "positioned DK must not match orphan parser"
        );
        let orphan = "| DK | DEVICE_KEY 0xDEADBEEF0001020304050607080900AA";
        let key = KeyDb::parse_orphan_dk(orphan).expect("orphan should parse");
        assert_eq!(key[..4], [0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_parse_host_cert() {
        let line = "| HC | HOST_PRIV_KEY 0xDEADBEEF000102030405060708090A0B0C0D0E0F | HOST_CERT 0x000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4F505152535455565758595A5B ; Revoked";
        let hc = KeyDb::parse_host_cert(line).unwrap();
        assert_eq!(hc.private_key[0], 0xDE);
        assert_eq!(hc.certificate.len(), 92);
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

        // Look up any disc entry carrying a full key set.
        let entry = db
            .disc_entries
            .values()
            .find(|e| e.vuk.is_some() && e.media_key.is_some() && !e.unit_keys.is_empty())
            .expect("no disc entry with a full key set");
        assert!(entry.media_key.is_some());
        assert!(entry.vuk.is_some());
        assert!(!entry.unit_keys.is_empty());

        eprintln!(
            "Parsed {} disc entries, {} DK, {} PK",
            db.disc_entries.len(),
            db.device_keys.len(),
            db.processing_keys.len()
        );
    }
}
