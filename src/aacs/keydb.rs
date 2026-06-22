//! AACS Key Database parsing — KEYDB.cfg format.

use std::collections::HashMap;

/// Upper bound on the on-disk keydb.cfg size accepted by [`KeyDb::load`].
/// The real public UHD keydb is a few MiB; 64 MiB is generous headroom while
/// still bounding the worst-case allocation from a hostile/corrupt file.
const MAX_KEYDB_BYTES: u64 = 64 * 1024 * 1024;

/// Upper bound on parsed disc entries. The real public keydb carries
/// ~170k+ entries, so the cap sits well above that while still bounding
/// memory against a pathological input. Surplus lines are ignored.
const MAX_DISC_ENTRIES: usize = 500_000;

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
///
/// Operates on bytes, not `&str` char boundaries: the keydb is
/// third-party content, so a non-ASCII scalar (e.g. a 4-byte UTF-8
/// codepoint) must not panic on a mid-codepoint slice. Any non-hex
/// byte yields `None`.
pub(crate) fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
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

            // Host Certificate (AACS 2.0).
            //
            // An HC2 row normally augments the preceding HC (AACS 1.0) row.
            // KEYDB line ordering is third-party, so an HC2 row may appear
            // before any HC row; rather than silently dropping the AACS 2.0
            // credentials, carry them on a fresh HostCert with an empty v1
            // cert (the v1 private_key/certificate stay zero/empty and are
            // ignored by the v1 handshake, which guards on cert length).
            if line.starts_with("| HC2") {
                if let Some((pk, cert)) = Self::parse_host_cert_v2(line) {
                    if let Some(hc) = db.host_certs.last_mut() {
                        hc.private_key_v2 = Some(pk);
                        hc.certificate_v2 = Some(cert);
                    } else {
                        db.host_certs.push(HostCert {
                            private_key: [0u8; 20],
                            certificate: Vec::new(),
                            private_key_v2: Some(pk),
                            certificate_v2: Some(cert),
                        });
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
                if db.disc_entries.len() >= MAX_DISC_ENTRIES {
                    continue;
                }
                if let Some(entry) = Self::parse_disc_entry(line) {
                    db.disc_entries.insert(entry.disc_hash.clone(), entry);
                }
            }
        }

        db
    }

    /// Load a KEYDB.cfg from disk.
    ///
    /// A read failure (missing/unreadable file, non-UTF-8 content) surfaces
    /// as [`crate::error::Error::KeydbLoad`] carrying the path, per the
    /// library contract that a missing/unparseable keydb is a structured
    /// error and not a raw `io::Error`. Note that [`Self::parse`] itself is
    /// lenient: a syntactically valid but key-less file parses to an empty
    /// [`KeyDb`] rather than an error — callers needing a non-empty db must
    /// check the parsed contents.
    pub fn load(path: &std::path::Path) -> crate::error::Result<Self> {
        // Stat-and-cap before reading so a hostile/corrupt file can't force an
        // unbounded allocation. A file at or over the cap is rejected outright.
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.len() > MAX_KEYDB_BYTES {
                return Err(crate::error::Error::KeydbLoad {
                    path: path.display().to_string(),
                });
            }
        }
        let data = std::fs::read_to_string(path).map_err(|_| crate::error::Error::KeydbLoad {
            path: path.display().to_string(),
        })?;
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
        use std::fmt::Write;
        // Lowercase hex written straight into the pre-sized buffer: find_disc
        // lowercases its input anyway, so emitting 'x' here avoids a wasted
        // to_lowercase() round-trip, and write! avoids 20 temporary Strings.
        let mut hex = String::with_capacity(42);
        hex.push_str("0x");
        for b in disc_hash {
            let _ = write!(hex, "{b:02x}");
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

        let certificate = parse_hex(cert_str)?;
        // AACS 1.0 host certs are 92 bytes; drop malformed/short rows at
        // parse time so the handshake never attempts junk (mirrors the v2
        // path, which enforces >= 132).
        if certificate.len() < 92 {
            return None;
        }

        Some(HostCert {
            private_key: parse_hex20(priv_str)?,
            certificate,
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
        // Clean title: "TITLE_NAME (Display Title)" → use display title if
        // present. keydb.cfg is untrusted third-party content, so a title with
        // ')' before '(' (e.g. "FILM) (X") would make start+1 > end; guard the
        // slice and fall back to the whole title.
        let title = match (title_part.find('('), title_part.rfind(')')) {
            (Some(start), Some(end)) => title_part
                .get(start + 1..end)
                .map(str::to_string)
                .unwrap_or_else(|| title_part.to_string()),
            _ => title_part.to_string(),
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
        // All-zero placeholders — synthetic; no real key material in code.
        let z40 = "00".repeat(20);
        let z32 = "00".repeat(16);
        let line = format!(
            "0x{z40} = SAMPLE_FILM (Sample Film) | D | 2024-01-01 | M | 0x{z32} | I | 0x{z32} | V | 0x{z32} | U | 1-0x{z32} ; MKBv77"
        );
        let entry = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(entry.title, "Sample Film");
        assert!(entry.media_key.is_some());
        assert!(entry.vuk.is_some());
        assert_eq!(entry.unit_keys.len(), 1);
        assert_eq!(entry.unit_keys[0].0, 1);
    }

    // NOTE: key fields below use obvious repeated-byte / zero placeholders
    // (0x01.., 0x02.., 0x03.., 0x00..). NEVER put real — or real-looking — host,
    // device, or processing key material in code; these tests exercise the
    // parser's field-splitting only, not any genuine key.

    #[test]
    fn test_parse_device_key() {
        let line = "| DK | DEVICE_KEY 0x00000000000000000000000000000000 | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17 ; MKBv01-MKBv48";
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
| DK | DEVICE_KEY 0x01010101010101010101010101010101 ; orphan, no position fields
| DK | DEVICE_KEY 0x02020202020202020202020202020202 | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17 ; positioned MKBv01-MKBv48
| PK | 0x03030303030303030303030303030303 ; legacy PK row still works
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
        assert_eq!(db.processing_keys[0][..4], [0x01, 0x01, 0x01, 0x01]);
        assert_eq!(db.processing_keys[1][..4], [0x03, 0x03, 0x03, 0x03]);
    }

    #[test]
    fn test_parse_orphan_dk_rejects_lines_with_position_fields() {
        // The parser must NOT pick up a positioned DK row as an orphan
        // (that would double-count). parse_orphan_dk explicitly checks.
        let positioned = "| DK | DEVICE_KEY 0x02020202020202020202020202020202 | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17";
        assert!(
            KeyDb::parse_orphan_dk(positioned).is_none(),
            "positioned DK must not match orphan parser"
        );
        let orphan = "| DK | DEVICE_KEY 0x01010101010101010101010101010101";
        let key = KeyDb::parse_orphan_dk(orphan).expect("orphan should parse");
        assert_eq!(key[..4], [0x01, 0x01, 0x01, 0x01]);
    }

    #[test]
    fn test_parse_host_cert() {
        // 20-byte priv + 92-byte cert, all zeros — placeholders, not a key.
        let line = format!(
            "| HC | HOST_PRIV_KEY 0x{} | HOST_CERT 0x{} ; Revoked",
            "00".repeat(20),
            "00".repeat(92)
        );
        let hc = KeyDb::parse_host_cert(&line).unwrap();
        assert_eq!(hc.private_key, [0u8; 20]);
        assert_eq!(hc.certificate.len(), 92);
    }

    #[test]
    fn test_parse_hex_rejects_non_ascii_without_panic() {
        // A 4-byte UTF-8 scalar has byte-len 4 (passes the even check); the
        // old &str-slice path panicked on the mid-codepoint boundary. The
        // byte-wise parser must instead return None.
        assert!(parse_hex("😀").is_none());
        // Mixed: leading hex then a 2-byte UTF-8 scalar (byte-len even).
        assert!(parse_hex("ABé").is_none());
        // Sanity: well-formed hex still parses.
        assert_eq!(parse_hex("0x00FF"), Some(vec![0x00, 0xFF]));
        // Odd byte length still rejected.
        assert!(parse_hex("ABC").is_none());
    }

    #[test]
    fn test_hc2_before_hc_is_not_dropped() {
        // An HC2 row appearing before any HC row must still land its AACS 2.0
        // credentials on a HostCert rather than being silently discarded.
        let cfg = format!(
            "| HC2 | HOST_PRIV_KEY 0x{} | HOST_CERT 0x{}\n",
            "00".repeat(32),
            "00".repeat(132)
        );
        let db = KeyDb::parse(&cfg);
        assert_eq!(
            db.host_certs.len(),
            1,
            "HC2-only row must create a HostCert"
        );
        assert!(db.host_certs[0].private_key_v2.is_some());
        assert!(db.host_certs[0].certificate_v2.is_some());
        assert!(
            db.host_certs[0].certificate.is_empty(),
            "v1 cert stays empty for an HC2-only carrier"
        );
    }

    #[test]
    fn test_hc2_after_hc_augments_existing() {
        let cfg = format!(
            "| HC | HOST_PRIV_KEY 0x{} | HOST_CERT 0x{}\n| HC2 | HOST_PRIV_KEY 0x{} | HOST_CERT 0x{}\n",
            "00".repeat(20),
            "00".repeat(92),
            "00".repeat(32),
            "00".repeat(132)
        );
        let db = KeyDb::parse(&cfg);
        assert_eq!(db.host_certs.len(), 1, "HC2 augments the preceding HC");
        assert_eq!(db.host_certs[0].certificate.len(), 92);
        assert!(db.host_certs[0].certificate_v2.is_some());
    }

    #[test]
    fn test_parse_host_cert_rejects_short_v1_cert() {
        // A too-short AACS 1.0 cert must be dropped at parse time.
        let line = format!(
            "| HC | HOST_PRIV_KEY 0x{} | HOST_CERT 0x{}",
            "00".repeat(20),
            "00".repeat(10)
        );
        assert!(KeyDb::parse_host_cert(&line).is_none());
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

    // ════════════════════════════════════════════════════════════════════
    // Hardening additions
    // ════════════════════════════════════════════════════════════════════

    use super::super::provider::KeyProvider;

    // ── parse_hex / parse_hex16 / parse_hex20 ──────────────────────────────

    #[test]
    fn parse_hex_strips_lower_and_upper_prefixes() {
        // Both lower- and upper-case prefixes are stripped (trim_start_matches
        // "0x" then "0X"). Without one of those strips a value would be off by
        // a nibble or fail length checks.
        assert_eq!(parse_hex("0xABCD"), Some(vec![0xAB, 0xCD]));
        assert_eq!(parse_hex("0XABCD"), Some(vec![0xAB, 0xCD]));
        assert_eq!(parse_hex("ABCD"), Some(vec![0xAB, 0xCD]));
    }

    #[test]
    fn parse_hex_mixed_case_nibbles() {
        // to_digit(16) accepts both cases.
        assert_eq!(parse_hex("aB"), Some(vec![0xAB]));
        assert_eq!(parse_hex("Ff00"), Some(vec![0xFF, 0x00]));
    }

    #[test]
    fn parse_hex_rejects_non_hex_digit() {
        // 'G' is not a hex digit → None (not silently 0).
        assert!(parse_hex("0xGG").is_none());
        assert!(parse_hex("12ZZ").is_none());
    }

    #[test]
    fn parse_hex_empty_is_empty_vec() {
        // Empty (or bare "0x") → Some(empty): even byte-length 0 passes, and
        // there are no nibbles to reject. parse_hex16/20 then reject on length.
        assert_eq!(parse_hex(""), Some(vec![]));
        assert_eq!(parse_hex("0x"), Some(vec![]));
    }

    #[test]
    fn parse_hex16_enforces_exactly_16_bytes() {
        assert!(parse_hex16(&format!("0x{}", "00".repeat(15))).is_none());
        assert!(parse_hex16(&format!("0x{}", "00".repeat(17))).is_none());
        assert_eq!(
            parse_hex16(&format!("0x{}", "00".repeat(16))),
            Some([0u8; 16])
        );
    }

    #[test]
    fn parse_hex20_enforces_exactly_20_bytes() {
        assert!(parse_hex20(&format!("0x{}", "00".repeat(19))).is_none());
        assert_eq!(
            parse_hex20(&format!("0x{}", "11".repeat(20))),
            Some([0x11u8; 20])
        );
    }

    // ── Disc entry field parsing ───────────────────────────────────────────

    #[test]
    fn disc_entry_hash_is_lowercased() {
        // The disc_hash key is lowercased so HashMap lookups are
        // case-insensitive (find_disc lowercases its query too).
        let z32 = "00".repeat(16);
        let line = format!("0xABCDEF = T | M | 0x{z32}");
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.disc_hash, "0xabcdef");
    }

    #[test]
    fn disc_entry_title_uses_display_in_parens() {
        // "RAW_NAME (Display Name)" → title is the parenthesised display name.
        let line = "0x00 = RAW_NAME (Display Name) | M | 0x".to_string() + &"00".repeat(16);
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.title, "Display Name");
    }

    #[test]
    fn disc_entry_title_without_parens_uses_whole() {
        let line = "0x00 = PlainTitle | M | 0x".to_string() + &"00".repeat(16);
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.title, "PlainTitle");
    }

    #[test]
    fn disc_entry_malformed_parens_falls_back_to_whole_title() {
        // ')' before '(' would make start+1 > end; the guarded get() returns
        // None and the parser falls back to the whole title (no panic).
        let line = "0x00 = FILM) (X | M | 0x".to_string() + &"00".repeat(16);
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.title, "FILM) (X");
    }

    #[test]
    fn disc_entry_parses_all_tagged_fields() {
        // M, I, V, U each populate their field. U accepts "n-0xKEY".
        let m = "11".repeat(16);
        let i = "22".repeat(16);
        let v = "33".repeat(16);
        let u = "44".repeat(16);
        let line = format!("0xAA = T | M | 0x{m} | I | 0x{i} | V | 0x{v} | U | 2-0x{u}");
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.media_key, Some([0x11u8; 16]));
        assert_eq!(e.disc_id, Some([0x22u8; 16]));
        assert_eq!(e.vuk, Some([0x33u8; 16]));
        assert_eq!(e.unit_keys, vec![(2, [0x44u8; 16])]);
    }

    #[test]
    fn disc_entry_multiple_unit_keys_space_separated() {
        // The U field carries space-separated "n-0xKEY" pairs.
        let k1 = "01".repeat(16);
        let k2 = "02".repeat(16);
        let line = format!("0xAA = T | U | 1-0x{k1} 2-0x{k2}");
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.unit_keys, vec![(1, [0x01u8; 16]), (2, [0x02u8; 16])]);
    }

    #[test]
    fn disc_entry_unit_key_strips_trailing_comment() {
        // "U | 1-0xKEY ; comment" — the ';' comment must be stripped before
        // splitting unit keys.
        let k = "05".repeat(16);
        let line = format!("0xAA = T | U | 1-0x{k} ; MKBv77 note");
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.unit_keys, vec![(1, [0x05u8; 16])]);
    }

    #[test]
    fn disc_entry_skips_unparseable_unit_key_pair() {
        // A bad nibble in one unit key drops just that pair (parse_hex16 →
        // None), keeping the valid ones — no panic, no half-garbage key.
        let good = "07".repeat(16);
        let line = format!("0xAA = T | U | 1-0xZZ 2-0x{good}");
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert_eq!(e.unit_keys, vec![(2, [0x07u8; 16])]);
    }

    #[test]
    fn disc_entry_field_with_short_hex_is_none_not_panic() {
        // A 30-hex-char (15-byte) M value fails parse_hex16 → media_key None.
        let short = "00".repeat(15);
        let line = format!("0xAA = T | M | 0x{short}");
        let e = KeyDb::parse_disc_entry(&line).unwrap();
        assert!(e.media_key.is_none());
    }

    // ── find_disc / find_vuk: prefix-agnostic lookup ───────────────────────

    #[test]
    fn find_disc_matches_with_and_without_0x_and_case() {
        let v = "33".repeat(16);
        let line = format!("0xABCDEF = T | V | 0x{v}");
        let db = KeyDb::parse(&line);
        // Stored key is "0xabcdef". Query in several shapes.
        assert!(db.find_disc("0xABCDEF").is_some());
        assert!(db.find_disc("ABCDEF").is_some()); // no prefix
        assert!(db.find_disc("0xabcdef").is_some());
        assert!(db.find_disc("  0xAbCdEf  ").is_some()); // padded + mixed case
        assert_eq!(db.find_vuk("ABCDEF"), Some([0x33u8; 16]));
        assert!(db.find_disc("0xDEADBE").is_none());
    }

    // ── KeyProvider impl over KeyDb ────────────────────────────────────────

    #[test]
    fn provider_lookup_by_hash_formats_lowercase_hex() {
        // lookup_disc_by_hash writes the 20-byte hash as lowercase hex with a
        // 0x prefix; it must hit an entry keyed that way.
        let hash = [
            0x00u8, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF, 0x01, 0x02, 0x03, 0x04,
        ];
        let hex = format!(
            "0x{}",
            hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
        let mut db = KeyDb::empty();
        db.disc_entries.insert(
            hex.clone(),
            DiscEntry {
                disc_hash: hex,
                title: "t".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some([0x9u8; 16]),
                unit_keys: Vec::new(),
            },
        );
        let found = db.lookup_disc_by_hash(&hash).expect("hash lookup hit");
        assert_eq!(found.vuk, Some([0x9u8; 16]));
        // A different hash misses.
        assert!(db.lookup_disc_by_hash(&[0xFFu8; 20]).is_none());
    }

    #[test]
    fn provider_lookup_by_vid_matches_disc_id() {
        let vid = [0x42u8; 16];
        let mut db = KeyDb::empty();
        db.disc_entries.insert(
            "0xa".to_string(),
            DiscEntry {
                disc_hash: "0xa".to_string(),
                title: "t".to_string(),
                media_key: Some([1u8; 16]),
                disc_id: Some(vid),
                vuk: None,
                unit_keys: Vec::new(),
            },
        );
        assert!(db.lookup_disc_by_vid(&vid).is_some());
        assert!(db.lookup_disc_by_vid(&[0x00u8; 16]).is_none());
    }

    #[test]
    fn provider_media_keys_collects_every_per_disc_mk() {
        // media_keys() returns every entry's Some(media_key). MKs are
        // MKB-scoped, so the resolver dedups later; the provider returns all.
        let mut db = KeyDb::empty();
        for (i, mk) in [[0x1u8; 16], [0x2u8; 16]].iter().enumerate() {
            db.disc_entries.insert(
                format!("0x{i}"),
                DiscEntry {
                    disc_hash: format!("0x{i}"),
                    title: "t".to_string(),
                    media_key: Some(*mk),
                    disc_id: None,
                    vuk: None,
                    unit_keys: Vec::new(),
                },
            );
        }
        // An entry with no MK contributes nothing.
        db.disc_entries.insert(
            "0x9".to_string(),
            DiscEntry {
                disc_hash: "0x9".to_string(),
                title: "t".to_string(),
                media_key: None,
                disc_id: None,
                vuk: None,
                unit_keys: Vec::new(),
            },
        );
        let mut mks = db.media_keys();
        mks.sort();
        assert_eq!(mks, vec![[0x1u8; 16], [0x2u8; 16]]);
    }

    // ── Comments / blank lines / unknown lines ─────────────────────────────

    #[test]
    fn parse_ignores_comments_and_blank_lines() {
        let cfg = "\n; a comment\n# another\n   \n";
        let db = KeyDb::parse(cfg);
        assert!(db.device_keys.is_empty());
        assert!(db.processing_keys.is_empty());
        assert!(db.disc_entries.is_empty());
        assert!(db.host_certs.is_empty());
    }

    #[test]
    fn parse_empty_or_keyless_file_is_lenient_not_error() {
        // parse() never errors; a keyless file is an empty KeyDb (documented
        // contract — load() errors only on read failure, not empty content).
        let db = KeyDb::parse("; nothing here\n");
        assert_eq!(db.disc_entries.len(), 0);
    }

    #[test]
    fn parse_device_key_requires_all_four_fields() {
        // Missing KEY_U_MASK_SHIFT → parse_device_key returns None; with no
        // position fields at all it would be an orphan DK instead. Here the
        // line has DEVICE_NODE + KEY_UV but no shift → neither parser accepts
        // it as a positioned DK, and parse_orphan_dk rejects it (has position
        // fields), so nothing is loaded.
        let line = "| DK | DEVICE_KEY 0x00000000000000000000000000000000 | DEVICE_NODE 0x0800 | KEY_UV 0x00000400";
        assert!(KeyDb::parse_device_key(line).is_none());
        let db = KeyDb::parse(line);
        assert!(db.device_keys.is_empty());
        assert!(db.processing_keys.is_empty());
    }

    #[test]
    fn parse_host_cert_v2_rejects_wrong_priv_len_and_short_cert() {
        // v2 priv must be exactly 32 bytes; cert must be >= 132.
        let bad_priv = format!(
            "| HC2 | HOST_PRIV_KEY 0x{} | HOST_CERT 0x{}",
            "00".repeat(31),
            "00".repeat(132)
        );
        assert!(KeyDb::parse_host_cert_v2(&bad_priv).is_none());
        let short_cert = format!(
            "| HC2 | HOST_PRIV_KEY 0x{} | HOST_CERT 0x{}",
            "00".repeat(32),
            "00".repeat(131)
        );
        assert!(KeyDb::parse_host_cert_v2(&short_cert).is_none());
    }

    #[test]
    fn parse_processing_key_pk_row() {
        // "| PK | 0x..." → 16-byte processing key. A trailing comment is
        // stripped at ';'.
        let line = format!("| PK | 0x{} ; MKBv64", "AB".repeat(16));
        let pk = KeyDb::parse_processing_key(&line).unwrap();
        assert_eq!(pk, [0xABu8; 16]);
    }
}
