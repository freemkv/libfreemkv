//! ddrescue-compatible mapfile for tracking rip progress.
//!
//! Records which byte ranges of a disc image are good, unreadable,
//! or not-yet-attempted. Written as plain text so it's greppable,
//! human-editable, and interoperates with ddrescue's own tools.
//!
//! Format:
//! ```text
//! # Rescue Logfile. Created by libfreemkv v0.11.21
//! # Current pos / status / pass / pass_time (ddrescue state machine — we only populate pos)
//! 0x000000000  ?  1  0
//! #      pos        size  status
//! 0x000000000  0x12345678    +
//! 0x012345678  0x00001000    -
//! 0x012346678  0x01234500    ?
//! ```
//!
//! Status chars: `?` non-tried · `*` non-trimmed · `/` non-scraped · `-` unreadable · `+` finished.
//!
//! The mapfile is flushed to disk at most once per `FLUSH_INTERVAL`
//! during `record()` calls, plus on explicit `flush()` and on `Drop`.
//! This bounds atomic-rename RPC rate on networked staging (e.g. NFS)
//! where per-record persists otherwise serialize the rip pipeline.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Minimum interval between mapfile persists. `record()` updates in-memory
/// state every call but only writes to disk when this interval has elapsed
/// since the last persist (or when `flush()` is called explicitly, or on
/// `Drop`). Bounds RPC rate on NFS staging where atomic-rename per record
/// otherwise dominates throughput. On crash the worst-case progress loss
/// is one interval's worth of records.
const FLUSH_INTERVAL: Duration = Duration::from_millis(1000);

/// Status of a byte range in the mapfile. ddrescue-compatible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectorStatus {
    /// `?` — not yet attempted. Initial state for a fresh mapfile.
    NonTried,
    /// `*` — fast-pass read failed; edges need trimming.
    NonTrimmed,
    /// `/` — trimmed; interior needs sector scrape.
    NonScraped,
    /// `-` — drive couldn't read it this session.
    Unreadable,
    /// `+` — good.
    Finished,
}

impl SectorStatus {
    pub fn to_char(self) -> char {
        match self {
            Self::NonTried => '?',
            Self::NonTrimmed => '*',
            Self::NonScraped => '/',
            Self::Unreadable => '-',
            Self::Finished => '+',
        }
    }
    pub fn from_char(c: char) -> Option<Self> {
        Some(match c {
            '?' => Self::NonTried,
            '*' => Self::NonTrimmed,
            '/' => Self::NonScraped,
            '-' => Self::Unreadable,
            '+' => Self::Finished,
            _ => return None,
        })
    }
}

/// One contiguous range of bytes with a status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapEntry {
    pub pos: u64,
    pub size: u64,
    pub status: SectorStatus,
}

/// Summary statistics over all entries.
///
/// `bytes_pending` aggregates `NonTried + NonTrimmed + NonScraped` for
/// back-compat. `bytes_nontried` and `bytes_retryable` (= NonTrimmed +
/// NonScraped) split that aggregate so UIs can distinguish *unread*
/// territory (still ahead of Pass 1's read head) from *needs-retry*
/// territory (Pass 1 already encountered, queued for Pass 2-N).
#[derive(Debug, Clone, Copy, Default)]
pub struct MapStats {
    pub bytes_total: u64,
    pub bytes_good: u64,
    pub bytes_unreadable: u64,
    pub bytes_pending: u64,
    /// Sectors Pass 1 hasn't reached yet (`NonTried`). Subset of
    /// `bytes_pending`.
    pub bytes_nontried: u64,
    /// Sectors flagged for Pass 2-N retry — `NonTrimmed` (multi-sector
    /// read failed; needs split) + `NonScraped` (small-block read
    /// partially recovered; remainder still pending). Subset of
    /// `bytes_pending`. This is the right signal for a "MAYBE / will
    /// retry" UI bucket; `bytes_pending` over-counts because it folds
    /// in `bytes_nontried`.
    pub bytes_retryable: u64,
    /// Number of unreadable ranges (for UI display). Computed from
    /// `ranges_with(&[Unreadable])`.
    pub num_bad_ranges: u32,
    /// Largest gap among unreadable ranges in milliseconds. Computed as
    /// largest range size / bytes_per_sec * 1000. Set by caller (autorip)
    /// since bytes_per_sec is application-specific.
    pub main_lost_ms: f64,
}

/// Time-batched mapfile. `record()` keeps in-memory state up-to-date on
/// every call; persists to disk at most once per `FLUSH_INTERVAL`.
/// Explicit `flush()` and `Drop` guarantee state is on disk after a sweep
/// or patch finishes. On hard crash the worst-case loss is one flush
/// interval of records — the file's payload bytes are unaffected.
pub struct Mapfile {
    path: PathBuf,
    entries: Vec<MapEntry>,
    total_size: u64,
    version: String,
    /// Incrementally maintained stats — updated on every `record()` call
    /// so `stats()` is O(1) instead of O(n).
    stats: MapStats,
    /// True when in-memory state has changed but `write_to_disk` has not
    /// yet captured it.
    dirty: bool,
    /// Wall-clock timestamp of the last successful `write_to_disk` (or
    /// the moment the mapfile was constructed, whichever is later).
    last_flushed: Instant,
    /// AACS Volume ID (16 bytes) for the disc, persisted as a
    /// `# freemkv-vid:` comment header so it survives to deferred-mux /
    /// resume without altering the ISO payload or breaking ddrescue
    /// data-line parsing. `None` for unencrypted / non-AACS discs.
    ///
    /// MUTUALLY EXCLUSIVE with `unit_keys`: a disc whose keys were resolved
    /// persists the keys (`unit_keys`) and NOT the VID — the keys are the final
    /// answer, so deferred-mux/resume decrypts directly with no key service. A
    /// disc that did NOT resolve persists only the VID, the retry-able "still
    /// need a key" marker (a future mux can re-ask the key service with it).
    vid: Option<[u8; 16]>,
    /// Decrypted AACS unit keys `(CPS unit, key)`, persisted as `# freemkv-uk:`
    /// comment headers when the disc was successfully keyed. Mutually exclusive
    /// with `vid` (see above). Empty when unresolved.
    unit_keys: Vec<(u32, [u8; 16])>,
}

impl Mapfile {
    /// Create a new mapfile with one `NonTried` region covering the whole disc.
    /// Writes to disk immediately so a resume can pick up even if the caller
    /// never records anything.
    pub fn create(path: &Path, total_size: u64, version: &str) -> io::Result<Self> {
        let mut mf = Self {
            path: path.to_path_buf(),
            entries: vec![MapEntry {
                pos: 0,
                size: total_size,
                status: SectorStatus::NonTried,
            }],
            total_size,
            version: version.to_string(),
            stats: MapStats {
                bytes_total: total_size,
                bytes_pending: total_size,
                bytes_nontried: total_size,
                ..Default::default()
            },
            dirty: false,
            last_flushed: Instant::now(),
            vid: None,
            unit_keys: Vec::new(),
        };
        // Eager initial persist so a resume can pick this up even if
        // `record()` is never called.
        mf.write_to_disk()?;
        mf.last_flushed = Instant::now();
        Ok(mf)
    }

    /// Load an existing mapfile from disk.
    pub fn load(path: &Path) -> io::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut entries = Vec::new();
        let mut saw_current_line = false;
        let mut version = String::from("unknown");
        let mut vid: Option<[u8; 16]> = None;
        let mut unit_keys: Vec<(u32, [u8; 16])> = Vec::new();
        for line in text.lines() {
            let t = line.trim();
            if t.is_empty() {
                continue;
            }
            if let Some(rest) = t.strip_prefix('#') {
                let rest = rest.trim();
                if let Some(v) = rest.strip_prefix("Rescue Logfile. Created by ") {
                    version = v.to_string();
                }
                if let Some(hex) = rest.strip_prefix("freemkv-vid:") {
                    // Best-effort: a malformed or short VID comment is
                    // ignored rather than failing the whole load.
                    vid = parse_vid_hex(hex.trim());
                }
                if let Some(uk) = rest.strip_prefix("freemkv-uk:") {
                    // `<cps>:<32hex>`. Best-effort: a malformed line is skipped.
                    if let Some(entry) = parse_uk_line(uk.trim()) {
                        unit_keys.push(entry);
                    }
                }
                continue;
            }
            // First non-comment line is the "current" state line (pos status [pass] [pass_time]).
            // We ignore its contents but skip over it.
            if !saw_current_line {
                saw_current_line = true;
                // But if the line looks like an entry (has at least 3 fields starting 0x...),
                // it's probably actually an entry for a mapfile we wrote without a current line.
                // Heuristic: current line has status char as 2nd field; entry has size as 2nd field.
                let fields: Vec<&str> = t.split_whitespace().collect();
                if fields.len() >= 3 && fields[1].starts_with("0x") {
                    // It's an entry, not a current line — fall through to entry parse.
                } else {
                    continue;
                }
            }
            // Entry: `pos size statuschar`
            let fields: Vec<&str> = t.split_whitespace().collect();
            if fields.len() < 3 {
                continue;
            }
            let pos = parse_hex(fields[0])?;
            let size = parse_hex(fields[1])?;
            let status = fields[2]
                .chars()
                .next()
                .and_then(SectorStatus::from_char)
                .ok_or_else(|| {
                    // No English text — the variant carries a stable
                    // language-neutral kind identifier (`status_char`).
                    let e: io::Error = crate::error::Error::MapfileInvalid {
                        kind: "status_char",
                    }
                    .into();
                    e
                })?;
            entries.push(MapEntry { pos, size, status });
        }
        entries.sort_by_key(|e| e.pos);
        let total_size = entries.last().map(|e| e.pos + e.size).unwrap_or(0);
        let stats = Self::compute_stats(&entries, total_size);
        Ok(Self {
            path: path.to_path_buf(),
            entries,
            total_size,
            version,
            stats,
            dirty: false,
            last_flushed: Instant::now(),
            vid,
            unit_keys,
        })
    }

    /// Load if the file exists, otherwise create a fresh mapfile.
    pub fn open_or_create(path: &Path, total_size: u64, version: &str) -> io::Result<Self> {
        match Self::load(path) {
            Ok(mf) => Ok(mf),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                Self::create(path, total_size, version)
            }
            Err(e) => Err(e),
        }
    }

    /// Mark a byte range as having the given status. Splits any overlapping
    /// existing entries, merges with adjacent same-status entries, and flushes
    /// to disk.
    pub fn record(&mut self, pos: u64, size: u64, status: SectorStatus) -> io::Result<()> {
        if size == 0 {
            return Ok(());
        }
        let end = pos.saturating_add(size);
        let mut new_entries = Vec::with_capacity(self.entries.len() + 2);

        for e in self.entries.drain(..) {
            let e_end = e.pos + e.size;
            if e_end <= pos || e.pos >= end {
                // entirely before or after — keep
                new_entries.push(e);
                continue;
            }
            // Overlap — keep portions outside [pos, end)
            if e.pos < pos {
                new_entries.push(MapEntry {
                    pos: e.pos,
                    size: pos - e.pos,
                    status: e.status,
                });
            }
            if e_end > end {
                new_entries.push(MapEntry {
                    pos: end,
                    size: e_end - end,
                    status: e.status,
                });
            }
        }
        new_entries.push(MapEntry { pos, size, status });
        new_entries.sort_by_key(|e| e.pos);

        // Coalesce adjacent same-status entries.
        let mut merged: Vec<MapEntry> = Vec::with_capacity(new_entries.len());
        for e in new_entries {
            if let Some(last) = merged.last_mut() {
                if last.pos + last.size == e.pos && last.status == e.status {
                    last.size += e.size;
                    continue;
                }
            }
            merged.push(e);
        }

        // Recompute stats from merged entries. record() is already O(n) due to
        // drain-and-rebuild, so this is a constant-factor overhead. The critical
        // win is that stats() is now O(1) — called millions of times in the hot
        // path during sweep/patch, it just returns the cached value.
        self.stats = Self::compute_stats(&merged, self.total_size);
        self.entries = merged;
        self.dirty = true;
        if self.last_flushed.elapsed() >= FLUSH_INTERVAL {
            self.write_to_disk()?;
            self.dirty = false;
            self.last_flushed = Instant::now();
        }
        Ok(())
    }

    /// Persist any pending in-memory changes to disk. No-op if clean.
    /// Callers (sweep/patch finalisation) invoke this after their last
    /// `record()` to guarantee state is durable before returning.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.dirty {
            self.write_to_disk()?;
            self.dirty = false;
            self.last_flushed = Instant::now();
        }
        Ok(())
    }

    /// Record the disc's 16-byte AACS Volume ID so it persists in the
    /// mapfile's comment header. Marks the mapfile dirty; the next
    /// `flush()` / `Drop` writes the `# freemkv-vid:` line. Does not
    /// touch the ISO payload or the ddrescue data lines.
    pub fn set_vid(&mut self, vid: [u8; 16]) {
        self.vid = Some(vid);
        self.dirty = true;
    }

    /// The disc's AACS Volume ID, if one was set or parsed from a
    /// `# freemkv-vid:` comment on load. `None` for unencrypted /
    /// non-AACS discs.
    pub fn vid(&self) -> Option<[u8; 16]> {
        self.vid
    }

    /// Record the disc's decrypted AACS unit keys so they persist in the
    /// mapfile header (`# freemkv-uk:` lines). The KEYED state: a deferred-mux /
    /// resume decrypts directly from these with no key-service round-trip.
    /// Setting keys clears any VID — the mapfile holds keys XOR VID, never both
    /// (keys are the final answer; VID is only the "still unresolved" marker).
    pub fn set_unit_keys(&mut self, keys: &[(u32, [u8; 16])]) {
        self.unit_keys = keys.to_vec();
        if !self.unit_keys.is_empty() {
            self.vid = None;
        }
        self.dirty = true;
    }

    /// The disc's decrypted AACS unit keys, if the disc was keyed (parsed from
    /// `# freemkv-uk:` comments on load). Empty = unresolved (check `vid()`).
    pub fn unit_keys(&self) -> &[(u32, [u8; 16])] {
        &self.unit_keys
    }

    pub fn entries(&self) -> &[MapEntry] {
        &self.entries
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// First range with a given status starting at or after `from`.
    pub fn next_with(&self, from: u64, status: SectorStatus) -> Option<(u64, u64)> {
        for e in &self.entries {
            if e.status != status {
                continue;
            }
            let e_end = e.pos + e.size;
            if e_end <= from {
                continue;
            }
            let start = e.pos.max(from);
            return Some((start, e_end - start));
        }
        None
    }

    /// All ranges matching one of the given statuses, in position order.
    pub fn ranges_with(&self, statuses: &[SectorStatus]) -> Vec<(u64, u64)> {
        self.entries
            .iter()
            .filter(|e| statuses.contains(&e.status))
            .map(|e| (e.pos, e.size))
            .collect()
    }

    pub fn stats(&self) -> MapStats {
        self.stats
    }

    fn compute_stats(entries: &[MapEntry], total_size: u64) -> MapStats {
        let mut s = MapStats {
            bytes_total: total_size,
            ..Default::default()
        };
        for e in entries {
            match e.status {
                SectorStatus::Finished => s.bytes_good += e.size,
                SectorStatus::Unreadable => s.bytes_unreadable += e.size,
                SectorStatus::NonTried => {
                    s.bytes_pending += e.size;
                    s.bytes_nontried += e.size;
                }
                SectorStatus::NonTrimmed | SectorStatus::NonScraped => {
                    s.bytes_pending += e.size;
                    s.bytes_retryable += e.size;
                }
            }
        }
        s
    }

    fn write_to_disk(&self) -> io::Result<()> {
        // Write to a tempfile then rename for atomicity. Appending ".tmp"
        // rather than `with_extension` so we don't clobber the original
        // extension (which may already be ".mapfile").
        let tmp = {
            let mut s = self.path.clone().into_os_string();
            s.push(".tmp");
            PathBuf::from(s)
        };
        {
            let file = std::fs::File::create(&tmp)?;
            let mut w = std::io::BufWriter::new(file);
            writeln!(w, "# Rescue Logfile. Created by {}", self.version)?;
            // VID comment lives in the header block. ddrescue treats any
            // `#`-prefixed line as a comment, so this round-trips through
            // our `load()` without affecting the `pos size status` data
            // parser. 16 bytes → 32 lowercase hex chars.
            // KEYS XOR VID: a keyed disc persists its unit keys (the final
            // answer — deferred-mux decrypts directly); an unresolved disc
            // persists only the VID (the retry marker, so a future mux can
            // re-ask the key service). Never both.
            use std::fmt::Write as _;
            if !self.unit_keys.is_empty() {
                for (cps, key) in &self.unit_keys {
                    let mut hex = String::with_capacity(32);
                    for b in key {
                        let _ = write!(hex, "{b:02x}");
                    }
                    writeln!(w, "# freemkv-uk: {cps}:{hex}")?;
                }
            } else if let Some(vid) = self.vid {
                let mut hex = String::with_capacity(32);
                for b in vid {
                    let _ = write!(hex, "{b:02x}");
                }
                writeln!(w, "# freemkv-vid: {hex}")?;
            }
            writeln!(w, "# Current pos / status / pass / pass_time")?;
            writeln!(w, "0x000000000  ?  1  0")?;
            writeln!(w, "#      pos        size  status")?;
            for e in &self.entries {
                writeln!(
                    w,
                    "0x{:09x}  0x{:09x}    {}",
                    e.pos,
                    e.size,
                    e.status.to_char()
                )?;
            }
            w.flush()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

impl Drop for Mapfile {
    /// Best-effort flush on drop so a sweep / patch that returns early
    /// (or unwinds) doesn't lose its in-memory state. Errors here are
    /// swallowed because Drop has no way to surface them; explicit
    /// `flush()` on the success path gives callers proper error handling.
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Parse a 32-char lowercase/uppercase hex string into a 16-byte VID.
/// Returns `None` on any malformation (wrong length, non-hex) — the
/// caller treats a bad VID comment as simply absent rather than an
/// error, so a corrupt header never fails a mapfile load.
fn parse_vid_hex(s: &str) -> Option<[u8; 16]> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Parse a `# freemkv-uk:` value `<cps>:<32hex>` into `(cps_unit, key)`. Returns
/// `None` on any malformation so a corrupt line is ignored, never fatal.
fn parse_uk_line(s: &str) -> Option<(u32, [u8; 16])> {
    let (cps, hex) = s.split_once(':')?;
    let cps: u32 = cps.trim().parse().ok()?;
    let key = parse_vid_hex(hex.trim())?; // 32-hex → [u8; 16], shared parser
    Some((cps, key))
}

fn parse_hex(s: &str) -> io::Result<u64> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(s, 16).map_err(|_| {
        // Underlying ParseIntError dropped — its Display is OS-locale text.
        // The typed variant carries `kind = "hex"` which is stable.
        let e: io::Error = crate::error::Error::MapfileInvalid { kind: "hex" }.into();
        e
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpfile(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "libfreemkv-mapfile-test-{}-{}-{}.mapfile",
            std::process::id(),
            tag,
            n
        );
        std::env::temp_dir().join(name)
    }

    #[test]
    fn create_has_one_nontried_region() {
        let p = tmpfile("create_has_one_nontried_region");
        let _ = std::fs::remove_file(&p);
        let mf = Mapfile::create(&p, 1000, "test").unwrap();
        assert_eq!(mf.entries().len(), 1);
        assert_eq!(mf.entries()[0].pos, 0);
        assert_eq!(mf.entries()[0].size, 1000);
        assert_eq!(mf.entries()[0].status, SectorStatus::NonTried);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn record_splits_overlap() {
        let p = tmpfile("record_splits_overlap");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(200, 100, SectorStatus::Finished).unwrap();
        let es = mf.entries();
        assert_eq!(es.len(), 3);
        assert_eq!(
            (es[0].pos, es[0].size, es[0].status),
            (0, 200, SectorStatus::NonTried)
        );
        assert_eq!(
            (es[1].pos, es[1].size, es[1].status),
            (200, 100, SectorStatus::Finished)
        );
        assert_eq!(
            (es[2].pos, es[2].size, es[2].status),
            (300, 700, SectorStatus::NonTried)
        );
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn record_coalesces_adjacent_same_status() {
        let p = tmpfile("record_coalesces_adjacent_same_status");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(100, 100, SectorStatus::Finished).unwrap();
        mf.record(200, 100, SectorStatus::Finished).unwrap();
        // Entries: [0..100 NonTried, 100..300 Finished (merged), 300..1000 NonTried]
        let es = mf.entries();
        assert_eq!(es.len(), 3);
        assert_eq!(
            (es[1].pos, es[1].size, es[1].status),
            (100, 200, SectorStatus::Finished)
        );
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn record_replaces_existing_status() {
        let p = tmpfile("record_replaces_existing_status");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(200, 100, SectorStatus::Unreadable).unwrap();
        mf.record(200, 100, SectorStatus::Finished).unwrap();
        let es = mf.entries();
        // The overwrite should result in all finished at 200..300, NonTried elsewhere — 3 entries.
        assert_eq!(es.len(), 3);
        assert_eq!(es[1].status, SectorStatus::Finished);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn round_trip_load() {
        let p = tmpfile("round_trip_load");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(100, 200, SectorStatus::Finished).unwrap();
        mf.record(500, 100, SectorStatus::Unreadable).unwrap();
        // record() batches; explicit flush before reading back from disk.
        mf.flush().unwrap();
        let loaded = Mapfile::load(&p).unwrap();
        assert_eq!(loaded.entries(), mf.entries());
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn stats_sum_correctly() {
        let p = tmpfile("stats_sum_correctly");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(0, 400, SectorStatus::Finished).unwrap();
        mf.record(400, 100, SectorStatus::Unreadable).unwrap();
        let s = mf.stats();
        assert_eq!(s.bytes_good, 400);
        assert_eq!(s.bytes_unreadable, 100);
        assert_eq!(s.bytes_pending, 500);
        assert_eq!(s.bytes_total, 1000);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn ranges_with_filters() {
        let p = tmpfile("ranges_with_filters");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(100, 50, SectorStatus::Unreadable).unwrap();
        mf.record(300, 50, SectorStatus::Unreadable).unwrap();
        let bad = mf.ranges_with(&[SectorStatus::Unreadable]);
        assert_eq!(bad, vec![(100, 50), (300, 50)]);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn stats_consistent_after_overlapping_records() {
        let p = tmpfile("stats_consistent_after_overlapping");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        // Record some finished, some unreadable, some nontrimmed
        mf.record(0, 300, SectorStatus::Finished).unwrap();
        mf.record(300, 200, SectorStatus::NonTrimmed).unwrap();
        mf.record(500, 100, SectorStatus::Unreadable).unwrap();
        mf.record(600, 400, SectorStatus::Finished).unwrap();

        // Final entries: [0..300 Finished, 300..500 NonTrimmed, 500..600 Unreadable, 600..1000 Finished]
        let s = mf.stats();
        assert_eq!(s.bytes_good, 700); // 300 + 400
        assert_eq!(s.bytes_unreadable, 100); // 100
        assert_eq!(s.bytes_pending, 200); // NonTrimmed only (NonTried=0)
        assert_eq!(s.bytes_nontried, 0);
        assert_eq!(s.bytes_retryable, 200); // NonTrimmed
        assert_eq!(s.bytes_total, 1000);

        // Overwrite a NonTrimmed range with Finished
        mf.record(300, 100, SectorStatus::Finished).unwrap();
        // Entries: [0..400 Finished, 400..500 NonTrimmed, 500..600 Unreadable, 600..1000 Finished]
        let s2 = mf.stats();
        assert_eq!(s2.bytes_good, 800); // 400 + 400
        assert_eq!(s2.bytes_unreadable, 100);
        assert_eq!(s2.bytes_pending, 100); // NonTrimmed only
        assert_eq!(s2.bytes_retryable, 100);

        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn unit_keys_round_trip_and_are_mutually_exclusive_with_vid() {
        let p = tmpfile("uk_round_trips");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(0, 500, SectorStatus::Finished).unwrap();
        // Set a VID first, then unit keys: keys must WIN and clear the VID.
        mf.set_vid([0xAA; 16]);
        let keys: Vec<(u32, [u8; 16])> = vec![
            (
                0,
                [
                    0x57, 0x60, 0xcc, 0x83, 0x3d, 0x86, 0x0e, 0x48, 0x92, 0x1f, 0x88, 0x16, 0xe1,
                    0x35, 0x9b, 0xad,
                ],
            ),
            (1, [0x11; 16]),
        ];
        mf.set_unit_keys(&keys);
        assert_eq!(
            mf.vid(),
            None,
            "set_unit_keys must clear vid (keys XOR vid)"
        );
        mf.flush().unwrap();

        let text = std::fs::read_to_string(&p).unwrap();
        assert!(
            text.contains("# freemkv-uk: 0:5760cc833d860e48921f8816e1359bad"),
            "uk comment format mismatch: {text}"
        );
        assert!(
            text.contains("# freemkv-uk: 1:11111111111111111111111111111111"),
            "second uk missing: {text}"
        );
        assert!(
            !text.contains("# freemkv-vid:"),
            "VID must NOT be written when keys are present: {text}"
        );

        // load() recovers the unit keys (and no VID).
        let loaded = Mapfile::load(&p).unwrap();
        assert_eq!(loaded.unit_keys(), keys.as_slice());
        assert_eq!(loaded.vid(), None);
        assert_eq!(loaded.entries(), mf.entries());

        // VID-only path (no keys) still persists the VID as the retry marker.
        let p2 = tmpfile("uk_vid_only");
        let _ = std::fs::remove_file(&p2);
        let mut mf2 = Mapfile::create(&p2, 1000, "test").unwrap();
        mf2.set_vid([0xBB; 16]);
        mf2.flush().unwrap();
        let loaded2 = Mapfile::load(&p2).unwrap();
        assert_eq!(loaded2.vid(), Some([0xBB; 16]));
        assert!(loaded2.unit_keys().is_empty());
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn vid_round_trips_and_data_lines_unaffected() {
        let p = tmpfile("vid_round_trips");
        let _ = std::fs::remove_file(&p);

        // Build a mapfile with some data ranges, set a VID, persist.
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        mf.record(100, 200, SectorStatus::Finished).unwrap();
        mf.record(500, 100, SectorStatus::Unreadable).unwrap();
        mf.record(700, 50, SectorStatus::NonTrimmed).unwrap();
        let vid: [u8; 16] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        mf.set_vid(vid);
        mf.flush().unwrap();

        // The saved file must contain the VID comment in lowercase hex.
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(
            text.contains("# freemkv-vid:"),
            "saved mapfile missing VID comment: {text}"
        );
        assert!(
            text.contains("# freemkv-vid: 00112233445566778899aabbccddeeff"),
            "VID comment format mismatch: {text}"
        );

        // load() recovers the VID and the identical data ranges.
        let loaded = Mapfile::load(&p).unwrap();
        assert_eq!(loaded.vid(), Some(vid));
        assert_eq!(loaded.entries(), mf.entries());

        // A mapfile WITHOUT the VID comment must parse the same +/-/?
        // data ranges as the one WITH it (comment ignored by parser).
        let p2 = tmpfile("vid_round_trips_novid");
        let _ = std::fs::remove_file(&p2);
        let mut mf2 = Mapfile::create(&p2, 1000, "test").unwrap();
        mf2.record(100, 200, SectorStatus::Finished).unwrap();
        mf2.record(500, 100, SectorStatus::Unreadable).unwrap();
        mf2.record(700, 50, SectorStatus::NonTrimmed).unwrap();
        mf2.flush().unwrap();
        let loaded_novid = Mapfile::load(&p2).unwrap();
        assert_eq!(loaded_novid.vid(), None);
        assert_eq!(loaded_novid.entries(), loaded.entries());

        // Malformed VID comments must not error the load (treated absent).
        let mut bad = text.replace("00112233445566778899aabbccddeeff", "zzzz");
        let pbad = tmpfile("vid_round_trips_bad");
        let _ = std::fs::remove_file(&pbad);
        std::fs::write(&pbad, &bad).unwrap();
        let loaded_bad = Mapfile::load(&pbad).unwrap();
        assert_eq!(loaded_bad.vid(), None);
        assert_eq!(loaded_bad.entries(), loaded.entries());

        // A load->save cycle preserves the VID (the patch-pass path).
        bad.clear();
        let resaved = tmpfile("vid_round_trips_resave");
        let _ = std::fs::remove_file(&resaved);
        let mut reloaded = Mapfile::load(&p).unwrap();
        // Repoint at a fresh path and flush; mark dirty via a no-op record.
        reloaded.path = resaved.clone();
        reloaded.dirty = true;
        reloaded.flush().unwrap();
        let again = Mapfile::load(&resaved).unwrap();
        assert_eq!(again.vid(), Some(vid));

        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(&p2);
        let _ = std::fs::remove_file(&pbad);
        let _ = std::fs::remove_file(&resaved);
    }

    #[test]
    fn stats_consistent_after_split_record() {
        let p = tmpfile("stats_consistent_after_split");
        let _ = std::fs::remove_file(&p);
        let mut mf = Mapfile::create(&p, 1000, "test").unwrap();
        // Mark middle as NonTrimmed
        mf.record(200, 400, SectorStatus::NonTrimmed).unwrap();
        // Entries: [0..200 NonTried, 200..600 NonTrimmed, 600..1000 NonTried]
        let s = mf.stats();
        assert_eq!(s.bytes_pending, 1000); // NonTried(600) + NonTrimmed(400)
        assert_eq!(s.bytes_retryable, 400); // NonTrimmed only
        assert_eq!(s.bytes_nontried, 600); // 200 + 400

        // Overwrite the NonTrimmed with Finished (splitting the remaining NonTried)
        mf.record(200, 400, SectorStatus::Finished).unwrap();
        // Entries: [0..200 NonTried, 200..600 Finished, 600..1000 NonTried]
        let s2 = mf.stats();
        assert_eq!(s2.bytes_good, 400);
        assert_eq!(s2.bytes_pending, 600); // NonTried(200 + 400)
        assert_eq!(s2.bytes_nontried, 600);
        assert_eq!(s2.bytes_retryable, 0);

        let _ = std::fs::remove_file(&p);
    }
}
