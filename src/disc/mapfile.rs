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
//! The mapfile is flushed to disk on every `record()` call so a crashed
//! rip loses at most one block of recorded state.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

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
}

/// Write-through mapfile. Every `record()` persists to disk immediately
/// so a crash during rip loses at most one block.
pub struct Mapfile {
    path: PathBuf,
    entries: Vec<MapEntry>,
    total_size: u64,
    version: String,
    /// Incrementally maintained stats — updated on every `record()` call
    /// so `stats()` is O(1) instead of O(n).
    stats: MapStats,
}

impl Mapfile {
    /// Create a new mapfile with one `NonTried` region covering the whole disc.
    /// Writes to disk immediately so a resume can pick up even if the caller
    /// never records anything.
    pub fn create(path: &Path, total_size: u64, version: &str) -> io::Result<Self> {
        let mf = Self {
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
        };
        mf.write_to_disk()?;
        Ok(mf)
    }

    /// Load an existing mapfile from disk.
    pub fn load(path: &Path) -> io::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut entries = Vec::new();
        let mut saw_current_line = false;
        let mut version = String::from("unknown");
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
        self.write_to_disk()?;
        Ok(())
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
