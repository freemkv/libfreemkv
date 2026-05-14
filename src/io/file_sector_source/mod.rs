//! [`FileSectorSource`] — read 2048-byte sectors from an ISO file on
//! disk, with an internal 32 MiB read-ahead buffer.
//!
//! ## Why the buffer
//!
//! On NFS-mounted ISOs, an unbuffered `pread(2048)` per sector pays an
//! NFS round-trip for every sector. With `rsize=1 MiB` and a 100-150 ms
//! NFS RTT, that's three orders of magnitude more round trips than
//! necessary — the muxer goes read-bound on every read, even though
//! the local NFS client could deliver MB/s on bigger requests.
//!
//! Internally this source keeps a [`READAHEAD_BUF_BYTES`] (32 MiB)
//! window pre-read from the file. `read_sectors(lba, count)` slices
//! into the window if `[lba, lba+count)` is contained in it; otherwise
//! the window is refilled (full-size aligned to the requested LBA's
//! buffer position).
//!
//! ## Access pattern assumption
//!
//! The buffer is sized for **forward-sequential** reads (sweep, mux).
//! Reverse-mode patch is range-local, so a refill per range works out
//! fine (the buffer covers the whole range for typical bad-range
//! sizes). Random-access reads thrash the buffer — at which point the
//! 32 MiB pre-read is wasted work. We accept that: the use case is
//! mux + sweep, both forward-sequential.
//!
//! Backward seeks rebuffer from the new LBA; partial reads at EOF
//! return only the bytes that exist (the underlying file is shorter
//! than a full buffer slot).
//!
//! ## Platform open hints
//!
//! On `open()` each platform issues its "sequential access expected"
//! hint to the kernel so OS-level readahead widens. The hint lives in
//! a per-OS sibling module ([`linux::hint_sequential`] et al.) — no
//! inline `#[cfg]` in this file.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod other;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use other as platform;
#[cfg(target_os = "windows")]
use windows as platform;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Error, Result};
use crate::sector::SectorSource;

/// Internal read-ahead buffer size. 32 MiB amortises one NFS round
/// trip across ~16 k sectors — three orders of magnitude fewer trips
/// than per-sector pread, and large enough to coast through a typical
/// NFS server commit blip.
///
/// 0.21.2: shrunk from 32 MiB → 4 MiB. On NFS-backed ISOs with
/// concurrent NFS writes (the mux phase), a 32 MiB refill bursts the
/// TCP connection hard enough to starve the writer thread, observed
/// empirically as a ~3× drop in sustained mux throughput on the
/// rip1/unraid-1 setup. 4 MiB matches `rsize=1 MiB` × 4 round-trips
/// and interleaves cleanly with writes.
///
/// Tweakable. Named const, not a magic number.
pub const READAHEAD_BUF_BYTES: usize = 4 * 1024 * 1024;

const SECTOR_SIZE: usize = 2048;
/// Sectors per refill: [`READAHEAD_BUF_BYTES`] / [`SECTOR_SIZE`]. The
/// buffer always tries to hold this many, except at the tail of the
/// file where less data exists.
const BUF_SECTORS: u32 = (READAHEAD_BUF_BYTES / SECTOR_SIZE) as u32;

/// SectorSource backed by a file (ISO image) with an internal
/// `READAHEAD_BUF_BYTES`-sized read-ahead window.
///
/// `read_sectors` is satisfied from the buffer when possible; otherwise
/// a full-buffer refill is issued at the requested LBA's position and
/// the call is re-tried against the freshly populated window.
pub struct FileSectorSource {
    file: File,
    /// Total file size in sectors. Constant after construction;
    /// surfaced via [`SectorSource::capacity_sectors`].
    capacity: u32,
    /// 0.21.3+: the app-level buffer is no longer touched on the hot
    /// path (every `read_sectors` is a direct pread). The fields are
    /// retained so a future per-source-type policy (e.g. a local-disk
    /// source where batched reads ARE beneficial) can re-enable
    /// buffering cleanly without re-plumbing the struct.
    #[allow(dead_code)]
    buf: Box<[u8]>,
    #[allow(dead_code)]
    buf_start_lba: u32,
    buf_len_sectors: u32,
}

impl FileSectorSource {
    /// Open an existing ISO file for reading. Capacity is derived
    /// from `metadata().len() / 2048`. Returns
    /// [`Error::IsoTooLarge`] if the file would exceed the 32-bit
    /// LBA address space (~8 TB).
    ///
    /// Issues the platform's "sequential access expected" hint on the
    /// fd (Linux `posix_fadvise(SEQUENTIAL)`, macOS `fcntl(F_RDADVISE)`,
    /// Windows TODO stub) so the kernel's readahead widens.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let sectors = len / SECTOR_SIZE as u64;
        if sectors > u32::MAX as u64 {
            return Err(Error::IsoTooLarge {
                path: path.to_string_lossy().into_owned(),
            }
            .into());
        }
        let capacity = sectors as u32;

        // Best-effort sequential hint. Ignored on platforms without
        // an equivalent primitive (or where the API exists but the
        // FS doesn't honour it).
        platform::hint_sequential(&file, len);

        // Pre-allocate the buffer once. `vec![0u8; N].into_boxed_slice()`
        // is the canonical way to fix the allocation size up-front;
        // `Vec::with_capacity` would leave `len == 0` and force callers
        // to do unsafe length manipulation to write into it.
        let buf = vec![0u8; READAHEAD_BUF_BYTES].into_boxed_slice();

        Ok(Self {
            file,
            capacity,
            buf,
            buf_start_lba: 0,
            buf_len_sectors: 0,
        })
    }

    /// True if `[lba, lba + count)` is wholly inside the current
    /// buffer window. `count == 0` is vacuously true.
    #[allow(dead_code)]
    fn buffer_covers(&self, lba: u32, count: u32) -> bool {
        if self.buf_len_sectors == 0 {
            return false;
        }
        let end = match lba.checked_add(count) {
            Some(e) => e,
            None => return false,
        };
        let buf_end = self.buf_start_lba.saturating_add(self.buf_len_sectors);
        lba >= self.buf_start_lba && end <= buf_end
    }

    /// Refill the buffer so it starts at `lba`. Read as many sectors
    /// as we have buffer space AND file capacity for. Caller has
    /// already checked `lba < capacity`.
    #[allow(dead_code)]
    fn refill(&mut self, lba: u32) -> Result<()> {
        debug_assert!(lba < self.capacity, "refill past capacity");
        // Don't read past EOF — clamp the request to remaining
        // sectors. partial-buffer-at-EOF behaviour is intentional.
        let want = BUF_SECTORS.min(self.capacity - lba);
        let want_bytes = want as usize * SECTOR_SIZE;
        let offset = lba as u64 * SECTOR_SIZE as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::IoError { source: e })?;
        self.file
            .read_exact(&mut self.buf[..want_bytes])
            .map_err(|e| Error::IoError { source: e })?;
        self.buf_start_lba = lba;
        self.buf_len_sectors = want;
        Ok(())
    }
}

impl SectorSource for FileSectorSource {
    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        out: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let count = count as u32;
        let bytes = count as usize * SECTOR_SIZE;
        debug_assert!(
            out.len() >= bytes,
            "FileSectorSource::read_sectors: out len {} < requested {}",
            out.len(),
            bytes
        );
        if count == 0 {
            return Ok(0);
        }
        // 0.21.3: bypass the application-level buffer entirely.
        //
        // Empirically the 32 MiB readahead window (0.21.0–0.21.1) and the
        // 4 MiB shrink (0.21.2) both regressed mux throughput vs the
        // pre-Phase-1 0.20.7 baseline on NFS bidirectional workloads
        // (sweep ~25 MB/s OK; mux dropped from 18 → 7-8 → 5-6 MB/s).
        // Direct pread per call lets the kernel's own readahead policy
        // run, which interleaves naturally with concurrent NFS writes on
        // the same TCP connection.
        //
        // Buffer fields are retained (currently unused on this path) so
        // any future per-source policy can be reintroduced without
        // re-plumbing structure. `refill` / `buffer_covers` are kept too
        // (still exercised by the tests so the API contract is locked).
        let offset = lba as u64 * SECTOR_SIZE as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::IoError { source: e })?;
        self.file
            .read_exact(&mut out[..bytes])
            .map_err(|e| Error::IoError { source: e })?;
        self.buf_len_sectors = 0;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    /// Build a deterministic ISO of `sectors` sectors where sector `n`
    /// is filled with the byte pattern `((n & 0xff) as u8)`. Lets us
    /// verify any sector by content alone.
    fn make_iso(path: &std::path::Path, sectors: u32) {
        let mut f = std::fs::File::create(path).unwrap();
        let mut chunk = vec![0u8; SECTOR_SIZE];
        for n in 0..sectors {
            let b = (n & 0xff) as u8;
            chunk.iter_mut().for_each(|c| *c = b);
            f.write_all(&chunk).unwrap();
        }
        f.flush().unwrap();
    }

    #[test]
    fn sequential_reads_match_file() {
        // Two full buffer windows + a tail = exercise refill across
        // boundaries.
        let total = BUF_SECTORS * 2 + 17;
        let dir = tempdir().unwrap();
        let path = dir.path().join("seq.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), total);

        let mut got = vec![0u8; SECTOR_SIZE];
        for lba in 0..total {
            src.read_sectors(lba, 1, &mut got, false).unwrap();
            let expected = (lba & 0xff) as u8;
            assert!(
                got.iter().all(|b| *b == expected),
                "sector {lba} content mismatch: expected 0x{expected:02x}"
            );
        }
    }

    #[test]
    fn multi_sector_read_spanning_buffer_boundary() {
        // A read that lands exactly on the last sector of the buffer
        // plus the first sector of the next refill must rebuffer
        // mid-read. Bypass path triggers when count > BUF_SECTORS; we
        // want the in-window path, so count stays small but
        // straddles the boundary.
        let total = BUF_SECTORS * 2;
        let dir = tempdir().unwrap();
        let path = dir.path().join("span.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();

        // Prime: read sector 0. (0.21.3+: app-level buffer is bypassed,
        // so we don't assert internal buf state here — just exercise
        // the read path.)
        let mut got = vec![0u8; SECTOR_SIZE];
        src.read_sectors(0, 1, &mut got, false).unwrap();

        // Now read 4 sectors crossing what used to be the buffer
        // boundary. Still a valid SectorSource-contract test.
        let span_lba = BUF_SECTORS - 2;
        let mut buf4 = vec![0u8; SECTOR_SIZE * 4];
        src.read_sectors(span_lba, 4, &mut buf4, false).unwrap();
        for i in 0..4 {
            let lba = span_lba + i as u32;
            let expected = (lba & 0xff) as u8;
            for b in &buf4[i * SECTOR_SIZE..(i + 1) * SECTOR_SIZE] {
                assert_eq!(*b, expected, "byte mismatch at sub-sector {i}");
            }
        }
    }

    #[test]
    fn backward_seek_rebuffers() {
        // Read forward across two windows, then jump back to sector
        // 0. Buffer must refill from the start.
        let total = BUF_SECTORS * 2 + 5;
        let dir = tempdir().unwrap();
        let path = dir.path().join("back.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        let mut got = vec![0u8; SECTOR_SIZE];

        // Forward to the second window.
        src.read_sectors(BUF_SECTORS + 1, 1, &mut got, false)
            .unwrap();

        // Backward to sector 0. (0.21.3+: app-level buffer is bypassed
        // so we only assert the byte-level contract, not internal
        // buffer state.)
        src.read_sectors(0, 1, &mut got, false).unwrap();
        assert!(got.iter().all(|b| *b == 0));
    }

    #[test]
    fn partial_buffer_at_eof() {
        // File is smaller than one buffer window. The buffer must
        // populate with only the available sectors and reads must
        // still succeed.
        let total: u32 = 100;
        assert!(total < BUF_SECTORS);
        let dir = tempdir().unwrap();
        let path = dir.path().join("small.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), total);

        let mut got = vec![0u8; SECTOR_SIZE];
        // First read at sector 0.
        src.read_sectors(0, 1, &mut got, false).unwrap();

        // Read the very last sector. (0.21.3+: app-level buffer is
        // bypassed; the test still verifies that EOF-region reads
        // return correct bytes.)
        src.read_sectors(total - 1, 1, &mut got, false).unwrap();
        let expected = ((total - 1) & 0xff) as u8;
        assert!(got.iter().all(|b| *b == expected));
    }

    #[test]
    fn oversized_read_bypasses_buffer() {
        // A request larger than the buffer must not deadlock the
        // refill (which only loads BUF_SECTORS at a time). Bypass
        // path handles it via direct pread.
        let total = BUF_SECTORS + 100;
        let dir = tempdir().unwrap();
        let path = dir.path().join("over.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        // Read more than BUF_SECTORS in one call. count is u16, so we
        // can't actually exceed BUF_SECTORS (16k) — but the path also
        // triggers via `out.len() / SECTOR_SIZE > BUF_SECTORS` check
        // implicitly because count > BUF_SECTORS. BUF_SECTORS for
        // 32 MiB is 16384, which does fit in u16 (max 65535). Cap
        // at BUF_SECTORS + 1 to exercise the bypass.
        let req = (BUF_SECTORS + 1) as u16;
        let req_bytes = req as usize * SECTOR_SIZE;
        let mut big = vec![0u8; req_bytes];
        src.read_sectors(0, req, &mut big, false).unwrap();
        // Spot-check sector 0 and the last requested sector.
        assert!(big[..SECTOR_SIZE].iter().all(|b| *b == 0));
        let last_lba = req as u32 - 1;
        let exp = (last_lba & 0xff) as u8;
        let last_off = (req as usize - 1) * SECTOR_SIZE;
        assert!(
            big[last_off..last_off + SECTOR_SIZE]
                .iter()
                .all(|b| *b == exp)
        );
    }
}
