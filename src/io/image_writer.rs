//! `write_image` — write an image-level source out as a sector image.
//!
//! This is the plain image writer: sectors in from any [`SectorSource`], bytes
//! out to a file, in order, once. It is what an `iso://` DESTINATION means when
//! the source is not a physical drive.
//!
//! Drive sources go through `freemkv_engine::copy` (the recovery path) instead;
//! this function is for everything else. See docs/image-writer.md for why the
//! two paths must stay separate.

use crate::consts::SECTOR_BYTES;
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::sector::SectorSource;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Sectors per read/write batch. 4 MiB — large enough that per-call overhead
/// disappears against a file-backed source, small enough that the buffer is not
/// a notable allocation and cancellation stays responsive.
const BATCH_SECTORS: u32 = 2048;

/// Write `total_sectors` sectors from `reader` to `dest`.
///
/// Reads sequentially, writes in order; does no decryption itself (caller's call).
/// `on_progress` runs after each batch with cumulative bytes and must not block;
/// `halt` is checked once per batch, and on cancellation the partial file is kept.
/// Returns bytes written. See docs/image-writer.md for rationale.
///
/// # Errors
///
/// - [`Error::Halted`]/[`Error::IoError`], or whatever `read_sectors` returns
///   (a short read is an error, not zero-filled).
pub fn write_image(
    reader: &mut dyn SectorSource,
    dest: &Path,
    total_sectors: u32,
    halt: &Halt,
    mut on_progress: impl FnMut(u64),
) -> Result<u64> {
    if total_sectors == 0 {
        return Err(Error::EmptyImage);
    }

    let file = File::create(dest).map_err(|source| Error::IoError { source })?;
    let mut out = BufWriter::with_capacity(BATCH_SECTORS as usize * SECTOR_BYTES, file);

    let mut buf = vec![0u8; BATCH_SECTORS as usize * SECTOR_BYTES];
    let mut written: u64 = 0;
    let mut lba: u32 = 0;

    while lba < total_sectors {
        if halt.is_cancelled() {
            return Err(Error::Halted);
        }
        let count = BATCH_SECTORS.min(total_sectors - lba);
        let want = count as usize * SECTOR_BYTES;
        // `recovery = false`: a file-backed source ignores the flag, and a
        // retry loop over a local file would only re-read the same bytes.
        let got = reader.read_sectors(lba, count as u16, &mut buf[..want], false)?;
        if got != want {
            return Err(Error::ShortImageRead {
                lba,
                expected: want as u32,
                got: got as u32,
            });
        }
        out.write_all(&buf[..want])
            .map_err(|source| Error::IoError { source })?;
        written += want as u64;
        lba += count;
        on_progress(written);
    }

    // flush() only pushes bytes into the kernel via write(2), no durability promise —
    // a crash or yanked volume could leave a truncated file reported as complete.
    // `into_inner` (not `flush`) also surfaces buffered-write errors, not silently drop them.
    let file = out.into_inner().map_err(|e| Error::IoError {
        source: e.into_error(),
    })?;
    file.sync_all()
        .map_err(|source| Error::IoError { source })?;
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result as FmResult;

    /// A source that yields a deterministic byte per sector, so the written
    /// image can be checked positionally rather than just by length.
    struct PatternSource {
        sectors: u32,
        /// Sectors after which `read_sectors` reports a short read.
        short_after: Option<u32>,
    }

    impl SectorSource for PatternSource {
        fn capacity_sectors(&self) -> u32 {
            self.sectors
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> FmResult<usize> {
            let want = count as usize * SECTOR_BYTES;
            if self.short_after.is_some_and(|after| lba >= after) {
                return Ok(want - 1);
            }
            for s in 0..count as usize {
                let byte = ((lba as usize + s) % 251) as u8;
                buf[s * SECTOR_BYTES..(s + 1) * SECTOR_BYTES].fill(byte);
            }
            Ok(want)
        }
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("fmkv-image-writer-{name}-{}", std::process::id()));
        p
    }

    /// The written image is byte-for-byte what the source presented, at the
    /// right offsets — not merely the right length.
    #[test]
    fn writes_every_sector_in_order() {
        let dest = tmp("order");
        let mut src = PatternSource {
            sectors: 5000,
            short_after: None,
        };
        let n = write_image(&mut src, &dest, 5000, &Halt::new(), |_| {}).expect("write");
        assert_eq!(n, 5000 * SECTOR_BYTES as u64);

        let data = std::fs::read(&dest).expect("read back");
        assert_eq!(data.len(), 5000 * SECTOR_BYTES);
        // Spot-check across batch boundaries (BATCH_SECTORS = 2048): the last
        // sector of batch 0, the first of batch 1, and the final sector.
        for lba in [0usize, 2047, 2048, 4095, 4096, 4999] {
            let want = (lba % 251) as u8;
            assert_eq!(
                data[lba * SECTOR_BYTES],
                want,
                "sector {lba} head byte wrong — batching lost or duplicated a sector"
            );
            assert_eq!(
                data[(lba + 1) * SECTOR_BYTES - 1],
                want,
                "sector {lba} tail"
            );
        }
        let _ = std::fs::remove_file(&dest);
    }

    /// A tail shorter than a full batch must still be written whole — the
    /// classic off-by-one when `total_sectors` is not a batch multiple.
    #[test]
    fn writes_a_partial_final_batch() {
        let dest = tmp("tail");
        let mut src = PatternSource {
            sectors: 2049,
            short_after: None,
        };
        let n = write_image(&mut src, &dest, 2049, &Halt::new(), |_| {}).expect("write");
        assert_eq!(n, 2049 * SECTOR_BYTES as u64);
        assert_eq!(
            std::fs::metadata(&dest).expect("stat").len(),
            2049 * SECTOR_BYTES as u64
        );
        let _ = std::fs::remove_file(&dest);
    }

    /// A short read is an error. Zero-filling would yield an image that looks
    /// complete and is not — the single worst outcome for an archival copy.
    #[test]
    fn short_read_is_an_error_not_a_zero_fill() {
        let dest = tmp("short");
        let mut src = PatternSource {
            sectors: 4096,
            short_after: Some(2048),
        };
        let err = write_image(&mut src, &dest, 4096, &Halt::new(), |_| {}).expect_err("must fail");
        assert!(
            matches!(err, Error::ShortImageRead { lba: 2048, .. }),
            "got {err:?}"
        );
        let _ = std::fs::remove_file(&dest);
    }

    /// Cancellation stops the run and reports it, rather than finishing quietly
    /// or reporting success on a partial image.
    #[test]
    fn cancellation_halts_and_reports() {
        let dest = tmp("halt");
        let mut src = PatternSource {
            sectors: 100_000,
            short_after: None,
        };
        let halt = Halt::new();
        halt.cancel();
        let err = write_image(&mut src, &dest, 100_000, &halt, |_| {}).expect_err("must halt");
        assert!(matches!(err, Error::Halted), "got {err:?}");
        let _ = std::fs::remove_file(&dest);
    }

    /// Progress is cumulative and monotonic, and its final value equals the
    /// returned byte count — a front-end that trusts the callback must not end
    /// up disagreeing with the return value.
    #[test]
    fn progress_is_cumulative_and_ends_at_the_total() {
        let dest = tmp("progress");
        let mut src = PatternSource {
            sectors: 5000,
            short_after: None,
        };
        let mut seen: Vec<u64> = Vec::new();
        let n = write_image(&mut src, &dest, 5000, &Halt::new(), |b| seen.push(b)).expect("write");
        assert!(
            seen.windows(2).all(|w| w[1] > w[0]),
            "not monotonic: {seen:?}"
        );
        assert_eq!(*seen.last().expect("at least one callback"), n);
        let _ = std::fs::remove_file(&dest);
    }

    /// A zero-sector source is a caller error, not a zero-byte image: an empty
    /// ISO is never what anyone wanted, and failing here names the problem.
    #[test]
    fn zero_sectors_is_an_error() {
        let dest = tmp("empty");
        let mut src = PatternSource {
            sectors: 0,
            short_after: None,
        };
        let err = write_image(&mut src, &dest, 0, &Halt::new(), |_| {}).expect_err("must fail");
        assert!(matches!(err, Error::EmptyImage), "got {err:?}");
        // The destination must not have been created — a failed run leaves no
        // stub for a later run to mistake for output.
        assert!(!dest.exists(), "empty run created a file");
    }
}
