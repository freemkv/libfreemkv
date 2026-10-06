//! `write_image` — write an image-level source out as a sector image.
//!
//! This is the plain image writer: sectors in from any [`SectorSource`], bytes
//! out to a file, in order, once. It is what an `iso://` DESTINATION means when
//! the source is not a physical drive.
//!
//! Drive sources go through `freemkv_engine::copy` (the recovery path) instead; this function
//! is for everything else.

use crate::consts::SECTOR_BYTES;
use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::block_sink::{BlockSink, Finish, IsoSink};
use crate::sector::SectorSource;
use std::path::Path;

/// Sectors per read/write batch. 4 MiB — large enough that per-call overhead
/// disappears against a file-backed source, small enough that the buffer is not
/// a notable allocation and cancellation stays responsive.
const BATCH_SECTORS: u32 = 2048;

/// Write `total_sectors` sectors from `reader` to `dest`.
///
/// Reads 2048-sector batches from LBA 0 in order; decrypts nothing. Batches are not whole
/// 3-sector AACS units: wrap an AACS decrypting source to read on the unit grid, or it fails
/// the first misaligned batch ([`Error::DecryptFailed`]). Each batch emits a cumulative
/// `BytesWritten`; a Stop keeps the partial file, any other failure removes it.
///
/// # Errors
///
/// - [`Error::Halted`]/[`Error::IoError`], or whatever `read_sectors` returns
///   (a short read is an error, not zero-filled).
pub fn write_image(
    reader: &mut dyn SectorSource,
    dest: &Path,
    total_sectors: u32,
    ctx: &Ctx,
) -> Result<u64> {
    write_image_with(
        reader,
        dest,
        total_sectors,
        ctx,
        crate::io::fsync::dir_checked,
    )
}

// `sync_dir` is a seam so tests can make the parent-directory fsync fail.
fn write_image_with(
    reader: &mut dyn SectorSource,
    dest: &Path,
    total_sectors: u32,
    ctx: &Ctx,
    sync_dir: fn(&Path) -> std::io::Result<()>,
) -> Result<u64> {
    if total_sectors == 0 {
        return Err(Error::EmptyImage);
    }
    let sink = IsoSink::create(dest)?;
    #[cfg(test)]
    let sink = sink.with_dir_sync(sync_dir);
    #[cfg(not(test))]
    let _ = sync_dir;
    let r = copy_out(reader, Box::new(sink), total_sectors, ctx);
    // A failed copy leaves no truncated image at the final name; a halt keeps its partial.
    if matches!(&r, Err(e) if !matches!(e, Error::Halted)) {
        let _ = std::fs::remove_file(dest);
    }
    r
}

// Sectors `0..total_sectors` of `reader`, in order, into `sink`.
fn copy_out(
    reader: &mut dyn SectorSource,
    mut sink: Box<dyn BlockSink>,
    total_sectors: u32,
    ctx: &Ctx,
) -> Result<u64> {
    let total = u64::from(total_sectors) * SECTOR_BYTES as u64;
    let mut buf = vec![0u8; BATCH_SECTORS as usize * SECTOR_BYTES];
    let mut written: u64 = 0;
    let mut lba: u32 = 0;

    while lba < total_sectors {
        if ctx.halt.is_cancelled() {
            let _ = sink.finish(Finish::Incomplete);
            return Err(Error::Halted);
        }
        let count = BATCH_SECTORS.min(total_sectors - lba);
        let want = count as usize * SECTOR_BYTES;
        // `recovery = false`: a file-backed source ignores the flag, and a
        // retry loop over a local file would only re-read the same bytes.
        let got = match reader.read_sectors(lba, count as u16, &mut buf[..want], false) {
            Ok(n) => n,
            Err(e) => {
                let _ = sink.finish(Finish::Incomplete);
                return Err(e);
            }
        };
        if got != want {
            let _ = sink.finish(Finish::Incomplete);
            return Err(Error::ShortImageRead {
                lba,
                expected: want as u32,
                got: got as u32,
            });
        }
        if let Err(e) = sink.write_at(u64::from(lba), &buf[..want]) {
            let _ = sink.finish(Finish::Incomplete);
            return Err(e);
        }
        written += want as u64;
        lba += count;
        ctx.emit(Event::BytesWritten {
            bytes: written,
            total,
        });
    }
    sink.finish(Finish::Complete)
}

#[cfg(test)]
#[path = "image_writer_tests.rs"]
mod tests;
