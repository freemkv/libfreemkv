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

/// Sectors per read/write batch. 3 MiB — large enough that per-call overhead disappears
/// against a file-backed source, and a whole number of 3-sector AACS units, so a stream file
/// on LBA 0's unit grid reads in place, with no unit read twice across a batch boundary.
const BATCH_SECTORS: u32 = 1536;

/// Write `total_sectors` sectors from `reader` to `dest`.
///
/// Reads 1536-sector batches from LBA 0, writing each while the next is read; decrypts nothing.
/// Batches sit on LBA 0's unit grid, not each file's: wrap an AACS decrypting source, or it
/// fails the first misaligned batch ([`Error::DecryptFailed`]). Each written batch emits a
/// cumulative `BytesWritten`; a Stop keeps the partial file, any other failure removes it.
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

// Sectors `0..total_sectors` of `reader`, in order, into `sink`. A writer thread takes each
// batch while the next is read; the first failure in sector order is the one reported.
fn copy_out(
    reader: &mut dyn SectorSource,
    sink: Box<dyn BlockSink>,
    total_sectors: u32,
    ctx: &Ctx,
) -> Result<u64> {
    let total = u64::from(total_sectors) * SECTOR_BYTES as u64;
    let batch_bytes = BATCH_SECTORS as usize * SECTOR_BYTES;
    // Zero capacity: the reader runs one batch ahead of the writer, so two buffers suffice.
    let (to_writer, batches) = crossbeam_channel::bounded::<(u32, usize, Vec<u8>)>(0);
    let (recycle, recycled) = crossbeam_channel::bounded::<Vec<u8>>(BUFFERS);
    std::thread::scope(|scope| {
        let writer = scope.spawn(move || {
            let mut sink = sink;
            let mut written: u64 = 0;
            for (lba, len, buf) in batches {
                if let Err(e) = sink.write_at(u64::from(lba), &buf[..len]) {
                    return (sink, Err(e));
                }
                written += len as u64;
                ctx.emit(Event::BytesWritten {
                    bytes: written,
                    total,
                });
                let _ = recycle.send(buf);
            }
            (sink, Ok(()))
        });
        let read = read_batches(
            reader,
            total_sectors,
            ctx,
            batch_bytes,
            &to_writer,
            &recycled,
        );
        drop(to_writer);
        let (sink, wrote) = match writer.join() {
            Ok(done) => done,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        // A write failure comes from an earlier batch than any read failure behind it.
        match wrote.and(read) {
            Ok(()) => sink.finish(Finish::Complete),
            Err(e) => {
                let _ = sink.finish(Finish::Incomplete);
                Err(e)
            }
        }
    })
}

/// Batch buffers in flight: one being read, one being written.
const BUFFERS: usize = 2;

// Read every batch in order and hand it to the writer; `Ok` also when the writer stopped
// first (its own error is the one to report).
fn read_batches(
    reader: &mut dyn SectorSource,
    total_sectors: u32,
    ctx: &Ctx,
    batch_bytes: usize,
    to_writer: &crossbeam_channel::Sender<(u32, usize, Vec<u8>)>,
    recycled: &crossbeam_channel::Receiver<Vec<u8>>,
) -> Result<()> {
    let mut made = 0;
    let mut lba: u32 = 0;
    while lba < total_sectors {
        if ctx.halt.is_cancelled() {
            return Err(Error::Halted);
        }
        let mut buf = match recycled.try_recv() {
            Ok(buf) => buf,
            Err(_) if made < BUFFERS => {
                made += 1;
                vec![0u8; batch_bytes]
            }
            Err(_) => match recycled.recv() {
                Ok(buf) => buf,
                Err(_) => return Ok(()), // the writer stopped
            },
        };
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
        if to_writer.send((lba, want, buf)).is_err() {
            return Ok(()); // the writer stopped
        }
        lba += count;
    }
    Ok(())
}

#[cfg(test)]
#[path = "image_writer_tests.rs"]
mod tests;
