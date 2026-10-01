//! Block sinks (pipeline design §2.2, slice 7): where a whole-disc copy's sectors go. A PES
//! sink takes frames; a block sink takes sector-addressed chunks — `iso://` writes a
//! sector-exact image, `null://` discards (a read test of the whole disc). [`open_block_sink`]
//! opens either from a URL, as [`crate::output`] opens a PES sink.

use crate::consts::SECTOR_BYTES;
use crate::error::{Error, Result};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// The platform's null device: the path a whole-disc copy to `null://` writes to, so the
/// copy runs (and its mapfile records the read) with nothing kept. `/dev/null` on Unix,
/// `NUL` on Windows.
pub fn null_device() -> &'static Path {
    if cfg!(windows) {
        Path::new("NUL")
    } else {
        Path::new("/dev/null")
    }
}

/// Whether `path` is the platform's null device (see [`null_device`]).
pub fn is_null_device(path: &Path) -> bool {
    path == null_device() || path.as_os_str() == "/dev/null"
}

/// How a block sink ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Finish {
    /// Every sector was written: flush and make the output durable.
    Complete,
    /// The copy stopped or failed early: an image keeps its partial bytes (a resumable
    /// copy continues it); nothing is made durable.
    Incomplete,
}

/// A sector-addressed output: `iso://` or `null://`.
pub trait BlockSink: Send {
    /// Write whole sectors at sector `lba`.
    fn write_at(&mut self, lba: u64, bytes: &[u8]) -> Result<()>;
    /// End the output; returns the bytes written.
    fn finish(self: Box<Self>, how: Finish) -> Result<u64>;
}

/// A sector-exact image file. Sequential writes stream through a buffer; a write at another
/// sector seeks there first.
pub struct IsoSink {
    out: BufWriter<File>,
    path: PathBuf,
    // The next sector the file position is at.
    at: u64,
    written: u64,
    // The parent-directory fsync (a seam so tests can make it fail).
    sync_dir: fn(&Path) -> std::io::Result<()>,
}

impl IsoSink {
    /// Create (truncate) the image at `path`.
    pub fn create(path: &Path) -> Result<IsoSink> {
        let file = File::create(path).map_err(|source| Error::IoError { source })?;
        Ok(IsoSink {
            out: BufWriter::with_capacity(4 * 1024 * 1024, file),
            path: path.to_path_buf(),
            at: 0,
            written: 0,
            sync_dir: crate::io::fsync::dir_checked,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_dir_sync(mut self, f: fn(&Path) -> std::io::Result<()>) -> Self {
        self.sync_dir = f;
        self
    }
}

impl BlockSink for IsoSink {
    fn write_at(&mut self, lba: u64, bytes: &[u8]) -> Result<()> {
        let io = |source| Error::IoError { source };
        if lba != self.at {
            self.out
                .seek(SeekFrom::Start(lba * SECTOR_BYTES as u64))
                .map_err(io)?;
        }
        self.out.write_all(bytes).map_err(io)?;
        self.at = lba + (bytes.len() / SECTOR_BYTES) as u64;
        self.written += bytes.len() as u64;
        Ok(())
    }

    fn finish(self: Box<Self>, how: Finish) -> Result<u64> {
        // `into_inner` (not `flush`) also surfaces buffered-write errors.
        let file = self.out.into_inner().map_err(|e| Error::IoError {
            source: e.into_error(),
        })?;
        if how == Finish::Complete {
            // flush() only pushes bytes into the kernel: a crash or yanked volume could
            // leave a truncated file reported as complete.
            file.sync_all()
                .map_err(|source| Error::IoError { source })?;
            // The file was just created: its directory entry needs its own fsync.
            let dir = match self.path.parent() {
                Some(dir) if !dir.as_os_str().is_empty() => dir,
                _ => Path::new("."),
            };
            (self.sync_dir)(dir).map_err(|source| Error::IoError { source })?;
        }
        Ok(self.written)
    }
}

/// `null://`: discards every sector, counting them.
#[derive(Default)]
pub struct NullBlockSink {
    written: u64,
}

impl BlockSink for NullBlockSink {
    fn write_at(&mut self, _lba: u64, bytes: &[u8]) -> Result<()> {
        self.written += bytes.len() as u64;
        Ok(())
    }

    fn finish(self: Box<Self>, _how: Finish) -> Result<u64> {
        Ok(self.written)
    }
}

/// Open the block output `url`: `iso://<path>` (an image, created now) or `null://`.
/// `dir://` is the folder output of a whole-disc extraction ([`crate::Disc::extract_tree`])
/// and is refused here, as is a PES output scheme, with [`Error::StreamUrlInvalid`].
pub fn open_block_sink(url: &str) -> Result<Box<dyn BlockSink>> {
    match crate::mux::parse_url(url) {
        crate::mux::StreamUrl::Iso { path } => {
            crate::mux::resolve::validate_path(&path, "iso")?;
            Ok(Box::new(IsoSink::create(&path)?))
        }
        crate::mux::StreamUrl::Null => Ok(Box::new(NullBlockSink::default())),
        _ => Err(Error::StreamUrlInvalid {
            url: url.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_iso_sink_writes_sectors_where_addressed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("o.iso");
        let mut sink = open_block_sink(&format!("iso://{}", path.display())).unwrap();
        sink.write_at(0, &[1u8; 2048]).unwrap();
        sink.write_at(2, &[3u8; 2048]).unwrap();
        sink.write_at(1, &[2u8; 2048]).unwrap();
        assert_eq!(sink.finish(Finish::Complete).unwrap(), 3 * 2048);
        let got = std::fs::read(&path).unwrap();
        assert_eq!(got.len(), 3 * 2048);
        assert!(got[..2048].iter().all(|&b| b == 1));
        assert!(got[2048..4096].iter().all(|&b| b == 2));
        assert!(got[4096..].iter().all(|&b| b == 3));
    }

    #[test]
    fn null_discards_and_counts() {
        let mut sink = open_block_sink("null://").unwrap();
        sink.write_at(7, &[0u8; 4096]).unwrap();
        assert_eq!(sink.finish(Finish::Complete).unwrap(), 4096);
    }

    #[test]
    fn a_pes_scheme_is_not_a_block_sink() {
        assert!(open_block_sink("mkv:///tmp/x.mkv").is_err());
    }

    #[test]
    fn the_null_device_is_recognised() {
        assert!(is_null_device(null_device()));
        assert!(is_null_device(Path::new("/dev/null")));
        assert!(!is_null_device(Path::new("/tmp/x.iso")));
    }
}
