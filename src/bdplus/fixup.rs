//! Applying patches: within one clip's byte stream, and over the disc's sectors.

use super::table::{PATCH_LEN, Patch};
use crate::consts::{BD_SOURCE_PACKET_BYTES, SECTOR_BYTES};
use std::sync::atomic::{AtomicU64, Ordering};

/// Write every byte of `patches` (sorted by offset) that falls in `buf`, which holds the
/// clip's bytes from offset `at`. A patch straddling either end of `buf` writes only its
/// bytes inside. Returns how many patches touched `buf`.
pub fn apply_patches(patches: &[Patch], at: u64, buf: &mut [u8]) -> usize {
    let end = at + buf.len() as u64;
    let first = patches.partition_point(|p| p.offset + (PATCH_LEN as u64) <= at);
    let mut touched = 0;
    for p in patches[first..].iter().take_while(|p| p.offset < end) {
        if write_overlap(p.offset, &p.bytes, at, buf) > 0 {
            touched += 1;
        }
    }
    touched
}

// Copy the part of `bytes` (placed at `offset`) that overlaps `buf` (placed at `at`).
fn write_overlap(offset: u64, bytes: &[u8], at: u64, buf: &mut [u8]) -> usize {
    let end = at + buf.len() as u64;
    let lo = offset.max(at);
    let hi = (offset + bytes.len() as u64).min(end);
    if lo >= hi {
        return 0;
    }
    let (src, dst) = ((lo - offset) as usize, (lo - at) as usize);
    let n = (hi - lo) as usize;
    buf[dst..dst + n].copy_from_slice(&bytes[src..src + n]);
    n
}

/// A clip file's place on the disc: `extents` are `(lba, bytes)` in file order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClipFile {
    pub(crate) clip: u32,
    pub(crate) extents: Vec<(u32, u64)>,
}

impl ClipFile {
    fn size(&self) -> u64 {
        self.extents.iter().map(|e| e.1).sum()
    }

    // The disc byte address of file offset `off`, or `None` past the end.
    fn disc_addr(&self, off: u64) -> Option<u64> {
        let mut base = 0u64;
        for &(lba, len) in &self.extents {
            if off < base + len {
                return Some(u64::from(lba) * SECTOR_BYTES as u64 + (off - base));
            }
            base += len;
        }
        None
    }
}

// One run of patch bytes contiguous on the disc. `sync` is the disc address of the TS sync
// byte of the patch's source packet; `last` marks the run holding the patch's final byte.
#[derive(Debug, Clone, Copy)]
struct Piece {
    disc: u64,
    len: u8,
    bytes: [u8; PATCH_LEN],
    sync: Option<u64>,
    last: bool,
}

/// Every patch of a resolved table placed on the disc's byte addresses, so a read at any
/// LBA (seek, resume, retry, any batch size) is patched by position alone.
#[derive(Default)]
pub(crate) struct DiscFixups {
    pieces: Vec<Piece>,
    applied: AtomicU64,
}

impl std::fmt::Debug for DiscFixups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscFixups")
            .field("pieces", &self.pieces.len())
            .field("applied", &self.applied())
            .finish()
    }
}

impl DiscFixups {
    /// Place `patches` (clip-relative) of each clip. Returns the map and how many patches
    /// fell wholly past their file's end.
    pub(crate) fn build<'a>(
        clips: impl IntoIterator<Item = (&'a ClipFile, &'a [Patch])>,
    ) -> (Self, usize) {
        let mut pieces: Vec<Piece> = Vec::new();
        let mut dropped = 0;
        for (file, patches) in clips {
            let size = file.size();
            for p in patches {
                if p.offset >= size {
                    dropped += 1;
                    continue;
                }
                let packet = p.offset - p.offset % BD_SOURCE_PACKET_BYTES as u64;
                let sync = file.disc_addr(packet + 4);
                let n = PATCH_LEN.min((size - p.offset) as usize);
                let start = pieces.len();
                for i in 0..n {
                    let Some(addr) = file.disc_addr(p.offset + i as u64) else {
                        break;
                    };
                    let joins = pieces.len() > start
                        && pieces
                            .last()
                            .is_some_and(|q: &Piece| q.disc + u64::from(q.len) == addr);
                    if joins && let Some(q) = pieces.last_mut() {
                        q.bytes[usize::from(q.len)] = p.bytes[i];
                        q.len += 1;
                        continue;
                    }
                    let mut bytes = [0u8; PATCH_LEN];
                    bytes[0] = p.bytes[i];
                    pieces.push(Piece {
                        disc: addr,
                        len: 1,
                        bytes,
                        sync,
                        last: false,
                    });
                }
                if pieces.len() > start {
                    let i = pieces.len() - 1;
                    pieces[i].last = true;
                }
            }
        }
        pieces.sort_by_key(|p| p.disc);
        let map = DiscFixups {
            pieces,
            applied: AtomicU64::new(0),
        };
        (map, dropped)
    }

    /// Patch `buf`, read from the disc at `lba`. A patch whose packet's sync byte is in
    /// `buf` but is not `0x47` (a blanked unit, or ciphertext) is skipped.
    pub(crate) fn apply(&self, lba: u32, buf: &mut [u8]) {
        let at = u64::from(lba) * SECTOR_BYTES as u64;
        let end = at + buf.len() as u64;
        let first = self
            .pieces
            .partition_point(|p| p.disc + PATCH_LEN as u64 <= at);
        let mut done = 0;
        for p in self.pieces[first..].iter().take_while(|p| p.disc < end) {
            let synced = p
                .sync
                .filter(|s| (at..end).contains(s))
                .is_none_or(|s| buf[(s - at) as usize] == 0x47);
            if !synced {
                continue;
            }
            let bytes = &p.bytes[..usize::from(p.len)];
            let n = write_overlap(p.disc, bytes, at, buf);
            let tail = p.disc + u64::from(p.len) - 1;
            if n > 0 && p.last && (at..end).contains(&tail) {
                done += 1;
            }
        }
        if done > 0 {
            self.applied.fetch_add(done, Ordering::Relaxed);
        }
    }

    /// Patches whose final byte has been written so far.
    pub(crate) fn applied(&self) -> u64 {
        self.applied.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[path = "fixup_tests.rs"]
mod tests;
