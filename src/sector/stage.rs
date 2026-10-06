//! The decryption stage's content verdict, and its byte-stream forms (crate-private and
//! transitional: the pipeline's chunk stream replaces the sector/byte split).
//!
//! Every `input()` source passes the stage. A sector source (`mpg://`, `m2ts://`) is wrapped
//! by the content-detected [`DecryptingSectorSource`](super::DecryptingSectorSource); a byte
//! reader (`mkv://`, `mp4://`, `network://`, `stdio://`) by [`Stage`], which hands a clear
//! container through untouched and refuses encrypted content it cannot decrypt. The verdict
//! comes from the bytes alone ([`classify`]), never from the URL scheme.

use crate::aacs::content::{ALIGNED_UNIT_LEN, aacs_unit_seed_encrypted, is_clean};
use crate::consts::{BD_SOURCE_PACKET_BYTES, SECTOR_BYTES};
use crate::css::{PACK_START, scrambled_at};
use crate::disc::ContentFormat;
use crate::error::Error;
use crate::sector::SectorSource;
use std::io::{self, Read, Seek, SeekFrom};

#[cfg(test)]
thread_local! {
    /// Stages built on this thread (test-only: every `input()` arm must build one).
    pub(crate) static STAGES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Sectors a sector source's verdict is read from.
pub(crate) const HEAD_SECTORS: u32 = 32;
// Bytes a file's verdict is read from, and a stream's (one aligned unit: its latency bound).
const FILE_HEAD: usize = HEAD_SECTORS as usize * SECTOR_BYTES;
const STREAM_HEAD: usize = ALIGNED_UNIT_LEN;
// Aligned units a BD-TS verdict checks at most.
const TS_UNITS: usize = 16;

/// What a stream's head says it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Nothing CSS or AACS can scramble (MKV, MP4, FMKV, arbitrary bytes): pass-through.
    Opaque,
    /// An MPEG program stream on the 2048-byte pack grid; `mpeg2` is the 13818-1 marker.
    Ps { mpeg2: bool },
    /// BD source packets (4-byte TP_extra_header + TS packet) on the 6144-byte unit grid.
    BdTs,
}

/// The stream kind `head` (the stream's first bytes) shows. A PS opens with a pack, as every
/// DVD-Video VOB does; BD-TS needs the seed sync of every head unit plus clean TS or a CPI flag.
pub(crate) fn classify(head: &[u8]) -> Kind {
    // freemkv's own m2ts/network/stdio output: clear, and its header is off the unit grid.
    if head.starts_with(b"FMKV") {
        return Kind::Opaque;
    }
    if let Some(ps) = pack_at(head) {
        return ps;
    }
    if is_bd_ts(head) {
        Kind::BdTs
    } else {
        Kind::Opaque
    }
}

/// [`classify`] for a sector source (a file): a VOB whose first sectors are damaged or blank
/// is still a PS by its first sector-aligned pack.
pub(crate) fn classify_sectors(head: &[u8]) -> Kind {
    match classify(head) {
        Kind::Opaque if !head.starts_with(b"FMKV") => head
            .chunks(SECTOR_BYTES)
            .find_map(pack_at)
            .unwrap_or(Kind::Opaque),
        kind => kind,
    }
}

// A pack header opening `s`: 13818-1 ('01', a start code past its stuffing) or 11172-1
// ('0010', a start code at byte 12). An MP4 box 0x1BA bytes long is neither.
fn pack_at(s: &[u8]) -> Option<Kind> {
    if s.get(..4)? != PACK_START {
        return None;
    }
    let mpeg2 = s.get(4)? >> 6 == 0b01;
    let at = match mpeg2 {
        true => 0x0E + usize::from(s.get(0x0D)? & 0x07),
        false if s[4] >> 4 == 0b0010 => 12,
        false => return None,
    };
    (s.get(at..at + 3)? == [0, 0, 1]).then_some(Kind::Ps { mpeg2 })
}

// KS-2: a source packet is "the TP_extra_header (4 bytes) and an MPEG Transport packet"; KS-4:
// a unit's clear seed keeps its sync. A damaged head unit must not hide the rest: half the
// non-blank head units, and two when there are two, must show that shape.
fn is_bd_ts(head: &[u8]) -> bool {
    const PKT: usize = BD_SOURCE_PACKET_BYTES;
    let units = head.as_chunks::<ALIGNED_UNIT_LEN>().0;
    let units = &units[..units.len().min(TS_UNITS)];
    if units.is_empty() {
        let pkts = head.as_chunks::<PKT>().0;
        return !pkts.is_empty() && pkts.iter().all(|p| p[4] == 0x47);
    }
    let live = units.iter().filter(|u| u.iter().any(|&b| b != 0)).count();
    let shaped = units
        .iter()
        .filter(|u| u[4] == 0x47 && (is_clean(*u, ContentFormat::BdTs) || u[0] & 0xC0 != 0))
        .count();
    shaped >= live.clamp(1, 2) && shaped * 2 >= live
}

// Clear TS: the seed's packet synced and half the other non-padding packets (16 of a whole
// unit's 31), not the 4-packet key-proof floor of `is_clean` (ciphertext passes ~7e-6). Damage
// to a few packets must not make a clear unit look encrypted; ciphertext passes this ~1e-30.
pub(crate) fn clear_ts(chunk: &[u8]) -> bool {
    let pkts = chunk.as_chunks::<BD_SOURCE_PACKET_BYTES>().0;
    let Some((seed, rest)) = pkts.split_first() else {
        return false;
    };
    let content = rest.iter().filter(|p| p[4..].iter().any(|&b| b != 0));
    let (n, synced) = content.fold((0, 0), |(n, s), p| (n + 1, s + usize::from(p[4] == 0x47)));
    seed[4] == 0x47 && synced * 2 >= n
}

// A BD-TS unit only a key opens: CPI-flagged and not clear TS.
fn needs_key(unit: &[u8]) -> bool {
    aacs_unit_seed_encrypted(unit, ContentFormat::BdTs) && !clear_ts(unit)
}

// `head` filled up to `max` bytes from `r`, fewer only at EOF; bytes read before an error stay.
fn fill_head(r: &mut impl Read, head: &mut Vec<u8>, max: usize) -> io::Result<()> {
    let want = max.saturating_sub(head.len()) as u64;
    r.by_ref().take(want).read_to_end(head).map(|_| ())
}

// Up to `max` bytes from the start of `r`, fewer only at EOF.
fn read_head(r: &mut impl Read, max: usize) -> io::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(max);
    fill_head(r, &mut head, max)?;
    Ok(head)
}

/// The stage over a byte reader: a clear container passes untouched; PS or BD-TS content is
/// watched, and an encrypted pack or unit is refused (E7023 / E7022) unless `raw`. A byte
/// reader cannot crack or prove a key (no side reads, X-2), so it never decrypts.
pub(crate) struct Stage<R> {
    inner: R,
    raw: bool,
    kind: Option<Kind>,
    // A lazily read head, served before `inner`.
    head: Vec<u8>,
    served: usize,
    // The watched block being assembled, and bytes to skip to the next block boundary.
    carry: Vec<u8>,
    skip: usize,
}

impl<R: Read> Stage<R> {
    /// A stream (`network://`, `stdio://`): the verdict is read on the first read.
    pub(crate) fn lazy(inner: R, raw: bool) -> Self {
        #[cfg(test)]
        STAGES.with(|n| n.set(n.get() + 1));
        Self {
            inner,
            raw,
            kind: None,
            head: Vec::new(),
            served: 0,
            carry: Vec::new(),
            skip: 0,
        }
    }

    pub(crate) fn get_ref(&self) -> &R {
        &self.inner
    }

    /// The verdict, once reached.
    #[cfg(test)]
    pub(crate) fn kind(&self) -> Option<Kind> {
        self.kind
    }

    fn block(&self) -> Option<usize> {
        match self.kind? {
            _ if self.raw => None,
            Kind::Opaque => None,
            Kind::Ps { .. } => Some(SECTOR_BYTES),
            Kind::BdTs => Some(ALIGNED_UNIT_LEN),
        }
    }

    // Judge the bytes just read as whole aligned blocks complete.
    fn judge(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        let Some(block) = self.block() else {
            return Ok(());
        };
        let skip = self.skip.min(bytes.len());
        self.skip -= skip;
        bytes = &bytes[skip..];
        while !bytes.is_empty() {
            let take = (block - self.carry.len()).min(bytes.len());
            self.carry.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.carry.len() == block {
                let refuse = match self.kind {
                    Some(Kind::BdTs) => needs_key(&self.carry).then(|| Error::NoDiscKey {
                        disc_hash: String::new(),
                    }),
                    _ => scrambled_at(&self.carry).map(|_| Error::CssKeyMissing),
                };
                self.carry.clear();
                if let Some(e) = refuse {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
}

impl<R: Read + Seek> Stage<R> {
    /// A seekable file (`mkv://`, `mp4://`): the verdict is read now, then `inner` is rewound.
    pub(crate) fn eager(mut inner: R, raw: bool) -> io::Result<Self> {
        let head = read_head(&mut inner, FILE_HEAD)?;
        inner.seek(SeekFrom::Start(0))?;
        let mut s = Self::lazy(inner, raw);
        s.kind = Some(classify(&head));
        Ok(s)
    }
}

impl<R: Read> Read for Stage<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.kind.is_none() {
            // Five bytes rule a stream out (no pack start, no TS sync at byte 4), so an FMKV
            // header is parsed as soon as it arrives; only a candidate waits for a whole unit.
            fill_head(&mut self.inner, &mut self.head, 5)?;
            if self.head.len() >= 5 && (self.head[..4] == PACK_START || self.head[4] == 0x47) {
                fill_head(&mut self.inner, &mut self.head, STREAM_HEAD)?;
            }
            self.kind = Some(classify(&self.head));
        }
        let n = if self.served < self.head.len() {
            let n = (self.head.len() - self.served).min(buf.len());
            buf[..n].copy_from_slice(&self.head[self.served..self.served + n]);
            self.served += n;
            n
        } else {
            self.inner.read(buf)?
        };
        self.judge(&buf[..n])?;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for Stage<R> {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        if self.served < self.head.len() {
            return Err(io::ErrorKind::Unsupported.into());
        }
        let at = self.inner.seek(to)?;
        self.carry.clear();
        self.skip = self
            .block()
            .map_or(0, |b| (b - (at % b as u64) as usize) % b);
        Ok(at)
    }
}

/// The byte view of a sector stage (`m2ts://`): its output as `Read`, clipped to `len` (the
/// file's real length, past its zero-padded last sector), read on the 6144-byte AACS grid.
pub(crate) struct SectorBytes<S> {
    src: S,
    len: u64,
    pos: u64,
    buf: Vec<u8>,
    buf_start: u64,
}

// Sectors per refill: whole aligned units (the AACS read gate), ~1 MiB.
const REFILL_SECTORS: u16 = 510;

impl<S: SectorSource> SectorBytes<S> {
    pub(crate) fn new(src: S, len: u64) -> Self {
        Self {
            src,
            len,
            pos: 0,
            buf: Vec::new(),
            buf_start: 0,
        }
    }
}

impl<S: SectorSource> SectorBytes<S> {
    /// The stage back, after a head read: the bytes it holds past the read position (up to
    /// the real length) and the sector its next refill would read.
    pub(crate) fn into_rest(self) -> (S, Vec<u8>, u32) {
        let buf_end = self.buf_start + self.buf.len() as u64;
        let held = self.pos.clamp(self.buf_start, buf_end)..buf_end.min(self.len);
        let rest = match held.start < held.end {
            true => self.buf
                [(held.start - self.buf_start) as usize..(held.end - self.buf_start) as usize]
                .to_vec(),
            false => Vec::new(),
        };
        let next = (buf_end / SECTOR_BYTES as u64) as u32;
        (self.src, rest, next)
    }
}

impl<S: SectorSource> Read for SectorBytes<S> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.len || out.is_empty() {
            return Ok(0);
        }
        let buf_end = self.buf_start + self.buf.len() as u64;
        if self.pos < self.buf_start || self.pos >= buf_end {
            let unit = crate::aacs::content::ALIGNED_UNIT_SECTORS;
            let lba = (self.pos / SECTOR_BYTES as u64) as u32 / unit * unit;
            let left = self.src.capacity_sectors().saturating_sub(lba);
            let count = left.min(u32::from(REFILL_SECTORS)) as u16;
            if count == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.buf.resize(count as usize * SECTOR_BYTES, 0);
            let n = match self.src.read_sectors(lba, count, &mut self.buf, true) {
                Ok(n) => n,
                // A refused read must not leave its raw bytes to serve a retry.
                Err(e) => {
                    self.buf.clear();
                    return Err(e.into());
                }
            };
            self.buf.truncate(n.min(self.buf.len()));
            self.buf_start = lba as u64 * SECTOR_BYTES as u64;
            if self.pos >= self.buf_start + self.buf.len() as u64 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
        let at = (self.pos - self.buf_start) as usize;
        let avail = (self.buf.len() - at).min((self.len - self.pos) as usize);
        let n = avail.min(out.len());
        out[..n].copy_from_slice(&self.buf[at..at + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
#[path = "stage_tests.rs"]
mod tests;
