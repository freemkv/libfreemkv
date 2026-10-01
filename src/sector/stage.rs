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
mod tests {
    use super::*;
    use crate::error::{E_CSS_KEY_MISSING, E_NO_DISC_KEY, error_code};
    use std::io::Cursor;

    // A DVD-Video pack: 14-byte pack header (stuffing 0), a PES of `id` at 0x0E whose
    // MPEG-2 flags byte (0x14) carries `scramble`, and a varied body.
    fn dvd_pack(id: u8, scramble: u8) -> Vec<u8> {
        let mut p: Vec<u8> = (0..SECTOR_BYTES).map(|i| (i * 37 + 11) as u8).collect();
        p[..4].copy_from_slice(&PACK_START);
        p[4] = 0x44;
        p[0x0D] = 0xF8;
        p[0x0E..0x11].copy_from_slice(&[0, 0, 1]);
        p[0x11] = id;
        p[0x12..0x14].copy_from_slice(&0x07ECu16.to_be_bytes());
        p[0x14] = 0x80 | (scramble << 4);
        p
    }

    fn ts_unit(cpi: bool) -> Vec<u8> {
        let mut u: Vec<u8> = (0..ALIGNED_UNIT_LEN)
            .map(|i| (i * 7 + 3) as u8 | 1)
            .collect();
        for p in u.chunks_mut(BD_SOURCE_PACKET_BYTES) {
            p[0] = if cpi { p[0] | 0xC0 } else { p[0] & 0x3F };
            p[4] = 0x47;
        }
        u
    }

    fn noise(n: usize) -> Vec<u8> {
        (0..n).map(|i| ((i * 2_654_435_761) >> 13) as u8).collect()
    }

    #[test]
    fn containers_and_arbitrary_bytes_are_opaque() {
        let mut mkv = vec![0x1A, 0x45, 0xDF, 0xA3, 0x47];
        mkv.extend(noise(FILE_HEAD));
        let mut mp4 = vec![0, 0, 0, 0x20];
        mp4.extend(b"ftypisom");
        mp4.extend(noise(FILE_HEAD));
        let mut fmkv = b"FMKV\0\x01\0\0".to_vec();
        fmkv.extend(noise(FILE_HEAD));
        let mut ifo = b"DVDVIDEO-VTS".to_vec();
        ifo.extend(noise(FILE_HEAD));
        // A 188-byte TS (sync at byte 0, not 4) carries no AACS unit.
        let mut ts188 = Vec::new();
        for _ in 0..200 {
            ts188.push(0x47);
            ts188.extend(noise(187));
        }
        for (name, head) in [
            ("mkv", mkv),
            ("mp4", mp4),
            ("fmkv", fmkv),
            ("ifo", ifo),
            ("udf zeros", vec![0u8; FILE_HEAD]),
            ("noise", noise(FILE_HEAD)),
            ("ts188", ts188),
            ("empty", Vec::new()),
        ] {
            assert_eq!(classify(&head), Kind::Opaque, "{name}");
        }
    }

    #[test]
    fn pack_streams_and_source_packets_are_recognised() {
        let mut ps = dvd_pack(0xE0, 0);
        ps.extend(noise(SECTOR_BYTES));
        assert_eq!(classify(&ps), Kind::Ps { mpeg2: true });
        let mut later = noise(SECTOR_BYTES);
        later.extend(dvd_pack(0xE0, 1));
        assert_eq!(
            classify(&later),
            Kind::Opaque,
            "a pack past the head of another stream"
        );
        let mut mpeg1 = dvd_pack(0xE0, 0);
        mpeg1[4] = 0x21;
        mpeg1[12..15].copy_from_slice(&[0, 0, 1]);
        assert_eq!(classify(&mpeg1), Kind::Ps { mpeg2: false });
        // Review: an MP4 whose first box is 0x1BA bytes opens `00 00 01 BA 'f'`.
        let mut mp4 = vec![0, 0, 1, 0xBA];
        mp4.extend(b"ftypisom");
        mp4.extend(noise(FILE_HEAD));
        assert_eq!(classify(&mp4), Kind::Opaque);
        let clear: Vec<u8> = (0..4).flat_map(|_| ts_unit(false)).collect();
        assert_eq!(classify(&clear), Kind::BdTs);
        let mut enc = ts_unit(true);
        enc[BD_SOURCE_PACKET_BYTES + 4] = 0x00; // an encrypted rest loses its syncs
        for p in enc[BD_SOURCE_PACKET_BYTES..].chunks_mut(BD_SOURCE_PACKET_BYTES) {
            p[4] = 0x5C;
        }
        assert_eq!(classify(&enc), Kind::BdTs);
        let mut noisy = noise(ALIGNED_UNIT_LEN);
        noisy[0] |= 0xC0;
        noisy[4] = 0x47;
        noisy.extend(noise(ALIGNED_UNIT_LEN));
        assert_eq!(
            classify(&noisy),
            Kind::Opaque,
            "a second unit with no seed sync"
        );
    }

    // An Opaque stream is handed through byte-identical, even if a later sector looks like a
    // scrambled pack (a VOB muxed into an MKV attachment stays untouched).
    #[test]
    fn an_opaque_stream_passes_untouched() {
        let mut bytes = b"FMKV\0\x01\0\0".to_vec();
        bytes.resize(SECTOR_BYTES, 0);
        bytes.extend(dvd_pack(0xE0, 1));
        bytes.extend(ts_unit(true));
        let mut stage = Stage::lazy(Cursor::new(bytes.clone()), false);
        let mut out = Vec::new();
        stage.read_to_end(&mut out).unwrap();
        assert_eq!(out, bytes);
        assert_eq!(stage.kind(), Some(Kind::Opaque));
    }

    #[test]
    fn a_byte_stream_refuses_encrypted_content_unless_raw() {
        let mut ps = dvd_pack(0xE0, 0);
        ps.extend(dvd_pack(0xE0, 1));
        let mut ts = ts_unit(false);
        let mut enc = ts_unit(true);
        for p in enc[BD_SOURCE_PACKET_BYTES..].chunks_mut(BD_SOURCE_PACKET_BYTES) {
            p[4] = 0x5C;
        }
        ts.extend(enc);
        for (bytes, code) in [(ps, E_CSS_KEY_MISSING), (ts, E_NO_DISC_KEY)] {
            let mut out = Vec::new();
            let e = Stage::lazy(Cursor::new(bytes.clone()), false)
                .read_to_end(&mut out)
                .unwrap_err();
            assert_eq!(error_code(&e), Some(code));
            let mut raw = Vec::new();
            Stage::lazy(Cursor::new(bytes.clone()), true)
                .read_to_end(&mut raw)
                .unwrap();
            assert_eq!(raw, bytes, "raw passes ciphertext");
        }
    }

    // A block straddling reads is judged once complete; a seek re-syncs to the next boundary.
    #[test]
    fn watched_blocks_are_judged_across_reads_and_seeks() {
        let mut ps = dvd_pack(0xE0, 0);
        ps.extend(dvd_pack(0xE0, 0));
        ps.extend(dvd_pack(0xE0, 2));
        let mut stage = Stage::eager(Cursor::new(ps.clone()), false).unwrap();
        let mut small = [0u8; 700];
        let mut e = None;
        for _ in 0..10 {
            if let Err(x) = stage.read(&mut small) {
                e = Some(x);
                break;
            }
        }
        assert_eq!(error_code(&e.unwrap()), Some(E_CSS_KEY_MISSING));
        let mut stage = Stage::eager(Cursor::new(ps), false).unwrap();
        stage.seek(SeekFrom::Start(100)).unwrap();
        let mut two = vec![0u8; 2 * SECTOR_BYTES - 100];
        stage.read_exact(&mut two).unwrap();
        let mut rest = Vec::new();
        assert!(
            stage.read_to_end(&mut rest).is_err(),
            "the scrambled pack after the seek"
        );
    }

    // X-3: ONE pack test for disc and file: every DVD-Video stream id and scramble value; an
    // IFO/UDF sector (the 38→10-titles guard) or a pack whose 0x14 only looks flagged (stuffed,
    // map-first, MPEG-1 PES) is never scrambled, so the disc path leaves it untouched too.
    #[test]
    fn one_pack_test_serves_disc_and_file() {
        for id in [0xBDu8, 0xBE, 0xBF, 0xC0, 0xC7, 0xE0] {
            for scramble in 0..4u8 {
                let want = (scramble != 0 && !matches!(id, 0xBE | 0xBF)).then_some(0x14);
                let got = scrambled_at(&dvd_pack(id, scramble));
                assert_eq!(got, want, "id {id:#x} scramble {scramble}");
            }
        }
        let mut ifo = b"DVDVIDEO-VTS".to_vec();
        ifo.resize(SECTOR_BYTES, 0x30);
        let mut udf = vec![0u8; SECTOR_BYTES];
        udf[..6].copy_from_slice(&[0x02, 0x00, 0x02, 0x00, 0x30, 0x30]);
        let mut stuffed = dvd_pack(0xE0, 0);
        stuffed[0x0D] = 0xF8 | 1;
        stuffed[0x14] = 0x30;
        let mut map_first = dvd_pack(0xBC, 0);
        map_first[0x14] = 0xB0;
        let mut mpeg1_pes = dvd_pack(0xE0, 0);
        mpeg1_pes[0x14] = 0xFF;
        for s in [
            ifo,
            udf,
            vec![0x30; SECTOR_BYTES],
            stuffed,
            map_first,
            mpeg1_pes,
        ] {
            assert_eq!(scrambled_at(&s), None);
            let mut region = s.clone();
            crate::css::descramble_region(&mut region, &mut [0x42; 5]).unwrap();
            assert_eq!(region, s, "the disc path's descramble leaves it untouched");
        }
    }

    // A VOB with a blank or damaged first sector is still a PS to a file stage, and one
    // damaged unit at a clip's start does not hide its source packets.
    #[test]
    fn damage_at_the_head_does_not_hide_the_stream() {
        let mut vob = vec![0u8; SECTOR_BYTES];
        vob.extend(dvd_pack(0xE0, 1));
        assert_eq!(
            classify(&vob),
            Kind::Opaque,
            "a byte stream needs a pack at 0"
        );
        assert_eq!(classify_sectors(&vob), Kind::Ps { mpeg2: true });
        let mut clip = vec![0u8; ALIGNED_UNIT_LEN];
        clip.extend((0..3).flat_map(|_| ts_unit(true)));
        assert_eq!(classify(&clip), Kind::BdTs, "a zeroed first unit");
        let mut garbled = noise(ALIGNED_UNIT_LEN);
        garbled.extend((0..3).flat_map(|_| ts_unit(false)));
        assert_eq!(classify(&garbled), Kind::BdTs, "a garbled first unit");
    }

    // An FMKV stream is decided from five bytes: its header is parsed with no more data.
    #[test]
    fn a_stream_header_is_not_held_for_a_whole_unit() {
        struct Trickle(Vec<u8>, usize);
        impl Read for Trickle {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                assert!(self.1 < self.0.len(), "the stage read past the sent header");
                let n = buf.len().min(self.0.len() - self.1);
                buf[..n].copy_from_slice(&self.0[self.1..self.1 + n]);
                self.1 += n;
                Ok(n)
            }
        }
        let header = b"FMKV\0\x01\0\0header".to_vec();
        let mut stage = Stage::lazy(Trickle(header.clone(), 0), false);
        let mut got = vec![0u8; header.len()];
        stage.read_exact(&mut got).unwrap();
        assert_eq!(got, header);
        assert_eq!(stage.kind(), Some(Kind::Opaque));
    }

    // Review: head bytes read before a transient error are kept for the retry.
    #[test]
    fn a_head_read_error_keeps_what_was_read() {
        struct Flaky(Vec<u8>, usize, bool);
        impl Read for Flaky {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.1 == 5 && !self.2 {
                    self.2 = true;
                    return Err(io::ErrorKind::ConnectionReset.into());
                }
                let n = buf.len().min(self.0.len() - self.1);
                buf[..n].copy_from_slice(&self.0[self.1..self.1 + n]);
                self.1 += n;
                Ok(n)
            }
        }
        let unit = ts_unit(false);
        let mut stage = Stage::lazy(Flaky(unit.clone(), 0, false), false);
        let mut out = Vec::new();
        assert!(stage.read_to_end(&mut out).is_err());
        stage.read_to_end(&mut out).unwrap();
        assert_eq!(out, unit);
    }

    // Review: a refused read leaves none of its raw bytes for a retry to serve.
    #[test]
    fn a_refused_read_is_refused_again() {
        struct Refuses;
        impl SectorSource for Refuses {
            fn capacity_sectors(&self) -> u32 {
                4
            }
            fn read_sectors(
                &mut self,
                _: u32,
                _: u16,
                buf: &mut [u8],
                _: bool,
            ) -> crate::error::Result<usize> {
                buf.fill(0xEE);
                Err(Error::CssKeyMissing)
            }
        }
        let mut bytes = SectorBytes::new(Refuses, 4 * SECTOR_BYTES as u64);
        let mut out = [0u8; 100];
        assert!(bytes.read(&mut out).is_err());
        assert!(
            bytes.read(&mut out).is_err(),
            "never the refused read's bytes"
        );
    }

    // N9: a source that ends early is an error, never a silent end of stream.
    #[test]
    fn a_source_ending_early_is_unexpected_eof() {
        struct Empty;
        impl SectorSource for Empty {
            fn capacity_sectors(&self) -> u32 {
                4
            }
            fn read_sectors(
                &mut self,
                _: u32,
                _: u16,
                _: &mut [u8],
                _: bool,
            ) -> crate::error::Result<usize> {
                Ok(0)
            }
        }
        let e = SectorBytes::new(Empty, 4 * SECTOR_BYTES as u64)
            .read(&mut [0u8; 100])
            .unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn sector_bytes_clip_at_the_real_length() {
        let data: Vec<u8> = noise(3 * SECTOR_BYTES + 700);
        let mut padded = data.clone();
        padded.resize(4 * SECTOR_BYTES, 0);
        let src = crate::test_util::MemSource::new(padded);
        let mut out = Vec::new();
        SectorBytes::new(src, data.len() as u64)
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, data);
    }
}
