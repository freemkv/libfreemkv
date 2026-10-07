//! Key evidence (pipeline design §2.6): what a source can tell key acquisition, with no
//! `Disc` in it. [`KeyRing::acquire`](super::KeyRing::acquire) takes evidence plus a
//! [`Sampler`] over the same source. Until the Layout stage exists, evidence comes from a
//! scanned disc through [`KeyEvidence::from_disc`].

use super::KeyScope;
use crate::ctx::Ctx;
use crate::disc::{ContentFormat, Disc, DiscFormat, Extent};
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::sector::SectorSource;
use crate::whole_disc::{UNIT, UnitSpan, probe_units, subtract_ranges, unit_head};

/// The identity a ring is bound to: [`KeyRing::is_for`](super::KeyRing::is_for) compares
/// it. No key bytes; the Volume ID only as its fingerprint.
#[derive(Clone, Debug, PartialEq)]
pub struct MediaId {
    /// SHA-1 of the title-key file (`Unit_Key_RO.inf` / VTKF), hex; empty when the medium
    /// has no AACS identity.
    pub disc_hash: String,
    /// Capacity in sectors; 0 when unknown (then never compared).
    pub capacity: u32,
    pub format: DiscFormat,
    /// [`KeyRing::vid_fingerprint`](super::KeyRing::vid_fingerprint) of the Volume ID, when
    /// one is known.
    pub vid_fingerprint: Option<[u8; 32]>,
}

impl Disc {
    /// This disc's [`MediaId`]: its AACS disc hash, capacity, format and, when the
    /// handshake read one, its Volume ID fingerprint.
    pub fn media_id(&self) -> MediaId {
        let aacs = self.aacs.as_ref();
        MediaId {
            // KA9: a capture with no hash is identified by its title-key file.
            disc_hash: aacs.map_or_else(String::new, |a| {
                if a.disc_hash.is_empty() && !a.uk_ro.is_empty() {
                    crate::aacs::inf::disc_hash_hex(&crate::aacs::inf::disc_hash(&a.uk_ro))
                } else {
                    a.disc_hash.clone()
                }
            }),
            capacity: self.capacity_sectors,
            format: self.format,
            vid_fingerprint: aacs
                .map(|a| a.volume_id)
                .filter(|v| *v != [0u8; 16])
                .map(|v| super::vid_fp(&v)),
        }
    }
}

/// How the medium flags its encrypted content and how a key is proven (design X-10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Detector {
    /// BD/UHD CPI flags: keys are proven on ciphertext units.
    Verified,
    /// HD DVD: each pack's `PES_scrambling_control` (`[HD]` §4.3.2). Keys are numbered by the
    /// CPI's `TITLE_KEY_PTR` and proven on decrypted packs, per EVOBU.
    PerPack,
}

impl Detector {
    // The format's own test of one aligned unit, feeding the one encryption decision
    // (`resolve::acquire`): the CPI flag, or any pack flagged scrambled.
    pub(crate) fn unit_encrypted(self, unit: &[u8], format: ContentFormat) -> bool {
        crate::aacs::content::aacs_unit_encrypted(unit, format)
    }
}

/// One unit of key proof (design §2.2): a stream file, a title extent no file covers, or a
/// whole loose file. Spans are sectors `(start, count, unit anchor)`, sorted pieces by
/// first sector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Piece {
    pub spans: Vec<UnitSpan>,
    /// The titles (indices) that play it: only pieces sharing a title lend samples (SG23).
    pub titles: Vec<usize>,
    /// Size in bytes of the largest of those titles: pieces are asked largest first.
    pub rank: u64,
}

impl Piece {
    /// The piece's first sector, its id.
    pub fn id(&self) -> u32 {
        self.spans.first().map_or(0, |s| s.0)
    }

    pub(crate) fn ranges(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.spans.iter().map(|&(s, n, _)| (s, s.saturating_add(n)))
    }

    // Unit heads on the piece's grid, as (first head, unit count) per span. A unit that
    // would cross its span's end is not a unit of the piece (KS-7, Informative).
    pub(crate) fn grid(&self) -> Vec<(u64, u64)> {
        self.spans
            .iter()
            .map(|&(s, n, anchor)| {
                let head = unit_head(s, anchor);
                let end = s as u64 + n as u64;
                (head, end.saturating_sub(head) / UNIT)
            })
            .collect()
    }

    // A loose clip file's one piece: every aligned unit of the file (KS-1) on its own grid
    // from byte 0, title 0.
    pub(crate) fn loose_file(capacity: u32) -> Self {
        Piece {
            spans: vec![(0, capacity, 0)],
            titles: vec![0],
            rank: 0,
        }
    }
}

// The AACS structures a source supplied. Key material: never printed.
pub(crate) struct AacsEvidence {
    pub(crate) version: u8,
    pub(crate) mkb: Vec<u8>,
    pub(crate) unit_key_ro: Vec<u8>,
    // In memory only; `None` when no handshake read one.
    pub(crate) vid: Option<[u8; 16]>,
    // `Num_of_CPS_Unit` the title-key file declares (KS-14); `None` if unparseable.
    pub(crate) n_declared: Option<usize>,
    pub(crate) volume_label: Option<String>,
}

// The main feature: the samples a sample-independent request carries.
pub(crate) struct MainTitle {
    pub(crate) extents: Vec<Extent>,
    pub(crate) format: ContentFormat,
}

/// What a source can tell key acquisition (design §2.6), scoped to what the rip decrypts:
/// the medium's identity and AACS structures, the pieces in scope, the main feature, the
/// detector and the forensic layout. Holds key material (the title-key file, MKB, VID), so
/// its `Debug` redacts them.
pub struct KeyEvidence {
    pub(crate) scope: KeyScope,
    pub(crate) media: MediaId,
    pub(crate) aacs: Option<AacsEvidence>,
    pub(crate) pieces: Vec<Piece>,
    // No stream file was listed (no `/BDMV/STREAM` or `/HVDVD_TS`, or an empty one).
    pub(crate) no_stream_files: bool,
    pub(crate) main: Option<MainTitle>,
    pub(crate) detector: Detector,
    pub(crate) container: ContentFormat,
    pub(crate) fmts: Option<super::fmts::Layout>,
}

impl std::fmt::Debug for KeyEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyEvidence")
            .field("scope", &self.scope)
            .field("media", &self.media)
            .field("aacs", &self.aacs.as_ref().map(|_| "<redacted>"))
            .field("pieces", &self.pieces.len())
            .field("detector", &self.detector)
            .field("container", &self.container)
            .field("fmts", &self.fmts.is_some())
            .finish()
    }
}

impl KeyEvidence {
    /// The scope this evidence was gathered for.
    pub fn scope(&self) -> &KeyScope {
        &self.scope
    }

    /// The medium's identity.
    pub fn media(&self) -> &MediaId {
        &self.media
    }

    /// The pieces in scope, sorted by first sector.
    pub fn pieces(&self) -> &[Piece] {
        &self.pieces
    }

    /// Evidence from a scanned disc over its raw `reader` (the interim adapter until the
    /// Layout stage supplies pieces). Reads the filesystem for the stream files and the
    /// forensic layout. An unreadable filesystem fails a `WholeDisc` scope; a title scope
    /// falls back to its title extents. A clear disc, or `KeyScope::None`, reads nothing.
    pub fn from_disc(
        disc: &Disc,
        reader: &mut dyn SectorSource,
        scope: KeyScope,
        ctx: &Ctx,
    ) -> Result<Self> {
        ctx.halt.check()?;
        let mut ev = KeyEvidence {
            scope,
            media: disc.media_id(),
            aacs: None,
            pieces: Vec::new(),
            no_stream_files: false,
            main: disc.main_title().map(|t| MainTitle {
                extents: t.extents.clone(),
                format: t.content_format,
            }),
            detector: match disc.format {
                DiscFormat::HdDvd => Detector::PerPack,
                _ => Detector::Verified,
            },
            container: disc.content_format,
            fmts: None,
        };
        let (Some(inputs), false) = (disc.inputs(), ev.scope == KeyScope::None) else {
            return Ok(ev);
        };
        let sel: Vec<usize> = match &ev.scope {
            KeyScope::Titles(v) => {
                if let Some(&bad) = v.iter().find(|&&t| t >= disc.titles.len()) {
                    return Err(Error::DiscTitleRange {
                        index: bad,
                        count: disc.titles.len(),
                    });
                }
                v.clone()
            }
            _ => (0..disc.titles.len()).collect(),
        };
        let n_declared = disc.declared_cps_units();
        ev.aacs = Some(AacsEvidence {
            version: inputs.version,
            mkb: inputs.mkb,
            unit_key_ro: inputs.unit_key_ro,
            vid: Some(inputs.volume_id).filter(|v| *v != [0u8; 16]),
            n_declared,
            volume_label: inputs.volume_label,
        });
        let whole = ev.scope == KeyScope::WholeDisc;
        // The filesystem the scan already read, re-read as the scan reads it: batched, with
        // the metadata partition prefetched, so the tree walk and every stream file's File
        // Entry cost a few commands, not one per sector.
        let mut buffered = crate::udf::BufferedSectorReader::new(reader, FS_BATCH_SECTORS);
        // An unreadable filesystem fails a whole-disc copy (it would otherwise ship
        // ciphertext); a title rip keys its title extents instead.
        let fs = match buffered_filesystem(&mut buffered) {
            Ok(fs) => Some(fs),
            Err(e) if whole => return Err(e),
            Err(e) => {
                tracing::warn!(target: "freemkv::keys", error = %e, "no filesystem: keying title extents");
                None
            }
        };
        let files = match &fs {
            Some(fs) => match crate::whole_disc::content_files_in(fs, &mut buffered) {
                Ok(f) => f,
                Err(e) if whole => return Err(e),
                Err(e) => {
                    tracing::warn!(target: "freemkv::keys", error = %e, "no file list: keying title extents");
                    Vec::new()
                }
            },
            None => Vec::new(),
        };
        ev.no_stream_files = files.is_empty();
        ev.pieces = pieces(disc, &files, &sel, whole);
        // HD DVD: listed whatever the declared count (acquisition judges a copy in the clear
        // from its pieces before it refuses a multi-key HD DVD); it has no forensic layout.
        if ev.detector != Detector::PerPack {
            ev.fmts = match &fs {
                Some(fs) => super::fmts::layout(fs, &mut buffered)?,
                None => None,
            };
        }
        Ok(ev)
    }
}

/// Sectors per batched read of the filesystem: the optical default the scan uses.
const FS_BATCH_SECTORS: u16 = crate::disc::DEFAULT_BATCH_SECTORS_OPTICAL;

// The UDF tree, then the metadata partition (every directory and File Entry) prefetched.
fn buffered_filesystem<S: SectorSource + ?Sized>(
    reader: &mut crate::udf::BufferedSectorReader<'_, S>,
) -> Result<crate::udf::UdfFs> {
    let fs = crate::udf::read_filesystem(reader)?;
    reader.prefetch(fs.metadata_start(), fs.metadata_sectors())?;
    Ok(fs)
}

pub(crate) fn sorted_ranges(mut v: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    v.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(v.len());
    for (s, e) in v {
        match out.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

// The pieces of `sel`: each content file the scope touches (on its own unit grid, KS-1),
// minus sectors an earlier file claimed (an SSIF re-lists m2ts sectors), then title extents
// no file covers (anchored at the extent start).
pub(crate) fn pieces(
    disc: &Disc,
    files: &[Vec<(u32, u32)>],
    sel: &[usize],
    whole: bool,
) -> Vec<Piece> {
    let title_ranges: Vec<(usize, u32, u32)> = sel
        .iter()
        .flat_map(|&t| {
            disc.titles[t]
                .extents
                .iter()
                .filter(|e| e.sector_count > 0)
                .map(move |e| (t, e.start_lba, e.start_lba.saturating_add(e.sector_count)))
        })
        .collect();
    let touches = |ranges: &[(u32, u32)]| -> Vec<usize> {
        let mut t: Vec<usize> = title_ranges
            .iter()
            .filter(|&&(_, s, e)| super::overlaps(ranges, s, e))
            .map(|&(t, _, _)| t)
            .collect();
        t.sort_unstable();
        t.dedup();
        t
    };
    let mut claimed: Vec<(u32, u32)> = Vec::new();
    let mut out = Vec::new();
    let push = |spans: Vec<UnitSpan>, out: &mut Vec<Piece>| {
        let ranges: Vec<(u32, u32)> = spans
            .iter()
            .map(|&(s, n, _)| (s, s.saturating_add(n)))
            .collect();
        let titles = touches(&ranges);
        let rank = titles
            .iter()
            .map(|&t| disc.titles[t].size_bytes)
            .max()
            .unwrap_or(0);
        out.push(Piece {
            spans,
            titles,
            rank,
        });
    };
    for file in files {
        let ends: Vec<(u32, u32)> = file
            .iter()
            .map(|&(s, n)| (s, s.saturating_add(n)))
            .collect();
        if !whole && touches(&ends).is_empty() {
            continue;
        }
        let mut spans = Vec::new();
        let mut off = 0u64;
        for &(lba, n) in file {
            let anchor = (lba as u64).saturating_sub(off % UNIT);
            for (s, e) in subtract_ranges(&[(lba, n)], &claimed) {
                spans.push((s, e - s, anchor));
            }
            off += n as u64;
        }
        claimed = sorted_ranges(claimed.into_iter().chain(ends).collect());
        if !spans.is_empty() {
            push(spans, &mut out);
        }
    }
    for &(_, s, e) in &title_ranges {
        let left = subtract_ranges(&[(s, e - s)], &claimed);
        if left.is_empty() {
            continue;
        }
        let spans = left.iter().map(|&(a, b)| (a, b - a, s as u64)).collect();
        claimed = sorted_ranges(claimed.into_iter().chain(left).collect());
        push(spans, &mut out);
    }
    out.sort_by_key(Piece::id);
    out
}

/// The one way key acquisition reads ciphertext (design KA6): whole aligned units from the
/// raw (never decrypted) source the evidence describes. Random access is required.
pub struct Sampler<'a> {
    reader: &'a mut dyn SectorSource,
    stats: ReadStats,
}

/// Read accounting for the sampler's units: how long key acquisition spent on the drive.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReadStats {
    pub(crate) reads: u32,
    pub(crate) faults: u32,
    pub(crate) total: std::time::Duration,
    pub(crate) slowest: std::time::Duration,
}

/// A sampled unit: `Enc` ciphertext, `Clear`, or a `Fault` (a soft read error).
pub(crate) enum Sample {
    Enc(Vec<u8>),
    Clear,
    Fault,
}

impl<'a> Sampler<'a> {
    /// A sampler over the raw source `reader`.
    pub fn new(reader: &'a mut dyn SectorSource) -> Self {
        Sampler {
            reader,
            stats: ReadStats::default(),
        }
    }

    pub(crate) fn stats(&self) -> ReadStats {
        self.stats
    }

    pub(crate) fn source(&mut self) -> &mut dyn SectorSource {
        self.reader
    }

    // The aligned unit at `lba`, or `None` on a short or failed read. A Stop, a gone source
    // or a transport failure is `Err`: not a soft fault.
    pub(crate) fn unit(&mut self, lba: u32) -> Result<Option<Vec<u8>>> {
        let t0 = std::time::Instant::now();
        let r = read_unit(self.reader, lba);
        let took = t0.elapsed();
        self.stats.reads += 1;
        self.stats.total += took;
        self.stats.slowest = self.stats.slowest.max(took);
        if matches!(r, Ok(None)) {
            self.stats.faults += 1;
        }
        tracing::trace!(target: "freemkv::keys", lba, took_ms = took.as_millis() as u64, "sample unit read");
        r
    }

    // Up to 32 units on `p`'s grid (KU §2.3 step 7), skipping FMTS segment units, each
    // judged by `detector`.
    pub(crate) fn probe(
        &mut self,
        p: &Piece,
        segments: &[(u32, u32)],
        format: ContentFormat,
        detector: Detector,
        halt: &Halt,
    ) -> Result<(u64, Vec<Sample>)> {
        let (units, lbas) = probe_lbas(p, segments);
        let mut out = Vec::new();
        for lba in lbas {
            halt.check()?;
            out.push(match self.unit(lba)? {
                Some(u) if detector.unit_encrypted(&u, format) => Sample::Enc(u),
                Some(_) => Sample::Clear,
                None => Sample::Fault,
            });
        }
        Ok((units, out))
    }

    // The first encrypted unit on `p`'s probe grid, stopping there: a stream file sits in one
    // CPS unit (KS-10), so one unit names its key. (grid units, the unit, soft faults seen).
    pub(crate) fn first_encrypted(
        &mut self,
        p: &Piece,
        segments: &[(u32, u32)],
        format: ContentFormat,
        detector: Detector,
        halt: &Halt,
    ) -> Result<(u64, Option<Vec<u8>>, usize)> {
        let (units, lbas) = probe_lbas(p, segments);
        let mut faults = 0;
        for lba in lbas {
            halt.check()?;
            match self.unit(lba)? {
                Some(u) if detector.unit_encrypted(&u, format) => {
                    return Ok((units, Some(u), faults));
                }
                Some(_) => {}
                None => faults += 1,
            }
        }
        Ok((units, None, faults))
    }

    // Up to `n` encrypted units on `p`'s probe grid: enough ciphertext to ask a source for a
    // unit the first request left unopened.
    pub(crate) fn encrypted_units(
        &mut self,
        p: &Piece,
        segments: &[(u32, u32)],
        format: ContentFormat,
        detector: Detector,
        halt: &Halt,
        n: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let (_, lbas) = probe_lbas(p, segments);
        let mut out = Vec::new();
        for lba in lbas {
            if out.len() >= n {
                break;
            }
            halt.check()?;
            if let Some(u) = self.unit(lba)?
                && detector.unit_encrypted(&u, format)
            {
                out.push(u);
            }
        }
        Ok(out)
    }

    // Up to `n` encrypted units spread over the main feature, none from an FMTS segment
    // (`segments`: its units carry a forensic key, not the unit key).
    pub(crate) fn main_samples(
        &mut self,
        main: Option<&MainTitle>,
        n: usize,
        segments: &[(u32, u32)],
    ) -> Vec<Vec<u8>> {
        let t0 = std::time::Instant::now();
        let out = main
            .map(|m| {
                crate::keysource::encrypted_units_outside(
                    self.reader,
                    &m.extents,
                    m.format,
                    n,
                    segments,
                )
            })
            .unwrap_or_default();
        tracing::info!(target: "freemkv::keys", phase = "main_samples", wanted = n, got = out.len(), elapsed_ms = t0.elapsed().as_millis() as u64, "main-title samples read");
        out
    }
}

// The units `probe` samples on `p`'s grid: (the grid's unit count, their LBAs), FMTS
// segment units skipped.
fn probe_lbas(p: &Piece, segments: &[(u32, u32)]) -> (u64, Vec<u32>) {
    let grid = p.grid();
    let units: u64 = grid.iter().map(|g| g.1).sum();
    let mut out = Vec::new();
    for idx in probe_units(units) {
        let mut k = idx;
        let Some(&(head, _)) = grid.iter().find(|g| {
            let hit = k < g.1;
            if !hit {
                k -= g.1;
            }
            hit
        }) else {
            continue;
        };
        let Ok(lba) = u32::try_from(head + k * UNIT) else {
            continue;
        };
        if !in_segment(segments, lba) {
            out.push(lba);
        }
    }
    (units, out)
}

pub(crate) fn read_unit(reader: &mut dyn SectorSource, lba: u32) -> Result<Option<Vec<u8>>> {
    let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
    match reader.read_sectors(lba, UNIT as u16, &mut buf, false) {
        Ok(n) if n == buf.len() => Ok(Some(buf)),
        Err(e) if fatal_read(&e) => Err(e),
        _ => Ok(None),
    }
}

pub(crate) fn fatal_read(e: &Error) -> bool {
    matches!(e, Error::Halted)
        || e.is_source_terminated()
        || (e.is_scsi_transport_failure() && !matches!(e, Error::IoError { .. }))
}

fn in_segment(segments: &[(u32, u32)], lba: u32) -> bool {
    let i = segments.partition_point(|s| s.0 <= lba);
    i > 0 && lba < segments[i - 1].1
}
