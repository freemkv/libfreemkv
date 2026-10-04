//! Content-based forced-subtitle detection for DVD VobSub tracks.
//!
//! The DVD counterpart of [`super::pgs_forced_probe`]: a subpicture unit (SPU) whose display
//! control sequence opens with FSTA_DSP (0x00, "Forced Start Display") is shown even with
//! subtitles off. A track is forced iff EVERY sampled displayed SPU is forced and at least one
//! was seen. Reads are bounded by the same budget and sample windows as the PGS probe, and the
//! probe only ever SETS `forced`: the IFO code extension 9 is an equal signal it never clears.

use super::pgs_forced_probe::{PROBE_BUDGET_SECTORS, PROMOTE_MIN_DISPLAY_SETS, plan_windows};
use crate::consts::SECTOR_BYTES;
use crate::disc::{Codec, DiscTitle, Extent, LabelQualifier, Stream};
use crate::mux::ps::PsDemuxer;
use crate::sector::SectorSource;
use std::collections::HashMap;

// Sectors per read (~2 MiB), the PGS probe's chunk.
const CHUNK_SECTORS: u16 = 1023;

// Sectors per read of the CSS crack, the image scan's batch.
const CRACK_BATCH_SECTORS: u16 = 32;

// An SPU's 16-bit size field bounds it; a unit growing past this is corrupt.
const MAX_SPU_BYTES: usize = 0xFFFF;

// SP_DCSQ commands (mpucoder "Sub-Pictures"; FFmpeg dvdsubdec.c decode_dvd_subtitles).
const FSTA_DSP: u8 = 0x00;
const STA_DSP: u8 = 0x01;
const CMD_END: u8 = 0xFF;

/// How a displayed subpicture unit is started.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SpuStart {
    /// FSTA_DSP: shown even when the player's subtitles are off.
    Forced,
    /// STA_DSP: shown only when the stream is selected.
    Normal,
}

/// How the SPU in `spu` (SPUH included) starts displaying, from the first FSTA_DSP or STA_DSP
/// command along its SP_DCSQ chain; `None` for a unit that never starts a display or does not
/// parse. Offsets are big-endian u16s relative to the SPU start.
pub(crate) fn spu_start(spu: &[u8]) -> Option<SpuStart> {
    let word = |o: usize| -> Option<usize> {
        Some(u16::from_be_bytes([*spu.get(o)?, *spu.get(o + 1)?]) as usize)
    };
    let size = word(0)?.min(spu.len());
    let mut dcsq = word(2)?;
    // Each SP_DCSQ moves forward or points at itself (the last one); bound the walk anyway.
    for _ in 0..64 {
        if dcsq + 4 > size {
            return None;
        }
        let next = word(dcsq + 2)?;
        let mut pos = dcsq + 4;
        while pos < size {
            let cmd = spu[pos];
            pos += 1;
            let args = match cmd {
                FSTA_DSP => return Some(SpuStart::Forced),
                STA_DSP => return Some(SpuStart::Normal),
                0x02 => 0,
                0x03 | 0x04 => 2,
                0x05 => 6,
                0x06 => 4,
                // CHG_COLCON: a size word counting itself, then its parameters.
                0x07 => word(pos)?,
                CMD_END => break,
                _ => return None,
            };
            pos += args;
        }
        if next <= dcsq {
            return None;
        }
        dcsq = next;
    }
    None
}

// Reassembles one sub-stream's SPUs: a PES with a PTS starts a unit whose first two bytes give
// its size; PTS-less PES continue it. A gap drops the open unit rather than splice across it.
#[derive(Default)]
struct SpuAssembler {
    pending: Option<(usize, Vec<u8>)>,
}

impl SpuAssembler {
    fn push(&mut self, starts_unit: bool, data: &[u8]) -> Option<Vec<u8>> {
        if starts_unit {
            let size = match data {
                [hi, lo, ..] => u16::from_be_bytes([*hi, *lo]) as usize,
                _ => 0,
            };
            self.pending = (size >= 4).then(|| (size, Vec::with_capacity(size)));
        }
        let (size, buf) = self.pending.as_mut()?;
        buf.extend_from_slice(data);
        if buf.len() > MAX_SPU_BYTES {
            self.pending = None;
            return None;
        }
        if buf.len() < *size {
            return None;
        }
        let (size, mut buf) = self.pending.take()?;
        buf.truncate(size);
        Some(buf)
    }

    fn gap(&mut self) {
        self.pending = None;
    }
}

// What the sampled SPUs showed about one subpicture track.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
struct SpuEvidence {
    /// Displayed SPUs seen.
    units: u32,
    /// At least one of them started with STA_DSP.
    non_forced: bool,
}

impl SpuEvidence {
    fn observe(&mut self, start: SpuStart) {
        self.units = self.units.saturating_add(1);
        self.non_forced |= start == SpuStart::Normal;
    }
}

// Why the read loop stopped; as in the PGS probe, only a designed stop lets "no non-forced SPU
// was seen" stand as a forced verdict.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StopReason {
    Exhausted,
    Budget,
    Halted,
    ReadFailed,
}

/// Conclusive evidence of probed titles keyed by their extent list (with whether it was a
/// sample), so a title reading the same cells as one already probed is not read again.
#[derive(Default)]
pub(crate) struct DvdForcedProbeCache(HashMap<Vec<(u32, u32)>, TitleEvidence>);

// One probed title's per-track evidence, and whether it rests on a sample.
type TitleEvidence = (HashMap<u16, SpuEvidence>, bool);

// CSS for the probe's reads: the first scrambled pack runs the title's one keyless crack
// (the mux's own), after which packs are descrambled; with no key a scrambled pack is a gap.
#[derive(Default)]
struct Descrambler {
    key: Option<[u8; 5]>,
    tried: bool,
}

impl Descrambler {
    // Leaves `sector` parseable (`Ok(true)`), or `Ok(false)` for a sector to skip; `Err` is a
    // Stop during the crack.
    fn clear(
        &mut self,
        sector: &mut [u8],
        reader: &mut dyn SectorSource,
        extents: &[Extent],
        halt: Option<&crate::halt::Halt>,
    ) -> Result<bool, StopReason> {
        if !crate::css::is_scrambled_pack(sector) {
            return Ok(true);
        }
        if !self.tried {
            self.tried = true;
            match crate::css::crack_key_outcome(reader, extents, CRACK_BATCH_SECTORS, halt) {
                crate::css::CrackOutcome::Cracked(state) => self.key = Some(state.title_key),
                crate::css::CrackOutcome::Halted => return Err(StopReason::Halted),
                _ => {}
            }
        }
        Ok(match self.key.as_mut() {
            Some(key) => crate::css::descramble_region(sector, key).is_ok(),
            None => false,
        })
    }
}

/// Read a sample of the title's VobSub streams and set `forced` on each whose sampled SPUs all
/// start with FSTA_DSP. Best-effort: never fails, never clears a flag, and an inconclusive run
/// (halt, read fault) asserts nothing and is not memoised.
pub(crate) fn probe_and_set_forced(
    reader: &mut dyn SectorSource,
    title: &mut DiscTitle,
    cache: &mut DvdForcedProbeCache,
    halt: Option<&crate::halt::Halt>,
) {
    // Streams the IFO already marks forced have nothing left to learn.
    let pids: Vec<u16> = title
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Subtitle(sub) if sub.codec == Codec::DvdSub && !sub.forced => Some(sub.pid),
            _ => None,
        })
        .collect();
    if pids.is_empty() || title.extents.is_empty() {
        return;
    }
    let key: Vec<(u32, u32)> = title
        .extents
        .iter()
        .map(|e| (e.start_lba, e.sector_count))
        .collect();
    if let Some((known, sampled)) = cache.0.get(&key)
        && pids.iter().all(|p| known.contains_key(p))
    {
        let forced = verdicts(known, *sampled);
        apply_forced(title, &forced, true);
        return;
    }

    let (evidence, stop, sampled, sectors_read) =
        read_evidence(reader, &title.extents, &pids, halt);
    let conclusive = matches!(stop, StopReason::Exhausted | StopReason::Budget);
    let forced = apply_forced(title, &verdicts(&evidence, sampled), conclusive);
    tracing::debug!(
        target: "freemkv::scan",
        title = title.playlist_id,
        stop = ?stop,
        sectors_read,
        tracks = pids.len(),
        forced,
        "dvd forced-subtitle probe"
    );
    if conclusive {
        cache.0.insert(key, (evidence, sampled));
    } else {
        tracing::debug!(
            target: "freemkv::scan",
            stop = ?stop,
            sectors_read,
            tracks = pids.len(),
            "forced-subtitle probe truncated; verdicts limited and truncated extents not cached"
        );
    }
}

// Sample the extents for the tracks in `pids`: (evidence, why reading stopped, whether the read
// was a sample rather than the whole title, sectors read).
fn read_evidence(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    pids: &[u16],
    halt: Option<&crate::halt::Halt>,
) -> (HashMap<u16, SpuEvidence>, StopReason, bool, u32) {
    let mut evidence: HashMap<u16, SpuEvidence> =
        pids.iter().map(|&p| (p, SpuEvidence::default())).collect();
    let total: u64 = extents.iter().map(|e| u64::from(e.sector_count)).sum();
    let share = |n: u32| -> u32 {
        if total == 0 {
            return 0;
        }
        (u64::from(PROBE_BUDGET_SECTORS) * u64::from(n) / total).min(u64::from(u32::MAX)) as u32
    };
    // Once every track has shown a non-forced SPU, nothing further can change a verdict.
    let settled = |ev: &HashMap<u16, SpuEvidence>| ev.values().all(|e| e.non_forced);

    let mut buf = vec![0u8; CHUNK_SECTORS as usize * SECTOR_BYTES];
    let mut css = Descrambler::default();
    let mut sectors_read: u32 = 0;
    let mut sampled = false;
    for ext in extents {
        let plan = plan_windows(ext.sector_count, share(ext.sector_count));
        sampled |= !matches!(plan.as_slice(), [w] if w.offset == 0 && w.len == ext.sector_count);
        reader.set_unit_base(ext.start_lba);
        for window in &plan {
            // Demux and reassembly state are per window: windows are discontiguous.
            let mut demux = PsDemuxer::new();
            let mut spus: HashMap<u16, SpuAssembler> =
                pids.iter().map(|&p| (p, SpuAssembler::default())).collect();
            let Some(mut lba) = ext.start_lba.checked_add(window.offset) else {
                continue;
            };
            let mut remaining = window.len;
            while remaining > 0 {
                if halt.is_some_and(|h| h.is_cancelled()) {
                    return (evidence, StopReason::Halted, true, sectors_read);
                }
                if sectors_read >= PROBE_BUDGET_SECTORS {
                    return (evidence, StopReason::Budget, true, sectors_read);
                }
                let count = remaining
                    .min(u32::from(CHUNK_SECTORS))
                    .min(PROBE_BUDGET_SECTORS - sectors_read) as u16;
                let want = count as usize * SECTOR_BYTES;
                let got = match reader.read_sectors(lba, count, &mut buf[..want], false) {
                    Ok(n) => (n.min(want) / SECTOR_BYTES) as u32,
                    Err(_) => return (evidence, StopReason::ReadFailed, true, sectors_read),
                };
                if got == 0 {
                    return (evidence, StopReason::ReadFailed, true, sectors_read);
                }
                for sector in buf[..got as usize * SECTOR_BYTES].chunks_mut(SECTOR_BYTES) {
                    let clear = match css.clear(sector, reader, extents, halt) {
                        Ok(clear) => clear,
                        Err(stop) => return (evidence, stop, true, sectors_read),
                    };
                    if !clear {
                        observe(demux.flush(), &mut spus, &mut evidence);
                        spus.values_mut().for_each(SpuAssembler::gap);
                        continue;
                    }
                    observe(demux.feed(sector), &mut spus, &mut evidence);
                }
                lba = lba.saturating_add(got);
                remaining = remaining.saturating_sub(got);
                sectors_read += got;
                if settled(&evidence) {
                    // Extents past this one go unread: the evidence is a sample.
                    return (evidence, StopReason::Exhausted, true, sectors_read);
                }
            }
            observe(demux.flush(), &mut spus, &mut evidence);
        }
    }
    (evidence, StopReason::Exhausted, sampled, sectors_read)
}

// Feed demuxed packets to their tracks' SPU reassembly, recording each completed unit's start.
fn observe(
    packets: Vec<crate::mux::ps::PsPacket>,
    spus: &mut HashMap<u16, SpuAssembler>,
    evidence: &mut HashMap<u16, SpuEvidence>,
) {
    for p in packets {
        let Some(pid) = p.dvd_pid() else { continue };
        let (Some(asm), Some(ev)) = (spus.get_mut(&pid), evidence.get_mut(&pid)) else {
            continue;
        };
        if let Some(start) = asm
            .push(p.pts.is_some(), &p.data)
            .as_deref()
            .and_then(spu_start)
        {
            ev.observe(start);
        }
    }
}

// The tracks the evidence calls forced: seen, never non-forced, and over a sample at least
// PROMOTE_MIN_DISPLAY_SETS units, the PGS probe's guard against one stray forced unit.
fn verdicts(evidence: &HashMap<u16, SpuEvidence>, sampled: bool) -> HashMap<u16, SpuEvidence> {
    evidence
        .iter()
        .filter(|(_, e)| e.units > 0 && !e.non_forced)
        .filter(|(_, e)| !sampled || e.units >= PROMOTE_MIN_DISPLAY_SETS)
        .map(|(&p, &e)| (p, e))
        .collect()
}

// Mark the tracks in `forced` as forced when the run that found them may assert it; returns
// how many were marked.
fn apply_forced(
    title: &mut DiscTitle,
    forced: &HashMap<u16, SpuEvidence>,
    conclusive: bool,
) -> usize {
    if !conclusive {
        return 0;
    }
    let mut marked = 0;
    for s in &mut title.streams {
        if let Stream::Subtitle(sub) = s
            && sub.codec == Codec::DvdSub
            && forced.contains_key(&sub.pid)
        {
            sub.forced = true;
            if sub.qualifier == LabelQualifier::None {
                sub.qualifier = LabelQualifier::Forced;
            }
            marked += 1;
        }
    }
    marked
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::SubtitleStream;

    // An SPU: header, 4 pixel bytes, a first SP_DCSQ opening with `start` (plus the usual
    // SET_COLOR/SET_CONTR/SET_DAREA/SET_DSPXA), then a last, self-pointing STP_DSP SP_DCSQ.
    fn spu(start: u8) -> Vec<u8> {
        let mut u = vec![0, 0, 0, 0, 0x11, 0x22, 0x33, 0x44];
        let first = u.len();
        u.extend_from_slice(&[0, 0, 0, 0, start]);
        u.extend_from_slice(&[0x03, 0x01, 0x23, 0x04, 0xFF, 0xF0]);
        u.extend_from_slice(&[0x05, 0, 0, 0x2C, 0, 0, 0x1F, 0x06, 0, 4, 0, 6, CMD_END]);
        let last = u.len();
        u.extend_from_slice(&[0, 0x10, 0, 0, 0x02, CMD_END]);
        u[first + 2..first + 4].copy_from_slice(&(last as u16).to_be_bytes());
        u[last + 2..last + 4].copy_from_slice(&(last as u16).to_be_bytes());
        let len = u.len() as u16;
        u[0..2].copy_from_slice(&len.to_be_bytes());
        u[2..4].copy_from_slice(&(first as u16).to_be_bytes());
        u
    }

    #[test]
    fn spu_start_reads_the_first_display_command() {
        assert_eq!(spu_start(&spu(FSTA_DSP)), Some(SpuStart::Forced));
        assert_eq!(spu_start(&spu(STA_DSP)), Some(SpuStart::Normal));
        // A unit that only stops a display starts none.
        assert_eq!(spu_start(&spu(0x02)), None);
        // Truncated or garbage units do not parse.
        assert_eq!(spu_start(&spu(FSTA_DSP)[..6]), None);
        assert_eq!(spu_start(&[0, 8, 0, 4, 0, 0, 0, 4]), None);
    }

    #[test]
    fn spu_start_skips_a_chg_colcon_parameter_area() {
        let mut u = vec![0, 0, 0, 4, 0, 0, 0, 4];
        // CHG_COLCON: size word 6 (itself + one LN_CTLI end marker), then STA_DSP.
        u.extend_from_slice(&[0x07, 0, 6, 0x0F, 0xFF, 0xFF, 0xFF, STA_DSP, CMD_END]);
        let len = u.len() as u16;
        u[0..2].copy_from_slice(&len.to_be_bytes());
        assert_eq!(spu_start(&u), Some(SpuStart::Normal));
    }

    #[test]
    fn assembler_joins_a_unit_split_across_packets() {
        let unit = spu(FSTA_DSP);
        let mut asm = SpuAssembler::default();
        assert_eq!(asm.push(true, &unit[..10]), None);
        assert_eq!(asm.push(false, &unit[10..]), Some(unit.clone()));
        // A gap drops the open unit; its continuation is not spliced on.
        assert_eq!(asm.push(true, &unit[..10]), None);
        asm.gap();
        assert_eq!(asm.push(false, &unit[10..]), None);
    }

    // One 2048-byte DVD pack carrying `spu` whole on sub-stream `sub`, padded to the sector.
    fn sector(sub: u8, spu: &[u8]) -> Vec<u8> {
        let mut s = vec![0, 0, 1, 0xBA, 0x44, 0, 4, 0, 4, 1, 1, 0x89, 0xC3, 0xF8];
        let pes_len = (3 + 5 + 1 + spu.len()) as u16;
        s.extend_from_slice(&[0, 0, 1, 0xBD]);
        s.extend_from_slice(&pes_len.to_be_bytes());
        s.extend_from_slice(&[0x81, 0x80, 0x05, 0x21, 0, 1, 0, 1, sub]);
        s.extend_from_slice(spu);
        let pad = (2048 - s.len() - 6) as u16;
        s.extend_from_slice(&[0, 0, 1, 0xBE]);
        s.extend_from_slice(&pad.to_be_bytes());
        s.resize(2048, 0xFF);
        s
    }

    struct Image(Vec<u8>);
    impl SectorSource for Image {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let at = lba as usize * SECTOR_BYTES;
            let n = (count as usize * SECTOR_BYTES).min(self.0.len().saturating_sub(at));
            buf[..n].copy_from_slice(&self.0[at..at + n]);
            Ok(n)
        }
    }

    fn vobsub(pid: u16) -> Stream {
        Stream::Subtitle(SubtitleStream {
            pid,
            codec: Codec::DvdSub,
            language: "deu".into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    }

    fn probed(sectors: &[Vec<u8>], pids: &[u16], halt: Option<&crate::halt::Halt>) -> Vec<bool> {
        let mut title = DiscTitle {
            content_format: crate::disc::ContentFormat::DvdPs,
            streams: pids.iter().map(|&p| vobsub(p)).collect(),
            extents: vec![Extent {
                start_lba: 0,
                sector_count: sectors.len() as u32,
            }],
            ..DiscTitle::empty()
        };
        let mut reader = Image(sectors.concat());
        let mut cache = DvdForcedProbeCache::default();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, halt);
        title
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Subtitle(s) => Some(s.forced),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_stream_of_only_forced_units_is_forced_and_a_mixed_one_is_not() {
        let sectors = vec![
            sector(0x20, &spu(FSTA_DSP)),
            sector(0x21, &spu(FSTA_DSP)),
            sector(0x20, &spu(FSTA_DSP)),
            sector(0x21, &spu(STA_DSP)),
            sector(0x22, &spu(STA_DSP)),
        ];
        // 0x20 all forced; 0x21 mixes both; 0x22 never forced; 0x23 never seen.
        assert_eq!(
            probed(&sectors, &[0x20, 0x21, 0x22, 0x23], None),
            vec![true, false, false, false]
        );
    }

    #[test]
    fn a_halted_probe_asserts_nothing() {
        let sectors = vec![sector(0x20, &spu(FSTA_DSP)), sector(0x20, &spu(FSTA_DSP))];
        let halt = crate::halt::Halt::new();
        halt.cancel();
        assert_eq!(probed(&sectors, &[0x20], Some(&halt)), vec![false]);
    }
}
