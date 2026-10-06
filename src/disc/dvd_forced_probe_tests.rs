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

// A title of thousands of small extents (interleaved units) is sampled across its length
// within the budget, not read from its head until the budget runs out.
#[test]
fn many_small_extents_are_sampled_across_the_title() {
    struct Spy(Vec<(u32, u16)>);
    impl SectorSource for Spy {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.0.push((lba, count));
            let n = count as usize * SECTOR_BYTES;
            buf[..n].fill(0);
            Ok(n)
        }
    }
    let extents: Vec<Extent> = (0..5000)
        .map(|i| Extent {
            start_lba: i * 200,
            sector_count: 100,
        })
        .collect();
    let mut reader = Spy(Vec::new());
    let (_, _, sampled, read) = read_evidence(&mut reader, &extents, &[0x20], None);
    assert!(sampled);
    assert!(read <= PROBE_BUDGET_SECTORS, "read {read}");
    let last = reader.0.iter().map(|&(lba, _)| lba).max().unwrap();
    assert!(last >= 4000 * 200, "reads stop at LBA {last}");
}

// A window's title sectors map onto the extents it crosses, in read order.
#[test]
fn title_runs_cross_extent_boundaries() {
    let extents = [
        Extent {
            start_lba: 10,
            sector_count: 5,
        },
        Extent {
            start_lba: 100,
            sector_count: 5,
        },
    ];
    assert_eq!(title_runs(&extents, 3, 4), vec![(10, 13, 2), (100, 100, 2)]);
    assert_eq!(title_runs(&extents, 5, 5), vec![(100, 100, 5)]);
    assert!(title_runs(&extents, 10, 5).is_empty());
}

#[test]
fn a_halted_probe_asserts_nothing() {
    let sectors = vec![sector(0x20, &spu(FSTA_DSP)), sector(0x20, &spu(FSTA_DSP))];
    let halt = crate::halt::Halt::new();
    halt.cancel();
    assert_eq!(probed(&sectors, &[0x20], Some(&halt)), vec![false]);
}
