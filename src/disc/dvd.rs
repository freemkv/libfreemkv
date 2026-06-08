//! DVD title scanning — IFO parsing, stream mapping, VOB extent building.

use super::*;
use crate::ifo;
use crate::sector::SectorSource;
use crate::udf;

impl Disc {
    /// Scan DVD titles from IFO files (VIDEO_TS.IFO + VTS_XX_0.IFO).
    pub(super) fn scan_dvd_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Vec<DiscTitle> {
        let dvd_info = match ifo::parse_vmg(reader, udf_fs) {
            Ok(info) => info,
            Err(_) => return Vec::new(),
        };

        let mut titles = Vec::new();
        let mut title_number: u16 = 0;

        for ts in &dvd_info.title_sets {
            let video_stream = Stream::Video(VideoStream {
                pid: 0xE0, // DVD video PID (standard MPEG PS video stream)
                codec: ts.video.codec,
                resolution: ts.video.resolution,
                frame_rate: match ts.video.standard.as_str() {
                    "PAL" => FrameRate::F25,
                    _ => FrameRate::F29_97,
                },
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                secondary: false,
                label: String::new(),
            });

            // Map DvdAudioAttr to Stream::Audio. The PID is derived from the
            // stream's REAL on-wire private_stream_1 sub-stream id (assigned
            // by per-codec ordinal in the IFO scan) via the same
            // `dvd_audio_pid` table the demuxer's `PsPacket::dvd_pid` uses,
            // so a mixed-codec title (AC-3 + DTS + LPCM) routes correctly
            // instead of colliding on 0xBD00. Streams carried as a regular
            // MPEG-audio PES (MP1/MP2, no sub-id) fall back to a distinct
            // 0xBD00+ordinal PID — disjoint from the 0xBD80+ canonical audio
            // space — though they are not routed via `dvd_pid` today.
            let audio_streams: Vec<Stream> = ts
                .audio_streams
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    let codec = a.codec;
                    let pid = a
                        .sub_stream_id
                        .and_then(crate::mux::ps::dvd_audio_pid)
                        .unwrap_or(0xBD00 + i as u16);
                    Stream::Audio(AudioStream {
                        pid,
                        codec,
                        channels: AudioChannels::from_count(a.channels),
                        language: a.language.clone(),
                        sample_rate: SampleRate::from_hz(a.sample_rate),
                        secondary: false,
                        purpose: crate::disc::LabelPurpose::Normal,
                        label: String::new(),
                    })
                })
                .collect();

            for dvd_title in &ts.titles {
                title_number += 1;

                // Build extents from cell sector ranges (absolute = vob_start + cell offset)
                let extents: Vec<Extent> = dvd_title
                    .cells
                    .iter()
                    .map(|cell| {
                        let start = ts.vob_start_sector.saturating_add(cell.first_sector);
                        let count = cell
                            .last_sector
                            .saturating_sub(cell.first_sector)
                            .saturating_add(1);
                        Extent {
                            start_lba: start,
                            sector_count: count,
                        }
                    })
                    .collect();

                let size_bytes: u64 = extents.iter().map(|e| e.sector_count as u64 * 2048).sum();

                // Build pre-formatted palette codec_data for VobSub subtitle streams
                let codec_data = dvd_title
                    .palette
                    .as_ref()
                    .map(|pal| crate::mux::codec::dvdsub::format_palette(pal));

                // Map DvdSubtitleAttr to Stream::Subtitle
                let subtitle_streams: Vec<Stream> = ts
                    .subtitle_streams
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        // VobSub sub-stream ids run 0x20..=0x3F; PID = sub-id
                        // (identity), shared with the demuxer via
                        // `dvd_subtitle_pid`.
                        let sub_id = 0x20u8.saturating_add(i.min(0x1F) as u8);
                        let pid = crate::mux::ps::dvd_subtitle_pid(sub_id).unwrap_or(sub_id as u16);
                        Stream::Subtitle(SubtitleStream {
                            pid,
                            codec: Codec::DvdSub,
                            language: s.language.clone(),
                            forced: false,
                            qualifier: crate::disc::LabelQualifier::None,
                            codec_data: codec_data.clone(),
                        })
                    })
                    .collect();

                let mut streams = vec![video_stream.clone()];
                streams.extend(audio_streams.iter().cloned());
                streams.extend(subtitle_streams);

                let chapters: Vec<Chapter> = dvd_title
                    .chapter_times
                    .iter()
                    .enumerate()
                    .map(|(i, &t)| Chapter {
                        time_secs: t,
                        name: chapter_name(i),
                    })
                    .collect();

                titles.push(DiscTitle {
                    playlist: format!("VTS_{:02}_{}.VOB", ts.vts_number, title_number),
                    playlist_id: title_number,
                    duration_secs: dvd_title.duration_secs,
                    size_bytes,
                    clips: Vec::new(),
                    streams,
                    chapters,
                    extents,
                    content_format: ContentFormat::MpegPs,
                    codec_privates: Vec::new(),
                });
            }
        }

        titles
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sector::SectorSource;
    use std::collections::HashMap;

    // ---------------------------------------------------------------
    // In-memory disc + minimal UDF image (single physical partition,
    // metadata_start == partition_start). Offsets cited against
    // udf.rs::read_filesystem / ECMA-167.
    // ---------------------------------------------------------------

    const PART_START: u32 = 3000;

    struct MemDisc {
        sectors: HashMap<u32, [u8; 2048]>,
    }
    impl MemDisc {
        fn new() -> Self {
            Self {
                sectors: HashMap::new(),
            }
        }
        fn put(&mut self, lba: u32, data: [u8; 2048]) {
            self.sectors.insert(lba, data);
        }
        fn put_bytes(&mut self, lba: u32, bytes: &[u8]) {
            for (i, chunk) in bytes.chunks(2048).enumerate() {
                let mut s = [0u8; 2048];
                s[..chunk.len()].copy_from_slice(chunk);
                self.put(lba + i as u32, s);
            }
        }
    }
    impl SectorSource for MemDisc {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let need = count as usize * 2048;
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
                buf[off..off + 2048].copy_from_slice(&s);
            }
            Ok(need)
        }
    }

    /// Extended File Entry ICB (tag 266) with one Short AD. info_length@56,
    /// l_ea@208, l_ad@212, AD len(4)@216 | lba(4)@220.
    fn build_file_icb(size: u32, data_lba: u32) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes());
        s[56..64].copy_from_slice(&(size as u64).to_le_bytes());
        s[208..212].copy_from_slice(&0u32.to_le_bytes());
        s[212..216].copy_from_slice(&8u32.to_le_bytes());
        s[216..220].copy_from_slice(&(size & 0x3FFF_FFFF).to_le_bytes());
        s[220..224].copy_from_slice(&data_lba.to_le_bytes());
        s
    }

    /// One FID (tag 257). file_chars@18, l_fi@19, ICB LBA@24, l_iu@36,
    /// name@(38). Name compression-id 8 (ASCII).
    fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
        let start = buf.len();
        let name_field: Vec<u8> = if is_parent {
            Vec::new()
        } else {
            let mut v = vec![0x08u8];
            v.extend_from_slice(name.as_bytes());
            v
        };
        let mut fid = vec![0u8; 38];
        fid[0..2].copy_from_slice(&257u16.to_le_bytes());
        let mut fc = 0u8;
        if is_dir {
            fc |= 0x02;
        }
        if is_parent {
            fc |= 0x08;
        }
        fid[18] = fc;
        fid[19] = name_field.len() as u8;
        fid[24..28].copy_from_slice(&icb_lba.to_le_bytes());
        fid[36..38].copy_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&fid);
        buf.extend_from_slice(&name_field);
        let used = buf.len() - start;
        buf.resize(start + ((used + 3) & !3), 0);
    }

    struct FileSpec {
        name: String,
        icb_lba: u32,
        data_lba: u32,
        contents: Vec<u8>,
    }

    fn build_udf_skeleton(disc: &mut MemDisc, root_icb_lba: u32) {
        let mut avdp = [0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        disc.put(256, avdp);
        let mut pd = [0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        disc.put(32, pd);
        let mut lvd = [0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
        disc.put(33, lvd);
        let mut td = [0u8; 2048];
        td[0..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put(34, td);
        let mut fsd = [0u8; 2048];
        fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
        fsd[404..408].copy_from_slice(&root_icb_lba.to_le_bytes());
        disc.put(PART_START, fsd);
    }

    /// Build a UDF tree with a single VIDEO_TS directory holding the given
    /// files, and return the navigable UdfFs over `disc`.
    fn build_video_ts_fs(disc: &mut MemDisc, files: &[FileSpec]) -> crate::udf::UdfFs {
        let mut fids = Vec::new();
        push_fid(&mut fids, "", 50, true, true);
        for f in files {
            push_fid(&mut fids, &f.name, f.icb_lba, false, false);
            disc.put(
                PART_START + f.icb_lba,
                build_file_icb(f.contents.len() as u32, f.data_lba),
            );
            disc.put_bytes(PART_START + f.data_lba, &f.contents);
        }
        // VIDEO_TS dir ICB + data.
        disc.put(PART_START + 50, build_file_icb(fids.len() as u32, 51));
        disc.put_bytes(PART_START + 51, &fids);
        // Root dir referencing VIDEO_TS.
        let mut root_fids = Vec::new();
        push_fid(&mut root_fids, "", 10, true, true);
        push_fid(&mut root_fids, "VIDEO_TS", 50, true, false);
        disc.put(PART_START + 10, build_file_icb(root_fids.len() as u32, 11));
        disc.put_bytes(PART_START + 11, &root_fids);
        build_udf_skeleton(disc, 10);
        crate::udf::read_filesystem(disc).expect("fs")
    }

    // ---------------------------------------------------------------
    // IFO builders (DVD-Video spec). Offsets cited against ifo.rs.
    // ---------------------------------------------------------------

    /// VMG (VIDEO_TS.IFO): magic "DVDVIDEO-VMG"@0, TT_SRPT sector ptr@0xC4.
    /// TT_SRPT lives at tt_srpt_sector*2048: num_titles(u16)@0, then 12-byte
    /// entries from +8. Each entry: num_chapters(u16)@+2, vts_number@+6,
    /// vts_title_num@+7.
    fn build_vmg(
        titles: &[(
            u16, /*chapters*/
            u8,  /*vts*/
            u8,  /*vts_title*/
        )],
    ) -> Vec<u8> {
        // Put TT_SRPT at sector 1 (offset 2048).
        let tt_srpt_sector = 1u32;
        let mut d = vec![0u8; 2 * 2048];
        d[0..12].copy_from_slice(b"DVDVIDEO-VMG");
        d[0xC4..0xC8].copy_from_slice(&tt_srpt_sector.to_be_bytes());
        let base = tt_srpt_sector as usize * 2048;
        d[base..base + 2].copy_from_slice(&(titles.len() as u16).to_be_bytes());
        for (i, (chapters, vts, vts_title)) in titles.iter().enumerate() {
            let e = base + 8 + i * 12;
            d[e + 2..e + 4].copy_from_slice(&chapters.to_be_bytes());
            d[e + 6] = *vts;
            d[e + 7] = *vts_title;
        }
        d
    }

    /// Cell playback info entry (24 bytes): BCD time@4..8 (unused here),
    /// first_sector(u32 BE)@8, last_sector(u32 BE)@20.
    fn write_cell(buf: &mut [u8], off: usize, first_sector: u32, last_sector: u32) {
        buf[off + 8..off + 12].copy_from_slice(&first_sector.to_be_bytes());
        buf[off + 20..off + 24].copy_from_slice(&last_sector.to_be_bytes());
    }

    /// Build a VTS_XX_0.IFO. Layout per ifo.rs:
    ///   magic "DVDVIDEO-VTS"@0
    ///   vob_start_sector(u32 BE)@0xC0
    ///   VTS_PGCIT sector ptr(u32 BE)@0xCC
    ///   video attr byte@0x200
    ///   num_audio(u16 BE)@0x202, audio blocks (8B) @0x204
    ///   num_subs(u16 BE)@0x254, subtitle blocks (6B) @0x256
    /// PGCIT (at pgcit_sector*2048): num_pgcs(u16)@0, PGC info entries (8B)
    ///   from +8 with PGC byte offset(u32 BE)@+4.
    /// PGC: nr_programs@0x02, nr_cells@0x03, BCD time@0x04, pgm_map ptr@0xE6,
    ///   cell_playback ptr@0xE8 (both u16 BE rel to PGC start).
    #[allow(clippy::too_many_arguments)]
    fn build_vts(
        vob_start: u32,
        video_b0: u8,
        audio: &[(
            u8,      /*b0 coding/sr*/
            u8,      /*b1 channels*/
            [u8; 2], /*lang*/
        )],
        subs: &[[u8; 2]],
        cells: &[(u32, u32)],
        palette_nonzero: bool,
    ) -> Vec<u8> {
        // Total file: header sector(s) + PGCIT at sector 2.
        let pgcit_sector = 2u32;
        let mut d = vec![0u8; 4 * 2048];
        d[0..12].copy_from_slice(b"DVDVIDEO-VTS");
        d[0xC0..0xC4].copy_from_slice(&vob_start.to_be_bytes());
        d[0xCC..0xD0].copy_from_slice(&pgcit_sector.to_be_bytes());
        d[0x200] = video_b0;
        d[0x202..0x204].copy_from_slice(&(audio.len() as u16).to_be_bytes());
        for (i, (b0, b1, lang)) in audio.iter().enumerate() {
            let a = 0x204 + i * 8;
            d[a] = *b0;
            d[a + 1] = *b1;
            d[a + 2] = lang[0];
            d[a + 3] = lang[1];
        }
        d[0x254..0x256].copy_from_slice(&(subs.len() as u16).to_be_bytes());
        for (i, lang) in subs.iter().enumerate() {
            let s = 0x256 + i * 6;
            d[s + 2] = lang[0];
            d[s + 3] = lang[1];
        }

        // PGCIT: one PGC.
        let pg = pgcit_sector as usize * 2048;
        d[pg..pg + 2].copy_from_slice(&1u16.to_be_bytes()); // num_pgcs = 1
        // PGC info entry 0 at pg+8; PGC byte offset (rel to PGCIT) at +4.
        let pgc_rel: u32 = 0x100; // PGC body 256 bytes into the PGCIT
        d[pg + 8 + 4..pg + 8 + 8].copy_from_slice(&pgc_rel.to_be_bytes());
        let pgc = pg + pgc_rel as usize;
        // Ensure room for PGC (needs >= 0xEA past pgc, plus cell table).
        d[pgc + 0x02] = 1; // nr_of_programs
        d[pgc + 0x03] = cells.len() as u8; // nr_of_cells
        // BCD playback time 00:00:30:00 → 30 s, frame-rate bits 0b01 (25fps)
        // not needed; keep simple 30s. BCD: hh,mm,ss,frame|rate.
        d[pgc + 0x04] = 0x00;
        d[pgc + 0x05] = 0x00;
        d[pgc + 0x06] = 0x30; // 30 seconds BCD
        d[pgc + 0x07] = 0b0100_0000; // rate bits = 01 (25fps); 0 frames
        // pgm map ptr @0xE6, cell playback ptr @0xE8 (rel to PGC start).
        let cell_tbl_rel: u16 = 0xF0;
        let pgm_map_rel: u16 = 0xEC;
        d[pgc + 0xE6..pgc + 0xE8].copy_from_slice(&pgm_map_rel.to_be_bytes());
        d[pgc + 0xE8..pgc + 0xEA].copy_from_slice(&cell_tbl_rel.to_be_bytes());
        // Program map: program 0 → first cell 1.
        d[pgc + pgm_map_rel as usize] = 1;
        // Cell playback table.
        let cell_base = pgc + cell_tbl_rel as usize;
        for (i, (first, last)) in cells.iter().enumerate() {
            write_cell(&mut d, cell_base + i * 24, *first, *last);
        }
        // Palette at PGC+0xA4: 16 × [pad,Y,Cb,Cr]. Non-zero if requested.
        if palette_nonzero {
            d[pgc + 0xA4 + 1] = 0x40; // Y of color 0
        }
        d
    }

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    /// scan_dvd_titles returns empty when VIDEO_TS.IFO can't be parsed
    /// (dvd.rs: `parse_vmg(...) Err → return Vec::new()`). Never panics.
    #[test]
    fn scan_dvd_titles_no_ifo_is_empty() {
        let mut disc = MemDisc::new();
        // VIDEO_TS exists but VIDEO_TS.IFO is missing.
        let udf = build_video_ts_fs(&mut disc, &[]);
        assert!(Disc::scan_dvd_titles(&mut disc, &udf).is_empty());
    }

    /// Single VTS, single title, one cell. Extent absolute LBA =
    /// vob_start + cell.first_sector (dvd.rs); sector_count = last - first
    /// + 1 (inclusive range); size_bytes = sectors * 2048 (DVD sector).
    #[test]
    fn scan_dvd_titles_single_cell_extent_math() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]); // 1 chapter, VTS 1, title 1
        // vob_start 1000; one cell sectors [10..=109] → 100 sectors.
        let vts = build_vts(
            1000,
            0x00, // NTSC, 4:3
            &[],
            &[],
            &[(10, 109)],
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let titles = Disc::scan_dvd_titles(&mut disc, &udf);
        assert_eq!(titles.len(), 1);
        let t = &titles[0];
        assert_eq!(t.extents.len(), 1);
        // absolute start = vob_start(1000) + first_sector(10) = 1010.
        assert_eq!(t.extents[0].start_lba, 1010);
        // inclusive: 109 - 10 + 1 = 100 sectors.
        assert_eq!(t.extents[0].sector_count, 100);
        // DVD sector = 2048 bytes.
        assert_eq!(t.size_bytes, 100 * 2048);
        // playlist field format VTS_XX_title.VOB; title_number is 1.
        assert_eq!(t.playlist, "VTS_01_1.VOB");
        assert_eq!(t.playlist_id, 1);
        assert_eq!(t.content_format, ContentFormat::MpegPs);
    }

    /// Multi-cell title: extents preserve cell order and each maps to its
    /// own (vob_start + first .. last) range. mux reads cells in order.
    #[test]
    fn scan_dvd_titles_multi_cell_extents_in_order() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(2, 1, 1)]);
        let vts = build_vts(
            500,
            0x00,
            &[],
            &[],
            &[(0, 99), (200, 299)], // two cells
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf)[0];
        assert_eq!(t.extents.len(), 2);
        assert_eq!(t.extents[0].start_lba, 500); // 500 + 0
        assert_eq!(t.extents[0].sector_count, 100);
        assert_eq!(t.extents[1].start_lba, 700); // 500 + 200
        assert_eq!(t.extents[1].sector_count, 100);
        assert_eq!(t.size_bytes, 200 * 2048);
    }

    /// PAL video standard (b0 low bits == 1) sets FrameRate::F25; NTSC sets
    /// F29_97 (dvd.rs match on ts.video.standard). The video PID is the
    /// fixed DVD MPEG-PS video stream id 0xE0.
    #[test]
    fn scan_dvd_titles_pal_frame_rate_and_video_pid() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        // video b0 low 2 bits = 1 → PAL.
        let vts = build_vts(0, 0x01, &[], &[], &[(0, 9)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf)[0];
        let v = t
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .expect("video stream");
        assert_eq!(v.pid, 0xE0, "DVD video PID is fixed 0xE0");
        assert_eq!(v.frame_rate, FrameRate::F25, "PAL → 25 fps");
        assert_eq!(v.resolution, Resolution::R576i, "PAL → 576i");
    }

    /// AC-3 audio gets sub_stream_id 0x80 → PID routed via dvd_audio_pid
    /// (dvd.rs uses `a.sub_stream_id.and_then(dvd_audio_pid)`). A mixed
    /// AC-3 + DTS title must NOT collide: AC-3 → 0x80 base, DTS → 0x88 base.
    #[test]
    fn scan_dvd_titles_mixed_audio_codecs_distinct_pids() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        // audio b0: coding_mode is (b0 >> 5) & 7. AC-3 = 0 → b0=0x00.
        // DTS = 6 → b0 = 6<<5 = 0xC0. b1 channels nibble high.
        let vts = build_vts(
            0,
            0x00,
            &[(0x00, 0x10, *b"en"), (0xC0, 0x50, *b"fr")], // AC-3 eng, DTS fra
            &[],
            &[(0, 9)],
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf)[0];
        let audios: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audios.len(), 2);
        assert_eq!(audios[0].codec, Codec::Ac3);
        assert_eq!(audios[0].language, "en");
        assert_eq!(audios[1].codec, Codec::Dts);
        // PIDs must differ (no 0xBD00 collision).
        assert_ne!(
            audios[0].pid, audios[1].pid,
            "mixed-codec audio must route to distinct PIDs"
        );
    }

    /// Subtitle streams map to Codec::DvdSub with palette codec_data when a
    /// non-zero palette is present (dvd.rs builds codec_data from
    /// dvd_title.palette). VobSub sub-id 0x20+i.
    #[test]
    fn scan_dvd_titles_subtitle_palette_codec_data() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts(
            0,
            0x00,
            &[],
            &[*b"en"],
            &[(0, 9)],
            true, // non-zero palette
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf)[0];
        let sub = t
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Subtitle(s) => Some(s),
                _ => None,
            })
            .expect("subtitle stream");
        assert_eq!(sub.codec, Codec::DvdSub);
        assert_eq!(sub.language, "en");
        assert!(
            sub.codec_data.is_some(),
            "non-zero palette must yield codec_data"
        );
    }

    /// Multiple titles in one VTS each become their own DiscTitle with a
    /// monotonically increasing title_number / playlist_id (dvd.rs
    /// `title_number += 1` per dvd_title). Both share the VTS streams.
    #[test]
    fn scan_dvd_titles_numbering_increments_per_title() {
        let mut disc = MemDisc::new();
        // Two titles in VTS 1 (title nums 1 and 2). num_pgcs must cover
        // pgc_index = vts_title - 1, so we need >=2 PGC entries; our
        // build_vts only emits 1 PGC. So the second title's PGC index (1)
        // exceeds num_pgcs (1) and is skipped. To exercise numbering we use
        // two separate VTS sets instead.
        let vmg = build_vmg(&[(1, 1, 1), (1, 2, 1)]);
        let vts1 = build_vts(100, 0x00, &[], &[], &[(0, 9)], false);
        let vts2 = build_vts(200, 0x00, &[], &[], &[(0, 19)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts1,
                },
                FileSpec {
                    name: "VTS_02_0.IFO".into(),
                    icb_lba: 64,
                    data_lba: 7000,
                    contents: vts2,
                },
            ],
        );
        let titles = Disc::scan_dvd_titles(&mut disc, &udf);
        assert_eq!(titles.len(), 2);
        // title_number is a running counter across all title sets.
        assert_eq!(titles[0].playlist_id, 1);
        assert_eq!(titles[1].playlist_id, 2);
        assert_eq!(titles[0].playlist, "VTS_01_1.VOB");
        assert_eq!(titles[1].playlist, "VTS_02_2.VOB");
        // Distinct vob_start → distinct extents.
        assert_eq!(titles[0].extents[0].start_lba, 100);
        assert_eq!(titles[1].extents[0].start_lba, 200);
    }

    /// chapter_times from the IFO become Chapter entries with ordinal
    /// names (dvd.rs maps chapter_times → Chapter{time_secs, chapter_name}).
    #[test]
    fn scan_dvd_titles_chapters_present() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts(0, 0x00, &[], &[], &[(0, 9)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf)[0];
        // One program in the program map → one chapter time (0.0 for the
        // first program). Name is the ordinal from chapter_name(0).
        assert_eq!(t.chapters.len(), 1);
        assert_eq!(t.chapters[0].name, chapter_name(0));
    }
}
