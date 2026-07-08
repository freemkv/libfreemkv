//! HD-DVD title scanning — `HVDVD_TS/` Enhanced-VOB (`.evo`) enumeration.
//!
//! HD-DVD is a **tree-level peer** of DVD and Blu-ray (not a stream variant like
//! FMTS): its content lives in `HVDVD_TS/` as `.evo` clips — Enhanced VOB, an
//! MPEG **program** stream — each with a small `.map` timemap sidecar, navigated
//! by `.xpl`/`.ifo` playlists in `HVDVD_TS/` and `ADV_OBJ/`. Because it is a
//! different tree with a different playlist format, it gets its OWN scanner
//! (this file), a peer to [`Disc::scan_bluray_titles`] — the two-format design
//! rule: a genuinely different format is a new enumerator, not an extension
//! bolted into the BD path.
//!
//! Scope today: enumerate the `.evo` clips and yield one [`DiscTitle`] per clip
//! (container [`ContentFormat::MpegPs`], so the existing PS mux path handles it).
//! What is NOT parsed yet — and is honestly stubbed, not faked:
//!   * `.xpl` playlist ordering (title composition / chapters),
//!   * per-clip stream enumeration (would demux the EVO program stream),
//!   * `.map` timemap → real durations.
//!
//! Extents and size ARE real (the ripper needs those to image a clip); the rest
//! is left empty rather than guessed.

use super::*;
use crate::sector::SectorSource;
use crate::udf;

/// Clip stream-file extension in the HD-DVD `HVDVD_TS/` tree. HD-DVD is a
/// separate tree from BD, so this is a separate constant — deliberately NOT an
/// entry in [`super::bluray`]'s BD-tree `CLIP_STREAM_EXTS`.
const HDDVD_CLIP_EXT: &str = ".evo";

impl Disc {
    /// Scan HD-DVD titles from the `HVDVD_TS/` `.evo` clips.
    ///
    /// One [`DiscTitle`] per `.evo` with real physical extents (from the UDF
    /// allocation descriptors) and declared size; `streams`/`chapters`/duration
    /// are left empty pending `.xpl`/EVO parsing (see module docs). Returns an
    /// empty vec when `HVDVD_TS/` is absent or holds no readable `.evo`.
    pub(super) fn scan_hddvd_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Vec<DiscTitle> {
        let mut titles = Vec::new();
        let Some(ts_dir) = udf_fs.find_dir("/HVDVD_TS") else {
            return titles;
        };
        // Snapshot clip (name, size) first: the `ts_dir` borrow must end before
        // the `udf_fs.file_extents` calls below re-borrow `udf_fs`.
        let clips: Vec<(String, u64)> = ts_dir
            .entries
            .iter()
            .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(HDDVD_CLIP_EXT))
            .map(|e| (e.name.clone(), e.size))
            .collect();

        for (idx, (name, size)) in clips.iter().enumerate() {
            let path = format!("/HVDVD_TS/{name}");
            let mut extents = Vec::new();
            if let Ok(file_exts) = udf_fs.file_extents(reader, &path) {
                for (lba, sectors) in file_exts {
                    if sectors > 0 && lba > 0 {
                        extents.push(Extent {
                            start_lba: lba,
                            sector_count: sectors,
                        });
                    }
                }
            }
            if extents.is_empty() {
                continue;
            }
            let clip_id = name
                .rsplit_once('.')
                .map(|(base, _)| base.to_string())
                .unwrap_or_else(|| name.clone());
            titles.push(DiscTitle {
                playlist: name.clone(),
                playlist_id: idx as u16,
                duration_secs: 0.0,
                size_bytes: *size,
                clips: vec![Clip {
                    clip_id,
                    in_time: 0,
                    out_time: 0,
                    duration_secs: 0.0,
                    source_packets: 0,
                }],
                streams: Vec::new(),
                chapters: Vec::new(),
                extents,
                content_format: ContentFormat::MpegPs,
                codec_privates: Vec::new(),
            });
        }
        titles
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udf::fixture::*;

    /// Build a UDF with an `HVDVD_TS/` tree holding the listed `.evo` clips
    /// (name, sector count, data LBA).
    fn make_hddvd_fs(disc: &mut MemDisc, evos: &[(&str, u32, u32)]) -> crate::udf::UdfFs {
        let mut files = Vec::new();
        let mut icb = 100u32;
        for (name, sectors, data_lba) in evos {
            files.push(file(name, icb, *data_lba, sectors * 2048, true));
            icb += 1;
        }
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        crate::udf::read_filesystem(disc).expect("fs")
    }

    /// HD-DVD's own enumerator yields one title per `.evo`, MpegPs container,
    /// with real physical extents (mirrors the BD `.m2ts` extent path).
    #[test]
    fn scan_hddvd_titles_enumerates_evo_extents() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs(
            &mut disc,
            &[("FEATURE.EVO", 2000, 5000), ("BLOOP.EVO", 300, 9000)],
        );
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf);
        assert_eq!(titles.len(), 2, "one title per .evo clip");
        for t in &titles {
            assert_eq!(
                t.content_format,
                ContentFormat::MpegPs,
                "EVO is a program stream"
            );
            assert_eq!(t.extents.len(), 1);
        }
        let feature = titles.iter().find(|t| t.playlist == "FEATURE.EVO").unwrap();
        assert_eq!(feature.extents[0].start_lba, PART_START + 5000);
        assert_eq!(feature.extents[0].sector_count, 2000);
        assert_eq!(
            feature.clips[0].clip_id, "FEATURE",
            "clip_id drops the extension"
        );
    }
}
