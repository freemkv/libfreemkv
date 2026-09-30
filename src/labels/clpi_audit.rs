//! CLPI vs MPLS cross-validation diagnostic.
//!
//! Walks per-clip CLPI program info and per-playlist MPLS STN tables, keys streams by
//! `(clip, PID)` — a PID is only unique within one clip — keeping the first
//! `(coding_type, language)` seen per source, then classifies each key as CLPI-only,
//! MPLS-only, Match, or Divergent.
//!
//! Each PlayItem has its own STN_table and only the first item's is parsed, so only a
//! playlist's first-item clip whose STN kept a stream is auditable; other clips are
//! left out entirely (CLPI-only means a real orphan PID, not "never compared").
//!
//! [`audit`] returns [`ClpiVsMplsAudit`]; diagnostic only, not used by
//! label selection.

use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::BTreeMap;

/// One row in the audit: a stream PID that's known to one source or
/// both, with the fields each source reported.
#[derive(Debug, Clone)]
pub struct ClpiVsMplsRow {
    /// Clip id (e.g. "00001") the PID belongs to.
    pub clip: String,
    pub pid: u16,
    pub clpi_coding_type: Option<u8>,
    pub clpi_language: Option<String>,
    pub mpls_coding_type: Option<u8>,
    pub mpls_language: Option<String>,
}

impl ClpiVsMplsRow {
    /// Classification rules:
    /// - one coding_type present, the other missing → `ClpiOnly` /
    ///   `MplsOnly`
    /// - both coding_types present, fields differ → `Divergent`
    /// - both coding_types present and identical → `Match`
    /// - both coding_types missing (`audit` never builds this, but a
    ///   caller can construct such a row) → compare the language fields:
    ///   `Divergent` if they differ, else `Match`
    pub fn class(&self) -> ClpiVsMplsClass {
        match (
            self.clpi_coding_type.is_some(),
            self.mpls_coding_type.is_some(),
        ) {
            (true, false) => ClpiVsMplsClass::ClpiOnly,
            (false, true) => ClpiVsMplsClass::MplsOnly,
            (true, true) => {
                let coding_match = self.clpi_coding_type == self.mpls_coding_type;
                let lang_match = self.clpi_language == self.mpls_language;
                if coding_match && lang_match {
                    ClpiVsMplsClass::Match
                } else {
                    ClpiVsMplsClass::Divergent
                }
            }
            (false, false) => {
                if self.clpi_language == self.mpls_language {
                    ClpiVsMplsClass::Match
                } else {
                    ClpiVsMplsClass::Divergent
                }
            }
        }
    }
}

/// Classification of one (PID) row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClpiVsMplsClass {
    /// PID seen in CLPI ProgramInfo but no MPLS STN table references
    /// it. Orphan on disc.
    ClpiOnly,
    /// PID seen in MPLS STN table but no CLPI ProgramInfo includes it.
    /// One of our parsers probably has a bug.
    MplsOnly,
    /// Both sources see this PID with the same coding_type + language.
    Match,
    /// Both sources see this PID but disagree on coding_type or language.
    /// MPLS wins for label rendering (playlist-authoritative view); CLPI
    /// is the per-clip ground truth.
    Divergent,
}

/// Full audit report.
#[derive(Debug, Clone, Default)]
pub struct ClpiVsMplsAudit {
    pub rows: Vec<ClpiVsMplsRow>,
}

impl ClpiVsMplsAudit {
    /// Count rows by class, returned in the fixed order
    /// `(clpi_only, mpls_only, matches, divergent)` matching the
    /// [`ClpiVsMplsClass`] variants.
    pub fn class_counts(&self) -> (usize, usize, usize, usize) {
        let mut clpi_only = 0;
        let mut mpls_only = 0;
        let mut matches = 0;
        let mut divergent = 0;
        for r in &self.rows {
            match r.class() {
                ClpiVsMplsClass::ClpiOnly => clpi_only += 1,
                ClpiVsMplsClass::MplsOnly => mpls_only += 1,
                ClpiVsMplsClass::Match => matches += 1,
                ClpiVsMplsClass::Divergent => divergent += 1,
            }
        }
        (clpi_only, mpls_only, matches, divergent)
    }
}

/// Walk `/BDMV/CLIPINF/*.clpi` and `/BDMV/PLAYLIST/*.mpls`, build a
/// `(clip, PID)`-keyed table of (CLPI fields, MPLS fields), return the
/// merged view. Missing files (read errors, parse failures) are
/// silently skipped — this is diagnostic, not correctness-critical.
pub fn audit(reader: &mut dyn SectorSource, udf: &UdfFs) -> ClpiVsMplsAudit {
    // CLPI: the clip id is the file stem.
    let mut clpi_by_key: BTreeMap<(String, u16), (u8, String)> = BTreeMap::new();
    for_each_dir_file(reader, udf, "/BDMV/CLIPINF", ".clpi", |name, data| {
        let Ok(clip) = crate::clpi::parse(&data) else {
            return;
        };
        let clip_id = name[..name.len() - ".clpi".len()].to_string();
        for s in clip.streams {
            // PID 0 means "no PID in stream entry" — skip rather than collide.
            if s.pid != 0 {
                clpi_by_key
                    .entry((clip_id.clone(), s.pid))
                    .or_insert((s.coding_type, s.language));
            }
        }
    });

    // MPLS: `streams` is the first PlayItem's STN_table, so it describes that clip.
    let mut mpls_by_key: BTreeMap<(String, u16), (u8, String)> = BTreeMap::new();
    let mut mpls_clips: Vec<String> = Vec::new();
    for_each_dir_file(reader, udf, "/BDMV/PLAYLIST", ".mpls", |_, data| {
        let Ok(pl) = crate::mpls::parse(&data) else {
            return;
        };
        // A first clip is covered only if its STN kept a stream to compare.
        if let Some(pi) = pl.play_items.first()
            && pl.streams.iter().any(|s| s.pid != 0)
        {
            mpls_clips.push(pi.clip_id.clone());
            for s in pl.streams.iter().filter(|s| s.pid != 0) {
                mpls_by_key
                    .entry((pi.clip_id.clone(), s.pid))
                    .or_insert_with(|| (s.coding_type, s.language.clone()));
            }
        }
    });

    // Merge views over auditable clips (those an MPLS STN describes).
    let covered: std::collections::BTreeSet<&str> = mpls_clips.iter().map(String::as_str).collect();
    let mut keys: std::collections::BTreeSet<&(String, u16)> = clpi_by_key
        .keys()
        .filter(|k| covered.contains(k.0.as_str()))
        .collect();
    keys.extend(mpls_by_key.keys());
    let rows = keys
        .into_iter()
        .map(|key| {
            let clpi = clpi_by_key.get(key);
            let mpls = mpls_by_key.get(key);
            ClpiVsMplsRow {
                clip: key.0.clone(),
                pid: key.1,
                clpi_coding_type: clpi.map(|(c, _)| *c),
                clpi_language: clpi.map(|(_, l)| l.clone()),
                mpls_coding_type: mpls.map(|(c, _)| *c),
                mpls_language: mpls.map(|(_, l)| l.clone()),
            }
        })
        .collect();

    ClpiVsMplsAudit { rows }
}

// Calls `f(file name, bytes)` for every readable `*<ext>` file directly under `dir`, reading
// and dropping one file at a time so a hostile directory can't pin N large files at once.
fn for_each_dir_file(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    dir: &str,
    ext: &str,
    mut f: impl FnMut(&str, Vec<u8>),
) {
    let Some(d) = udf.find_dir(dir) else {
        return;
    };
    let names: Vec<String> = d
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(ext))
        .map(|e| e.name.clone())
        .collect();
    for name in names {
        if let Ok(data) = udf.read_file(reader, &format!("{dir}/{name}")) {
            f(&name, data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_match_when_identical() {
        let r = ClpiVsMplsRow {
            clip: String::new(),
            pid: 0x1100,
            clpi_coding_type: Some(0x83),
            clpi_language: Some("eng".into()),
            mpls_coding_type: Some(0x83),
            mpls_language: Some("eng".into()),
        };
        assert_eq!(r.class(), ClpiVsMplsClass::Match);
    }

    #[test]
    fn class_clpi_only_when_mpls_missing() {
        let r = ClpiVsMplsRow {
            clip: String::new(),
            pid: 0x1100,
            clpi_coding_type: Some(0x83),
            clpi_language: Some("eng".into()),
            mpls_coding_type: None,
            mpls_language: None,
        };
        assert_eq!(r.class(), ClpiVsMplsClass::ClpiOnly);
    }

    #[test]
    fn class_mpls_only_when_clpi_missing() {
        let r = ClpiVsMplsRow {
            clip: String::new(),
            pid: 0x1100,
            clpi_coding_type: None,
            clpi_language: None,
            mpls_coding_type: Some(0x90),
            mpls_language: Some("fra".into()),
        };
        assert_eq!(r.class(), ClpiVsMplsClass::MplsOnly);
    }

    #[test]
    fn class_divergent_on_lang_disagreement() {
        let r = ClpiVsMplsRow {
            clip: String::new(),
            pid: 0x1100,
            clpi_coding_type: Some(0x83),
            clpi_language: Some("eng".into()),
            mpls_coding_type: Some(0x83),
            mpls_language: Some("und".into()),
        };
        assert_eq!(r.class(), ClpiVsMplsClass::Divergent);
    }

    #[test]
    fn class_both_coding_missing_divergent_on_lang() {
        // Caller-built row with neither coding_type but disagreeing
        // languages must classify Divergent, not Match.
        let r = ClpiVsMplsRow {
            clip: String::new(),
            pid: 0x1100,
            clpi_coding_type: None,
            clpi_language: Some("eng".into()),
            mpls_coding_type: None,
            mpls_language: Some("fra".into()),
        };
        assert_eq!(r.class(), ClpiVsMplsClass::Divergent);
    }

    #[test]
    fn class_both_coding_missing_match_on_equal_lang() {
        let r = ClpiVsMplsRow {
            clip: String::new(),
            pid: 0x1100,
            clpi_coding_type: None,
            clpi_language: Some("eng".into()),
            mpls_coding_type: None,
            mpls_language: Some("eng".into()),
        };
        assert_eq!(r.class(), ClpiVsMplsClass::Match);
    }

    #[test]
    fn class_counts_sum_rows() {
        let audit = ClpiVsMplsAudit {
            rows: vec![
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 0x1100,
                    clpi_coding_type: Some(0x83),
                    clpi_language: Some("eng".into()),
                    mpls_coding_type: Some(0x83),
                    mpls_language: Some("eng".into()),
                }, // Match
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 0x1101,
                    clpi_coding_type: Some(0x83),
                    clpi_language: Some("fra".into()),
                    mpls_coding_type: None,
                    mpls_language: None,
                }, // ClpiOnly
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 0x1102,
                    clpi_coding_type: None,
                    clpi_language: None,
                    mpls_coding_type: Some(0x90),
                    mpls_language: Some("eng".into()),
                }, // MplsOnly
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 0x1103,
                    clpi_coding_type: Some(0x86),
                    clpi_language: Some("spa".into()),
                    mpls_coding_type: Some(0x86),
                    mpls_language: Some("ita".into()),
                }, // Divergent
            ],
        };
        let (co, mo, m, d) = audit.class_counts();
        assert_eq!(co, 1);
        assert_eq!(mo, 1);
        assert_eq!(m, 1);
        assert_eq!(d, 1);
    }

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: coding_type mismatch with matching language → Divergent (not Match).
    /// The spec doc says both coding_type AND language must agree for Match.
    /// Mutation: only check language for match → coding_type mismatch silently classified as Match.
    #[test]
    fn class_divergent_on_coding_type_mismatch_same_lang() {
        let r = ClpiVsMplsRow {
            clip: String::new(),
            pid: 0x1100,
            clpi_coding_type: Some(0x83), // TrueHD
            clpi_language: Some("eng".into()),
            mpls_coding_type: Some(0x86), // DTS-HD MA
            mpls_language: Some("eng".into()),
        };
        assert_eq!(r.class(), ClpiVsMplsClass::Divergent);
    }

    /// Spec: empty rows → class_counts returns (0,0,0,0). Never panics on empty audit.
    /// Mutation: access rows[0] unconditionally → panic on empty audit.
    #[test]
    fn class_counts_empty_audit() {
        let audit = ClpiVsMplsAudit { rows: Vec::new() };
        let (co, mo, m, d) = audit.class_counts();
        assert_eq!((co, mo, m, d), (0, 0, 0, 0));
    }

    /// Spec: class_counts tuple order is (clpi_only, mpls_only, matches, divergent).
    /// Verifies each counter increments the RIGHT slot.
    /// Mutation: swap any two counters → wrong slot increments.
    #[test]
    fn class_counts_each_counter_in_correct_slot() {
        // One of each class — verify tuple slots separately.
        let audit = ClpiVsMplsAudit {
            rows: vec![
                // 2 ClpiOnly
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 1,
                    clpi_coding_type: Some(0x83),
                    clpi_language: None,
                    mpls_coding_type: None,
                    mpls_language: None,
                },
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 2,
                    clpi_coding_type: Some(0x82),
                    clpi_language: None,
                    mpls_coding_type: None,
                    mpls_language: None,
                },
                // 1 MplsOnly
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 3,
                    clpi_coding_type: None,
                    clpi_language: None,
                    mpls_coding_type: Some(0x90),
                    mpls_language: None,
                },
                // 3 Match
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 4,
                    clpi_coding_type: Some(0x83),
                    clpi_language: Some("eng".into()),
                    mpls_coding_type: Some(0x83),
                    mpls_language: Some("eng".into()),
                },
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 5,
                    clpi_coding_type: Some(0x82),
                    clpi_language: Some("fra".into()),
                    mpls_coding_type: Some(0x82),
                    mpls_language: Some("fra".into()),
                },
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 6,
                    clpi_coding_type: Some(0x86),
                    clpi_language: Some("deu".into()),
                    mpls_coding_type: Some(0x86),
                    mpls_language: Some("deu".into()),
                },
                // 1 Divergent
                ClpiVsMplsRow {
                    clip: String::new(),
                    pid: 7,
                    clpi_coding_type: Some(0x83),
                    clpi_language: Some("eng".into()),
                    mpls_coding_type: Some(0x83),
                    mpls_language: Some("spa".into()),
                },
            ],
        };
        let (co, mo, m, d) = audit.class_counts();
        assert_eq!(co, 2, "clpi_only slot");
        assert_eq!(mo, 1, "mpls_only slot");
        assert_eq!(m, 3, "matches slot");
        assert_eq!(d, 1, "divergent slot");
        assert_eq!(co + mo + m + d, audit.rows.len(), "all rows accounted for");
    }

    // Minimal MPLS: one play item on `clip` whose STN lists one primary audio
    // stream `(pid, coding_type, lang)`.
    fn build_mpls(clip: &[u8; 5], pid: u16, coding: u8, lang: &[u8; 3]) -> Vec<u8> {
        build_mpls_items(&[clip], pid, coding, lang)
    }

    // As `build_mpls`, over several play items (STN on the first, as parsed).
    fn build_mpls_items(clips: &[&[u8; 5]], pid: u16, coding: u8, lang: &[u8; 3]) -> Vec<u8> {
        build_mpls_stn(clips, Some((pid, coding, lang)))
    }

    // As above; `None` builds a first item whose STN keeps no stream.
    fn build_mpls_stn(clips: &[&[u8; 5]], stream: Option<(u16, u8, &[u8; 3])>) -> Vec<u8> {
        let mut item = Vec::new();
        item.extend_from_slice(clips[0]);
        item.extend_from_slice(b"M2TS");
        item.extend_from_slice(&[0u8; 3]);
        item.extend_from_slice(&0u32.to_be_bytes());
        item.extend_from_slice(&(7000u32 * 45000).to_be_bytes());
        item.extend_from_slice(&[0u8; 12]); // UO mask, misc, still
        let n_audio = u8::from(stream.is_some());
        let mut stn = vec![0u8, 0, 0, 0, 0, n_audio, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        if let Some((pid, coding, lang)) = stream {
            stn.extend_from_slice(&[3, 0x01]);
            stn.extend_from_slice(&pid.to_be_bytes());
            stn.extend_from_slice(&[5, coding, 0x61]);
            stn.extend_from_slice(lang);
        }
        item.extend_from_slice(&stn);
        let mut pl = vec![0u8; 6];
        pl.extend_from_slice(&(clips.len() as u16).to_be_bytes());
        pl.extend_from_slice(&[0u8; 2]);
        pl.extend_from_slice(&(item.len() as u16).to_be_bytes());
        pl.extend_from_slice(&item);
        for clip in &clips[1..] {
            let mut more = clip.to_vec();
            more.extend_from_slice(b"M2TS");
            more.extend_from_slice(&[0u8; 3]);
            more.extend_from_slice(&0u32.to_be_bytes());
            more.extend_from_slice(&(60u32 * 45000).to_be_bytes());
            more.extend_from_slice(&[0u8; 12]);
            pl.extend_from_slice(&(more.len() as u16).to_be_bytes());
            pl.extend_from_slice(&more);
        }
        let pl_len = (pl.len() - 4) as u32;
        pl[0..4].copy_from_slice(&pl_len.to_be_bytes());
        let mut buf = b"MPLS0200".to_vec();
        buf.extend_from_slice(&40u32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 28]);
        buf.extend_from_slice(&pl);
        buf
    }

    // A PID is scoped to its clip: the same PID in two clips is two streams, and
    // each playlist entry is compared with its own clip's CLPI.
    #[test]
    fn same_pid_in_two_clips_is_compared_per_clip() {
        use crate::consts::coding_type as c;
        use crate::udf::fixture::*;
        let build_clpi = super::super::clpi_orphan_tests::build_clpi;
        let dir = |name: &str, icb: u32, files| DirSpec {
            name: name.to_string(),
            icb_lba: icb,
            dir_data_lba: icb + 1,
            files,
            subdirs: vec![],
        };
        let clipinf = dir(
            "CLIPINF",
            24,
            vec![
                file_with(
                    "00000.clpi",
                    26,
                    8000,
                    build_clpi(&[(0x1100, c::AC3, "eng")]),
                    false,
                ),
                file_with(
                    "00001.clpi",
                    27,
                    8100,
                    build_clpi(&[(0x1100, c::TRUEHD, "eng")]),
                    false,
                ),
            ],
        );
        let mpls = build_mpls(b"00001", 0x1100, c::TRUEHD, b"eng");
        let playlist = dir(
            "PLAYLIST",
            30,
            vec![file_with("00800.mpls", 32, 8200, mpls, false)],
        );
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![clipinf, playlist],
            }],
        };
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let a = audit(&mut disc, &udf);
        assert_eq!(a.class_counts(), (0, 0, 1, 0), "{:?}", a.rows);
    }

    // Each PlayItem has its own STN_table; the parsed (first) one describes only the
    // first item's clip, so a later clip is unauditable and gets no row at all.
    #[test]
    fn mpls_streams_are_keyed_by_the_first_play_item_clip_only() {
        use crate::consts::coding_type as c;
        use crate::udf::fixture::*;
        let build_clpi = super::super::clpi_orphan_tests::build_clpi;
        let dir = |name: &str, icb: u32, files| DirSpec {
            name: name.to_string(),
            icb_lba: icb,
            dir_data_lba: icb + 1,
            files,
            subdirs: vec![],
        };
        let clpi = || build_clpi(&[(0x1100, c::TRUEHD, "eng")]);
        let clipinf = dir(
            "CLIPINF",
            24,
            vec![
                file_with("00001.clpi", 26, 8000, clpi(), false),
                file_with("00002.clpi", 27, 8100, clpi(), false),
            ],
        );
        let mpls = build_mpls_items(&[b"00001", b"00002"], 0x1100, c::TRUEHD, b"eng");
        let playlist = dir(
            "PLAYLIST",
            30,
            vec![file_with("00800.mpls", 32, 8200, mpls, false)],
        );
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![clipinf, playlist],
            }],
        };
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        let a = audit(&mut disc, &udf);
        assert_eq!(a.class_counts(), (0, 0, 1, 0), "{:?}", a.rows);
    }

    // A covered clip's orphan PID is a real CLPI-only row; a first clip whose STN
    // kept no stream is not covered, so its CLPI streams get no row.
    #[test]
    fn covered_clip_orphan_is_clpi_only_and_streamless_stn_covers_nothing() {
        use crate::consts::coding_type as c;
        use crate::udf::fixture::*;
        let build_clpi = super::super::clpi_orphan_tests::build_clpi;
        let dir = |name: &str, icb: u32, files| DirSpec {
            name: name.to_string(),
            icb_lba: icb,
            dir_data_lba: icb + 1,
            files,
            subdirs: vec![],
        };
        let one = build_clpi(&[(0x1100, c::TRUEHD, "eng"), (0x1200, c::PG, "eng")]);
        let two = build_clpi(&[(0x1100, c::AC3, "fra")]);
        let clipinf = dir(
            "CLIPINF",
            24,
            vec![
                file_with("00001.clpi", 26, 8000, one, false),
                file_with("00002.clpi", 27, 8100, two, false),
            ],
        );
        let a = build_mpls_stn(&[b"00001"], Some((0x1100, c::TRUEHD, b"eng")));
        let b = build_mpls_stn(&[b"00002"], None);
        let playlist = dir(
            "PLAYLIST",
            30,
            vec![
                file_with("00800.mpls", 32, 8200, a, false),
                file_with("00801.mpls", 33, 8300, b, false),
            ],
        );
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![clipinf, playlist],
            }],
        };
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        let a = audit(&mut disc, &udf);
        let orphan = a.rows.iter().find(|r| r.pid == 0x1200).expect("orphan row");
        assert_eq!(
            (orphan.clip.as_str(), orphan.class()),
            ("00001", ClpiVsMplsClass::ClpiOnly)
        );
        assert_eq!(a.class_counts(), (1, 0, 1, 0), "{:?}", a.rows);
    }
}
