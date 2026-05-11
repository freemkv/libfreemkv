//! CLPI vs MPLS cross-validation diagnostic.
//!
//! Empirical question (raised 2026-05-11): is CLPI's per-stream
//! language and codec data truly redundant with MPLS's STN-table data
//! on real-world Blu-rays?
//!
//! Build a quick audit that walks both sources, normalizes their stream
//! lists by (PID, language, coding_type), and flags any disagreement.
//!
//! Three classes of mismatch we want to detect:
//!
//! 1. **CLPI has streams MPLS doesn't reference.** Orphan streams in
//!    the .m2ts that no playlist's STN table includes. Means the user
//!    can't reach them through the menu but they're physically on the
//!    disc.
//! 2. **MPLS has streams CLPI doesn't list.** Should never happen if
//!    both parsers are correct — playlists reference clips which
//!    reference streams. If it happens, one of our parsers has a bug.
//! 3. **Same PID, different language / coding_type.** The playlist re-
//!    tagged a stream's metadata. Rare but spec-permitted. Means CLPI
//!    and MPLS disagree about the same physical stream's properties.
//!
//! If audits across the corpus show zero mismatches of any class, CLPI
//! program_info extraction is **empirically redundant** for labels and
//! we can leave it out of the registry. If even one mismatch surfaces,
//! we add a CLPI label parser to the registry as belt-and-suspenders.
//!
//! This module exposes `audit(reader, udf)` returning a structured
//! report. Surfaced via the labels-analyze tool — not part of the
//! `analyze()` pipeline (no impact on the label output).

use crate::sector::SectorReader;
use crate::udf::UdfFs;
use std::collections::BTreeMap;

/// One row in the audit: a stream PID that's known to one source or
/// both, with the fields each source reported.
#[derive(Debug, Clone)]
pub struct ClpiVsMplsRow {
    pub pid: u16,
    pub clpi_coding_type: Option<u8>,
    pub clpi_language: Option<String>,
    pub mpls_coding_type: Option<u8>,
    pub mpls_language: Option<String>,
}

impl ClpiVsMplsRow {
    /// Three rules for classification:
    /// - both sources missing (impossible — caller wouldn't insert)
    /// - one source missing → class A or B (orphan-on-disc / playlist-only)
    /// - both present but fields differ → class C (metadata divergence)
    /// - both present and identical → no mismatch
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
            (false, false) => ClpiVsMplsClass::Match,
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
/// dedup-by-PID table of (CLPI fields, MPLS fields), return the
/// merged view. Missing files (read errors, parse failures) are
/// silently skipped — this is diagnostic, not correctness-critical.
pub fn audit(reader: &mut dyn SectorReader, udf: &UdfFs) -> ClpiVsMplsAudit {
    // Aggregate by PID across all CLPI files. If a PID appears in
    // multiple clips (typical — main movie clip + trailers reference
    // the same audio stream PIDs), first encountered wins (they should
    // all agree per BD spec).
    let mut clpi_by_pid: BTreeMap<u16, (u8, String)> = BTreeMap::new();
    if let Some(dir) = udf.find_dir("/BDMV/CLIPINF") {
        let names: Vec<String> = dir
            .entries
            .iter()
            .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".clpi"))
            .map(|e| e.name.clone())
            .collect();
        for name in names {
            let path = format!("/BDMV/CLIPINF/{}", name);
            let Ok(data) = udf.read_file(reader, &path) else {
                continue;
            };
            let Ok(clip) = crate::clpi::parse(&data) else {
                continue;
            };
            for s in clip.streams {
                clpi_by_pid
                    .entry(s.pid)
                    .or_insert((s.coding_type, s.language));
            }
        }
    }

    // Same for MPLS streams.
    let mut mpls_by_pid: BTreeMap<u16, (u8, String)> = BTreeMap::new();
    if let Some(dir) = udf.find_dir("/BDMV/PLAYLIST") {
        let names: Vec<String> = dir
            .entries
            .iter()
            .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".mpls"))
            .map(|e| e.name.clone())
            .collect();
        for name in names {
            let path = format!("/BDMV/PLAYLIST/{}", name);
            let Ok(data) = udf.read_file(reader, &path) else {
                continue;
            };
            let Ok(pl) = crate::mpls::parse(&data) else {
                continue;
            };
            for s in pl.streams {
                if s.pid == 0 {
                    // PID 0 means "no PID in stream entry" — skip rather
                    // than collide with other entries.
                    continue;
                }
                mpls_by_pid
                    .entry(s.pid)
                    .or_insert((s.coding_type, s.language));
            }
        }
    }

    // Merge views: every PID seen anywhere gets a row.
    let mut all_pids: std::collections::BTreeSet<u16> = std::collections::BTreeSet::new();
    all_pids.extend(clpi_by_pid.keys().copied());
    all_pids.extend(mpls_by_pid.keys().copied());

    let mut rows = Vec::with_capacity(all_pids.len());
    for pid in all_pids {
        let clpi = clpi_by_pid.get(&pid);
        let mpls = mpls_by_pid.get(&pid);
        rows.push(ClpiVsMplsRow {
            pid,
            clpi_coding_type: clpi.map(|(c, _)| *c),
            clpi_language: clpi.map(|(_, l)| l.clone()),
            mpls_coding_type: mpls.map(|(c, _)| *c),
            mpls_language: mpls.map(|(_, l)| l.clone()),
        });
    }

    ClpiVsMplsAudit { rows }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_match_when_identical() {
        let r = ClpiVsMplsRow {
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
            pid: 0x1100,
            clpi_coding_type: Some(0x83),
            clpi_language: Some("eng".into()),
            mpls_coding_type: Some(0x83),
            mpls_language: Some("und".into()),
        };
        assert_eq!(r.class(), ClpiVsMplsClass::Divergent);
    }

    #[test]
    fn class_counts_sum_rows() {
        let audit = ClpiVsMplsAudit {
            rows: vec![
                ClpiVsMplsRow {
                    pid: 0x1100,
                    clpi_coding_type: Some(0x83),
                    clpi_language: Some("eng".into()),
                    mpls_coding_type: Some(0x83),
                    mpls_language: Some("eng".into()),
                }, // Match
                ClpiVsMplsRow {
                    pid: 0x1101,
                    clpi_coding_type: Some(0x83),
                    clpi_language: Some("fra".into()),
                    mpls_coding_type: None,
                    mpls_language: None,
                }, // ClpiOnly
                ClpiVsMplsRow {
                    pid: 0x1102,
                    clpi_coding_type: None,
                    clpi_language: None,
                    mpls_coding_type: Some(0x90),
                    mpls_language: Some("eng".into()),
                }, // MplsOnly
                ClpiVsMplsRow {
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
}
