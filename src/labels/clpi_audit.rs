//! CLPI vs MPLS cross-validation diagnostic.
//!
//! Walks both per-clip CLPI program info and per-playlist MPLS STN
//! tables, normalizes their stream lists by `(PID, language,
//! coding_type)`, and classifies each PID into one of four buckets:
//!
//! 1. **CLPI only** — a stream present in a `.clpi` ProgramInfo that no
//!    playlist STN table references (orphan on disc).
//! 2. **MPLS only** — a stream a playlist references that no `.clpi`
//!    ProgramInfo lists (indicates a parser disagreement).
//! 3. **Match** — both sources agree on coding_type and language.
//! 4. **Divergent** — both sources see the PID but disagree on
//!    coding_type or language.
//!
//! [`audit`] returns a structured [`ClpiVsMplsAudit`] report. This is a
//! diagnostic surface only; it does not feed the label-selection
//! pipeline.

use crate::sector::SectorSource;
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
/// dedup-by-PID table of (CLPI fields, MPLS fields), return the
/// merged view. Missing files (read errors, parse failures) are
/// silently skipped — this is diagnostic, not correctness-critical.
pub fn audit(reader: &mut dyn SectorSource, udf: &UdfFs) -> ClpiVsMplsAudit {
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
                if s.pid == 0 {
                    // PID 0 means "no PID in stream entry" — skip rather
                    // than collide, mirroring the MPLS side below.
                    continue;
                }
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
    fn class_both_coding_missing_divergent_on_lang() {
        // Caller-built row with neither coding_type but disagreeing
        // languages must classify Divergent, not Match.
        let r = ClpiVsMplsRow {
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

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: coding_type mismatch with matching language → Divergent (not Match).
    /// The spec doc says both coding_type AND language must agree for Match.
    /// Mutation: only check language for match → coding_type mismatch silently classified as Match.
    #[test]
    fn class_divergent_on_coding_type_mismatch_same_lang() {
        let r = ClpiVsMplsRow {
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

    /// Spec: (false, false) branch — both coding_types absent, equal language → Match.
    /// The spec comment says "compare language fields; Divergent if they differ, else Match".
    /// Mutation: return Divergent for any (false, false) case → this test goes red.
    #[test]
    fn class_both_coding_absent_equal_none_lang_is_match() {
        let r = ClpiVsMplsRow {
            pid: 0x1100,
            clpi_coding_type: None,
            clpi_language: None,
            mpls_coding_type: None,
            mpls_language: None,
        };
        // Both languages are None == None → Match.
        assert_eq!(r.class(), ClpiVsMplsClass::Match);
    }

    /// Spec: all four classes form an exhaustive disjoint cover.
    /// This test verifies the discriminant logic using boundary coding_type values.
    /// Mutation: swap the ClpiOnly/MplsOnly branches → wrong classification.
    #[test]
    fn class_boundary_coding_types_all_four_classes_reachable() {
        let clpi_only = ClpiVsMplsRow {
            pid: 1,
            clpi_coding_type: Some(1),
            clpi_language: None,
            mpls_coding_type: None,
            mpls_language: None,
        };
        let mpls_only = ClpiVsMplsRow {
            pid: 2,
            clpi_coding_type: None,
            clpi_language: None,
            mpls_coding_type: Some(1),
            mpls_language: None,
        };
        let match_ = ClpiVsMplsRow {
            pid: 3,
            clpi_coding_type: Some(0x83),
            clpi_language: Some("eng".into()),
            mpls_coding_type: Some(0x83),
            mpls_language: Some("eng".into()),
        };
        let divergent = ClpiVsMplsRow {
            pid: 4,
            clpi_coding_type: Some(0x83),
            clpi_language: Some("eng".into()),
            mpls_coding_type: Some(0x83),
            mpls_language: Some("fra".into()),
        };
        assert_eq!(clpi_only.class(), ClpiVsMplsClass::ClpiOnly);
        assert_eq!(mpls_only.class(), ClpiVsMplsClass::MplsOnly);
        assert_eq!(match_.class(), ClpiVsMplsClass::Match);
        assert_eq!(divergent.class(), ClpiVsMplsClass::Divergent);
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
                    pid: 1,
                    clpi_coding_type: Some(0x83),
                    clpi_language: None,
                    mpls_coding_type: None,
                    mpls_language: None,
                },
                ClpiVsMplsRow {
                    pid: 2,
                    clpi_coding_type: Some(0x82),
                    clpi_language: None,
                    mpls_coding_type: None,
                    mpls_language: None,
                },
                // 1 MplsOnly
                ClpiVsMplsRow {
                    pid: 3,
                    clpi_coding_type: None,
                    clpi_language: None,
                    mpls_coding_type: Some(0x90),
                    mpls_language: None,
                },
                // 3 Match
                ClpiVsMplsRow {
                    pid: 4,
                    clpi_coding_type: Some(0x83),
                    clpi_language: Some("eng".into()),
                    mpls_coding_type: Some(0x83),
                    mpls_language: Some("eng".into()),
                },
                ClpiVsMplsRow {
                    pid: 5,
                    clpi_coding_type: Some(0x82),
                    clpi_language: Some("fra".into()),
                    mpls_coding_type: Some(0x82),
                    mpls_language: Some("fra".into()),
                },
                ClpiVsMplsRow {
                    pid: 6,
                    clpi_coding_type: Some(0x86),
                    clpi_language: Some("deu".into()),
                    mpls_coding_type: Some(0x86),
                    mpls_language: Some("deu".into()),
                },
                // 1 Divergent
                ClpiVsMplsRow {
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
}
