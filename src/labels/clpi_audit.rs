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
#[path = "clpi_audit_tests.rs"]
mod tests;
