//! "dbp" framework — a BD-J authoring framework identified by `com/dbp/`
//! package paths in a top-level `/BDMV/JAR/<x>.jar`. Seen on UHD discs.
//!
//! Stream labels live as plain ASCII strings inside compiled `.class`
//! files in the jar. Observed format:
//!
//! ```text
//! LTextField,Audio1,English Dolby Atmos,Fontstrip_Composite,...
//! RTextField,Audio2,English Descriptive Audio,Fontstrip_Composite,...
//! HTextField,Subtitle1,English SDH,Fontstrip_Composite,...
//! ATextField,Subtitle0,None,Fontstrip_Composite,...
//! ```
//!
//! The parser ignores any prefix before `TextField,`; `Subtitle0` is the disable-subtitles
//! button, skipped. Classification lives in [`super::vocab`].

use super::class_reader::CpInfo;
use super::{ParseResult, StreamLabel, StreamLabelType, jar, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::BTreeMap;

/// The real dbp signal is the `com/dbp/` package prefix inside a top-level
/// jar's central directory. With a reader in `detect`, we check that directly
/// (a cheap central-directory scan, no class decode) so this parser claims
/// only dbp discs instead of firing on every BD-J disc. `parse()` repeats the
/// check as belt-and-suspenders.
pub fn detect(reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    jar::any_jar_has_prefix(reader, udf, "com/dbp/")
}

/// Scan every top-level `/BDMV/JAR/*.jar` for the dbp framework and
/// extract its stream labels. Returns `None` if no jar carries a
/// `com/dbp/` package path or none yields any labels.
pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    // One inflate budget for every jar this parse sweeps.
    let mut budget = jar::PARSE_INFLATE_BUDGET;
    jar::for_each_jar(reader, udf, |_entry_name, archive| {
        if !jar::has_path_prefix(archive, "com/dbp/") {
            return None;
        }
        let labels = scan_jar(archive, &mut budget);
        if labels.is_empty() {
            None
        } else {
            // High confidence: TextField,Audio1,... is a stable anchor
            // pattern + vocab routes language/purpose/qualifier.
            Some(ParseResult::high(labels))
        }
    })
}

fn scan_jar(archive: &mut jar::Jar, budget: &mut u64) -> Vec<StreamLabel> {
    // BTreeMap keeps the last-written label per stream slot deterministic.
    // The same TextField,Audio1,... string can appear in multiple classes
    // (button-state variants, fallbacks); last write should agree, but wins defensively.
    let mut audios: BTreeMap<u16, String> = BTreeMap::new();
    let mut subs: BTreeMap<u16, String> = BTreeMap::new();

    jar::for_each_class_budgeted(archive, budget, |_class_name, class| {
        for (_idx, cp) in class.constant_pool.iter() {
            if let CpInfo::Utf8(s) = cp {
                collect_textfield(s, &mut audios, &mut subs);
            }
        }
    });

    let mut out = Vec::new();
    for (num, label) in audios {
        out.push(make_label(num, label, StreamLabelType::Audio));
    }
    for (num, label) in subs {
        out.push(make_label(num, label, StreamLabelType::Subtitle));
    }
    out
}

// Cap on bytes retained per stream label. CONSTANT_Utf8_info's `length` is a u16 (JVMS §4.4.7),
// so a crafted constant could contribute up to 65535 bytes.
const MAX_LABEL_BYTES: usize = 256;

// Cap on retained stream slots per type. Keys come from parse::<u16> on disc bytes, so all
// 65536 slots per type are reachable.
const MAX_LABELS_PER_TYPE: usize = 512;

/// Record `label` for stream `n`, honouring the retention caps. Existing
/// slots are still overwritten at the cap so the documented last-write-wins
/// behaviour is preserved; only NEW slots are refused.
fn retain_label(map: &mut BTreeMap<u16, String>, n: u16, label: &str) {
    if label.len() > MAX_LABEL_BYTES {
        return;
    }
    if map.len() >= MAX_LABELS_PER_TYPE && !map.contains_key(&n) {
        return;
    }
    map.insert(n, label.to_string());
}

fn collect_textfield(
    s: &str,
    audios: &mut BTreeMap<u16, String>,
    subs: &mut BTreeMap<u16, String>,
) {
    // Anchor on "TextField," — the prefix character before it varies
    // (string-pool ordering inside compiled Java) and is irrelevant.
    let Some(idx) = s.find("TextField,") else {
        return;
    };
    let after = &s[idx + "TextField,".len()..];
    let mut parts = after.splitn(3, ',');
    let kind_n = parts.next().unwrap_or("").trim();
    let label = parts.next().unwrap_or("").trim();
    if label.is_empty() {
        return;
    }
    if let Some(rest) = kind_n.strip_prefix("Audio") {
        // Stream numbers are 1-based; 0 (NO_STN_SLOT) can never bind.
        if let Ok(n) = rest.parse::<u16>()
            && n > 0
        {
            retain_label(audios, n, label);
        }
    } else if let Some(rest) = kind_n.strip_prefix("Subtitle")
        && let Ok(n) = rest.parse::<u16>()
    {
        // Subtitle0 is conventionally the "None / Off" disable
        // button, not an actual subtitle stream.
        if n > 0 {
            retain_label(subs, n, label);
        }
    }
}

fn make_label(num: u16, label: String, stream_type: StreamLabelType) -> StreamLabel {
    let lang_info = vocab::lang(&label);
    let language = lang_info.map(|l| l.code).unwrap_or("").to_string();
    let variant = lang_info.map(|l| l.variant).unwrap_or("").to_string();
    let qualifier = vocab::qualifier(&label);
    let purpose = vocab::purpose(&label);
    StreamLabel {
        stream_id: None,
        stream_number: num,
        stream_type,
        language,
        name: label,
        purpose,
        qualifier,
        codec_hint: String::new(),
        variant,
    }
}

#[cfg(test)]
#[path = "dbp_tests.rs"]
mod tests;
