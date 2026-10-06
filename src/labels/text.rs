//! Text-extraction helpers used by parsers that scan binary blobs for
//! embedded label strings.
//!
//! Promoted from a byte-scanning helper (`bluray_project.bin`, min_len=4);
//! single implementation, threshold passed in. Callers with a more
//! structured parse path (e.g. `class_reader` for `.class`, which reads
//! `CpInfo::Utf8` entries directly) should prefer that — this helper is
//! for genuinely unstructured input.

/// Upper bound on returned strings, so a hostile blob of tiny runs cannot amplify into memory.
const MAX_STRINGS: usize = 65_536;

/// Walk `data`, emit every maximal run of printable-ASCII bytes
/// (`0x20..=0x7E`) whose length is at least `min_len`.
///
/// Non-printable bytes (including `\t`, `\n`, NUL) terminate the
/// current run. Output strings are guaranteed valid UTF-8 (they're
/// pure 7-bit ASCII). Strings shorter than `min_len` are dropped; at most
/// `MAX_STRINGS` are returned.
pub fn extract_ascii_strings(data: &[u8], min_len: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for &b in data {
        if out.len() >= MAX_STRINGS {
            return out;
        }
        if (0x20..=0x7E).contains(&b) {
            current.push(b as char);
        } else if !current.is_empty() && current.len() >= min_len {
            out.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if !current.is_empty() && current.len() >= min_len && out.len() < MAX_STRINGS {
        out.push(current);
    }
    out
}

#[cfg(test)]
#[path = "text_tests.rs"]
mod tests;
