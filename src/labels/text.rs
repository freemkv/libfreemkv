//! Text-extraction helpers used by parsers that scan binary blobs for
//! embedded label strings.
//!
//! Promoted from two near-duplicate implementations:
//! - `pixelogic::extract_strings` (`bluray_project.bin`, min_len=4)
//! - `dbp::extract_printable` (`.class` files in jars, min_len=5)
//!
//! Single implementation, threshold passed in. Callers that have a
//! more structured parse path (e.g. `class_reader` for .class) should
//! prefer that — this helper is for genuinely unstructured input.

// Staged for the pixelogic refactor: pixelogic still has its own
// extract_strings copy; this is the shared replacement waiting for
// the refactor. dead-code allow comes off when pixelogic switches.
#![allow(dead_code)]

/// Walk `data`, emit every maximal run of printable-ASCII bytes
/// (`0x20..=0x7E`) whose length is at least `min_len`.
///
/// Non-printable bytes (including `\t`, `\n`, NUL) terminate the
/// current run. Output strings are guaranteed valid UTF-8 (they're
/// pure 7-bit ASCII). Strings shorter than `min_len` are dropped.
pub fn extract_ascii_strings(data: &[u8], min_len: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for &b in data {
        if (0x20..=0x7E).contains(&b) {
            current.push(b as char);
        } else if current.len() >= min_len {
            out.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if current.len() >= min_len {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_simple_runs() {
        let got = extract_ascii_strings(b"hello\0world\0", 3);
        assert_eq!(got, vec!["hello", "world"]);
    }

    #[test]
    fn applies_minimum_length() {
        let got = extract_ascii_strings(b"hi\0ok\0longer\0", 5);
        assert_eq!(got, vec!["longer"]);
    }

    #[test]
    fn treats_tab_and_newline_as_separators() {
        // \t (0x09) and \n (0x0A) are below 0x20, so they break runs.
        let got = extract_ascii_strings(b"alpha\tbeta\ngamma", 3);
        assert_eq!(got, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn handles_trailing_run_without_terminator() {
        // Run at the very end of the buffer should still be emitted.
        let got = extract_ascii_strings(b"prefix\0tail", 3);
        assert_eq!(got, vec!["prefix", "tail"]);
    }

    #[test]
    fn rejects_high_bit_bytes() {
        // 0x80+ is non-printable per this helper's definition.
        let mut buf = b"good".to_vec();
        buf.push(0xC3);
        buf.push(0xA9);
        buf.extend_from_slice(b"more");
        let got = extract_ascii_strings(&buf, 3);
        assert_eq!(got, vec!["good", "more"]);
    }

    #[test]
    fn empty_input_returns_empty() {
        let got = extract_ascii_strings(&[], 1);
        assert!(got.is_empty());
    }

    #[test]
    fn min_len_zero_emits_singletons() {
        // Pathological but well-defined.
        let got = extract_ascii_strings(b"a\0b", 0);
        assert_eq!(got, vec!["a", "b"]);
    }
}
