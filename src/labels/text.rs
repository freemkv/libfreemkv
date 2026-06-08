//! Text-extraction helpers used by parsers that scan binary blobs for
//! embedded label strings.
//!
//! Promoted from a byte-scanning helper (`bluray_project.bin`,
//! min_len=4). Single implementation, threshold passed in.
//!
//! `dbp` no longer uses a byte-scanning helper — it iterates
//! `class_reader::CpInfo::Utf8` constant-pool entries directly. Callers
//! that have a more structured parse path (e.g. `class_reader` for
//! `.class`) should prefer that; this helper is for genuinely
//! unstructured input.

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
        } else if !current.is_empty() && current.len() >= min_len {
            out.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if !current.is_empty() && current.len() >= min_len {
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

    #[test]
    fn min_len_zero_skips_empty_runs_on_consecutive_separators() {
        // Consecutive separators must NOT emit empty strings even at
        // min_len=0 — an empty string is not a "run of printable bytes".
        let got = extract_ascii_strings(b"\0\0abc", 0);
        assert_eq!(got, vec!["abc"]);
        let got = extract_ascii_strings(b"ab\0\0\0cd\0\0", 0);
        assert_eq!(got, vec!["ab", "cd"]);
    }

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: printable ASCII is 0x20..=0x7E inclusive. 0x1F (US) and 0x7F (DEL)
    /// are NOT printable and must terminate a run.
    /// Mutation: change the range to 0x20..=0x7F → DEL included.
    #[test]
    fn del_character_0x7f_terminates_run() {
        // 0x7F is DEL — not printable per our definition.
        let mut buf = b"hello".to_vec();
        buf.push(0x7F);
        buf.extend_from_slice(b"world");
        let got = extract_ascii_strings(&buf, 3);
        assert_eq!(got, vec!["hello", "world"]);
    }

    /// Spec: 0x1F (unit separator) is below 0x20 — must terminate a run.
    /// Mutation: change range to start at 0x00 → control chars included.
    #[test]
    fn unit_separator_0x1f_terminates_run() {
        let mut buf = b"abc".to_vec();
        buf.push(0x1F);
        buf.extend_from_slice(b"defg");
        let got = extract_ascii_strings(&buf, 3);
        assert_eq!(got, vec!["abc", "defg"]);
    }

    /// Spec: 0x20 (space) is the lower bound — MUST be included in runs.
    /// Mutation: change range to start at 0x21 → spaces excluded, "hello world" splits.
    #[test]
    fn space_0x20_included_in_run() {
        let got = extract_ascii_strings(b"hello world\0", 5);
        assert_eq!(got, vec!["hello world"]);
    }

    /// Spec: 0x7E (tilde) is the upper bound — MUST be included.
    /// Mutation: change range to 0x20..0x7E (exclusive) → tilde excluded.
    #[test]
    fn tilde_0x7e_included_in_run() {
        let got = extract_ascii_strings(b"hello~world\0", 3);
        assert_eq!(got, vec!["hello~world"]);
    }

    /// Spec: min_len=4 (Pixelogic's minimum). Token "abc" (length 3) must be dropped.
    /// Mutation: use `>` instead of `>=` for the length check → "abcd" (len 4) dropped.
    #[test]
    fn min_len_4_boundary() {
        let got = extract_ascii_strings(b"abc\0abcd\0abcde\0", 4);
        assert_eq!(got, vec!["abcd", "abcde"]);
    }

    /// Spec: output strings are guaranteed valid UTF-8 (pure 7-bit ASCII).
    /// This test verifies the invariant: no string contains non-ASCII bytes.
    /// Mutation: skip the 0x80..=0xFF filter → high bytes appear in output.
    #[test]
    fn output_strings_are_pure_ascii() {
        let mut buf = Vec::new();
        for b in 0x20u8..=0x7Eu8 {
            buf.push(b);
        }
        buf.push(0u8);
        let got = extract_ascii_strings(&buf, 1);
        assert_eq!(got.len(), 1);
        for s in &got {
            assert!(s.is_ascii(), "output must be pure ASCII: {:?}", s);
        }
    }

    /// Large all-printable buffer: verify the tail run is emitted.
    /// Mutation: skip the final `if !current.is_empty()` emit → trailing run lost.
    #[test]
    fn large_buffer_trailing_run_emitted() {
        let buf: Vec<u8> = (0..1000u32).map(|i| (0x41u8 + (i % 26) as u8)).collect();
        let got = extract_ascii_strings(&buf, 1);
        // All printable, so one big run at the end.
        assert!(!got.is_empty());
        let total: usize = got.iter().map(|s| s.len()).sum();
        assert_eq!(total, 1000);
    }

    /// Consecutive non-printable bytes must not produce empty strings.
    /// Mutation: remove the `!current.is_empty()` guard on the emit → empty strings pushed.
    #[test]
    fn no_empty_strings_in_output() {
        let got = extract_ascii_strings(b"\x00\x00\x00hello\x00\x00\x00world\x00\x00", 3);
        for s in &got {
            assert!(!s.is_empty(), "output must contain no empty strings");
        }
        assert_eq!(got, vec!["hello", "world"]);
    }

    /// The Pixelogic token grammar starts at length 4 (`{lang3}_{…}`).
    /// Verify that a token of exactly 4 chars `eng_` is emitted when min_len=4.
    /// Mutation: use `>` instead of `>=` → len-4 token dropped.
    #[test]
    fn exact_min_len_token_emitted() {
        let got = extract_ascii_strings(b"\x00eng_\x00", 4);
        assert_eq!(got, vec!["eng_"]);
    }

    /// Single printable byte with min_len=1 must be emitted.
    /// Mutation: use `> 1` → single-char tokens dropped.
    #[test]
    fn single_byte_at_min_len_1() {
        let got = extract_ascii_strings(b"A\x00B\x00C", 1);
        assert_eq!(got, vec!["A", "B", "C"]);
    }
}
