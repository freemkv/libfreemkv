# `src/labels/png_filenames.rs`

## Why Low confidence, and why this is a first-class parser

This is a language-only hint (no per-stream purpose/codec), so it runs at
`Confidence::Low` — it never displaces a real framework parser, and it sits
at the same tier as the MPLS floor. It is here so the pattern is a
first-class, testable parser that keeps picking up discs as the corpus
grows, rather than lost logic. Detection is precise: it fires only on the
`_UHD01_{LANG}_Composite` grammar with a `{LANG}` the vocab recognizes.

## Why the marker+suffix avoid false positives

`filename_lang` extracts the ISO-639-2 language code from a
`{title}_UHD01_{LANG}_Composite` menu-graphic filename, or returns `None` if
the name does not match the grammar or carries a `{LANG}` the vocab does not
recognize.

The `_UHD01_` marker plus the `_Composite` suffix keep this from firing on
unrelated PNGs (`KeyComposite4.png`, `LoadingComposite1.png` have no
`_UHD01_{LANG}_` segment).
