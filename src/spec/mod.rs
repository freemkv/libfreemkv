//! Verbatim quotes from the specifications (and, where none is public, the
//! evidence) that govern libfreemkv's behaviour. Text only: no logic.
//!
//! One submodule per design, each with its own ID prefix: [`keys`] holds `KS-n`
//! (AACS, UDF, libaacs corroboration, evidence), [`stop`] holds `SS-n`. IDs are sequential per prefix and
//! never reused or renumbered. Const names are `<PREFIX>_<n>_<SHORT>`.
//!
//! `text` is the passage as rendered in the source (radix subscripts as `₂`/`₁₆`,
//! `⊕` for XOR, curly quotes as printed, hyphenation and line breaks removed);
//! `" … "` separates elided fragments. `tests/spec_quotes.txt` holds the unelided
//! passage per ID, and `spec_quotes_match_registry` checks every const against it.
//! Registry == source document is a review item (open the PDF page or URL).
//!
//! A comment or test may say "per spec" only for [`QuoteKind::Normative`] or
//! [`QuoteKind::Informative`] rows; otherwise "corroborated by" or "per evidence".

pub mod keys;
pub mod stop;

/// Where a quote's text comes from; binding on how a citing site may word it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteKind {
    /// Normative text of a published specification.
    Normative,
    /// Informative text of a published specification (cite it as Informative).
    Informative,
    /// A reference implementation that agrees with (or refines) a spec.
    Corroboration,
    /// No public spec exists: a fixture, measurement or service contract.
    Evidence,
}

/// One quote. `kind` and `source` describe the document `text` was copied from.
#[derive(Debug)]
pub struct SpecQuote {
    /// `"KS-5"`: unique, never reused; the const name's number agrees.
    pub id: &'static str,
    pub kind: QuoteKind,
    /// The document, e.g. "AACS Blu-ray Disc Pre-recorded Book, Final Rev 0.953".
    pub source: &'static str,
    /// The section (and table or figure) the text sits in.
    pub section: &'static str,
    /// Physical PDF page, `file:line @rev`, or where the evidence lives.
    pub locator: &'static str,
    /// An accessible copy of the source; empty when the evidence is not public.
    pub url: &'static str,
    /// Rendered verbatim text; `" … "` separates elided fragments.
    pub text: &'static str,
}

/// Every quote group, one per submodule. New designs append their group.
pub const ALL: &[&[&SpecQuote]] = &[keys::ALL, stop::ALL];

/// Every quote in [`ALL`], group by group.
pub fn quotes() -> impl Iterator<Item = &'static SpecQuote> {
    ALL.iter().flat_map(|group| group.iter().copied())
}
