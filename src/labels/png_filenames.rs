//! PNG-filename language token parser — stubbed (noop) pending need.
//!
//! ## What this would do
//!
//! Some discs encode per-language menu localization as pre-rendered PNG
//! menu buttons, one per language, with the language token embedded in
//! the filename. Examples observed in the 2026-05-10 corpus:
//!
//! - **disc-01 (The Amateur)** — `<region>_<lang>_<context>_<format>.png`
//!   Region prefix: `USA` / `UK` / `JPN` / etc.
//!   Lang tokens (3-char, uppercase): `ENG`, `FRC`, `FRP`, `DEU`, `DUT`,
//!   `ITA`, `JPN`, `LAS`, `CSP`, `POL`, `CZE` (11 languages)
//!
//! - **disc-09 (Dune orig)** — `<title>_<variant>_<lang>_Composite<N>.png`
//!   Lang tokens (3-char, mixed-case): `Eng`, `Ger` (2 languages)
//!
//! ## Why stubbed
//!
//! MPLS already gives per-stream `language` + `coding_type` + stream-type
//! (audio vs subtitle) on every disc. For the 2 unknown-framework discs
//! that PNG filenames would close (disc-01, disc-09), MPLS will produce
//! a strict superset of what filenames could give us, because MPLS knows
//! per-stream attribution while filenames only know "the disc offers
//! these N language buttons."
//!
//! The **only** thing PNG filenames give us that MPLS doesn't is **studio
//! variant disambiguation**:
//! - `FRC` (French Canadian) vs `FRP` (French Parisian) — MPLS just says `fra`
//! - `LAS` (Latin American Spanish) vs `CSP` (Castilian Spanish) — MPLS just says `spa`
//!
//! That's niche enough that it doesn't justify implementing right now.
//! Reactivate this parser only when:
//! 1. We hit a disc where MPLS is malformed/empty AND PNG filenames are
//!    the only language hint, OR
//! 2. A downstream consumer needs the studio variant suffix for output
//!    naming (e.g. `Title (French Canadian).mkv` vs `Title (French).mkv`).
//!
//! ## When reactivating
//!
//! Implement `parse` to:
//! 1. Iterate top-level PNG paths in `/BDMV/JAR/` (and `<id>/` subdirs).
//! 2. Tokenize each filename on `_` / `-` / `.`
//! 3. Match each token against an alias table:
//!    - ISO 639-1 / 639-2 standard codes
//!    - Studio variants: `FRC`/`FRP` → `fra-CA`/`fra-FR`,
//!      `LAS`/`CSP` → `spa-419`/`spa-ES`,
//!      mixed-case shortforms `Eng`/`Ger`/`Fra`/`Spa`/`Jpn` → ISO 639-2
//!    - Country prefix filter: drop `USA`/`UK`/`JPN`/`AUS`/`GER`/`FR` when
//!      they appear in position 0 (those are region markers, not langs).
//! 4. Deduplicate. Confidence stays `Low` because we still don't know
//!    per-stream codec or audio/subtitle attribution.
//!
//! Wire as ENRICHMENT after MPLS in `mod.rs::analyze`, not as a primary
//! parser: PNG filenames upgrade `lang=fra` to `lang=fra-CA` when both
//! sources agree on the disc; they should never overwrite MPLS data.

use super::ParseResult;
use crate::sector::SectorReader;
use crate::udf::UdfFs;

/// Stub: returns false so the dispatcher never calls `parse`. Reactivate
/// by checking for the patterns described in the module docs.
#[allow(dead_code)] // module-level noop, not wired into PARSERS until needed
pub fn detect(_udf: &UdfFs) -> bool {
    false
}

/// Stub: returns None. See module docs for the implementation sketch.
#[allow(dead_code)] // module-level noop, not wired into PARSERS until needed
pub fn parse(_reader: &mut dyn SectorReader, _udf: &UdfFs) -> Option<ParseResult> {
    None
}
