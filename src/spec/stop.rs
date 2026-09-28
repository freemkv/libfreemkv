//! `SS-n`: the quotes behind the Stop design's external behaviour (SCSI, POSIX, RFC,
//! OS and language docs). IDs SS-1…SS-25 are allocated by the Stop design §5.9;
//! SS-16…SS-19, SS-24 and SS-25 are retired and never reused. Rows land with the
//! step that first cites them.

use super::{QuoteKind, SpecQuote};

const RUST_STD: &str = "The Rust Standard Library, core::sync::atomic (Rust 1.98.1)";
const ORDERING_URL: &str = "https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html";

pub const SS_23_RELEASE_ACQUIRE: SpecQuote = SpecQuote {
    id: "SS-23",
    kind: QuoteKind::Normative,
    source: RUST_STD,
    section: "enum Ordering, variants Release and Acquire",
    locator: "doc.rust-lang.org std::sync::atomic::Ordering; library/core/src/sync/atomic.rs",
    url: ORDERING_URL,
    text: "In particular, all previous writes become visible to all threads that perform an \
           Acquire (or stronger) load of this value. … In particular, all subsequent loads will \
           see data written before the store.",
};

/// Every `SS-n` quote, in ID order.
pub const ALL: &[&SpecQuote] = &[&SS_23_RELEASE_ACQUIRE];
