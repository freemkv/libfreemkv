//! Test-only capture of `tracing` events, so the crate's logging contract is
//! ENFORCED rather than merely commented.
//!
//! # Why this exists
//!
//! The error contract is "Account / Log / Classify", and the round-2 audit
//! found the third leg unverifiable: three separate sites carry long comments
//! insisting they log **the error's OWN code, not a fixed one** — because
//! flattening a scratched sector (E6000) or an over-long allocation-descriptor
//! chain (E6016) into E6017 sends whoever triages them after authoring holes
//! and hides the population that actually exists. Nothing tested that. Putting
//! a literal back at `bluray.rs`'s or `hddvd.rs`'s warn sites broke no test, so
//! the guarantee was a convention one careless edit away from being false. Two
//! of those very sites were changed in round 1, and a third still carried a
//! hardcoded `code = 6017`.
//!
//! Likewise "absence of a log is itself a bug": a refusal that returns the
//! right error but says nothing produces the wrong population downstream (a
//! residual-underrunning drive is indistinguishable from a scratched disc).
//! That is only checkable by looking at what was emitted.
//!
//! # Why a hand-rolled subscriber and not `tracing-subscriber`
//!
//! Same posture as [`crate::harness`]: this crate has exactly one
//! dev-dependency on purpose. `tracing-subscriber` would pull a tree of them to
//! do what forty lines of [`tracing::Subscriber`] does here. The capture is
//! installed with [`tracing::subscriber::with_default`], which is
//! THREAD-LOCAL — so it composes with `cargo test`'s parallel harness and two
//! capturing tests cannot see each other's events.
//!
//! Field values are stringified through [`std::fmt::Debug`]/`Display` because
//! that is all the visitor API offers without a typed schema; tests compare
//! against the string form of the expected constant, which is exactly the
//! comparison that catches a hardcoded code.

#![cfg(test)]

use std::sync::{Arc, Mutex};

/// One captured `tracing` event: its target, level, message and fields.
#[derive(Debug, Clone)]
pub(crate) struct CapturedEvent {
    pub target: String,
    pub level: tracing::Level,
    /// Every field, in emission order, stringified. The implicit `message`
    /// field (the format string) is included under the name `message`.
    pub fields: Vec<(String, String)>,
}

impl CapturedEvent {
    /// The stringified value of `name`, or `None` if the event has no such
    /// field.
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The event's message (the `tracing` format string), or `""`.
    pub fn message(&self) -> &str {
        self.field("message").unwrap_or("")
    }
}

#[derive(Default)]
struct Visitor(Vec<(String, String)>);

impl tracing::field::Visit for Visitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
}

struct Capture(Arc<Mutex<Vec<CapturedEvent>>>);

impl tracing::Subscriber for Capture {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    // Spans are irrelevant here — nothing in this crate asserts on span
    // structure, only on events — so they get a constant id and no storage.
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut v = Visitor::default();
        event.record(&mut v);
        let meta = event.metadata();
        self.0.lock().expect("capture mutex").push(CapturedEvent {
            target: meta.target().to_string(),
            level: *meta.level(),
            fields: v.0,
        });
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Process-wide lock serialising captures. See [`capture`].
static CAPTURE_LOCK: Mutex<()> = Mutex::new(());

/// Run `f` with every `tracing` event emitted on THIS thread captured.
///
/// Returns `f`'s value alongside the events, in emission order.
///
/// # Why captures are serialised across the whole test binary
///
/// `tracing`'s per-callsite INTEREST CACHE is global, while
/// `with_default` is thread-local, and the two race under `cargo test`'s
/// parallel harness. `tracing_core` rebuilds that cache only on the 0 -> 1 and
/// 1 -> 0 transitions of its scoped-dispatcher count: entering the first
/// capture flips every callsite to "ask the subscriber", leaving the last one
/// flips them back to "never" (there being no global subscriber in tests).
/// With two capturing tests on two threads, the exiting one's rebuild can land
/// AFTER the entering one's, leaving the cache at "never" while a capture is
/// live — so the callsite is short-circuited and the capture observes NOTHING.
///
/// That was not theoretical: `parse_playlist_unreadable_clip_icb_yields_no_title`
/// passed alone and failed in the full suite, with an empty event list, the
/// first time two capturing tests existed in one module. A logging assertion
/// that fails at random is worse than none — it teaches the next person to
/// re-run until green, which is how a real regression gets waved through.
///
/// Holding this lock across the whole of `with_default` — including the
/// guard's drop, which is where the exiting rebuild happens — orders the
/// transitions strictly. Captures are few and short, so the serialisation
/// costs nothing measurable.
///
/// The lock is deliberately taken through the poison, not `expect`ed: a
/// capturing test that panics (i.e. a genuine assertion failure) must not
/// convert every other capturing test into a confusing secondary failure.
pub(crate) fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<CapturedEvent>) {
    let _guard = CAPTURE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let sink: Arc<Mutex<Vec<CapturedEvent>>> = Arc::default();
    let out = tracing::subscriber::with_default(Capture(sink.clone()), f);
    let events = std::mem::take(&mut *sink.lock().expect("capture mutex"));
    (out, events)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The capture must actually see events and their field VALUES — if it
    /// silently recorded nothing, every logging assertion built on it would
    /// pass vacuously, which is worse than having no harness at all.
    ///
    /// Mutation: an `enabled()` returning `false`, or an `event()` that drops
    /// the visitor's fields, fails here.
    #[test]
    fn capture_records_target_level_and_fields() {
        let ((), events) = capture(|| {
            tracing::warn!(target: "freemkv::testlog", code = 6017u16, clip = ?"A.EVO", "E6017");
        });
        assert_eq!(events.len(), 1, "exactly one event: {events:?}");
        assert_eq!(events[0].target, "freemkv::testlog");
        assert_eq!(events[0].level, tracing::Level::WARN);
        assert_eq!(events[0].field("code"), Some("6017"));
        assert_eq!(events[0].field("clip"), Some("\"A.EVO\""));
        assert_eq!(events[0].message(), "E6017");
    }

    /// A field that is absent must read as `None`, not as an empty string — an
    /// assertion of the shape `field("code") == Some(..)` has to be able to
    /// fail when the site stops logging the code at all.
    #[test]
    fn missing_field_is_none_and_capture_is_scoped() {
        let ((), events) = capture(|| tracing::warn!(target: "freemkv::testlog", "no fields"));
        assert_eq!(events[0].field("code"), None);
        // Emitted outside `capture`, so it must not appear in a later capture.
        tracing::warn!(target: "freemkv::testlog", code = 1u16, "outside");
        let ((), later) = capture(|| {});
        assert!(
            later.is_empty(),
            "capture is scoped to its closure: {later:?}"
        );
    }
}
