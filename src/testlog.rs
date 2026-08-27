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

type Sink = Arc<Mutex<Vec<CapturedEvent>>>;

thread_local! {
    /// The sink for a capture ACTIVE ON THIS THREAD, if any. Thread-local so
    /// concurrent captures across `cargo test`'s parallel harness never see
    /// each other's events, and so a non-capturing thread simply has `None`.
    static SINK: std::cell::RefCell<Option<Sink>> = const { std::cell::RefCell::new(None) };
}

/// The ONE process-wide subscriber. Installed once and left installed; it is
/// offered every event and records into whichever thread's sink is active,
/// dropping the event when none is.
struct Capture;

impl tracing::Subscriber for Capture {
    // `sometimes`, not `always`/`never`: a cacheable interest lets tracing's global
    // per-callsite cache latch to "off" if another thread's rebuild races a live
    // capture. `sometimes` forces `enabled` to run per-event, so captures always see their own.
    fn register_callsite(
        &self,
        _meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        SINK.with(|s| s.borrow().is_some())
    }
    // Spans are irrelevant here — nothing in this crate asserts on span
    // structure, only on events — so they get a constant id and no storage.
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        SINK.with(|s| {
            if let Some(sink) = s.borrow().as_ref() {
                let mut v = Visitor::default();
                event.record(&mut v);
                let meta = event.metadata();
                sink.lock().expect("capture mutex").push(CapturedEvent {
                    target: meta.target().to_string(),
                    level: *meta.level(),
                    fields: v.0,
                });
            }
        });
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Install the single global capturing subscriber, exactly once.
fn install() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        // This is the crate's ONLY `set_global_default`; a second call cannot
        // happen. Tolerate it via `.ok()` rather than panic if that ever
        // changes — capture then no-ops, which the non-empty assertions catch.
        let _ = tracing::subscriber::set_global_default(Capture);
    });
}

/// Run `f` with every `tracing` event it emits ON THIS THREAD captured.
///
/// Returns `f`'s value alongside the events, in emission order.
///
/// # Why one global subscriber, not scoped `with_default`
///
/// `tracing`'s per-callsite INTEREST CACHE is GLOBAL, but `with_default` is
/// thread-local. Under `cargo test`'s parallel harness a rebuild of that cache
/// — triggered by ANY other thread registering any callsite for the first time
/// — re-evaluates the target callsite against the global dispatcher, which a
/// scoped subscriber is NOT part of, and can leave it cached "off" while a
/// capture is live, so the capture observes NOTHING. Serialising captures
/// against each other does not help: the poisoning thread is not itself
/// capturing. `parse_playlist_unreadable_clip_icb_yields_no_title` flaked ~1
/// run in 15 on exactly this — an empty event list.
///
/// The robust shape is a single subscriber installed globally for the whole
/// run, whose `register_callsite` returns `sometimes` (so no callsite is ever
/// hard-cached) and which routes each event to the emitting thread's own sink.
/// No scoped-dispatcher transitions, no cross-thread cache race, and concurrent
/// captures on different threads stay isolated by the thread-local sink.
pub(crate) fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<CapturedEvent>) {
    install();
    let sink: Sink = Arc::default();
    // Save/restore any outer sink so a nested capture on one thread still works.
    let prev = SINK.with(|s| s.borrow_mut().replace(sink.clone()));
    let out = f();
    SINK.with(|s| *s.borrow_mut() = prev);
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
