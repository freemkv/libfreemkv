//! Test-only capture of `tracing` events, so the crate's logging contract
//! ("Account / Log / Classify") is ENFORCED rather than merely commented.
//!
//! Uses a hand-rolled [`tracing::Subscriber`] rather than `tracing-subscriber`
//! to keep this crate's one dev-dependency; installed once globally (see
//! [`capture`]) and routes each event to the emitting thread's own sink so
//! `cargo test`'s parallel harness can't cross-contaminate captures.
//!
//! See docs/testlog.md for the full rationale.

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

// Run `f` with every `tracing` event it emits on this thread captured;
// returns `f`'s value alongside the events, in emission order. One global
// subscriber, not scoped `with_default` — see docs/testlog.md for why.
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

    // The capture must actually see events and field values, not silently
    // record nothing (see docs/testlog.md). Mutation: `enabled()` returning
    // false, or `event()` dropping fields, fails here.
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
