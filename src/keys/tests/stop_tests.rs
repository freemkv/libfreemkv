//! Stop design (v5.6) ST-L3 over KU's `resolve`: the one ctx struct hands every source
//! the op's token and progress (§2.12, :575), and each source call is busy (§2.1 item 2).

use super::*;
use crate::halt::Liveness;

// What one spy call saw: `ctx.halt()` is the op token, it was cancelled, and
// `ctx.progress()` was present and busy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    halt_is_op: Option<bool>,
    cancelled: bool,
    busy: Option<bool>,
}

struct Spy {
    who: &'static str,
    op: Halt,
    cancel: bool,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl KeySource for Spy {
    fn get_unit_keys(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>> {
        if self.cancel {
            self.op.cancel();
        }
        self.seen.lock().unwrap().push(Seen {
            halt_is_op: ctx
                .halt()
                .map(|h| Arc::ptr_eq(h.as_arc(), self.op.as_arc())),
            cancelled: ctx.halt().is_some_and(Halt::is_cancelled),
            busy: ctx.progress().map(Liveness::is_busy),
        });
        Ok(Vec::new())
    }
    fn label(&self) -> &'static str {
        self.who
    }
}

// Two spies; the second cancels the op token inside its call when `cancel_last`.
fn spies(op: &Halt, cancel_last: bool) -> (KeySourceFactory, Arc<Mutex<Vec<Seen>>>) {
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let (op, log) = (op.clone(), seen.clone());
    let f: KeySourceFactory = Arc::new(move || {
        [("spy-a", false), ("spy-b", cancel_last)]
            .into_iter()
            .map(|(who, cancel)| {
                Box::new(Spy {
                    who,
                    op: op.clone(),
                    cancel,
                    seen: log.clone(),
                }) as Box<dyn KeySource>
            })
            .collect()
    });
    (f, seen)
}

/// LSe4 (stop design §5.1, retargeted by §2.12 onto `KeyRing::acquire`): "a spy
/// sees `ctx.halt()` cancelled". Every source's ctx carries the run context's token
/// itself, so a Stop reaches a source mid-call; `acquire` then ends `Halted`, no ring.
#[test]
fn resolve_passes_halt_to_every_source_ctx() {
    let fx = two_units();
    let op = Halt::new();
    let (f, seen) = spies(&op, true);
    let r = KeyRing::acquire_for_disc(
        &fx.disc,
        &mut fx.source(),
        KeyScope::Titles(vec![0]),
        &f,
        AcquireOptions::default(),
        &crate::ctx::Ctx::new(op.clone()),
    );
    assert!(matches!(r, Err(Error::Halted)), "{:?}", r.err());
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "both sources were asked: {seen:?}");
    assert!(
        seen.iter().all(|s| s.halt_is_op == Some(true)),
        "every ctx.halt() is the op token: {seen:?}"
    );
    assert!(
        seen[1].cancelled,
        "the spy sees ctx.halt() cancelled: {seen:?}"
    );
}

/// ST4-2 / §2.1 item 2: "Each source call and each CSS crack in KU's `resolve` holds
/// `busy()` on `ResolveCtx::progress()`" — so the idle-only T29 probe never counts a
/// slow keydb parse as idle. The op's `Liveness` is the ctx's, busy inside each call only.
#[test]
fn resolve_holds_busy_around_every_source_call() {
    let fx = two_units();
    let (op, p) = (Halt::new(), Liveness::new());
    let (f, seen) = spies(&op, false);
    let opts = AcquireOptions {
        liveness: Some(&p),
        ..Default::default()
    };
    let r = KeyRing::acquire_for_disc(
        &fx.disc,
        &mut fx.source(),
        KeyScope::Titles(vec![0]),
        &f,
        opts,
        &crate::ctx::Ctx::new(op.clone()),
    );
    assert!(r.is_err(), "no source holds a key: a refusal");
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(
        seen.iter().all(|s| s.busy == Some(true)),
        "busy() is held on ctx.progress() across every source call: {seen:?}"
    );
    assert!(!p.is_busy(), "no busy span outlives resolve");
}

/// Guard: with no liveness in the options, a source's ctx hands out no progress.
#[test]
fn resolve_without_liveness_hands_out_no_progress() {
    let fx = two_units();
    let (f, seen) = spies(&Halt::new(), false);
    let r = KeyRing::acquire_for_disc(
        &fx.disc,
        &mut fx.source(),
        KeyScope::Titles(vec![0]),
        &f,
        AcquireOptions::default(),
        &crate::ctx::Ctx::default(),
    );
    assert!(r.is_err());
    let seen = seen.lock().unwrap().clone();
    assert!(seen.iter().all(|s| s.busy.is_none()), "{seen:?}");
}

/// Review minor 1 (§2.2 alias rule, as `Drive::alias`): under `open_with` the session's
/// op token wins over a different caller token, so a Stop on the session token reaches
/// a key call in flight and the resolve ends `Halted`.
#[test]
fn session_token_wins_over_the_callers_in_resolve() {
    let fx = two_units();
    let reader = fx.source();
    let (session_tok, caller_tok) = (Halt::new(), Halt::new());
    let (f, seen) = spies(&session_tok, true);
    let mut s =
        crate::session::DiscSession::from_parts_for_test(Some(fx.disc), Some(Box::new(reader)));
    s.set_halt_for_test(&session_tok);
    let ctx = crate::ctx::Ctx::new(caller_tok.clone());
    let r = s.acquire_keys(
        KeyScope::Titles(vec![0]),
        &f,
        AcquireOptions::default(),
        &ctx,
    );
    assert!(matches!(r, Err(Error::Halted)), "{:?}", r.err());
    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.iter().all(|s| s.halt_is_op == Some(true)),
        "every source sees the session token: {seen:?}"
    );
    assert!(
        seen[1].cancelled,
        "the call in flight sees the Stop: {seen:?}"
    );
    assert!(!caller_tok.is_cancelled());
}
