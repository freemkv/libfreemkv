use super::*;

// Clones share the token, the events and the counters: a stage holding a clone
// observes the caller's Stop and reports into the caller's totals.
#[test]
fn a_clone_shares_halt_and_stats() {
    let ctx = Ctx::default();
    let stage = ctx.clone();
    ctx.halt.cancel();
    stage.stats.add_skip(2048);
    stage.stats.add_blanked(2);
    stage.stats.add_resync_dropped(3);
    assert!(stage.halt.is_cancelled());
    assert_eq!(
        ctx.stats.snapshot(),
        LossReport {
            read_skips: 1,
            bytes_lost: 2048,
            units_blanked: 2,
            resync_dropped: 3,
        }
    );
}

// Each switch follows its own variable; no other test reads either one.
#[test]
fn diag_from_env_reads_each_switch_from_its_own_variable() {
    let set = |k: &str, on: bool| unsafe {
        if on {
            std::env::set_var(k, "1")
        } else {
            std::env::remove_var(k)
        }
    };
    set("FREEMKV_SKIP_PARSE", true);
    set("FREEMKV_PROFILE", false);
    let d = Diag::from_env();
    assert!(d.skip_parse && !d.profile);
    set("FREEMKV_SKIP_PARSE", false);
    set("FREEMKV_PROFILE", true);
    let d = Diag::from_env();
    assert!(!d.skip_parse && d.profile);
    set("FREEMKV_PROFILE", false);
    assert_eq!(Diag::from_env(), Diag::default());
}
