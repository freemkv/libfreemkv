use super::*;

// A release after the generation was read must end the stall even with a zero limit.
#[test]
fn stall_returns_true_when_released_after_gen_read() {
    let (t, h) = FakeTransport::new();
    let g = h.0.lock().released;
    h.release();
    assert!(t.stall(Duration::ZERO, g));
}
