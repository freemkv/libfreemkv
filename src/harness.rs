//! Seeded robustness harness for the untrusted-input parsers.
//!
//! Every parser reached from here takes bytes that came off a disc, and this
//! crate's primary boundary is that the disc is untrusted: a malformed, damaged
//! or hostile image must never crash the library. These tests assert exactly
//! that one property — **the parser returns `Ok` or `Err`, and never panics.**
//!
//! # Why this exists rather than `cargo-fuzz`
//!
//! `cargo-fuzz` needs a nightly toolchain (`-Zsanitizer` plus SanitizerCoverage
//! for libFuzzer's coverage feedback) and this project pins stable. So the
//! generator lives here instead. It gives up coverage-guided mutation — the real
//! loss — and keeps everything else: millions of cases, structure-aware input,
//! and a crash corpus. It also gains determinism, which a fuzzer does not have:
//! the same seed replays the same cases on any machine.
//!
//! # Why no `proptest` or `arbitrary`
//!
//! This crate has exactly one dev-dependency. That is a deliberate posture, and
//! a randomness crate is not worth ten transitive dependencies when the parsers
//! take plain `&[u8]` and a good enough generator is forty lines.
//!
//! # Budget
//!
//! `FREEMKV_HARNESS_CASES` sets cases per generator per target (default 256, low
//! enough that the per-commit gate stays under a second). The overnight run sets
//! it to millions. `FREEMKV_HARNESS_SEED` overrides the seed; the default is
//! fixed so a failure in CI reproduces locally verbatim.
//!
//! # On failure
//!
//! The panic message carries the seed, generator and case index. Re-run with
//! `FREEMKV_HARNESS_SEED=<seed>` to reproduce, then write the offending bytes
//! into `tests/corpus/` as a permanent regression fixture — discovery happens
//! here, defence happens there.

#![cfg(test)]

/// Marsaglia xorshift64. Not cryptographic and does not need to be: the job is
/// a reproducible spread of bytes, and a named algorithm beats an ad-hoc LCG
/// whose period nobody has checked.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // A zero seed is a fixed point of xorshift — it would emit zeros forever
        // and every generated case would be identical.
        Self(if seed == 0 {
            0x2545_F491_4F6C_DD1D
        } else {
            seed
        })
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn byte(&mut self) -> u8 {
        (self.next() >> 24) as u8
    }

    /// Uniform-ish in `0..n`. The modulo bias is irrelevant at these magnitudes.
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }

    fn fill(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.byte()).collect()
    }
}

/// Budget per generator per target.
fn cases() -> usize {
    std::env::var("FREEMKV_HARNESS_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}

fn seed() -> u64 {
    std::env::var("FREEMKV_HARNESS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5EED_1234_ABCD_0001)
}

/// Largest generated input. Big enough to carry a plausible header plus a body,
/// small enough that millions of cases stay quick.
const MAX_LEN: usize = 4096;

/// Drive `f` over three generators and report which case broke it.
///
/// A panic inside `f` fails the test on its own — nothing is caught here,
/// because catching would risk reporting a pass on an input that aborted. The
/// wrapper exists to make the failing case *identifiable*: the harness prints
/// the seed, generator and index before each call, so the last line before a
/// panic names the exact case to reproduce.
fn sweep<F: FnMut(&[u8])>(target: &str, magic: &[u8], f: F) {
    sweep_n(target, magic, cases(), f)
}

/// `sweep` with an explicit budget. The budget is a PARAMETER rather than read
/// from the environment inside the loop: the meta-tests below need a small,
/// fixed count, and `std::env::set_var` is unsound once the test harness runs
/// tests in parallel — two tests setting the same variable race, which is
/// exactly what happened on the first run of this file.
fn sweep_n<F: FnMut(&[u8])>(target: &str, magic: &[u8], n: usize, mut f: F) {
    let s = seed();

    // 1. Pure random bytes. Cheap, and almost always rejected at the magic
    //    number — it exercises the entry guards and little else. Kept because
    //    the entry guards are themselves worth exercising.
    let mut rng = Rng::new(s);
    for i in 0..n {
        let len = rng.below(MAX_LEN);
        let buf = rng.fill(len);
        run(target, "random", s, i, &buf, &mut f);
    }

    // 2. Valid magic, random body. THE generator that matters: pure random
    //    input dies at the magic check and never reaches the parser body, so
    //    without this the sweep only ever tests the first few lines.
    let mut rng = Rng::new(s ^ 0xA5A5_A5A5_A5A5_A5A5);
    for i in 0..n {
        let mut buf = magic.to_vec();
        let tail = rng.below(MAX_LEN.saturating_sub(magic.len()));
        buf.extend(rng.fill(tail));
        run(target, "magic+noise", s, i, &buf, &mut f);
    }

    // 3. Structured mutation of a plausible record: a valid magic, then mostly
    //    zeroes, with a handful of bytes corrupted and a truncation. Length and
    //    offset fields live in those early bytes, so this is what reaches the
    //    arithmetic — the offsets, counts and sizes a hostile image would lie
    //    about.
    let mut rng = Rng::new(s ^ 0x1234_5678_9ABC_DEF0);
    for i in 0..n {
        let mut buf = vec![0u8; 512];
        buf[..magic.len().min(512)].copy_from_slice(&magic[..magic.len().min(512)]);
        for _ in 0..rng.below(24) + 1 {
            let at = rng.below(buf.len());
            buf[at] = rng.byte();
        }
        buf.truncate(rng.below(buf.len()) + 1);
        run(target, "mutate", s, i, &buf, &mut f);
    }
}

fn run<F: FnMut(&[u8])>(target: &str, generator: &str, seed: u64, i: usize, buf: &[u8], f: &mut F) {
    // Printed, not asserted: `cargo test` swallows stdout for passing tests and
    // shows it for failing ones, so this line is invisible until it is the last
    // thing before a panic — at which point it is exactly what is needed.
    println!(
        "harness {target}/{generator} seed={seed:#x} case={i} len={} :: \
         FREEMKV_HARNESS_SEED={seed} to reproduce",
        buf.len()
    );
    f(buf);
}

#[test]
fn mpls_parse_never_panics() {
    sweep("mpls", b"MPLS", |b| {
        let _ = crate::mpls::parse(b);
    });
}

#[test]
fn clpi_parse_never_panics() {
    sweep("clpi", b"HDMV", |b| {
        let _ = crate::clpi::parse(b);
    });
}

#[test]
fn udf_name_parse_never_panics() {
    // No magic: the compression ID is the first byte and every value is legal
    // input to reject, so the "magic" is a byte the sweep will mutate anyway.
    sweep("udf_name", &[8], |b| {
        let _ = crate::udf::parse_udf_name(b);
    });
}

#[test]
fn class_reader_parse_never_panics() {
    // Java `.class` files are a documented format we READ (never execute) to
    // recover BD-J stream labels — a peer of mpls/clpi here. Every count,
    // length and constant-pool index in the file is attacker-controlled disc
    // metadata, so the parser must reject malformed input with `Err`, never
    // panic. Magic is the class-file magic `0xCAFEBABE`.
    sweep("class_reader", &[0xCA, 0xFE, 0xBA, 0xBE], |b| {
        if let Ok(class) = crate::labels::class_reader::ClassFile::parse(b) {
            // Exercise the accessors a label parser drives, so their indexing
            // is fuzzed too, not just the structural parse.
            let _ = class.this_class_name();
            let _ = class.super_class_name();
            for m in &class.methods {
                let _ = class.member_name(m);
                if let Some(code) = m.code(&class.constant_pool) {
                    for insn in code.instructions() {
                        let _ = insn.cp_index();
                        if let Some(idx) = insn.cp_index() {
                            let _ = class.constant_pool.get(idx);
                            let _ = class.constant_pool.member_ref(idx);
                            let _ = class.constant_pool.load_constant_display(idx);
                        }
                    }
                }
            }
        }
    });
}

#[test]
fn jar_class_walk_never_panics() {
    // The jar layer opens a `/BDMV/JAR/<x>.jar` (a zip) and walks every
    // `.class` inside it, handing each to the class reader. Feed the sweep's
    // bytes as a zip archive; when they parse as one, walk the classes exactly
    // as `deluxe`/`dbp` do. Neither the zip open nor the class walk may panic.
    // Magic is the zip local-file-header signature `PK\x03\x04`.
    sweep("jar", &[0x50, 0x4B, 0x03, 0x04], |b| {
        if let Ok(mut archive) = zip::ZipArchive::new(std::io::Cursor::new(b.to_vec())) {
            crate::labels::jar::for_each_class(&mut archive, |_name, _class| {});
        }
    });
}

#[test]
fn ps_demuxer_feed_never_panics() {
    // Stateful, unlike the others: the demuxer carries a buffer across feeds, so
    // each case is fed to a FRESH demuxer and then a shared one. The shared pass
    // is what exercises cross-feed state — a start code split over a boundary,
    // a held PES completed by later bytes, the carry-over cap.
    let mut shared = crate::mux::ps::PsDemuxer::new();
    sweep("ps_demux", &[0x00, 0x00, 0x01, 0xBA], |b| {
        let mut fresh = crate::mux::ps::PsDemuxer::new();
        let _ = fresh.feed(b);
        let _ = shared.feed(b);
    });
}

#[test]
fn mkv_lacing_split_never_panics() {
    // All four lacing modes, including the reserved bit pattern. A degenerate
    // fixed lace was a real defect found by audit round 5.
    sweep("mkv_lacing", &[0x00], |b| {
        for lacing in 0u8..=3 {
            let _ = crate::mux::mkvstream::split_lacing(lacing, b);
        }
    });
}

/// The generators must actually differ, or the sweep is one generator run three
/// times and the coverage claim is false.
#[test]
fn the_three_generators_produce_different_inputs() {
    let mut seen: Vec<Vec<u8>> = Vec::new();
    sweep_n("probe", b"MPLS", 1, |b| seen.push(b.to_vec()));
    assert_eq!(seen.len(), 3, "one case per generator");
    assert_ne!(seen[0], seen[1], "random and magic+noise must differ");
    assert_ne!(seen[1], seen[2], "magic+noise and mutate must differ");
    assert!(
        seen[1].starts_with(b"MPLS"),
        "the magic+noise generator must actually carry the magic, or it never \
         reaches the parser body"
    );
}

/// The same seed must replay the same bytes, or a reported failure cannot be
/// reproduced and the harness is worthless as a regression tool.
#[test]
fn a_seed_replays_identically() {
    let mut a = Vec::new();
    let mut b = Vec::new();
    sweep_n("probe", b"MPLS", 4, |x| a.push(x.to_vec()));
    sweep_n("probe", b"MPLS", 4, |x| b.push(x.to_vec()));
    assert_eq!(a, b, "the same seed must produce the same cases");
}

/// The harness is worthless if its cases die at the entry guards, so this
/// MEASURES how deep they actually reach instead of assuming. A generator that
/// never gets past a length or magic check exercises the first ten lines and
/// nothing else — the fuzzing equivalent of a test that cannot fail.
#[test]
fn the_generators_actually_reach_the_parser_bodies() {
    // mpls::parse rejects at: len < 40, bad magic, then playlist_start + 10 >
    // len. Anything that returns Ok got all the way through the play-item loop.
    let mut ok = 0usize;
    let mut total = 0usize;
    sweep_n("reach", b"MPLS", 20000, |b| {
        total += 1;
        if crate::mpls::parse(b).is_ok() {
            ok += 1;
        }
    });
    assert!(
        ok > 0,
        "not one of {total} generated cases parsed successfully — the generators \
         are all being rejected at the entry guards, so this harness is testing \
         the guards and nothing behind them"
    );
    println!("mpls reach: {ok}/{total} cases parsed to completion");
}
