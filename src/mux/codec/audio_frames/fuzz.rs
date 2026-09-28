//! Seeded property fuzz of resync framing for ADTS and MPEG audio (no dependency beyond a
//! xorshift). Each case generates a truth stream, feeds it through the parser, and checks the
//! output against that truth only, never against the parser's own state:
//! - I3: per stream key, emitted PTS strictly increase; no junk AU, none repeated.
//! - every good frame with delivered good neighbours is emitted.
//! - I1: `certain <= verified <= pieces + gaps` (one false sync per gap may open a run in the
//!   fragment a PES carries after a discontinuity: the documented allowance).
//! - I2: dropped within the byte cap, and at most the clock skip plus verified faults.
//! - dropped at least: a corrupt piece's full length where a later PTS places its lock with an
//!   unbroken walk, else one (the documented under-count without a later PTS).
//! - PTS error at most the AUs lost between the frame's bracketing timestamps.
//!
//! AUDIO_FUZZ_CASES sets the case count (CI default below); run 20k+ locally.

use super::super::adts::{AdtsParser, adts_frame_len};
use super::super::mpegaudio::{MpegAudioParser, mpa_frame_len};
use super::super::{CodecParser, Frame, PesPacket, pts_to_ns};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

// splitmix64: distinct, well-mixed streams for consecutive seeds.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) | 1
}

#[derive(Clone, Copy, PartialEq)]
enum Codec {
    Adts,
    Mpeg,
}
impl Codec {
    // The header size rule (pinned by the spec_* tests), to spot ambiguous candidate locks.
    fn frame_len(self, d: &[u8]) -> Option<usize> {
        match self {
            Codec::Adts => adts_frame_len(d),
            Codec::Mpeg => mpa_frame_len(d),
        }
    }
}

struct Truth {
    bytes: Vec<u8>,
    // What the parser should emit for this frame if it is kept.
    data: Vec<u8>,
    good: bool,
    keeps_sync: bool,
    ticks: i64,
    key: u8,
    // Bytes of it that reached the parser (a gap may cut it).
    delivered: usize,
}

// One frame: `id` makes kept payloads unique; payload bytes are uniform (0xFF at 1/256).
fn frame(c: Codec, rng: &mut Rng, id: usize, key: u8, good: bool) -> Truth {
    let (mut bytes, skip, ticks) = match c {
        Codec::Adts => {
            let payload = [rng.below(12), rng.below(400), rng.below(2041)][rng.below(3) as usize];
            let len = 7 + payload.max(1) as usize;
            let mut f = vec![0xFF, 0xF1, 0x40 | 3 << 2, 0, 0, 0, 0];
            let ch = if key == 0 { 2u8 } else { 1 };
            f[2] |= ch >> 2;
            f[3] = (ch & 3) << 6 | (len >> 11) as u8;
            f[4] = (len >> 3) as u8;
            f[5] = ((len & 7) as u8) << 5 | 0x1F;
            f[6] = 0xFC;
            f.extend(rng.bytes(len - 7));
            (f, 7, 1920)
        }
        Codec::Mpeg => {
            let (sf, ticks) = if key == 0 { (1u8, 2160) } else { (2, 3240) };
            let rate = if key == 0 { 48_000 } else { 32_000 };
            let bi = 1 + rng.below(14) as usize;
            let kbps = [
                0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
            ][bi];
            let len = 144 * kbps * 1000 / rate;
            let mut f = vec![
                0xFF,
                0xFB,
                (bi as u8) << 4 | sf << 2,
                (rng.below(4) as u8) << 6,
            ];
            f.extend(rng.bytes(len - 4));
            (f, 0, ticks)
        }
    };
    bytes[skip.max(4)] = (id & 0x7F) as u8; // the first payload byte
    let data = bytes[skip..].to_vec();
    let keeps_sync = !good && rng.chance(500);
    if !good {
        if keeps_sync {
            match c {
                Codec::Adts => bytes[2] = (bytes[2] & 0xC3) | 13 << 2, // reserved rate index
                Codec::Mpeg => bytes[2] |= 3 << 2,                     // reserved sampling rate
            }
        } else {
            bytes[..2].copy_from_slice(&[0, 0]);
        }
    }
    Truth {
        bytes,
        data,
        good,
        keeps_sync,
        ticks,
        key,
        delivered: 0,
    }
}

enum Parser {
    Adts(AdtsParser),
    Mpeg(MpegAudioParser),
}
impl Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        match self {
            Parser::Adts(p) => p.parse(pes),
            Parser::Mpeg(p) => p.parse(pes),
        }
    }
    fn flush(&mut self) -> Vec<Frame> {
        match self {
            Parser::Adts(p) => p.flush(),
            Parser::Mpeg(p) => p.flush(),
        }
    }
    fn dropped(&self) -> u64 {
        match self {
            Parser::Adts(p) => p.dropped_frames(),
            Parser::Mpeg(p) => p.dropped_frames(),
        }
    }
    fn verified(&self) -> u64 {
        match self {
            Parser::Adts(p) => p.verified_dropped(),
            Parser::Mpeg(p) => p.verified_dropped(),
        }
    }
}

pub(super) struct Outcome {
    pub violations: Vec<String>,
}

pub(super) fn case(seed: u64) -> Outcome {
    let mut rng = Rng(mix(seed));
    let codec = if rng.chance(500) {
        Codec::Adts
    } else {
        Codec::Mpeg
    };
    let min_frame = if codec == Codec::Adts { 7 } else { 24 };
    let n = 20 + rng.below(60) as usize;
    // Good/corrupt pattern: three good frames first, runs of 1-4 separated by two or more
    // good frames; a run may reach EOS.
    let mut good = vec![true; n];
    let mut i = 3;
    while i < n {
        if rng.chance(150) {
            let len = (1 + rng.below(4) as usize).min(n - i);
            good[i..i + len].iter_mut().for_each(|g| *g = false);
            i += len + 2;
        } else {
            i += 1;
        }
    }
    let switch = if rng.chance(300) {
        4 + rng.below(n as u64 - 4) as usize
    } else {
        n
    };
    let mut frames: Vec<Truth> = (0..n)
        .map(|i| frame(codec, &mut rng, i, u8::from(i >= switch), good[i]))
        .collect();
    let starts: Vec<usize> = frames
        .iter()
        .scan(0, |o, f| {
            let s = *o;
            *o += f.bytes.len();
            Some(s)
        })
        .collect();
    let times: Vec<i64> = frames
        .iter()
        .scan(0, |t, f| {
            let s = *t;
            *t += f.ticks;
            Some(s)
        })
        .collect();
    // Gaps anywhere after the first frames: bytes dropped from inside frame j into frame j+1.
    let mut lost = vec![false; n];
    let mut cut = Vec::new();
    for _ in 0..rng.below(3) {
        let j = 3 + rng.below((n - 4) as u64) as usize;
        if !lost[j] && !lost[j + 1] && !lost[j - 1] {
            let a = starts[j] + 1 + rng.below(frames[j].bytes.len() as u64 - 1) as usize;
            let b = starts[j + 1] + 1 + rng.below(frames[j + 1].bytes.len() as u64 - 1) as usize;
            lost[j] = true;
            lost[j + 1] = true;
            cut.push((a, b));
        }
    }
    let gaps = cut.len();
    let (mut stream, mut frame_at, mut gap_at) = (Vec::new(), Vec::new(), Vec::new());
    let mut delivered = vec![0usize; n];
    for (i, f) in frames.iter().enumerate() {
        for (k, &b) in f.bytes.iter().enumerate() {
            let off = starts[i] + k;
            if cut.iter().any(|&(a, e)| off >= a && off < e) {
                continue;
            }
            if cut.iter().any(|&(_, e)| off == e) {
                gap_at.push(stream.len());
            }
            if k == 0 {
                frame_at.push((stream.len(), i));
            }
            delivered[i] += 1;
            stream.push(b);
        }
    }
    frames
        .iter_mut()
        .zip(&delivered)
        .for_each(|(f, &d)| f.delivered = d);
    let header = if codec == Codec::Adts { 7 } else { 4 };
    let pts_rate = [1000, 700, 300][rng.below(3) as usize];
    let mut cuts = gap_at.clone();
    let mut o = 0;
    loop {
        o += 1 + rng.below(3000) as usize;
        if o >= stream.len() {
            break;
        }
        cuts.push(o);
    }
    cuts.push(stream.len());
    cuts.sort();
    cuts.dedup();
    let mut p = match codec {
        Codec::Adts => Parser::Adts(AdtsParser::new()),
        Codec::Mpeg => Parser::Mpeg(MpegAudioParser::new()),
    };
    let (mut out, mut named, mut need_pts, mut from) = (Vec::new(), Vec::new(), false, 0);
    for &to in &cuts {
        let gap = gap_at.contains(&from);
        need_pts |= gap; // after a discontinuity the first AU to start carries a PTS
        let first = frame_at
            .iter()
            .find(|&&(at, _)| at >= from && at < to)
            .map(|&(_, i)| i);
        let pts = first.filter(|_| need_pts || from == 0 || rng.chance(pts_rate));
        need_pts &= first.is_none();
        if let Some(i) = pts {
            named.push((i, from));
        }
        let pes = PesPacket {
            source: None,
            pid: 0x1100,
            pts: pts.map(|i| times[i]),
            dts: None,
            data: stream[from..to].to_vec(),
            discontinuity: gap,
        };
        out.extend(p.parse(&pes));
        from = to;
    }
    out.extend(p.flush());
    check(
        &frames, &lost, &times, &named, &frame_at, gaps, min_frame, header, codec, &stream, &out,
        &p,
    )
}

#[allow(clippy::too_many_arguments)]
fn check(
    frames: &[Truth],
    lost: &[bool],
    times: &[i64],
    named: &[(usize, usize)],
    frame_at: &[(usize, usize)],
    gaps: usize,
    min_frame: usize,
    header: usize,
    codec: Codec,
    stream: &[u8],
    out: &[Frame],
    p: &Parser,
) -> Outcome {
    let n = frames.len();
    let mut v = Vec::new();
    let ns = |i: usize| pts_to_ns(times[i]);
    let dur = |i: usize| pts_to_ns(frames[i].ticks);
    // Match output to truth in order: every emitted frame is a distinct good frame.
    let mut matched = Vec::new();
    let mut next = 0;
    for fr in out {
        match (next..n).find(|&i| frames[i].good && frames[i].data == fr.data) {
            Some(i) => {
                matched.push((i, fr.pts_ns));
                next = i + 1;
            }
            None => v.push(format!("junk or repeated AU at {}", fr.pts_ns)),
        }
    }
    for key in 0..2u8 {
        let t: Vec<i64> = matched
            .iter()
            .filter(|&&(i, _)| frames[i].key == key)
            .map(|&(_, t)| t)
            .collect();
        if !t.windows(2).all(|w| w[0] < w[1]) {
            v.push(format!("I3: key {key} PTS not strictly increasing: {t:?}"));
        }
    }
    // A good frame is kept if delivered whole and, when it must chain (first after a gap or a
    // corrupt frame), its successor confirms it: delivered with a sync-shaped header in the
    // stream's key, or, in a new key (a false header's likely look), two whole good frames of it.
    let whole = |i: usize| frames[i].good && !lost[i];
    let mut kept_v = vec![false; n];
    let mut last_key = None;
    for i in 0..n {
        let same = |k: usize| k < n && whole(k) && frames[k].key == frames[i].key;
        let confirms = if last_key.is_some_and(|k| k != frames[i].key) {
            same(i + 1) && same(i + 2)
        } else {
            i + 1 == n || (!lost[i + 1] && (frames[i + 1].good || frames[i + 1].keeps_sync))
        };
        // Framing is locked only after a kept frame; otherwise this frame must chain.
        // Two candidates ending at one byte are both refused: a header in the bytes skipped
        // before this frame (or in it) whose frame would end where this one ends.
        let at = |k: usize| frame_at.iter().find(|&&(_, f)| f == k).map(|&(a, _)| a);
        let ambiguous = || {
            let (Some(g), true) = (at(i), i > 0) else {
                return false;
            };
            let end = g + frames[i].bytes.len();
            let from = (0..i)
                .rev()
                .find(|&k| kept_v[k])
                .and_then(|k| at(k).map(|a| a + frames[k].bytes.len()));
            (from.unwrap_or(0)..end.min(stream.len())).any(|q| {
                q != g && stream[q] == 0xFF && codec.frame_len(&stream[q..]) == Some(end - q)
            })
        };
        let locked = i == 0 || kept_v[i - 1];
        kept_v[i] = whole(i) && (locked || (confirms && !ambiguous()));
        if kept_v[i] {
            last_key = Some(frames[i].key);
        }
    }
    let kept = |i: usize| kept_v[i];
    for i in 0..n {
        let safe = kept(i) && (i == 0 || kept(i - 1)) && (i + 1 == n || kept(i + 1));
        if safe && !matched.iter().any(|&(m, _)| m == i) {
            v.push(format!("good frame {i} (good neighbours) not emitted"));
        }
    }
    // Corrupt pieces: corrupt frames whose start (header) was delivered, consecutive within one
    // gap-free segment. A gap cuts from inside frame j into j+1: j's start is delivered.
    let started = |i: usize| frame_at.iter().any(|&(_, k)| k == i);
    let mut pieces: Vec<(usize, usize)> = Vec::new();
    for i in 0..n {
        if frames[i].good || !started(i) {
            continue;
        }
        match pieces.last_mut() {
            Some((_, e)) if *e == i && !lost[i - 1] => *e = i + 1,
            _ => pieces.push((i, i + 1)),
        }
    }
    let after_gap = |a: usize| (0..a).rev().take_while(|&k| !lost[k]).count();
    // Runs as defined: one opens at a corrupt frame whose header reached the parser and closes
    // at a kept frame or a gap (good frames that cannot be kept do not close it). It is certain
    // to be counted if it opened where a header was due or holds a delivered sync-intact header.
    let seen = |k: usize| frames[k].delivered >= header;
    let certain = |&(a, b): &(usize, usize)| {
        let settled = after_gap(a) >= 2 && kept(a - 1) && kept(a - 2) && seen(a);
        settled || (a..b).any(|k| frames[k].keeps_sync && seen(k))
    };
    let (mut runs, mut certain_n, mut open) = (0u64, 0u64, None::<bool>);
    for i in 0..n {
        let closes = kept(i) || (lost[i] && !started(i));
        if closes && let Some(c) = open.take() {
            runs += 1;
            certain_n += u64::from(c);
        }
        if frames[i].good || !started(i) {
            continue;
        }
        let sure = frames[i].keeps_sync && seen(i);
        match &mut open {
            Some(c) => *c |= sure,
            None => open = Some(sure || certain(&(i, i + 1))),
        }
    }
    if let Some(c) = open {
        runs += 1;
        certain_n += u64::from(c);
    }
    let verified = p.verified();
    if verified < certain_n || verified > runs + gaps as u64 {
        v.push(format!(
            "I1: verified {verified} outside [{certain_n}, {runs} + {gaps}]"
        ));
    }
    let dropped = p.dropped();
    let bad_bytes: usize = pieces
        .iter()
        .flat_map(|&(a, b)| a..b)
        .map(|i| frames[i].bytes.len())
        .sum();
    let frag_bytes: usize = (0..n)
        .filter(|&i| lost[i])
        .map(|i| frames[i].bytes.len())
        .sum();
    let cap = ((bad_bytes + frag_bytes) / min_frame + pieces.len() + gaps) as u64;
    if dropped > cap {
        v.push(format!("I2: dropped {dropped} > byte cap {cap}"));
    }
    let skip: i64 = matched
        .windows(2)
        .map(|w| {
            let d = dur(w[0].0);
            ((w[1].1 - w[0].1 + d / 2) / d - 1).max(0)
        })
        .sum();
    // Runs no frame ends (cut by a gap or reaching EOS) are counted by bytes, within I2.
    let unlocked: usize = pieces
        .iter()
        .filter(|&&(_, b)| !(b + 1 < n && kept(b) && kept(b + 1)))
        .map(|&(a, b)| (a..b).map(|i| frames[i].bytes.len()).sum::<usize>() / min_frame + 1)
        .sum::<usize>()
        + gaps * (frag_bytes / min_frame + 1);
    if dropped as i64 > skip + verified as i64 + unlocked as i64 {
        v.push(format!(
            "I2/I4: dropped {dropped} > skip {skip} + faults {verified} + {unlocked}"
        ));
    }
    // Lower bound: exact where a later PTS names a frame reached from the piece through
    // delivered good frames of one key, with a clock before it; else one per certain piece.
    let exact = |&(a, b): &(usize, usize)| {
        let before = after_gap(a) >= 2 && kept(a - 1) && kept(a - 2);
        let key = frames[a - 1].key;
        let reach = named
            .iter()
            .map(|&(i, _)| i)
            .find(|&t| t >= b)
            .filter(|&t| {
                (b..=t).all(|k| kept(k) && frames[k].key == key)
                    && (a - 2..b).all(|k| frames[k].key == key)
                    && frame_at.iter().find(|&&(_, i)| i == t).map(|&(at, _)| at)
                        < frame_at
                            .iter()
                            .find(|&&(_, i)| i == b)
                            .map(|&(at, _)| at + 60 * 1024)
            });
        before && reach.is_some()
    };
    // One per certain run (pieces merge into runs); an exact piece is a whole run, all counted.
    let lower: u64 = certain_n
        + pieces
            .iter()
            .filter(|p| certain(p) && exact(p))
            .map(|p| (p.1 - p.0) as u64 - 1)
            .sum::<u64>();
    if dropped < lower {
        v.push(format!("dropped {dropped} < lower bound {lower}"));
    }
    // PTS error bound: at most the AUs lost between the frame's bracketing timestamps.
    let max_d = (0..n).map(dur).max().unwrap_or(1);
    for &(i, t) in &matched {
        let before = named
            .iter()
            .map(|&(k, _)| k)
            .filter(|&k| k <= i)
            .max()
            .unwrap_or(0);
        let after = named
            .iter()
            .map(|&(k, _)| k)
            .filter(|&k| k > i)
            .min()
            .unwrap_or(n);
        let lost_between = (before..after).filter(|&k| !kept(k)).count() as i64;
        if (t - ns(i)).abs() > lost_between * max_d + 16 {
            v.push(format!(
                "PTS error {} ns at frame {i} > {lost_between} lost AUs",
                t - ns(i)
            ));
        }
    }
    Outcome { violations: v }
}

fn cases() -> u64 {
    std::env::var("AUDIO_FUZZ_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400)
}

#[test]
fn resync_properties_hold_on_generated_adts_and_mpeg_streams() {
    let mut failing = Vec::new();
    for c in 0..cases() {
        let o = case(0x5EED_0000 + c);
        if !o.violations.is_empty() {
            failing.push((c, o.violations));
        }
    }
    let shown: Vec<_> = failing.iter().take(3).collect();
    assert!(
        failing.is_empty(),
        "{} of {} cases fail; first: {shown:?}",
        failing.len(),
        cases()
    );
}
