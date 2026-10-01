//! Synthetic, muxable BD media for the parity goldens: [`synthetic_bd_clip`] is a clear
//! BD-TS clip (PAT, PMT, one MPEG-2 video and two AC-3 audio tracks) that the demuxer, every
//! sink and the AACS fixtures ([`BdFile::with_clip`](super::BdFile::with_clip)) can carry.
//! Fully deterministic: no clock, no RNG.

use crate::aacs::content::ALIGNED_UNIT_LEN;
use crate::consts::BD_SOURCE_PACKET_BYTES as PKT;

/// PID of the clip's MPEG-2 video.
pub const CLIP_VIDEO_PID: u16 = 0x1011;
/// PIDs of the clip's two AC-3 audio tracks.
pub const CLIP_AUDIO_PIDS: [u16; 2] = [0x1100, 0x1101];
const PMT_PID: u16 = 0x0100;
/// Video frame period (23.976 fps) in nanoseconds.
const FRAME_NS: i64 = 41_708_333;
/// AC-3 frame period (1536 samples at 48 kHz) in nanoseconds.
const AC3_NS: i64 = 32_000_000;

// One source packet: a zero TP_extra_header (CPI 00) and a TS packet with `body` after
// the 4-byte header, padded to 188 with an adaptation-field stuffing run when short.
fn ts_packet(pid: u16, cc: u8, body: &[u8]) -> Vec<u8> {
    let mut p = vec![0u8; PKT];
    p[4] = 0x47;
    p[5] = (pid >> 8) as u8 & 0x1F;
    p[6] = pid as u8;
    let room = 184;
    if body.len() >= room {
        p[7] = 0x10 | (cc & 0x0F);
        p[8..8 + room].copy_from_slice(&body[..room]);
    } else {
        p[7] = 0x30 | (cc & 0x0F);
        let af = room - body.len();
        p[8] = (af - 1) as u8;
        if af > 1 {
            p[9] = 0;
            p[10..8 + af].fill(0xFF);
        }
        p[8 + af..8 + room].copy_from_slice(body);
    }
    p
}

// A one-section PSI packet (pointer field 0), the section's CRC left zero (the demuxer
// does not check it).
fn psi(pid: u16, mut section: Vec<u8>) -> Vec<u8> {
    section.extend_from_slice(&[0, 0, 0, 0]);
    let mut body = vec![0u8];
    body.extend(section);
    let mut p = ts_packet(pid, 0, &body);
    p[5] |= 0x40;
    p
}

fn pat() -> Vec<u8> {
    psi(
        0,
        vec![
            0x00,
            0xB0,
            0x0D,
            0x00,
            0x01,
            0xC1,
            0x00,
            0x00,
            0x00,
            0x01,
            0xE0 | (PMT_PID >> 8) as u8,
            PMT_PID as u8,
        ],
    )
}

fn pmt() -> Vec<u8> {
    let es = [
        (0x02u8, CLIP_VIDEO_PID),
        (0x81, CLIP_AUDIO_PIDS[0]),
        (0x81, CLIP_AUDIO_PIDS[1]),
    ];
    let len = 9 + 5 * es.len() + 4;
    let mut s = vec![
        0x02,
        0xB0,
        len as u8,
        0x00,
        0x01,
        0xC1,
        0x00,
        0x00,
        0xE0 | (CLIP_VIDEO_PID >> 8) as u8,
        CLIP_VIDEO_PID as u8,
        0xF0,
        0x00,
    ];
    for (ty, pid) in es {
        s.extend_from_slice(&[ty, 0xE0 | (pid >> 8) as u8, pid as u8, 0xF0, 0x00]);
    }
    psi(PMT_PID, s)
}

fn pes_header(stream_id: u8, pts_ns: i64, len: usize) -> Vec<u8> {
    let pts = ((pts_ns.max(0) as u128 * 90_000) / 1_000_000_000) as u64 + 90_000;
    let n = if stream_id == 0xE0 { 0 } else { len + 8 };
    let mut h = vec![0, 0, 1, stream_id, (n >> 8) as u8, n as u8, 0x80, 0x80, 5];
    h.push(0x21 | ((pts >> 29) as u8 & 0x0E));
    h.push((pts >> 22) as u8);
    h.push(0x01 | ((pts >> 14) as u8 & 0xFE));
    h.push((pts >> 7) as u8);
    h.push(0x01 | ((pts << 1) as u8 & 0xFE));
    h
}

// A PES (header + `es`) split into source packets on `pid`.
fn pes_packets(pid: u16, cc: &mut u8, stream_id: u8, pts_ns: i64, es: &[u8]) -> Vec<Vec<u8>> {
    let mut pes = pes_header(stream_id, pts_ns, es.len());
    pes.extend_from_slice(es);
    let mut out = Vec::new();
    for (i, chunk) in pes.chunks(184).enumerate() {
        let mut p = ts_packet(pid, *cc, chunk);
        *cc = (*cc + 1) & 0x0F;
        if i == 0 {
            p[5] |= 0x40;
        }
        out.push(p);
    }
    out
}

// MPEG-2 sequence header (1920x1080, 23.976) + extension, GOP header, then a coded frame.
fn video_frame(n: u32) -> Vec<u8> {
    let mut v = Vec::new();
    let coding = if n.is_multiple_of(12) { 1u8 } else { 2 };
    if coding == 1 {
        v.extend_from_slice(&[
            0, 0, 1, 0xB3, 0x78, 0x04, 0x38, 0x31, 0xFF, 0xFF, 0xE0, 0x18,
        ]);
        v.extend_from_slice(&[0, 0, 1, 0xB5, 0x14, 0x8A, 0x00, 0x01, 0x00, 0x00]);
        v.extend_from_slice(&[0, 0, 1, 0xB8, 0x00, 0x08, 0x00, 0x40]);
    }
    let tr = (n % 12) as u8;
    v.extend_from_slice(&[
        0,
        0,
        1,
        0x00,
        tr >> 2,
        ((tr & 3) << 6) | (coding << 3),
        0xFF,
        0xF8,
    ]);
    v.extend_from_slice(&[0, 0, 1, 0xB5, 0x8F, 0xFF, 0xF3, 0x80, 0x80]);
    v.extend_from_slice(&[0, 0, 1, 0x01, 0x12, 0x34, 0x56]);
    // An I frame is long and tail-padded with a constant run, as an encoder pads: a CSS
    // crack needs a known run in the scrambled sector.
    let size = if coding == 1 { 4000 } else { 300 };
    v.extend((0..size).map(|i| match (coding, i) {
        (1, 64..) => 0x55,
        _ => ((n.wrapping_mul(131) + i as u32) % 251) as u8 | 1,
    }));
    v
}

// A decodable AC-3 syncframe (48 kHz, frmsizecod 8 = 128 words, bsid 8, valid CRC) whose
// body is varied by `n` and `track`.
fn ac3_frame(track: u8, n: u32) -> Vec<u8> {
    let mut f = vec![0u8; 256];
    f[..2].copy_from_slice(&[0x0B, 0x77]);
    f[4] = 8;
    f[5] = 0x08 << 3;
    for (i, b) in f[6..254].iter_mut().enumerate() {
        *b = (n.wrapping_mul(17) as u8)
            .wrapping_add(i as u8)
            .wrapping_add(track * 61)
            | 1;
    }
    let c = crate::mux::codec::crc::crc16_ansi(&f[2..254]);
    f[254..].copy_from_slice(&c.to_be_bytes());
    f
}

/// A clear BD-TS clip of `units` whole aligned units (6144 bytes each, KS-1): PAT and PMT
/// first, then interleaved video (I frame every 12) and two AC-3 tracks, padded with null
/// packets to the unit boundary. Every packet has CPI `00₂`. Panics if `units` is too few
/// to hold the PSI.
pub fn synthetic_bd_clip(units: usize) -> Vec<u8> {
    let total = units * ALIGNED_UNIT_LEN / PKT;
    let mut pk = vec![pat(), pmt()];
    let (mut cv, mut ca) = (0u8, [0u8; 2]);
    let (mut vn, mut an) = (0u32, 0u32);
    // Emit by presentation time: one video frame, then the audio due before the next.
    while pk.len() + 12 < total {
        let t = i64::from(vn) * FRAME_NS;
        pk.extend(pes_packets(
            CLIP_VIDEO_PID,
            &mut cv,
            0xE0,
            t,
            &video_frame(vn),
        ));
        vn += 1;
        while i64::from(an) * AC3_NS < i64::from(vn) * FRAME_NS && pk.len() + 12 < total {
            for (k, pid) in CLIP_AUDIO_PIDS.iter().enumerate() {
                let t = i64::from(an) * AC3_NS;
                let f = ac3_frame(k as u8, an);
                pk.extend(pes_packets(*pid, &mut ca[k], 0xBD, t, &f));
            }
            an += 1;
        }
    }
    pk.truncate(total);
    while pk.len() < total {
        pk.push(ts_packet(0x1FFF, 0, &[0xFF; 184]));
    }
    pk.concat()
}
