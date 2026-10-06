//! DVD bitmap subtitle (VobSub) parser.
//!
//! DVD subtitles are carried in PS private stream 1 with sub-stream IDs 0x20-0x3F.
//! A single subpicture unit (SPU — one displayed bitmap) may span multiple PES
//! packets: only the first PES carries a PTS; continuation packets have none.
//! The SPU begins with a 2-byte big-endian `SPU_size` giving the unit's total
//! byte length. We reassemble across PES boundaries into one Frame so large
//! subtitles aren't split/garbled, inheriting the head PES's PTS.
//!
//! For MKV: codec ID "S_VOBSUB". All frames are keyframes.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Upper bound on a single reassembled SPU. The SPU_size field is 16 bits, so a
/// well-formed unit is at most 0xFFFF bytes; cap accumulation here to bound
/// memory if the field is corrupt or the stream never completes a unit.
const MAX_SPU_BYTES: usize = 0xFFFF;

pub struct DvdSubParser {
    /// Pre-formatted VobSub .idx palette header for codec_private.
    codec_data: Option<Vec<u8>>,
    /// In-progress SPU reassembly: (facts of the PES that STARTED it, declared SPU_size, bytes).
    /// An SPU spans PES packets, so its timestamp and source offset are the opening packet's.
    pending: Option<(super::pesbuf::PesFacts, usize, Vec<u8>)>,
}

impl DvdSubParser {
    pub fn new(codec_data: Option<Vec<u8>>) -> Self {
        Self {
            codec_data,
            pending: None,
        }
    }

    /// Emit `pending` as a Frame if it is complete (or `force` at EOF),
    /// returning it and clearing the buffer. Returns None if nothing to emit.
    fn take_if_complete(&mut self, force: bool) -> Option<Frame> {
        let complete = match self.pending.as_ref() {
            Some((_, size, buf)) => force || buf.len() >= *size,
            None => false,
        };
        if !complete {
            return None;
        }
        // `complete` is only true when `self.pending` was `Some` above, but
        // borrow through `?` rather than a prod `unwrap()` on a fact the
        // compiler can't see across the two lines.
        let (facts, size, data) = self.pending.take()?;
        if data.len() < size {
            tracing::warn!(
                target: "mux",
                have = data.len(),
                declared = size,
                "dvdsub: emitting a truncated subpicture unit"
            );
        }
        // Guaranteed Some: `pending` is only ever created from a PES with
        // `pts.is_some()` (L054 dropped the no-PTS orphan path), so this
        // never takes the fallback; kept as unwrap_or, never a prod unwrap.
        let pts_ns = facts.presentation_ns().unwrap_or(0);
        Some(Frame {
            discontinuity: false,
            coding: None,
            source: facts.source,
            pts_ns,
            keyframe: true,
            data,
            duration_ns: None,
        })
    }
}

impl CodecParser for DvdSubParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::new();

        // PTS present == start of a new SPU (continuations carry none); a PTS
        // while `pending` is still open (lost continuation/corrupt SPU_size)
        // force-emits it truncated rather than swallowing all later SPUs.
        let pts = match pes.pts {
            None => {
                // A gap truncated the open SPU; its continuation cannot be spliced.
                if pes.discontinuity {
                    self.pending = None;
                }
                if self.pending.is_some() {
                    // Continuation: append, bounded by MAX_SPU_BYTES.
                    if let Some((_, _, buf)) = self.pending.as_mut() {
                        let room = MAX_SPU_BYTES.saturating_sub(buf.len());
                        let take = room.min(pes.data.len());
                        buf.extend_from_slice(&pes.data[..take]);
                    }
                    if let Some(frame) = self.take_if_complete(false) {
                        out.push(frame);
                    }
                    return out;
                }
                // Orphan continuation (no pending, no PTS): a fresh SPU at
                // pts 0 would put a garbage bitmap at 00:00:00. PGS drops
                // the same case, so drop here too (L054).
                return out;
            }
            Some(pts) => {
                if let Some(frame) = self.take_if_complete(true) {
                    // New SPU starting while a previous one is still open →
                    // flush stale.
                    out.push(frame);
                }
                pts
            }
        };

        // Start of a new SPU. The first 2 bytes are the big-endian total size.
        // `pts` is always real here — the no-PTS arm above always returns.
        let pts_ns = pts_to_ns(pts);
        let declared = if pes.data.len() >= 2 {
            // SPU_size includes the 2-byte header, so a declared size < 2 is
            // always malformed; treat it like the too-short path (lone frame)
            // rather than emit an immediate oversized unit.
            let d = ((pes.data[0] as usize) << 8) | pes.data[1] as usize;
            if d < 2 {
                out.push(Frame {
                    discontinuity: false,
                    coding: None,
                    source: super::pesbuf::PesFacts::of(pes).source,
                    pts_ns,
                    keyframe: true,
                    data: pes.data.clone(),
                    duration_ns: None,
                });
                return out;
            }
            d
        } else {
            // Too short to carry SPU_size — pass through as a lone frame.
            out.push(Frame {
                discontinuity: false,
                coding: None,
                source: super::pesbuf::PesFacts::of(pes).source,
                pts_ns,
                keyframe: true,
                data: pes.data.clone(),
                duration_ns: None,
            });
            return out;
        };

        let mut buf = pes.data.clone();
        if buf.len() > MAX_SPU_BYTES {
            buf.truncate(MAX_SPU_BYTES);
        }
        self.pending = Some((super::pesbuf::PesFacts::of(pes), declared, buf));
        if let Some(frame) = self.take_if_complete(false) {
            out.push(frame);
        }
        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        // At EOF, emit whatever SPU bytes remain even if the declared size was
        // never reached (truncated final subtitle is better than dropping it).
        self.take_if_complete(true).into_iter().collect()
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        self.codec_data.clone()
    }
}

// ── YCbCr → RGB conversion and palette formatting ─────────────────────────

/// Convert a single YCbCr color to RGB, clamping to [0, 255].
///
/// Input: `[padding, Y, Cr, Cb]` (as stored in DVD IFO PGC data). Note the
/// chroma order: the on-disc PGC CLUT is **Cr before Cb** — byte 2 is Cr and
/// byte 3 is Cb. The order is fixed by the DVD-Video PGC format, not by us.
///
/// Returns `[R, G, B]`.
///
/// The DVD CLUT is studio-range (Y 16..=235, chroma 16..=240), so this applies
/// the limited-range BT.601 expansion: Y=16 maps to black, Y=235 to white. The
/// result is the full-range RGB a VobSub `.idx` palette carries.
pub fn ycbcr_to_rgb(color: &[u8; 4]) -> [u8; 3] {
    let y = 1.164 * (color[1] as f64 - 16.0);
    let cr = color[2] as f64 - 128.0;
    let cb = color[3] as f64 - 128.0;

    let r = y + 1.596 * cr;
    let g = y - 0.392 * cb - 0.813 * cr;
    let b = y + 2.017 * cb;

    [clamp_u8(r), clamp_u8(g), clamp_u8(b)]
}

fn clamp_u8(v: f64) -> u8 {
    if v < 0.0 {
        0
    } else if v > 255.0 {
        255
    } else {
        v.round() as u8
    }
}

// The `size:` line is the frame the subpicture coords were authored against;
// without it some renderers assume a default frame and mis-place subtitles.
/// Format a 16-color YCbCr palette as a VobSub `.idx` header for S_VOBSUB
/// CodecPrivate.
///
/// Each entry is `[padding, Y, Cr, Cb]`. Output is a UTF-8 text block carrying
/// the two `.idx` header lines mkvmerge / libvobsub expect:
///
/// ```text
/// size: <width>x<height>
/// palette: rrggbb, rrggbb, ...
/// ```
///
/// `width`/`height` are the title's coded video dimensions; when either is 0
/// (unknown) the `size:` line is omitted rather than emitting a `0x0` frame.
///
/// Returns the formatted bytes suitable for MKV codec_private.
pub fn format_palette(palette: &[[u8; 4]], width: u32, height: u32) -> Vec<u8> {
    let mut parts: Vec<String> = Vec::with_capacity(palette.len());
    for color in palette {
        let [r, g, b] = ycbcr_to_rgb(color);
        parts.push(format!("{r:02x}{g:02x}{b:02x}"));
    }
    let mut out = String::new();
    if width > 0 && height > 0 {
        out.push_str(&format!("size: {width}x{height}\n"));
    }
    out.push_str(&format!("palette: {}\n", parts.join(", ")));
    out.into_bytes()
}

#[cfg(test)]
#[path = "dvdsub_tests.rs"]
mod tests;
