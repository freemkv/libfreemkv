//! Bounded framing and timestamp accounting shared by ADTS and MPEG audio.

use super::dropgate::DropTally;
use super::pesbuf::{PesBuf, PesFacts};
use super::{Frame, PesPacket};

pub(super) struct Header {
    pub bytes: usize,
    pub skip: usize,
    pub samples: u32,
    pub rate: u32,
}

pub(super) struct AudioFrames {
    buf: PesBuf,
    framed: bool,
    anchor: Option<i64>,
    next_pts: i64,
    tally: DropTally,
}

impl AudioFrames {
    pub fn new(codec: &'static str) -> Self {
        Self {
            buf: PesBuf::with_capacity(8192),
            framed: false,
            anchor: None,
            next_pts: 0,
            tally: DropTally::new(codec),
        }
    }

    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }

    pub fn parse(
        &mut self,
        pes: &PesPacket,
        min_header: usize,
        mut header: impl FnMut(&[u8]) -> Option<Header>,
    ) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let facts = PesFacts::of(pes);
        if pes.discontinuity {
            self.buf.clear();
            self.anchor = None;
        }
        if self.tally.is_poisoned() {
            self.tally.record_drop(
                facts.presentation_ns().unwrap_or(self.next_pts),
                0,
                pes.data.len(),
                "track-poisoned",
            );
            return Vec::new();
        }
        // Preserve the existing raw-AAC/nonframed passthrough contract. Once
        // sync has been seen, later nonsync bytes are continuations, not units.
        if !self.framed && self.buf.is_empty() && pes.data[0] != 0xff {
            self.tally.record_kept();
            let pts_ns = facts.presentation_ns().unwrap_or(self.next_pts);
            self.next_pts = pts_ns;
            return vec![Frame {
                pts_ns,
                keyframe: true,
                data: pes.data.clone(),
                source: facts.source,
                discontinuity: facts.discontinuity,
                ..Frame::default()
            }];
        }
        self.framed = true;
        // Audio PES packets are small. Refuse pathological accumulation before
        // copying; a corrupt size must not grow memory without bound.
        if self.buf.len().saturating_add(pes.data.len()) > 1024 * 1024 {
            self.buf.clear();
            self.tally.record_drop(
                facts.presentation_ns().unwrap_or(self.next_pts),
                0,
                pes.data.len(),
                "buffer-limit",
            );
            return Vec::new();
        }
        self.buf.push(pes);
        let mut frames = Vec::new();
        let mut consumed = 0;
        while self.buf.len() - consumed >= min_header {
            let data = &self.buf.as_slice()[consumed..];
            let Some(h) = header(data) else {
                if data[0] == 0xff && data[1] & 0xe0 == 0xe0 {
                    self.tally
                        .record_drop(self.next_pts, 0, min_header, "header");
                }
                consumed += 1;
                continue;
            };
            if data.len() < h.bytes {
                break;
            }
            let facts = self.buf.facts_at(consumed);
            if let Some(pts) = facts.presentation_ns()
                && self.anchor != Some(pts)
            {
                self.anchor = Some(pts);
                self.next_pts = pts;
            }
            let duration = u64::from(h.samples) * 1_000_000_000 / u64::from(h.rate);
            frames.push(Frame {
                pts_ns: self.next_pts,
                keyframe: true,
                data: data[h.skip..h.bytes].to_vec(),
                duration_ns: Some(duration),
                source: facts.source,
                discontinuity: facts.discontinuity,
                coding: None,
            });
            self.next_pts = self.next_pts.saturating_add(duration as i64);
            self.tally.record_kept();
            consumed += h.bytes;
        }
        self.buf.drain(consumed);
        frames
    }

    pub fn flush(&mut self) -> Vec<Frame> {
        // A trailing partial syncframe is not decodable; never manufacture one.
        self.buf.clear();
        self.tally.log_summary();
        Vec::new()
    }
}
