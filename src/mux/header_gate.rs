//! Shared `headers_ready` rule for the PES read streams.

use crate::disc::{Codec, DiscTitle, Stream};
use crate::pes::PesFrame;

// Source time an AAC track may take to deliver its first frame (the only source
// of its AudioSpecificConfig) before headers are finalised without it.
const AAC_CONFIG_WAIT_NS: i64 = 5_000_000_000;
// Larger PTS steps are discontinuities/wraps, not elapsed time.
const MAX_PTS_STEP_NS: i64 = 1_000_000_000;
// Backstops, well inside the driver's cap: per-track frames that did not
// advance PTS (a stuck clock), and total buffered bytes.
const AAC_CONFIG_WAIT_FRAMES: u32 = 2048;
const AAC_CONFIG_WAIT_BYTES: usize = super::driver::HEADER_BUFFER_CAP_BYTES / 2;

// One track's view of source time, so interleaved tracks far apart in PTS
// neither rebase nor double-count each other.
#[derive(Debug)]
struct TrackClock {
    track: usize,
    max_pts: i64,
    elapsed_ns: i64,
    stalled: u32,
}

/// Tracks how long the header pump has waited on in-band codec configs.
#[derive(Debug, Default)]
pub(crate) struct HeaderGate {
    clocks: Vec<TrackClock>,
    bytes: usize,
    expired: bool,
}

impl HeaderGate {
    /// Account for a frame handed to the caller.
    pub(crate) fn observe(&mut self, frame: &PesFrame) {
        self.account(frame.track, frame.pts, frame.data.len());
    }

    // Per track, elapsed time sums plausible forward PTS steps past the highest
    // PTS seen: small backward steps are reordering; large steps either way rebase.
    fn account(&mut self, track: usize, pts: i64, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
        let clock = match self.clocks.iter().position(|c| c.track == track) {
            Some(i) => &mut self.clocks[i],
            None => {
                self.clocks.push(TrackClock {
                    track,
                    max_pts: pts,
                    elapsed_ns: 0,
                    stalled: 0,
                });
                let last = self.clocks.len() - 1;
                &mut self.clocks[last]
            }
        };
        let step = pts.saturating_sub(clock.max_pts);
        if step > 0 && step <= MAX_PTS_STEP_NS {
            clock.elapsed_ns = clock.elapsed_ns.saturating_add(step);
            clock.max_pts = pts;
        } else {
            clock.stalled = clock.stalled.saturating_add(1);
            if step.saturating_abs() > MAX_PTS_STEP_NS {
                clock.max_pts = pts;
            }
        }
        if clock.stalled >= AAC_CONFIG_WAIT_FRAMES
            || clock.elapsed_ns >= AAC_CONFIG_WAIT_NS
            || self.bytes >= AAC_CONFIG_WAIT_BYTES
        {
            self.expired = true;
        }
    }

    /// End of stream: no further frame can supply a missing config.
    pub(crate) fn expire(&mut self) {
        self.expired = true;
    }

    /// Primary video always needs its config; AAC (ASC, mandatory for ADTS-stripped
    /// A_AAC) and BD LPCM (layout byte: playlists say "5.1") until the wait expires.
    pub(crate) fn ready(
        &self,
        title: &DiscTitle,
        codec_private: impl Fn(usize) -> Option<Vec<u8>>,
    ) -> bool {
        title.streams.iter().enumerate().all(|(idx, s)| {
            let needs = match s {
                Stream::Video(v) => !v.secondary,
                // Secondary (commentary) AAC is not exempt: without its ASC it is
                // undecodable too, and the wait is bounded anyway.
                Stream::Audio(a) => {
                    let bd_lpcm = a.codec == Codec::Lpcm
                        && title.content_format == crate::disc::ContentFormat::BdTs;
                    (a.codec == Codec::Aac || bd_lpcm) && !self.expired
                }
                Stream::Subtitle(_) => false,
            };
            !needs || codec_private(idx).is_some()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1_000_000_000;

    fn aac_title() -> DiscTitle {
        aac_title_with(false)
    }

    fn aac_title_with(secondary: bool) -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams.push(Stream::Audio(crate::disc::AudioStream {
            pid: 0x1100,
            codec: Codec::Aac,
            channels: crate::disc::AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: crate::disc::SampleRate::S48,
            secondary,
            purpose: crate::disc::LabelPurpose::Normal,
            label: String::new(),
        }));
        t
    }

    fn waiting(g: &HeaderGate) -> bool {
        !g.ready(&aac_title(), |_| None)
    }

    fn lpcm_title(format: crate::disc::ContentFormat) -> DiscTitle {
        let mut t = aac_title();
        if let Stream::Audio(a) = &mut t.streams[0] {
            a.codec = Codec::Lpcm;
        }
        t.content_format = format;
        t
    }

    #[test]
    fn bd_lpcm_waits_for_its_layout_byte_until_expiry() {
        use crate::disc::ContentFormat;
        let bd = lpcm_title(ContentFormat::BdTs);
        let mut g = HeaderGate::default();
        assert!(!g.ready(&bd, |_| None), "BD LPCM needs its layout byte");
        assert!(g.ready(&bd, |_| Some(vec![1])));
        assert!(
            g.ready(&lpcm_title(ContentFormat::MpegPs), |_| None),
            "DVD LPCM has no layout byte to wait for"
        );
        g.expire();
        assert!(g.ready(&bd, |_| None), "bounded like AAC");
    }

    #[test]
    fn frame_count_backstop_expires_when_pts_never_advances() {
        let mut g = HeaderGate::default();
        for _ in 0..AAC_CONFIG_WAIT_FRAMES - 1 {
            g.account(0, 0, 0);
        }
        assert!(waiting(&g));
        g.account(0, 0, 0);
        assert!(!waiting(&g), "backstop reached");
    }

    // A PTS wrap/backstep must not reset the clock: elapsed is the sum of
    // plausible forward steps, so 3 s before + 2 s after the wrap is 5 s.
    #[test]
    fn elapsed_time_survives_a_pts_backstep() {
        let mut g = HeaderGate::default();
        for pts in [0, S, 2 * S, 3 * S, S / 100, S + S / 100, 2 * S + S / 100] {
            g.account(0, pts, 0);
        }
        assert!(!waiting(&g));
    }

    // A forward jump (discontinuity) is not elapsed time.
    #[test]
    fn a_forward_pts_jump_is_not_counted_as_waiting_time() {
        let mut g = HeaderGate::default();
        for pts in [0, S, 100 * S, 101 * S] {
            g.account(0, pts, 0);
        }
        assert!(waiting(&g));
    }

    // Decode-order reordering (B-frames) must not inflate elapsed time.
    #[test]
    fn reordered_pts_count_once() {
        let mut g = HeaderGate::default();
        let mut pts = Vec::new();
        for gop in 0..4 {
            let b = gop * 3 * (S / 3);
            pts.extend([b + S, b + S / 3, b + 2 * S / 3]);
        }
        for p in pts {
            g.account(0, p, 0);
        }
        assert!(waiting(&g), "PTS 1 s -> 4 s is 3 s of content, not 5+");
    }

    // Two dense tracks 1.2 s apart in PTS, muxed in bursts: 3 s of content
    // must count as 3 s, not the sum over both tracks.
    #[test]
    fn interleaved_tracks_far_apart_in_pts_do_not_double_count() {
        let mut g = HeaderGate::default();
        let step = S / 25;
        for burst in 0..7i64 {
            for i in 0..10 {
                let t = (burst * 10 + i) * step;
                g.account(0, S + S / 5 + t, 0);
            }
            for i in 0..10 {
                let t = (burst * 10 + i) * step;
                g.account(1, t, 0);
            }
        }
        assert!(waiting(&g), "about 2.8 s of content per track");
    }

    // A high-rate track (TrueHD, ~1200 frames/s) advancing normally is not a
    // stuck-PTS source: 2 s of it must not trip the frame backstop.
    #[test]
    fn high_frame_rate_track_does_not_trip_the_frame_backstop() {
        let mut g = HeaderGate::default();
        let step = S / 1200;
        for i in 0..2400 {
            g.account(0, i * step, 0);
        }
        assert!(waiting(&g));
    }

    #[test]
    fn byte_bound_expires_before_the_driver_cap() {
        let mut g = HeaderGate::default();
        g.account(0, 0, AAC_CONFIG_WAIT_BYTES - 1);
        assert!(waiting(&g));
        g.account(0, 0, 1);
        assert!(!waiting(&g));
    }

    #[test]
    fn secondary_aac_track_is_waited_for() {
        let t = aac_title_with(true);
        let g = HeaderGate::default();
        assert!(!g.ready(&t, |_| None), "commentary AAC still needs its ASC");
        assert!(g.ready(&t, |_| Some(vec![0x12, 0x10])));
    }

    #[test]
    fn video_gate_is_never_expired() {
        let mut g = HeaderGate::default();
        g.expire();
        let mut t = DiscTitle::empty();
        t.streams.push(Stream::Video(crate::disc::VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: crate::disc::Resolution::R2160p,
            frame_rate: crate::disc::FrameRate::F23_976,
            hdr: crate::disc::HdrFormat::Hdr10,
            color_space: crate::disc::ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }));
        assert!(!g.ready(&t, |_| None));
    }
}
