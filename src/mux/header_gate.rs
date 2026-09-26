//! Shared `headers_ready` rule for the PES read streams.

use crate::disc::{Codec, DiscTitle, Stream};
use crate::pes::PesFrame;

// Source time an AAC track may take to deliver its first frame (the only source
// of its AudioSpecificConfig) before headers are finalised without it.
const AAC_CONFIG_WAIT_NS: i64 = 5_000_000_000;
// Backstop for sources whose PTS never advances.
const AAC_CONFIG_WAIT_FRAMES: u32 = 2048;

/// Tracks how long the header pump has waited on in-band codec configs.
#[derive(Debug, Default)]
pub(crate) struct HeaderGate {
    first_pts: Option<i64>,
    frames: u32,
    expired: bool,
}

impl HeaderGate {
    /// Account for a frame handed to the caller.
    pub(crate) fn observe(&mut self, frame: &PesFrame) {
        self.frames = self.frames.saturating_add(1);
        let first = *self.first_pts.get_or_insert(frame.pts);
        if self.frames >= AAC_CONFIG_WAIT_FRAMES
            || frame.pts.saturating_sub(first) >= AAC_CONFIG_WAIT_NS
        {
            self.expired = true;
        }
    }

    /// End of stream: no further frame can supply a missing config.
    pub(crate) fn expire(&mut self) {
        self.expired = true;
    }

    /// Primary video always needs its config; AAC needs it until the wait expires
    /// (Matroska A_AAC frames are ADTS-stripped, so the ASC is mandatory).
    pub(crate) fn ready(
        &self,
        title: &DiscTitle,
        codec_private: impl Fn(usize) -> Option<Vec<u8>>,
    ) -> bool {
        title.streams.iter().enumerate().all(|(idx, s)| {
            let needs = match s {
                Stream::Video(v) => !v.secondary,
                Stream::Audio(a) => matches!(a.codec, Codec::Aac) && !self.expired,
                Stream::Subtitle(_) => false,
            };
            !needs || codec_private(idx).is_some()
        })
    }
}
