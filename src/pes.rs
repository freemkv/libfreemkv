//! PES frame — the universal intermediate format.
//!
//! Every input stream produces PES frames. Every output stream consumes them.
//! The pipeline just moves frames: input.next_frame() → output.write_frame().
//!
//! A PES frame is one unit of elementary stream data: a video frame,
//! an audio frame, a subtitle packet. It carries a track ID, timestamp,
//! and the raw codec data.

/// One frame of elementary stream data.
#[derive(Debug, Clone)]
pub struct PesFrame {
    /// Track index (0-based, matches stream info track order).
    pub track: usize,
    /// Presentation timestamp in nanoseconds.
    pub pts: i64,
    /// True if this is a keyframe (IDR for video).
    pub keyframe: bool,
    /// Raw elementary stream data (NAL units, audio samples, etc).
    pub data: Vec<u8>,
}

impl PesFrame {
    /// Create from a codec::Frame with a track index.
    pub fn from_codec_frame(track: usize, frame: crate::mux::codec::Frame) -> Self {
        Self {
            track,
            pts: frame.pts_ns,
            keyframe: frame.keyframe,
            data: frame.data,
        }
    }
}

/// Input stream — produces PES frames from any source.
pub trait InputStream {
    /// Get the next frame. Returns None at end of stream.
    fn next_frame(&mut self) -> std::io::Result<Option<PesFrame>>;

    /// Stream metadata (tracks, duration, etc).
    fn info(&self) -> &crate::disc::DiscTitle;

    /// Codec initialization data for a track (SPS/PPS for HEVC, etc).
    /// Returns None until enough frames have been parsed.
    fn codec_private(&self, track: usize) -> Option<Vec<u8>>;

    /// True when codec_private is available for all video tracks.
    fn headers_ready(&self) -> bool;
}

/// Output stream — consumes PES frames to any destination.
pub trait OutputStream {
    /// Write one frame.
    fn write_frame(&mut self, frame: &PesFrame) -> std::io::Result<()>;

    /// Finalize (flush, write index, close).
    fn finish(&mut self) -> std::io::Result<()>;
}
