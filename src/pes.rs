//! Stream — read PES frames in, write PES frames out.
//!
//! A stream is a stream. You read() from it or write() to it.
//! The stream handles its own format internally.
//!
//! disc.read()  → PES frame (sectors → decrypt → demux internally)
//! mkv.write(frame) → MKV file (mux internally)

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
    /// Serialize to bytes: track(1) | pts(8) | keyframe(1) | len(4) | data
    pub fn serialize(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        if self.track > 255 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "track index exceeds 255",
            ));
        }
        if self.data.len() > u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame data exceeds 4 GB",
            ));
        }
        w.write_all(&[self.track as u8])?;
        w.write_all(&self.pts.to_le_bytes())?;
        w.write_all(&[if self.keyframe { 1 } else { 0 }])?;
        w.write_all(&(self.data.len() as u32).to_le_bytes())?;
        w.write_all(&self.data)
    }

    /// Deserialize from bytes. Returns None at EOF.
    pub fn deserialize(r: &mut dyn std::io::Read) -> std::io::Result<Option<Self>> {
        const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024; // 256 MB

        let mut header = [0u8; 14]; // 1 + 8 + 1 + 4
        match r.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let track = header[0] as usize;
        let pts = i64::from_le_bytes([
            header[1], header[2], header[3], header[4],
            header[5], header[6], header[7], header[8],
        ]);
        let keyframe = header[9] != 0;
        let len = u32::from_le_bytes([header[10], header[11], header[12], header[13]]) as usize;
        if len > MAX_FRAME_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame size {} exceeds maximum {}", len, MAX_FRAME_SIZE),
            ));
        }
        let mut data = vec![0u8; len];
        r.read_exact(&mut data)?;
        Ok(Some(Self { track, pts, keyframe, data }))
    }

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

/// A stream. Read from it or write to it. Not both.
pub trait Stream {
    /// Read the next frame. Returns None at end of stream.
    fn read(&mut self) -> std::io::Result<Option<PesFrame>>;

    /// Write a frame.
    fn write(&mut self, frame: &PesFrame) -> std::io::Result<()>;

    /// Finalize (flush, write index, close).
    fn finish(&mut self) -> std::io::Result<()>;

    /// Stream metadata.
    fn info(&self) -> &crate::disc::DiscTitle;

    /// Codec initialization data for a track (SPS/PPS, etc).
    fn codec_private(&self, _track: usize) -> Option<Vec<u8>> { None }

    /// True when codec_private is available for all video tracks.
    fn headers_ready(&self) -> bool { true }
}
