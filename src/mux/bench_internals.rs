//! Stable entry points for the criterion benches in `benches/` (feature `bench-internals`).
//!
//! The demuxer, codec parsers and MKV muxer are crate-private; these thin wrappers let a
//! bench drive them without making them API. Not part of the public contract.

use std::io::{self, Seek, Write};

use super::codec::{CodecParser, parser_for_codec};
use super::mkv::{MkvMuxer, MkvTrack};
use super::ts::{PesPacket, TsDemuxer};
use crate::disc::{
    AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, LabelPurpose, Resolution,
    SampleRate, Stream, VideoStream,
};

/// BD-TS demuxer: counts what each feed completes instead of handing the packets out.
pub struct TsDemux(TsDemuxer);

impl TsDemux {
    /// Track `pids`.
    pub fn new(pids: &[u16]) -> Self {
        Self(TsDemuxer::new(pids))
    }

    /// Feed `data` (BD-TS, 192-byte packets) at source offset `base`.
    /// Returns `(PES completed, ES bytes completed)`.
    pub fn feed_at(&mut self, base: u64, data: &[u8]) -> (usize, usize) {
        count(self.0.feed_at(base, data))
    }

    /// Flush every open PES; same counts as [`Self::feed_at`].
    pub fn flush(&mut self) -> (usize, usize) {
        count(self.0.flush())
    }
}

fn count(pes: Vec<PesPacket>) -> (usize, usize) {
    (pes.len(), pes.iter().map(|p| p.data.len()).sum())
}

/// The production codec parser for `codec` (BD flavour).
pub struct Parser {
    inner: Box<dyn CodecParser>,
    out: Vec<super::codec::Frame>,
}

impl Parser {
    /// Parser for `codec`, as the BD pipeline builds it.
    pub fn new(codec: Codec) -> Self {
        Self {
            inner: parser_for_codec(codec, None, false),
            out: Vec::new(),
        }
    }

    /// Parse one PES. Returns `(frames, frame bytes)` emitted by it.
    pub fn parse(&mut self, pts: Option<i64>, data: Vec<u8>) -> (usize, usize) {
        let pes = PesPacket {
            pid: 0x1100,
            pts,
            dts: None,
            data,
            source: None,
            discontinuity: false,
        };
        self.out.clear();
        self.inner.parse_into(pes, &mut self.out);
        (self.out.len(), self.out.iter().map(|f| f.data.len()).sum())
    }

    /// Drain the parser at end of stream; same counts as [`Self::parse`].
    pub fn flush(&mut self) -> (usize, usize) {
        self.out.clear();
        self.inner.flush_into(&mut self.out);
        (self.out.len(), self.out.iter().map(|f| f.data.len()).sum())
    }
}

/// A valid 48 kHz AC-3 syncframe (`frmsizecod` 0..37) with a correct frame CRC.
pub fn ac3_frame(frmsizecod: u8) -> Vec<u8> {
    let size = super::codec::ac3::ac3_frame_size(&[0x0B, 0x77, 0, 0, frmsizecod]);
    let mut frame = vec![0u8; size];
    frame[0] = 0x0B;
    frame[1] = 0x77;
    frame[4] = frmsizecod;
    frame[5] = 0x08 << 3;
    let n = frame.len();
    let c = super::codec::crc::crc16_ansi(&frame[2..n - 2]);
    frame[n - 2..].copy_from_slice(&c.to_be_bytes());
    frame
}

/// A TrueHD access unit of `size` bytes (even, >= 8) with no major sync.
pub fn truehd_unit(size: usize) -> Vec<u8> {
    let words = size / 2;
    let mut au = vec![0u8; size];
    au[0] = ((words >> 8) & 0x0F) as u8;
    au[1] = (words & 0xFF) as u8;
    au
}

/// A 48 kHz, 512-sample DTS core frame of `size` bytes.
pub fn dts_core(size: usize) -> Vec<u8> {
    let fsize = size - 1;
    let mut f = vec![0u8; size];
    f[0..4].copy_from_slice(&[0x7F, 0xFE, 0x80, 0x01]);
    f[4] = 0x80 | (31u8 << 2);
    f[5] = (15u8 << 2) | ((fsize >> 12) & 0x03) as u8;
    f[6] = ((fsize >> 4) & 0xFF) as u8;
    f[7] = ((fsize & 0x0F) << 4) as u8;
    f[8] = 13u8 << 2;
    f
}

/// An MKV muxer over `writer` with an H.264 video track (0) and an AC-3 track (1).
/// `mvc` gives the video track an mvcC mapping, so a block additional is written.
pub struct MkvWriter<W: Write + Seek>(MkvMuxer<W>);

impl<W: Write + Seek> MkvWriter<W> {
    /// Write the header for the two-track title.
    pub fn new(writer: W, mvc: bool) -> io::Result<Self> {
        let video = Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::H264,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        });
        let audio = Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        });
        let mut tracks: Vec<MkvTrack> = [video, audio]
            .iter()
            .filter_map(MkvTrack::from_stream)
            .collect();
        tracks[0].codec_private = Some(vec![1, 100, 0, 40, 0xFF, 0xE0, 0]);
        if mvc {
            tracks[0].mvc_params = Some((vec![0x0F, 128, 0, 40], vec![0x08, 0xCE]));
        }
        MkvMuxer::new_with_timing(writer, &tracks, &[], None, 7200.0, &[]).map(Self)
    }

    /// Write one frame; `duration_ns` makes it a BlockGroup, as does `additional`.
    pub fn write(
        &mut self,
        track: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
        duration_ns: Option<u64>,
        additional: Option<&[u8]>,
    ) -> io::Result<()> {
        self.0.write_frame_at_with_padding(
            track,
            pts_ns,
            keyframe,
            data,
            duration_ns,
            additional,
            None,
            None,
            0,
        )
    }

    /// Close the last cluster and write Cues, Tags and the back-patches.
    pub fn finish(self) -> io::Result<()> {
        self.0.finish()
    }
}
