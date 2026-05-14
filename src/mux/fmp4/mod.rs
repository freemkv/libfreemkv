//! Fragmented MP4 muxer — **stub** for Phase 3.
//!
//! Goal: ISO/IEC 14496-12 fragmented MP4 (`ftyp` + `moov` init segment,
//! then a sequence of `moof+mdat` media fragments) targeting a
//! [`SequentialSink`](crate::io::sink::SequentialSink). DASH-friendly,
//! no Cues backpatch.
//!
//! Status (v0.21.0 Phase 3): **STUB**. We ship the init segment
//! (`ftyp` + a minimal HEVC `moov` skeleton with one video track) so
//! the muxer's shape and call site are validated, but media fragments
//! are NOT yet emitted — calls to [`Fmp4Mux::write_video`] currently
//! accumulate frames into an internal buffer and discard them on
//! [`Fmp4Mux::finish`].
//!
//! ## What's TODO (tracked in Phase 4 / v0.22.0 scope)
//!
//! - `moof` box: `mfhd` (sequence_number) + `traf` (`tfhd` + `tfdt`
//!   + `trun` with sample sizes, durations, flags, composition offsets).
//! - `mdat` box: concatenated sample data.
//! - Fragment cadence: one fragment per GOP or every N seconds,
//!   whichever comes first.
//! - HEVC `hvcC` box inside `moov.trak.mdia.minf.stbl.stsd` so the
//!   init segment is self-describing.
//! - Sample-flags computation (sync vs. delta, depends_on, etc.).
//! - Edit lists / fragment_duration for accurate seeking.
//!
//! Reference: ISO/IEC 14496-12 §8 (Movie Fragments).

use std::io::{self, Write};

// Box type literals — four-character codes per ISO/IEC 14496-12 §4.2.

const FTYP: [u8; 4] = *b"ftyp";
const MOOV: [u8; 4] = *b"moov";
const MVHD: [u8; 4] = *b"mvhd";
const TRAK: [u8; 4] = *b"trak";
const TKHD: [u8; 4] = *b"tkhd";
const MDIA: [u8; 4] = *b"mdia";
const MDHD: [u8; 4] = *b"mdhd";
const HDLR: [u8; 4] = *b"hdlr";
const MINF: [u8; 4] = *b"minf";
const VMHD: [u8; 4] = *b"vmhd";
const DINF: [u8; 4] = *b"dinf";
const DREF: [u8; 4] = *b"dref";
const URL_: [u8; 4] = *b"url ";
const STBL: [u8; 4] = *b"stbl";
const STSD: [u8; 4] = *b"stsd";
const STTS: [u8; 4] = *b"stts";
const STSC: [u8; 4] = *b"stsc";
const STSZ: [u8; 4] = *b"stsz";
const STCO: [u8; 4] = *b"stco";
const MVEX: [u8; 4] = *b"mvex";
const TREX: [u8; 4] = *b"trex";

/// Default movie timescale — 90 kHz lines up with MPEG-TS PTS and the
/// HEVC SPS `vui_time_scale` for film content, simplifying the math
/// when fragment emission lands.
const MOVIE_TIMESCALE: u32 = 90_000;
/// Video track ID. fMP4 init segments conventionally use track_ID=1
/// for the primary video track; a single-track DASH representation has
/// no reason to deviate.
const VIDEO_TRACK_ID: u32 = 1;

/// Fragmented MP4 muxer — stub.
///
/// See the module-level doc comment for what is and isn't shipped in
/// this stub.
pub struct Fmp4Mux<W: Write> {
    writer: W,
    header_written: bool,
    /// Pending frames — held for the future fragment-emit path. The
    /// stub drops these on `finish` but keeping them around lets the
    /// post-stub work re-attach without changing the public API.
    pending: Vec<PendingSample>,
    /// hvcC bytes, if provided. Embedded in the `moov.…stsd.hvc1.hvcC`
    /// box once that path lands.
    #[allow(dead_code)]
    codec_private: Option<Vec<u8>>,
}

struct PendingSample {
    #[allow(dead_code)]
    pts_ns: i64,
    #[allow(dead_code)]
    keyframe: bool,
    #[allow(dead_code)]
    data: Vec<u8>,
}

impl<W: Write> Fmp4Mux<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            header_written: false,
            pending: Vec::new(),
            codec_private: None,
        }
    }

    /// Provide the `HEVCDecoderConfigurationRecord` for the video track.
    /// The stub stores it but doesn't yet embed it in `moov` — that's
    /// part of the post-stub work.
    pub fn set_video_codec_private(&mut self, hvcc: Vec<u8>) {
        self.codec_private = Some(hvcc);
    }

    /// Write one video PES frame.
    ///
    /// **Stub behaviour:** the first call emits the init segment
    /// (`ftyp` + `moov`) so any consumer that just wants the shape can
    /// receive it. Subsequent calls accumulate frames in memory for
    /// the future fragmenting path; **no media bytes are written yet**.
    pub fn write_video(&mut self, pts_ns: i64, keyframe: bool, data: &[u8]) -> io::Result<()> {
        if !self.header_written {
            self.write_init_segment()?;
            self.header_written = true;
        }
        // TODO(0.22.0): emit one `moof+mdat` per GOP. For now stash the
        // frame so the future patch can hot-wire emission without API
        // churn.
        self.pending.push(PendingSample {
            pts_ns,
            keyframe,
            data: data.to_vec(),
        });
        Ok(())
    }

    /// Flush. The stub additionally drops accumulated `pending` frames.
    pub fn finish(&mut self) -> io::Result<()> {
        // TODO(0.22.0): emit final fragment from pending; today the
        // stub just clears the buffer to release memory.
        self.pending.clear();
        self.writer.flush()
    }

    fn write_init_segment(&mut self) -> io::Result<()> {
        let ftyp = build_ftyp();
        let moov = build_moov();
        self.writer.write_all(&ftyp)?;
        self.writer.write_all(&moov)?;
        Ok(())
    }
}

/// Build the `ftyp` box. `major_brand = "iso6"`, `minor_version = 1`,
/// compatible brands `iso6 dash msdh hvc1` — the same conservative set
/// shaka-packager uses for HEVC-in-fMP4 outputs.
fn build_ftyp() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"iso6");
    body.extend_from_slice(&1u32.to_be_bytes());
    body.extend_from_slice(b"iso6");
    body.extend_from_slice(b"dash");
    body.extend_from_slice(b"msdh");
    body.extend_from_slice(b"hvc1");
    wrap_box(&FTYP, &body)
}

/// Build the `moov` box — minimal skeleton. Single video trak, no
/// hvcC inside stsd yet (TODO: full hvc1 sample entry).
fn build_moov() -> Vec<u8> {
    let mvhd = build_mvhd();
    let trak = build_video_trak();
    let mvex = build_mvex();
    let mut body = Vec::new();
    body.extend_from_slice(&mvhd);
    body.extend_from_slice(&trak);
    body.extend_from_slice(&mvex);
    wrap_box(&MOOV, &body)
}

fn build_mvhd() -> Vec<u8> {
    // Version 0, 100 bytes total body. Fields per ISO/IEC 14496-12 §8.2.2.
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 0, 0, 0]); // version + flags
    body.extend_from_slice(&0u32.to_be_bytes()); // creation_time
    body.extend_from_slice(&0u32.to_be_bytes()); // modification_time
    body.extend_from_slice(&MOVIE_TIMESCALE.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes()); // duration = 0 (fragmented)
    body.extend_from_slice(&0x0001_0000u32.to_be_bytes()); // rate 1.0
    body.extend_from_slice(&0x0100u16.to_be_bytes()); // volume 1.0
    body.extend_from_slice(&[0u8; 2]); // reserved
    body.extend_from_slice(&[0u8; 8]); // reserved
    // 3x3 identity transformation matrix in 16.16 fixed point.
    for v in [0x1_0000u32, 0, 0, 0, 0x1_0000, 0, 0, 0, 0x4000_0000] {
        body.extend_from_slice(&v.to_be_bytes());
    }
    body.extend_from_slice(&[0u8; 24]); // pre_defined[6]
    body.extend_from_slice(&2u32.to_be_bytes()); // next_track_ID (1 reserved for video)
    wrap_box(&MVHD, &body)
}

fn build_video_trak() -> Vec<u8> {
    let tkhd = build_tkhd();
    let mdia = build_mdia();
    let mut body = Vec::new();
    body.extend_from_slice(&tkhd);
    body.extend_from_slice(&mdia);
    wrap_box(&TRAK, &body)
}

fn build_tkhd() -> Vec<u8> {
    let mut body = Vec::new();
    // version=0 | flags=0x000007 (track_enabled | in_movie | in_preview)
    body.extend_from_slice(&[0, 0, 0, 7]);
    body.extend_from_slice(&0u32.to_be_bytes()); // creation_time
    body.extend_from_slice(&0u32.to_be_bytes()); // modification_time
    body.extend_from_slice(&VIDEO_TRACK_ID.to_be_bytes());
    body.extend_from_slice(&[0u8; 4]); // reserved
    body.extend_from_slice(&0u32.to_be_bytes()); // duration
    body.extend_from_slice(&[0u8; 8]); // reserved
    body.extend_from_slice(&0u16.to_be_bytes()); // layer
    body.extend_from_slice(&0u16.to_be_bytes()); // alternate_group
    body.extend_from_slice(&0u16.to_be_bytes()); // volume (video=0)
    body.extend_from_slice(&[0u8; 2]); // reserved
    // 3x3 identity matrix.
    for v in [0x1_0000u32, 0, 0, 0, 0x1_0000, 0, 0, 0, 0x4000_0000] {
        body.extend_from_slice(&v.to_be_bytes());
    }
    // width / height in 16.16 fixed point — placeholder 1920x1080.
    body.extend_from_slice(&(1920u32 << 16).to_be_bytes());
    body.extend_from_slice(&(1080u32 << 16).to_be_bytes());
    wrap_box(&TKHD, &body)
}

fn build_mdia() -> Vec<u8> {
    let mdhd = build_mdhd();
    let hdlr = build_hdlr_vide();
    let minf = build_minf();
    let mut body = Vec::new();
    body.extend_from_slice(&mdhd);
    body.extend_from_slice(&hdlr);
    body.extend_from_slice(&minf);
    wrap_box(&MDIA, &body)
}

fn build_mdhd() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 0, 0, 0]); // version + flags
    body.extend_from_slice(&0u32.to_be_bytes()); // creation_time
    body.extend_from_slice(&0u32.to_be_bytes()); // modification_time
    body.extend_from_slice(&MOVIE_TIMESCALE.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes()); // duration
    // language: 'und' in 5-bit-per-char ISO 639-2 packed (bit 15 = 0).
    body.extend_from_slice(&[0x55, 0xC4]);
    body.extend_from_slice(&0u16.to_be_bytes()); // pre_defined
    wrap_box(&MDHD, &body)
}

fn build_hdlr_vide() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 0, 0, 0]); // version + flags
    body.extend_from_slice(&0u32.to_be_bytes()); // pre_defined
    body.extend_from_slice(b"vide");
    body.extend_from_slice(&[0u8; 12]); // reserved
    body.extend_from_slice(b"VideoHandler\0");
    wrap_box(&HDLR, &body)
}

fn build_minf() -> Vec<u8> {
    let vmhd = build_vmhd();
    let dinf = build_dinf();
    let stbl = build_stbl();
    let mut body = Vec::new();
    body.extend_from_slice(&vmhd);
    body.extend_from_slice(&dinf);
    body.extend_from_slice(&stbl);
    wrap_box(&MINF, &body)
}

fn build_vmhd() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 0, 0, 1]); // version + flags=1
    body.extend_from_slice(&0u16.to_be_bytes()); // graphicsmode
    body.extend_from_slice(&[0u8; 6]); // opcolor
    wrap_box(&VMHD, &body)
}

fn build_dinf() -> Vec<u8> {
    let mut dref_body = Vec::new();
    dref_body.extend_from_slice(&[0, 0, 0, 0]);
    dref_body.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    // url with flags=1 (self-contained) and zero name.
    let url_body = [0u8, 0, 0, 1];
    dref_body.extend_from_slice(&wrap_box(&URL_, &url_body));
    let dref = wrap_box(&DREF, &dref_body);
    wrap_box(&DINF, &dref)
}

fn build_stbl() -> Vec<u8> {
    // Stub stsd: empty sample description (zero entries). Replace with
    // hvc1+hvcC once the fragmenting path lands so the init segment is
    // actually decodable.
    let mut stsd_body = Vec::new();
    stsd_body.extend_from_slice(&[0, 0, 0, 0]);
    stsd_body.extend_from_slice(&0u32.to_be_bytes()); // entry_count
    let stsd = wrap_box(&STSD, &stsd_body);

    // Empty stts/stsc/stsz/stco — fragmented init has no samples here.
    let stts = wrap_box(&STTS, &[0, 0, 0, 0, 0, 0, 0, 0]); // version+flags, count=0
    let stsc = wrap_box(&STSC, &[0, 0, 0, 0, 0, 0, 0, 0]);
    let stsz = wrap_box(
        &STSZ,
        &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // version+flags, sample_size=0, count=0
    );
    let stco = wrap_box(&STCO, &[0, 0, 0, 0, 0, 0, 0, 0]);

    let mut body = Vec::new();
    body.extend_from_slice(&stsd);
    body.extend_from_slice(&stts);
    body.extend_from_slice(&stsc);
    body.extend_from_slice(&stsz);
    body.extend_from_slice(&stco);
    wrap_box(&STBL, &body)
}

fn build_mvex() -> Vec<u8> {
    // trex: track_ID=1, default_sample_description_index=1, others=0.
    let mut trex_body = Vec::new();
    trex_body.extend_from_slice(&[0, 0, 0, 0]); // version + flags
    trex_body.extend_from_slice(&VIDEO_TRACK_ID.to_be_bytes());
    trex_body.extend_from_slice(&1u32.to_be_bytes()); // default_sample_description_index
    trex_body.extend_from_slice(&0u32.to_be_bytes()); // default_sample_duration
    trex_body.extend_from_slice(&0u32.to_be_bytes()); // default_sample_size
    trex_body.extend_from_slice(&0u32.to_be_bytes()); // default_sample_flags
    let trex = wrap_box(&TREX, &trex_body);
    wrap_box(&MVEX, &trex)
}

/// Wrap a box body in `[size:u32-BE][type:4]`. Suitable for any body
/// that fits in u32; oversized boxes (size > 4 GiB) need the 64-bit
/// large-size extension which we don't generate in the stub.
fn wrap_box(box_type: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let size = (body.len() + 8) as u32;
    let mut out = Vec::with_capacity(body.len() + 8);
    out.extend_from_slice(&size.to_be_bytes());
    out.extend_from_slice(box_type);
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode the first box's size + type from `buf`.
    fn read_box_header(buf: &[u8]) -> (u32, [u8; 4]) {
        let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let bt = [buf[4], buf[5], buf[6], buf[7]];
        (size, bt)
    }

    #[test]
    fn init_segment_starts_with_ftyp_then_moov() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = Fmp4Mux::new(&mut sink);
        // Trigger init emission via a single (stubbed) write.
        mux.write_video(0, true, &[0x00, 0x00, 0x00, 0x01, 0x40]).unwrap();
        mux.finish().unwrap();
        drop(mux);

        let (ftyp_size, ftyp_type) = read_box_header(&sink);
        assert_eq!(&ftyp_type, b"ftyp");
        assert!(ftyp_size >= 24, "ftyp too small: {ftyp_size}");

        let (moov_size, moov_type) = read_box_header(&sink[ftyp_size as usize..]);
        assert_eq!(&moov_type, b"moov");
        assert!(moov_size > 100, "moov skeleton too small: {moov_size}");

        // Stub guarantee: no media bytes after the init segment.
        let total = ftyp_size as usize + moov_size as usize;
        assert_eq!(sink.len(), total, "stub leaked media bytes past moov");
    }

    #[test]
    fn moov_contains_trak_mvex() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = Fmp4Mux::new(&mut sink);
        mux.write_video(0, true, &[]).unwrap();
        mux.finish().unwrap();
        drop(sink);

        // Re-emit into a fresh buffer for parsing.
        let mut buf: Vec<u8> = Vec::new();
        let mut mux2 = Fmp4Mux::new(&mut buf);
        mux2.write_video(0, true, &[]).unwrap();
        mux2.finish().unwrap();
        drop(mux2);

        // Find moov payload start.
        let (ftyp_size, _) = read_box_header(&buf);
        let moov_start = ftyp_size as usize;
        let (moov_size, _) = read_box_header(&buf[moov_start..]);
        let moov_payload = &buf[moov_start + 8..moov_start + moov_size as usize];

        // Scan for the trak and mvex four-CC anywhere in the moov payload.
        let has_trak = moov_payload.windows(4).any(|w| w == b"trak");
        let has_mvex = moov_payload.windows(4).any(|w| w == b"mvex");
        assert!(has_trak, "moov missing trak");
        assert!(has_mvex, "moov missing mvex");
    }
}
