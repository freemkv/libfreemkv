//! Fragmented MP4 muxer — **STUB**: fragment emission is not implemented.
//!
//! Goal: ISO/IEC 14496-12 fragmented MP4 (`ftyp` + `moov` init segment,
//! then a sequence of `moof+mdat` media fragments) targeting a
//! [`SequentialSink`](crate::io::sink::SequentialSink). DASH-friendly,
//! no Cues backpatch.
//!
//! Status: **STUB**. The muxer can emit the init segment (`ftyp` + a
//! minimal HEVC `moov` skeleton with one video track) via
//! [`Fmp4Mux::write_init_segment`], so the shape and call site are
//! validated, but media fragments (`moof`/`mdat`) are NOT emitted.
//! [`Fmp4Mux::write_video`] therefore returns
//! [`Error::Fmp4Unimplemented`](crate::error::Error::Fmp4Unimplemented)
//! rather than silently accepting and discarding frames. It buffers
//! nothing, so it cannot accumulate memory.
//!
//! ## Not yet implemented
//!
//! - `moof` box: `mfhd` (sequence_number) + `traf` (`tfhd` + `tfdt`
//!   + `trun` with sample sizes, durations, flags, composition offsets).
//! - `mdat` box: concatenated sample data.
//! - Fragment cadence: one fragment per GOP or every N seconds,
//!   whichever comes first.
//! - HEVC `hvcC` box inside `moov.trak.mdia.minf.stbl.stsd` so the
//!   init segment is self-describing (`stsd` currently has zero entries).
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
/// this stub. Fragment emission is not implemented:
/// [`write_video`](Self::write_video) returns
/// [`Error::Fmp4Unimplemented`](crate::error::Error::Fmp4Unimplemented)
/// rather than discarding media.
pub struct Fmp4Mux<W: Write> {
    writer: W,
    header_written: bool,
    /// hvcC bytes, if provided. Embedded in the `moov.…stsd.hvc1.hvcC`
    /// box once the emission path lands.
    #[allow(dead_code)]
    codec_private: Option<Vec<u8>>,
}

impl<W: Write> Fmp4Mux<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            header_written: false,
            codec_private: None,
        }
    }

    /// Provide the `HEVCDecoderConfigurationRecord` for the video track.
    /// The stub stores it but doesn't yet embed it in `moov` — that's
    /// part of the unimplemented emission path.
    pub fn set_video_codec_private(&mut self, hvcc: Vec<u8>) {
        self.codec_private = Some(hvcc);
    }

    /// Emit the init segment (`ftyp` + `moov`) once. Idempotent — a
    /// second call is a no-op. Lets a consumer that just wants the
    /// container shape obtain a valid (if sample-less) init segment.
    pub fn write_init_segment(&mut self) -> io::Result<()> {
        if self.header_written {
            return Ok(());
        }
        let ftyp = build_ftyp();
        let moov = build_moov();
        self.writer.write_all(&ftyp)?;
        self.writer.write_all(&moov)?;
        self.header_written = true;
        Ok(())
    }

    /// Write one video PES frame.
    ///
    /// **Stub:** `moof`/`mdat` emission is not implemented. To avoid
    /// silently dropping media (and avoid unbounded buffering), this
    /// emits the init segment on the first call and then returns
    /// [`Error::Fmp4Unimplemented`](crate::error::Error::Fmp4Unimplemented).
    /// No frame bytes are buffered or written.
    pub fn write_video(&mut self, _pts_ns: i64, _keyframe: bool, _data: &[u8]) -> io::Result<()> {
        self.write_init_segment()?;
        Err(crate::error::Error::Fmp4Unimplemented.into())
    }

    /// Flush the underlying writer.
    pub fn finish(&mut self) -> io::Result<()> {
        self.writer.flush()
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
    // width / height in 16.16 fixed point — placeholder; replace with
    // SPS-derived dimensions (and matching stsd visual width/height) when
    // fragment emission lands.
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
    // actually decodable. Must be populated together with build_mvex:
    // when the hvc1+hvcC sample entry lands and entry_count becomes 1,
    // the trex default_sample_description_index=1 becomes valid.
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
    // dsdi=1 only becomes valid once build_stbl's stsd carries the
    // matching hvc1 sample entry (entry_count=1) — keep the two in sync
    // when fragment emission lands.
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
///
/// All callers build tiny init-segment boxes (kilobytes at most), so the
/// `u32` size never overflows; the saturating cast plus the debug assert
/// documents and guards that invariant rather than silently emitting a
/// truncated, structurally corrupt size field. `body` is always internally
/// constructed here, never untrusted input — a future caller feeding a
/// multi-gigabyte body trips the debug assert instead of writing a malformed
/// box.
fn wrap_box(box_type: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let total = body.len() + 8;
    debug_assert!(total <= u32::MAX as usize, "fMP4 box exceeds u32 size");
    let size = u32::try_from(total).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(total);
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
        mux.write_init_segment().unwrap();
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
    fn write_video_reports_unimplemented_and_buffers_nothing() {
        // write_video must NOT silently accept-and-drop media: it emits the
        // init segment, then signals that fragment emission is unimplemented.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = Fmp4Mux::new(&mut sink);
        let err = mux.write_video(0, true, &[0xDE; 4096]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        mux.finish().unwrap();
        drop(mux);
        // Only the init segment (ftyp + moov) was written — no media bytes.
        let (ftyp_size, _) = read_box_header(&sink);
        let (moov_size, _) = read_box_header(&sink[ftyp_size as usize..]);
        assert_eq!(sink.len(), ftyp_size as usize + moov_size as usize);
    }

    #[test]
    fn moov_contains_trak_mvex() {
        let mut buf: Vec<u8> = Vec::new();
        let mut mux2 = Fmp4Mux::new(&mut buf);
        mux2.write_init_segment().unwrap();
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
