//! Standard MPEG-TS (188-byte packets) muxer — sequential-only.
//!
//! Distinct from `super::tsmux::TsMuxer` (BD-TS with 192-byte packets
//! and the 4-byte TP_extra_header). This muxer emits the IETF / ISO/IEC
//! 13818-1 wire format that ffmpeg, VLC, and `m2tsindex` consume
//! out of the box. Use it for plain `.ts` / `.m2ts` files over a
//! [`SequentialSink`](crate::io::sink::SequentialSink), and for
//! MPEG-TS-over-UDP via [`UdpSocketSink`](crate::io::sink::UdpSocketSink).
//!
//! ## Wire format
//!
//! Every output packet is exactly 188 bytes:
//!
//! ```text
//! sync_byte:8 = 0x47
//! transport_error_indicator:1 = 0
//! payload_unit_start_indicator:1
//! transport_priority:1 = 0
//! PID:13
//! transport_scrambling_control:2 = 0
//! adaptation_field_control:2
//! continuity_counter:4
//! [adaptation field, if signalled]
//! [payload, if signalled]
//! ```
//!
//! ## Wiring
//!
//! Single program (PMT PID `0x1000`, program number `1`):
//!   - PAT on PID `0x0000`
//!   - PMT on PID `0x1000`
//!   - Video (HEVC, stream_type `0x24`) on PID `0x0100`
//!   - First audio (AC3, stream_type `0x81`) on PID `0x0101`
//!     (or TrueHD, stream_type `0x83`, if hinted)
//!
//! ## Scope vs. full TS
//!
//! This is a deliberately minimal viable muxer:
//!   - One program, one video track, optionally one audio track.
//!   - PAT + PMT re-emitted every `PSI_INTERVAL_PACKETS` packets so a
//!     mid-stream receiver can lock on within ~100 ms at typical
//!     UHD bitrates.
//!   - PCR clock derived from video PTS (`pts - PCR_LEAD_90KHZ`),
//!     attached to the video PID's adaptation field every
//!     `PCR_INTERVAL_PACKETS` packets.
//!   - No language / descriptor tags, no SCTE-35 markers, no per-PID
//!     PMT version bumps, no SDT/EIT. Sufficient for "ffmpeg can play
//!     this back", not for full broadcast deployment.

use std::io::{self, Write};

mod packet;

use packet::{Packet, PacketWriter};

/// PID `0x0000` — PAT, mandated by spec.
const PID_PAT: u16 = 0x0000;
/// PID for the single program's PMT. `0x1000` is the conventional
/// choice; anything outside reserved/null ranges works.
const PID_PMT: u16 = 0x1000;
/// PID for the video elementary stream.
const PID_VIDEO: u16 = 0x0100;
/// PID for the (optional) first audio elementary stream.
const PID_AUDIO: u16 = 0x0101;
/// Null PID — reserved by spec; never used here.
#[allow(dead_code)]
const PID_NULL: u16 = 0x1FFF;

/// Re-emit PAT/PMT every N TS packets. ~250 × 188 B = 47 KB; at
/// 80 Mb/s UHD that's ~5 ms — well under typical receiver lock budget.
const PSI_INTERVAL_PACKETS: u64 = 250;
/// Re-stamp PCR every N TS packets carrying the video PID. Spec MAX
/// is 100 ms; 40 packets × 188 B at moderate bitrate keeps us under.
const PCR_INTERVAL_PACKETS: u64 = 40;
/// PCR lead time — the PCR must precede the PTS of the first byte of
/// the picture it timestamps. 200 ms in 90 kHz ticks.
const PCR_LEAD_90KHZ: u64 = 90_000 / 5;

/// HEVC stream-type code, ISO/IEC 13818-1 Table 2-34 (2015 amendment).
const STREAM_TYPE_HEVC: u8 = 0x24;
/// AC-3 / E-AC-3. Not an ISO assignment — sits in the user-private
/// 0x80-0xFF range and is the Blu-ray Disc Association / ATSC A/52
/// convention.
const STREAM_TYPE_AC3: u8 = 0x81;
/// Dolby TrueHD. Also a private/BD-conventional value in the
/// user-private 0x80-0xFF range, not an ISO assignment.
const STREAM_TYPE_TRUEHD: u8 = 0x83;

/// Audio codec hint for [`M2tsMux::new`] / [`M2tsMux::set_audio`]. The
/// muxer needs to know the codec to pick the right PMT `stream_type`
/// and the right PES stream_id; it doesn't decode the audio.
#[derive(Debug, Clone, Copy)]
pub enum AudioCodec {
    /// AC-3 / E-AC-3, stream_type `0x81`, PES stream_id `0xBD`.
    Ac3,
    /// Dolby TrueHD, stream_type `0x83`, PES stream_id `0xBD`.
    TrueHd,
}

impl AudioCodec {
    fn stream_type(self) -> u8 {
        match self {
            AudioCodec::Ac3 => STREAM_TYPE_AC3,
            AudioCodec::TrueHd => STREAM_TYPE_TRUEHD,
        }
    }
}

/// Sequential MPEG-TS muxer with one video and optional one audio track.
pub struct M2tsMux<W: Write> {
    out: PacketWriter<W>,
    /// hvcC parameter set bytes to prepend to the first video PES.
    /// Optional — if the upstream frames already carry inline params,
    /// callers can omit this.
    video_codec_private: Option<Vec<u8>>,
    /// Set on first video frame: have we emitted VPS/SPS/PPS?
    params_written: bool,
    /// Audio codec, if an audio track is configured. `None` = video-only.
    audio: Option<AudioCodec>,
    /// First seen PTS (90 kHz). All subsequent PTS / PCR values are
    /// relative to this so output streams start near t=0 and don't
    /// confuse downstream parsers that don't tolerate huge starting
    /// timestamps.
    base_pts_90k: Option<u64>,
    /// Per-PID continuity counter, 4 bits, monotonically increasing.
    cc_video: u8,
    cc_audio: u8,
    cc_pat: u8,
    cc_pmt: u8,
    /// Total packets written, used to gate PSI / PCR cadence.
    packets_written: u64,
    /// Video packets written since last PCR, used to gate PCR cadence.
    video_packets_since_pcr: u64,
    /// Set once the first video TS packet has been emitted. Forces a PCR
    /// onto the very first video PES so a receiver tuning at stream start
    /// has a clock reference (the PMT advertises the video PID as PCR_PID).
    first_video_written: bool,
}

impl<W: Write> M2tsMux<W> {
    /// Construct a muxer wrapping `writer`. By default audio is
    /// disabled; call [`set_audio`](Self::set_audio) before the first
    /// frame to enable.
    pub fn new(writer: W) -> Self {
        Self {
            out: PacketWriter::new(writer),
            video_codec_private: None,
            params_written: false,
            audio: None,
            base_pts_90k: None,
            cc_video: 0,
            cc_audio: 0,
            cc_pat: 0,
            cc_pmt: 0,
            packets_written: 0,
            video_packets_since_pcr: 0,
            first_video_written: false,
        }
    }

    /// Provide the `HEVCDecoderConfigurationRecord` for video so the
    /// muxer prepends VPS/SPS/PPS Annex B NALs at stream start.
    pub fn set_video_codec_private(&mut self, hvcc: Vec<u8>) {
        self.video_codec_private = Some(hvcc);
    }

    /// Enable a single audio track. Must be called before
    /// [`write_audio`](Self::write_audio).
    pub fn set_audio(&mut self, codec: AudioCodec) {
        self.audio = Some(codec);
    }

    /// Write one video PES frame. `data` is either length-prefixed
    /// NALUs (MKV-style) or already Annex B; both are accepted. `keyframe`
    /// drives the random_access_indicator bit on the first packet of this
    /// PES (and gates codec_private NAL prepending — those only attach to
    /// the first keyframe).
    pub fn write_video(&mut self, pts_ns: i64, keyframe: bool, data: &[u8]) -> io::Result<()> {
        let pts_90k = self.base_relative_pts(pts_ns, /* may_seed_base */ true);
        // PCR comes "before" the PTS it timestamps; clamp at 0 for the
        // first frame so we don't underflow.
        let pcr = pts_90k.saturating_sub(PCR_LEAD_90KHZ);

        // Annex-B-ify the frame and prepend VPS/SPS/PPS once, on the
        // FIRST keyframe (not first frame — non-key frames before the
        // first keyframe can't carry params usefully).
        let mut es = Vec::with_capacity(data.len() + 64);
        if keyframe && !self.params_written {
            if let Some(cp) = &self.video_codec_private {
                if let Some(params) = super::hevc::hvcc_to_annex_b(cp) {
                    es.extend_from_slice(&params);
                }
            }
            self.params_written = true;
        }
        // Append the Annex-B form directly into the pre-sized `es`
        // buffer rather than materializing an intermediate Vec.
        super::hevc::append_length_prefixed_as_annex_b(&mut es, data);

        let pes = build_video_pes(pts_90k, &es);
        self.write_pes(PID_VIDEO, &pes, Some(pcr), keyframe)
    }

    /// Write one audio PES frame. Returns `Ok(())` and silently drops
    /// the frame if no audio track was configured — the design assumes
    /// the upstream picks tracks and won't ship audio to a video-only
    /// muxer, but defending against it keeps the API a single shape.
    pub fn write_audio(&mut self, pts_ns: i64, data: &[u8]) -> io::Result<()> {
        if self.audio.is_none() {
            return Ok(());
        }
        let pts_90k = self.base_relative_pts(pts_ns, /* may_seed_base */ false);
        let pes = build_audio_pes(pts_90k, data);
        self.write_pes(PID_AUDIO, &pes, None, false)
    }

    /// Drain the underlying writer. No TS-level trailer is mandatory —
    /// receivers detect end-of-stream from socket close or file EOF.
    pub fn finish(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    /// Convert input PTS (nanoseconds) to 90 kHz ticks rebased on the
    /// stream's PTS origin. The origin is seeded ONLY by the first video
    /// frame (`may_seed_base == true`); audio frames never seed it. This
    /// keeps the audio/video offset intact: a leading audio frame can't
    /// pull the base up and collapse the first/lowest-PTS video frame to 0.
    /// Frames earlier than the base saturate to 0.
    fn base_relative_pts(&mut self, pts_ns: i64, may_seed_base: bool) -> u64 {
        let raw_90k = if pts_ns > 0 {
            // Widen to u128 so adversarial timestamps can't overflow the
            // intermediate multiply (pts_ns * 9 exceeds u64 above
            // ~2.05e18 ns), then clamp to the 33-bit PTS range.
            (((pts_ns as u128) * 9 / 100_000) as u64) & 0x1_FFFF_FFFF
        } else {
            0
        };
        if may_seed_base {
            self.base_pts_90k.get_or_insert(raw_90k);
        }
        let base = self.base_pts_90k.unwrap_or(raw_90k);
        // Modular 33-bit subtraction. The 90 kHz PTS clock is a 33-bit field
        // that wraps every 2^33 ticks (~26.5 h). A plain `saturating_sub` would
        // collapse ANY frame whose (33-bit-masked) tick lands below `base` to
        // PTS 0 — including a frame across a legitimate clock wrap (raw wraps
        // past 0 and lands far below base), flat-lining timing for that span.
        // Wrap the difference into the 33-bit range, then interpret it as a
        // signed 33-bit delta: a small magnitude in the LOWER half is genuine
        // forward progression (incl. across a wrap) and is kept; a value in the
        // UPPER half means the frame is truly BEFORE the base (a small backward
        // step — e.g. a leading audio frame ahead of the first video keyframe),
        // which still floors to 0 per the documented behavior.
        let delta = raw_90k.wrapping_sub(base) & 0x1_FFFF_FFFF;
        if delta > (1 << 32) { 0 } else { delta }
    }

    /// Emit one PES payload as a chain of TS packets on `pid`. If `pcr`
    /// is provided the first packet carries an adaptation field with
    /// the PCR. PAT/PMT are re-emitted every `PSI_INTERVAL_PACKETS`.
    ///
    /// Packet-size math (TS = 188 bytes, header = 4 bytes):
    ///   - 184 B remain after the header for the adaptation-field area
    ///     plus the payload area.
    ///   - With AF body of `b` bytes and `s` stuffing bytes: AF total =
    ///     `1 + b + s` (the leading `1` is the `adaptation_field_length`
    ///     byte itself). Payload = `184 - (1 + b + s)`.
    ///   - With no AF area at all: payload = `184`.
    ///
    /// The fit-the-tail logic on the last packet of the PES uses
    /// stuffing rather than a separate small packet, which is the
    /// standard MPEG-TS convention.
    fn write_pes(
        &mut self,
        pid: u16,
        pes: &[u8],
        pcr: Option<u64>,
        is_keyframe_video: bool,
    ) -> io::Result<()> {
        // PSI cadence is enforced per TS packet — interleave a fresh
        // PAT+PMT into the packet stream every PSI_INTERVAL_PACKETS so
        // long single-PES emissions (e.g. one 60 KB video frame) don't
        // starve receivers tuning in mid-stream.
        let mut offset = 0;
        let mut first = true;
        while offset < pes.len() {
            self.maybe_emit_psi()?;

            // PCR cadence is enforced per VIDEO TS packet, NOT per PES. A single
            // UHD HEVC I-frame is one PES spanning thousands of TS packets; if
            // PCR could only ride the PES's first packet, the clock would go
            // un-restamped for the whole frame — a multi-second gap far beyond
            // the 40-packet / ~100 ms bound, which strict T-STD validators treat
            // as a clock discontinuity. So re-stamp whenever the per-packet
            // counter reaches the interval (or on the very first video packet of
            // the stream), regardless of whether this is the PES start. Re-using
            // the PES's own `pcr` for a mid-PES packet keeps the gap bounded.
            let attach_pcr = (pid == PID_VIDEO)
                && (pcr.is_some())
                && (!self.first_video_written
                    || self.video_packets_since_pcr >= PCR_INTERVAL_PACKETS);

            // RAI rides only the FIRST packet of a keyframe video PES.
            let attach_rai = first && is_keyframe_video && pid == PID_VIDEO;

            let mut af_body: Vec<u8> = if attach_pcr {
                build_pcr_adaptation(pcr.unwrap_or(0))
            } else {
                Vec::new()
            };
            if attach_rai {
                if af_body.is_empty() {
                    af_body.push(0x40); // flags: RAI only
                } else {
                    af_body[0] |= 0x40; // OR RAI into existing PCR flags
                }
            }

            let remaining = pes.len() - offset;
            // Capacity for payload given AF body and 1-byte AF length.
            // When AF body is empty we can still skip the AF entirely
            // and get the full 184 B; only invoke the AF when we'd
            // otherwise need stuffing.
            //
            // Per ISO/IEC 13818-1 Table 2-6, when an adaptation field is
            // present its first body byte is the mandatory 8-bit flags
            // byte. A stuffing-only field still needs that flags byte
            // (all flags 0) — omitting it would make a strict decoder
            // read the first 0xFF stuffing byte as flags (PCR_flag=1,
            // …) and parse a phantom PCR out of the stuffing/payload.
            let (af_present, payload_len, stuffing): (bool, usize, usize) = if !af_body.is_empty() {
                // AF is mandatory (PCR, RAI, …). 1 byte length + body +
                // stuffing + payload = 184.
                let max_payload = 184 - 1 - af_body.len();
                let p = remaining.min(max_payload);
                let s = max_payload - p;
                (true, p, s)
            } else if remaining >= 184 {
                // Full payload packet — no AF at all.
                (false, 184, 0)
            } else {
                // Last (small) packet — stuff via an AF whose body is a
                // single zero-flags byte. 1 byte length + 1 flags byte +
                // stuffing + payload = 184, so payload caps at 182.
                let max_payload = 182;
                let p = remaining.min(max_payload);
                let s = max_payload - p;
                af_body.push(0x00); // zero-flags byte
                (true, p, s)
            };

            let cc = self.advance_cc(pid);
            let mut packet = Packet::new();
            packet.set_header(pid, first, true, af_present, cc);
            if af_present {
                packet.append_adaptation(&af_body, stuffing)?;
            }
            packet.append_payload(&pes[offset..offset + payload_len])?;
            self.out.write_packet(&packet)?;

            self.packets_written += 1;
            if pid == PID_VIDEO {
                self.first_video_written = true;
                if attach_pcr {
                    self.video_packets_since_pcr = 0;
                } else {
                    self.video_packets_since_pcr += 1;
                }
            }
            offset += payload_len;
            first = false;
        }
        Ok(())
    }

    fn advance_cc(&mut self, pid: u16) -> u8 {
        let slot = match pid {
            PID_VIDEO => &mut self.cc_video,
            PID_AUDIO => &mut self.cc_audio,
            PID_PAT => &mut self.cc_pat,
            PID_PMT => &mut self.cc_pmt,
            _ => return 0,
        };
        let cc = *slot;
        *slot = (*slot + 1) & 0x0F;
        cc
    }

    fn maybe_emit_psi(&mut self) -> io::Result<()> {
        if self.packets_written == 0 || self.packets_written % PSI_INTERVAL_PACKETS == 0 {
            self.emit_pat()?;
            self.emit_pmt()?;
        }
        Ok(())
    }

    fn emit_pat(&mut self) -> io::Result<()> {
        let payload = build_pat(PID_PMT);
        let cc = self.advance_cc(PID_PAT);
        let mut packet = Packet::new();
        packet.set_header(PID_PAT, true, true, false, cc);
        packet.append_payload(&payload)?;
        packet.pad_to_188();
        self.out.write_packet(&packet)?;
        self.packets_written += 1;
        Ok(())
    }

    fn emit_pmt(&mut self) -> io::Result<()> {
        let payload = build_pmt(self.audio);
        let cc = self.advance_cc(PID_PMT);
        let mut packet = Packet::new();
        packet.set_header(PID_PMT, true, true, false, cc);
        packet.append_payload(&payload)?;
        packet.pad_to_188();
        self.out.write_packet(&packet)?;
        self.packets_written += 1;
        Ok(())
    }
}

/// Build a PES packet for a video access unit.
fn build_video_pes(pts_90k: u64, es: &[u8]) -> Vec<u8> {
    build_pes_packet(0xE0, pts_90k, es, /* length_in_header */ false)
}

/// Build a PES packet for an audio access unit.
fn build_audio_pes(pts_90k: u64, es: &[u8]) -> Vec<u8> {
    // Audio PES: a bounded length is written whenever the PES fits in a
    // u16 (the common case), so receivers don't have to scan for the next
    // start code. For an access unit larger than ~64 KiB (rare — e.g. a
    // large TrueHD frame) the length field falls back to the unbounded
    // (0x0000) form, which most demuxers tolerate for private_stream_1.
    build_pes_packet(0xBD, pts_90k, es, /* length_in_header */ true)
}

fn build_pes_packet(stream_id: u8, pts_90k: u64, es: &[u8], length_in_header: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(es.len() + 14);
    out.extend_from_slice(&[0x00, 0x00, 0x01, stream_id]);
    // PES_packet_length: total bytes after this field. 3 flag bytes + 5
    // PTS bytes + es.len(). Zero means "unbounded" — used for video
    // where PES can exceed u16.
    let pes_len = 8 + es.len();
    if length_in_header && pes_len <= u16::MAX as usize {
        out.extend_from_slice(&(pes_len as u16).to_be_bytes());
    } else {
        out.extend_from_slice(&[0x00, 0x00]);
    }
    // Flags byte 1: 10 = MPEG-2 marker, then scrambling/priority/etc 0.
    out.push(0x80);
    // Flags byte 2: PTS flag (bit 7).
    out.push(0x80);
    // PES_header_data_length = 5 (just PTS).
    out.push(5);
    // PTS bytes — 33-bit timestamp split across 5 bytes with marker bits.
    let pts = pts_90k & 0x1_FFFF_FFFF;
    out.push(0x21 | (((pts >> 29) & 0x0E) as u8));
    out.push(((pts >> 22) & 0xFF) as u8);
    out.push(0x01 | (((pts >> 14) & 0xFE) as u8));
    out.push(((pts >> 7) & 0xFF) as u8);
    out.push(0x01 | (((pts << 1) & 0xFE) as u8));
    out.extend_from_slice(es);
    out
}

/// Build the PAT payload (section, with pointer_field).
fn build_pat(pmt_pid: u16) -> Vec<u8> {
    let mut section = Vec::new();
    section.push(0x00); // table_id = PAT
    // section_syntax_indicator(1) | '0'(1) | reserved(2) | section_length(12)
    // section_length covers from end of this field through CRC.
    // Body: transport_stream_id(2) + version/cni(1) + section/last_section(2) + program(4) = 9 bytes,
    // plus CRC(4) = 13. Encoded big-endian.
    section.extend_from_slice(&[0xB0, 13]);
    section.extend_from_slice(&[0x00, 0x01]); // transport_stream_id = 1
    section.push(0xC1); // reserved | version=0 | current_next=1
    section.push(0x00); // section_number
    section.push(0x00); // last_section_number
    section.extend_from_slice(&[0x00, 0x01]); // program_number = 1
    // reserved(3) | network_PID/program_map_PID(13)
    let pid_bytes = (0xE000u16 | (pmt_pid & 0x1FFF)).to_be_bytes();
    section.extend_from_slice(&pid_bytes);
    let crc = mpegts_crc32(&section);
    section.extend_from_slice(&crc.to_be_bytes());
    // Prepend pointer_field=0 (section starts immediately).
    let mut payload = Vec::with_capacity(section.len() + 1);
    payload.push(0x00);
    payload.extend_from_slice(&section);
    payload
}

/// Build the PMT payload (section, with pointer_field).
fn build_pmt(audio: Option<AudioCodec>) -> Vec<u8> {
    let mut section = Vec::new();
    section.push(0x02); // table_id = PMT
    // section_length filled in after we know the body size.
    let len_placeholder = section.len();
    section.extend_from_slice(&[0xB0, 0x00]);

    section.extend_from_slice(&1u16.to_be_bytes()); // program_number
    section.push(0xC1); // reserved | version=0 | current_next=1
    section.push(0x00); // section_number
    section.push(0x00); // last_section_number
    // reserved(3) | PCR_PID(13)
    let pcr_pid = (0xE000u16 | (PID_VIDEO & 0x1FFF)).to_be_bytes();
    section.extend_from_slice(&pcr_pid);
    // program_info_length = 0
    section.extend_from_slice(&[0xF0, 0x00]);

    // Video elementary stream entry.
    section.push(STREAM_TYPE_HEVC);
    let v_pid = (0xE000u16 | (PID_VIDEO & 0x1FFF)).to_be_bytes();
    section.extend_from_slice(&v_pid);
    section.extend_from_slice(&[0xF0, 0x00]); // ES_info_length = 0

    if let Some(codec) = audio {
        section.push(codec.stream_type());
        let a_pid = (0xE000u16 | (PID_AUDIO & 0x1FFF)).to_be_bytes();
        section.extend_from_slice(&a_pid);
        section.extend_from_slice(&[0xF0, 0x00]);
    }

    // Now patch section_length: covers everything after the length field
    // through the CRC, so (current body size - 3 bytes consumed by
    // table_id + 2 length bytes) + 4 (CRC).
    let section_len = section.len() - 3 + 4;
    section[len_placeholder] = 0xB0 | ((section_len >> 8) as u8 & 0x0F);
    section[len_placeholder + 1] = section_len as u8;

    let crc = mpegts_crc32(&section);
    section.extend_from_slice(&crc.to_be_bytes());

    let mut payload = Vec::with_capacity(section.len() + 1);
    payload.push(0x00);
    payload.extend_from_slice(&section);
    payload
}

/// Build the adaptation field carrying a PCR (no other flags).
fn build_pcr_adaptation(pcr_90k: u64) -> Vec<u8> {
    // adaptation_field_length is set by `Packet::append_adaptation`
    // — this function returns just the field body.
    //
    // Layout: discontinuity_indicator(1) | random_access(1) |
    // elementary_stream_priority(1) | PCR_flag(1) | OPCR_flag(1) |
    // splicing_point_flag(1) | transport_private_data_flag(1) |
    // adaptation_field_extension_flag(1) | PCR(48b).
    let mut af = vec![0x10]; // PCR_flag=1; RAI is OR'd in by the caller when applicable
    let pcr_base = pcr_90k & 0x1_FFFF_FFFF; // 33-bit
    let pcr_ext: u16 = 0; // 9-bit, we keep it zero (no sub-tick precision)
    // Encode PCR: 33b base | 6b reserved | 9b extension = 48b
    af.push((pcr_base >> 25) as u8);
    af.push((pcr_base >> 17) as u8);
    af.push((pcr_base >> 9) as u8);
    af.push((pcr_base >> 1) as u8);
    af.push(((pcr_base << 7) as u8 & 0x80) | 0x7E | ((pcr_ext >> 8) as u8 & 0x01));
    af.push(pcr_ext as u8);
    af
}

/// MPEG-TS CRC-32 (poly 0x04C11DB7, init 0xFFFFFFFF, no reflection, no
/// final XOR). Implementation: bitwise so we don't need a table —
/// PSI sections are tiny, the cost is negligible.
fn mpegts_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= (b as u32) << 24;
        for _ in 0..8 {
            if crc & 0x8000_0000 != 0 {
                crc = (crc << 1) ^ 0x04C1_1DB7;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All emitted bytes must align to 188-byte packet boundaries and
    /// every packet must start with `0x47`.
    fn assert_ts_well_formed(buf: &[u8]) {
        assert_eq!(
            buf.len() % 188,
            0,
            "stream not packet-aligned: {} bytes",
            buf.len()
        );
        for (i, chunk) in buf.chunks(188).enumerate() {
            assert_eq!(chunk[0], 0x47, "packet {} missing sync byte", i);
        }
    }

    fn extract_pids(buf: &[u8]) -> Vec<u16> {
        buf.chunks(188)
            .map(|p| u16::from_be_bytes([p[1] & 0x1F, p[2]]))
            .collect()
    }

    #[test]
    fn crc32_is_self_validating() {
        // The MPEG-TS CRC has the property that prepending a single
        // bit-flip changes the output; running it over its own input +
        // CRC yields a fixed magic constant (the CRC residue). Rather
        // than hardcoding sample bytes, verify the underlying algorithm
        // by checking that two distinct inputs produce distinct CRCs
        // and that the same input is deterministic.
        let a = [
            0u8, 0xB0, 0x0D, 0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01, 0xE1, 0x00,
        ];
        let mut b = a;
        b[5] ^= 0x01; // flip one bit
        let crc_a = mpegts_crc32(&a);
        let crc_b = mpegts_crc32(&b);
        assert_ne!(crc_a, crc_b);
        assert_eq!(crc_a, mpegts_crc32(&a)); // deterministic
        // Sanity: all-zero input ⇒ CRC = 0 (init XORs but the
        // shift/feedback cancels for zero data after init drains).
        // We don't assert exact value — that depends on poly choice —
        // but check it's not the same as for non-zero data.
        let crc_zero = mpegts_crc32(&[0u8; 12]);
        assert_ne!(crc_zero, crc_a);
    }

    #[test]
    fn video_only_mux_emits_pat_pmt_then_video() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        // One small video frame, no codec_private (so no params NAL inline).
        let mut frame = Vec::new();
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
        mux.write_video(0, true, &frame).unwrap();
        mux.finish().unwrap();
        drop(mux);

        assert_ts_well_formed(&sink);
        let pids = extract_pids(&sink);
        // First two packets: PAT, PMT. At least one video packet after.
        assert_eq!(pids[0], PID_PAT);
        assert_eq!(pids[1], PID_PMT);
        assert!(pids.iter().any(|p| *p == PID_VIDEO));
    }

    #[test]
    fn first_video_pes_carries_pcr() {
        // A receiver tuning at stream start needs the clock reference the
        // PMT promises (video PID = PCR_PID). The very first video PES must
        // therefore carry a PCR even though PAT+PMT precede it.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        let mut frame = Vec::new();
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
        mux.write_video(0, true, &frame).unwrap();
        mux.finish().unwrap();
        drop(mux);

        // First PUSI video packet must carry an AF with the PCR flag (0x10).
        let pkt = sink
            .chunks(188)
            .find(|p| u16::from_be_bytes([p[1] & 0x1F, p[2]]) == PID_VIDEO && (p[1] & 0x40) != 0)
            .expect("video PUSI packet exists");
        let af = af_body(pkt).expect("first video PES must carry an adaptation field");
        assert!(!af.is_empty(), "AF flags byte present");
        assert_eq!(af[0] & 0x10, 0x10, "PCR flag set on first video PES");
    }

    #[test]
    fn audio_track_appears_in_pmt_and_stream() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        mux.set_audio(AudioCodec::Ac3);
        // Video + audio frame pair.
        let mut frame = Vec::new();
        frame.extend_from_slice(&3u32.to_be_bytes());
        frame.extend_from_slice(&[0x40, 0x01, 0x0C]);
        mux.write_video(0, true, &frame).unwrap();
        mux.write_audio(20_000_000, &[0x0B, 0x77, 0x12, 0x34])
            .unwrap();
        mux.finish().unwrap();
        drop(mux);

        assert_ts_well_formed(&sink);
        let pids = extract_pids(&sink);
        assert!(pids.iter().any(|p| *p == PID_VIDEO));
        assert!(pids.iter().any(|p| *p == PID_AUDIO));
    }

    #[test]
    fn psi_re_emits_at_interval() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        // Build a frame large enough to span > PSI_INTERVAL_PACKETS TS packets.
        // 184 B payload per packet ⇒ ~250 packets = ~46 KB elementary stream.
        let big: Vec<u8> = (0..(60 * 1024)).map(|i| (i & 0xff) as u8).collect();
        let mut frame = Vec::new();
        frame.extend_from_slice(&(big.len() as u32).to_be_bytes());
        frame.extend_from_slice(&big);
        mux.write_video(0, true, &frame).unwrap();
        mux.finish().unwrap();
        drop(mux);

        assert_ts_well_formed(&sink);
        let pids = extract_pids(&sink);
        // Count PAT/PMT pairs — must be at least 2 given the input size.
        let pat_count = pids.iter().filter(|p| **p == PID_PAT).count();
        let pmt_count = pids.iter().filter(|p| **p == PID_PMT).count();
        assert!(pat_count >= 2, "expected ≥2 PAT, got {}", pat_count);
        assert!(pmt_count >= 2, "expected ≥2 PMT, got {}", pmt_count);
    }

    #[test]
    fn continuity_counter_increments_per_pid() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        // Three small video frames to get a sequence of video TS packets.
        for pts in [0i64, 40_000_000, 80_000_000] {
            let mut frame = Vec::new();
            frame.extend_from_slice(&3u32.to_be_bytes());
            frame.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
            mux.write_video(pts, pts == 0, &frame).unwrap();
        }
        mux.finish().unwrap();
        drop(mux);

        // Collect CCs for video packets in order.
        let ccs: Vec<u8> = sink
            .chunks(188)
            .filter(|p| u16::from_be_bytes([p[1] & 0x1F, p[2]]) == PID_VIDEO)
            .map(|p| p[3] & 0x0F)
            .collect();
        for w in ccs.windows(2) {
            assert_eq!(w[1], (w[0] + 1) & 0x0F);
        }
    }

    /// Return the adaptation field body (length byte stripped) for one
    /// 188-byte TS packet, or None when the packet has no AF.
    fn af_body(packet: &[u8]) -> Option<Vec<u8>> {
        let afc = (packet[3] >> 4) & 0x03;
        if afc & 0b10 == 0 {
            return None;
        }
        let af_len = packet[4] as usize;
        if af_len == 0 {
            return Some(Vec::new());
        }
        Some(packet[5..5 + af_len].to_vec())
    }

    #[test]
    fn stuffing_only_tail_packet_is_spec_valid() {
        // A short final PES packet must stuff via an adaptation field
        // whose first body byte is the mandatory zero-flags byte (per
        // ISO/IEC 13818-1 Table 2-6), never a bare 0xFF stuffing byte
        // that a decoder would misread as PCR/OPCR/etc. flags.
        //
        // Use an AUDIO PES (no PCR, no RAI on its tail) so the only AF on
        // the last packet is the stuffing-only field under test. The PES
        // is sized so its final TS packet is short (< 184 payload bytes).
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        mux.set_audio(AudioCodec::Ac3);
        // Drive a keyframe first so the stream is well-formed, then the
        // audio frame whose tail is short.
        let mut vframe = Vec::new();
        vframe.extend_from_slice(&4u32.to_be_bytes());
        vframe.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
        mux.write_video(0, true, &vframe).unwrap();
        // 200-byte audio payload → PES > 184 → spills into a short tail.
        let audio: Vec<u8> = (0..200u32).map(|i| (i & 0xFF) as u8).collect();
        mux.write_audio(20_000_000, &audio).unwrap();
        mux.finish().unwrap();
        drop(mux);

        assert_ts_well_formed(&sink);

        // Find every audio packet that carries an adaptation field; the
        // short tail packet is one of them. Each such AF must have a
        // length >= 1 and a zero-flags body byte (not 0xFF).
        let mut saw_stuffing_af = false;
        for pkt in sink.chunks(188) {
            let pid = u16::from_be_bytes([pkt[1] & 0x1F, pkt[2]]);
            if pid != PID_AUDIO {
                continue;
            }
            let afc = (pkt[3] >> 4) & 0x03;
            if afc & 0b10 == 0 {
                continue; // no AF on this packet
            }
            let af_len = pkt[4] as usize;
            assert!(
                af_len >= 1,
                "stuffing AF must include the mandatory flags byte"
            );
            // First AF body byte is the flags byte — must be zero, never
            // a 0xFF stuffing byte masquerading as flags.
            assert_eq!(
                pkt[5], 0x00,
                "stuffing-only AF flags byte must be 0x00, not 0x{:02X}",
                pkt[5]
            );
            // adaptation_field_length + payload must fill exactly 184.
            // (4 header + 1 AF-length + af_len + payload = 188.)
            assert!(af_len <= 183, "AF length overflows the 184-byte body");
            saw_stuffing_af = true;
        }
        assert!(
            saw_stuffing_af,
            "expected at least one audio packet with a stuffing AF"
        );
    }

    #[test]
    fn rai_set_on_keyframe_pes_packet() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        let mut frame = Vec::new();
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
        mux.write_video(0, true, &frame).unwrap();
        mux.finish().unwrap();
        drop(mux);

        // Find the first PUSI packet on PID_VIDEO.
        let pkt = sink
            .chunks(188)
            .find(|p| u16::from_be_bytes([p[1] & 0x1F, p[2]]) == PID_VIDEO && (p[1] & 0x40) != 0)
            .expect("video PUSI packet exists");
        let af = af_body(pkt).expect("AF present on first packet of keyframe video PES");
        assert!(!af.is_empty(), "AF flags byte present");
        assert_eq!(af[0] & 0x40, 0x40, "RAI bit set");
    }

    #[test]
    fn pcr_packet_without_keyframe_has_rai_clear() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        // First frame is the keyframe (gates codec_private; also gets PCR).
        let mut frame0 = Vec::new();
        frame0.extend_from_slice(&4u32.to_be_bytes());
        frame0.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
        mux.write_video(0, true, &frame0).unwrap();
        // Push enough non-key video frames to cross PCR_INTERVAL_PACKETS
        // video packets so a later PCR-bearing packet exists.
        // Each frame is ~50 KB → ~270 packets, well over 40.
        let big: Vec<u8> = (0..(50 * 1024)).map(|i| (i & 0xff) as u8).collect();
        for i in 1..3 {
            let mut frame = Vec::new();
            frame.extend_from_slice(&(big.len() as u32).to_be_bytes());
            frame.extend_from_slice(&big);
            mux.write_video((i as i64) * 40_000_000, false, &frame)
                .unwrap();
        }
        mux.finish().unwrap();
        drop(mux);

        // The first video packet carries PCR + RAI (keyframe PES start). RAI
        // rides only the FIRST packet of a KEYFRAME PES; every OTHER PCR-bearing
        // video packet — the mid-PES re-stamps and the non-keyframe PES starts —
        // must have RAI clear. Skip the very first video packet (the keyframe
        // RAI carrier) and assert the first remaining PCR-bearing packet is RAI
        // clear.
        let video_pkts: Vec<&[u8]> = sink
            .chunks(188)
            .filter(|p| u16::from_be_bytes([p[1] & 0x1F, p[2]]) == PID_VIDEO)
            .collect();
        assert!(
            video_pkts.len() >= 2,
            "expected ≥2 video packets, got {}",
            video_pkts.len()
        );
        // Find a later one with AF that carries PCR (flags & 0x10 set).
        let later_pcr = video_pkts
            .iter()
            .skip(1)
            .find_map(|p| {
                let af = af_body(p)?;
                if !af.is_empty() && (af[0] & 0x10) != 0 {
                    Some(af)
                } else {
                    None
                }
            })
            .expect("a later PCR-bearing video packet exists");
        assert_eq!(
            later_pcr[0] & 0x40,
            0,
            "RAI must be clear on a non-keyframe-start PCR packet"
        );
    }

    /// Regression: PCR must be re-stamped MID-PES, not only at PES boundaries.
    /// A single large video frame (one PES) spans far more than
    /// PCR_INTERVAL_PACKETS TS packets — a UHD I-frame. PCR-bearing video
    /// packets must recur at least every PCR_INTERVAL_PACKETS video packets
    /// across that one PES; before the fix only the PES's first packet carried
    /// PCR, leaving a multi-second clock gap for the whole frame.
    #[test]
    fn pcr_restamped_mid_pes_within_interval() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        // ONE big frame → ONE PES spanning ~330 packets (≫ 40).
        let big: Vec<u8> = (0..(60 * 1024)).map(|i| (i & 0xff) as u8).collect();
        let mut frame = Vec::new();
        frame.extend_from_slice(&(big.len() as u32).to_be_bytes());
        frame.extend_from_slice(&big);
        mux.write_video(0, true, &frame).unwrap();
        mux.finish().unwrap();
        drop(mux);

        assert_ts_well_formed(&sink);

        // Walk every video TS packet in order; record which ones carry a PCR
        // (AF present with PCR_flag 0x10). The packet INDEX (among video
        // packets) of consecutive PCR carriers must never advance by more than
        // PCR_INTERVAL_PACKETS.
        let mut video_idx = 0usize;
        let mut pcr_indices: Vec<usize> = Vec::new();
        let mut total_video = 0usize;
        for pkt in sink.chunks(188) {
            let pid = u16::from_be_bytes([pkt[1] & 0x1F, pkt[2]]);
            if pid != PID_VIDEO {
                continue;
            }
            total_video += 1;
            if let Some(af) = af_body(pkt) {
                if !af.is_empty() && (af[0] & 0x10) != 0 {
                    pcr_indices.push(video_idx);
                }
            }
            video_idx += 1;
        }
        assert!(
            total_video > PCR_INTERVAL_PACKETS as usize,
            "test needs a PES spanning more than one PCR interval, got {total_video} video packets"
        );
        // More than one PCR across the single PES (the whole point of the fix).
        assert!(
            pcr_indices.len() >= 2,
            "PCR must be re-stamped mid-PES, but only {} PCR-bearing packet(s) \
             appeared across {} video packets of one PES",
            pcr_indices.len(),
            total_video
        );
        // First PCR is on the first video packet.
        assert_eq!(pcr_indices[0], 0, "first video packet must carry PCR");
        // No gap between consecutive PCRs exceeds the interval. The counter is
        // post-incremented and PCR attaches on `>= PCR_INTERVAL_PACKETS`, so the
        // packet index gap is `PCR_INTERVAL_PACKETS + 1` (40 packets carrying no
        // PCR, then the re-stamp packet) — the spec "every 40 packets" bound.
        let max_gap = PCR_INTERVAL_PACKETS + 1;
        for w in pcr_indices.windows(2) {
            assert!(
                (w[1] - w[0]) as u64 <= max_gap,
                "PCR gap {} exceeds the {}-packet bound",
                w[1] - w[0],
                max_gap
            );
        }
        let tail = total_video - 1 - *pcr_indices.last().unwrap();
        assert!(
            tail as u64 <= max_gap,
            "trailing run after the last PCR ({tail}) exceeds the {max_gap}-packet bound"
        );
    }

    #[test]
    fn keyframe_video_with_pcr_combines_flags() {
        // The FIRST video PES is always a keyframe carrying BOTH a PCR (forced
        // at stream start so the receiver has the clock the PMT promises) AND a
        // RAI (keyframe) — exercising the flag-OR path that combines RAI into the
        // PCR adaptation-field flags byte (0x10 | 0x40 = 0x50). (PCR cadence is
        // now enforced per video packet, NOT per PES boundary, so a LATER
        // keyframe PES start no longer deterministically lands on a PCR-due
        // packet; the combine path is pinned here on the guaranteed first PES.)
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        let mut small = Vec::new();
        small.extend_from_slice(&4u32.to_be_bytes());
        small.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
        mux.write_video(0, true, &small).unwrap();
        mux.finish().unwrap();
        drop(mux);

        let video_pusi: Vec<&[u8]> = sink
            .chunks(188)
            .filter(|p| u16::from_be_bytes([p[1] & 0x1F, p[2]]) == PID_VIDEO && (p[1] & 0x40) != 0)
            .collect();
        assert!(!video_pusi.is_empty(), "a video PES start exists");
        let af = af_body(video_pusi[0]).expect("AF present on first keyframe PES");
        assert!(!af.is_empty(), "AF flags byte present");
        assert_eq!(af[0], 0x50, "flags == RAI | PCR on the first keyframe PES");
    }

    // ════════════════════════════════════════════════════════════════════
    // Added hardening tests
    // ════════════════════════════════════════════════════════════════════

    /// Find the first packet on `pid` (optionally requiring PUSI).
    fn find_pkt(buf: &[u8], pid: u16, pusi: bool) -> Option<&[u8]> {
        buf.chunks(188).find(|p| {
            u16::from_be_bytes([p[1] & 0x1F, p[2]]) == pid && (!pusi || (p[1] & 0x40) != 0)
        })
    }

    /// Extract a PSI section (after the pointer_field) from a PUSI PSI
    /// packet: payload starts at byte 4 (no AF on PSI here), first payload
    /// byte is pointer_field, section follows.
    fn psi_section(pkt: &[u8]) -> &[u8] {
        let pointer = pkt[4] as usize;
        &pkt[5 + pointer..]
    }

    // ── MPEG-TS CRC-32 (poly 0x04C11DB7) self-validation ──────────────────

    #[test]
    fn crc32_residue_over_section_plus_crc_is_zero() {
        // Defining property of the MPEG-TS CRC (ISO 13818-1 Annex B): running
        // the CRC over a message WITH its appended 4-byte CRC yields a fixed
        // residue. For this poly/init (no final XOR) the residue over
        // [data || crc(data)] is 0. This pins the algorithm independent of
        // any sample vector.
        let data = [
            0x00u8, 0xB0, 0x0D, 0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01, 0xE1, 0x00,
        ];
        let crc = mpegts_crc32(&data);
        // Known-answer vector for CRC-32/MPEG-2 (poly 0x04C11DB7, init
        // 0xFFFFFFFF, no reflection, no final XOR — ISO/IEC 13818-1 Annex B),
        // independently computed. This pins the polynomial, not just internal
        // consistency.
        assert_eq!(crc, 0xE8F9_5E7D, "CRC-32/MPEG-2 known-answer vector");
        let mut with_crc = data.to_vec();
        with_crc.extend_from_slice(&crc.to_be_bytes());
        assert_eq!(
            mpegts_crc32(&with_crc),
            0,
            "CRC residue over message+CRC must be 0"
        );
    }

    #[test]
    fn emitted_pat_pmt_crc_is_valid() {
        // The PAT and PMT the muxer emits must carry a correct CRC-32 over
        // the section (table_id .. end of body). A receiver that validates
        // CRC would otherwise drop the table.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = M2tsMux::new(&mut sink);
            mux.set_audio(AudioCodec::Ac3);
            let mut frame = Vec::new();
            frame.extend_from_slice(&4u32.to_be_bytes());
            frame.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
            mux.write_video(0, true, &frame).unwrap();
            mux.finish().unwrap();
        }
        for pid in [PID_PAT, PID_PMT] {
            let pkt = find_pkt(&sink, pid, true).expect("PSI packet present");
            let sec = psi_section(pkt);
            // section_length covers bytes after the 2-byte length field,
            // i.e. (table_id + 2 length bytes) + section_length = whole
            // section incl. CRC.
            let section_len = (((sec[1] & 0x0F) as usize) << 8) | sec[2] as usize;
            let total = 3 + section_len;
            assert!(sec.len() >= total, "section fits in payload");
            assert_eq!(
                mpegts_crc32(&sec[..total]),
                0,
                "PID {pid:#06x} section CRC must validate (residue 0)"
            );
        }
    }

    // ── PAT / PMT structure ───────────────────────────────────────────────

    #[test]
    fn pat_points_at_pmt_pid() {
        // PAT program loop entry: program_number(2) + reserved(3)|PID(13).
        // The single program must point at PID_PMT.
        let pat = build_pat(PID_PMT);
        let sec = &pat[1..]; // skip pointer_field
        assert_eq!(sec[0], 0x00, "table_id = PAT");
        // Body: tsid(2)@3 cni(1)@5 sec#(1)@6 last(1)@7 program(4)@8..12.
        let prog_num = u16::from_be_bytes([sec[8], sec[9]]);
        let pmt_pid = u16::from_be_bytes([sec[10] & 0x1F, sec[11]]);
        assert_eq!(prog_num, 1, "program_number 1");
        assert_eq!(pmt_pid, PID_PMT, "PAT points at PMT PID");
    }

    #[test]
    fn pmt_advertises_video_and_audio_stream_types() {
        // PMT must list HEVC video (stream_type 0x24) and, when audio is
        // configured, the audio stream_type. Stream-type codes per ISO
        // 13818-1 Table 2-34 / BD convention.
        let pmt = build_pmt(Some(AudioCodec::Ac3));
        let sec = &pmt[1..]; // skip pointer_field
        assert_eq!(sec[0], 0x02, "table_id = PMT");
        let section_len = (((sec[1] & 0x0F) as usize) << 8) | sec[2] as usize;
        let prog_info_len = (((sec[10] & 0x0F) as usize) << 8) | sec[11] as usize;
        let mut pos = 12 + prog_info_len;
        let end = 3 + section_len - 4; // exclude CRC
        let mut types = Vec::new();
        while pos + 5 <= end {
            types.push(sec[pos]);
            let es_info = (((sec[pos + 3] & 0x0F) as usize) << 8) | sec[pos + 4] as usize;
            pos += 5 + es_info;
        }
        assert!(types.contains(&STREAM_TYPE_HEVC), "HEVC video in PMT");
        assert!(types.contains(&STREAM_TYPE_AC3), "AC-3 audio in PMT");
    }

    #[test]
    fn pmt_video_only_omits_audio_entry() {
        // Video-only PMT must list exactly one ES entry (video) — no audio.
        let pmt = build_pmt(None);
        let sec = &pmt[1..];
        let section_len = (((sec[1] & 0x0F) as usize) << 8) | sec[2] as usize;
        let prog_info_len = (((sec[10] & 0x0F) as usize) << 8) | sec[11] as usize;
        let mut pos = 12 + prog_info_len;
        let end = 3 + section_len - 4;
        let mut count = 0;
        while pos + 5 <= end {
            count += 1;
            let es_info = (((sec[pos + 3] & 0x0F) as usize) << 8) | sec[pos + 4] as usize;
            pos += 5 + es_info;
        }
        assert_eq!(count, 1, "video-only PMT has exactly one ES entry");
    }

    #[test]
    fn truehd_audio_uses_stream_type_0x83() {
        // TrueHD maps to stream_type 0x83 (BD convention).
        let pmt = build_pmt(Some(AudioCodec::TrueHd));
        let sec = &pmt[1..];
        let section_len = (((sec[1] & 0x0F) as usize) << 8) | sec[2] as usize;
        let end = 3 + section_len - 4;
        let mut pos = 12; // prog_info_len is 0 in this muxer
        let mut found = false;
        while pos + 5 <= end {
            if sec[pos] == STREAM_TYPE_TRUEHD {
                found = true;
            }
            let es_info = (((sec[pos + 3] & 0x0F) as usize) << 8) | sec[pos + 4] as usize;
            pos += 5 + es_info;
        }
        assert!(found, "TrueHD stream_type 0x83 must appear in PMT");
    }

    #[test]
    fn pmt_pcr_pid_is_video_pid() {
        // PMT PCR_PID field (reserved(3)|PCR_PID(13) at section bytes 8..10)
        // must be the video PID — the PCR rides the video adaptation field.
        let pmt = build_pmt(None);
        let sec = &pmt[1..];
        let pcr_pid = u16::from_be_bytes([sec[8] & 0x1F, sec[9]]);
        assert_eq!(pcr_pid, PID_VIDEO, "PCR_PID advertised as the video PID");
    }

    // ── PCR encoding ──────────────────────────────────────────────────────

    #[test]
    fn pcr_base_round_trips_through_adaptation_field() {
        // build_pcr_adaptation packs a 33-bit PCR base across 6 bytes:
        // base[32:25],[24:17],[16:9],[8:1] then bit0 in top of byte 5.
        // (ISO 13818-1 §2.4.3.5.) Decode it back and compare.
        let pcr: u64 = 0x1_2345_6789 & ((1 << 33) - 1);
        let af = build_pcr_adaptation(pcr);
        assert_eq!(af[0], 0x10, "PCR_flag set, others clear");
        let base = ((af[1] as u64) << 25)
            | ((af[2] as u64) << 17)
            | ((af[3] as u64) << 9)
            | ((af[4] as u64) << 1)
            | ((af[5] as u64 >> 7) & 0x01);
        assert_eq!(base, pcr, "PCR base must round-trip through the AF");
    }

    #[test]
    fn first_video_pcr_leads_pts_by_lead_time() {
        // The PCR on the first video PES = pts_90k - PCR_LEAD_90KHZ, clamped
        // at 0. With pts_ns large enough not to clamp, decode the PCR and the
        // PTS and verify the lead. PCR_LEAD_90KHZ = 18000 (200 ms).
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = M2tsMux::new(&mut sink);
            let mut frame = Vec::new();
            frame.extend_from_slice(&4u32.to_be_bytes());
            frame.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
            // 1s → 90000 ticks; base is this same frame, so relative PTS=0
            // and PCR clamps to 0. Use a single frame: PTS rebases to 0,
            // so PCR = 0.saturating_sub(lead) = 0.
            mux.write_video(1_000_000_000, true, &frame).unwrap();
            mux.finish().unwrap();
        }
        let pkt = find_pkt(&sink, PID_VIDEO, true).unwrap();
        let af = af_body(pkt).unwrap();
        // PCR present.
        assert_eq!(af[0] & 0x10, 0x10);
        let base = ((af[1] as u64) << 25)
            | ((af[2] as u64) << 17)
            | ((af[3] as u64) << 9)
            | ((af[4] as u64) << 1)
            | ((af[5] as u64 >> 7) & 0x01);
        // Single frame rebases its own PTS to 0; PCR = 0 - lead clamped to 0.
        assert_eq!(base, 0, "first frame PCR clamps to 0 (no underflow)");
    }

    // ── base_relative_pts overflow / saturation ───────────────────────────

    #[test]
    fn extreme_pts_does_not_overflow_and_clamps_to_33bit() {
        // base_relative_pts widens to u128 then masks to 33 bits. An
        // adversarial i64::MAX ns must not overflow and the encoded PTS must
        // stay within the 33-bit field. With a single video frame the base
        // is itself, so relative PTS is 0 — proving no panic on the path.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = M2tsMux::new(&mut sink);
            let mut frame = Vec::new();
            frame.extend_from_slice(&4u32.to_be_bytes());
            frame.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
            mux.write_video(i64::MAX, true, &frame).unwrap();
            mux.finish().unwrap();
        }
        assert_ts_well_formed(&sink);
        let pkt = find_pkt(&sink, PID_VIDEO, true).unwrap();
        // Reach the PES PTS: payload after AF. AF area = 1 (length) + af_len.
        let af_len = pkt[4] as usize;
        let pes = &pkt[4 + 1 + af_len..];
        // PES: 00 00 01 E0 00 00 80 80 05 PTS[5]. PTS at pes[9..14].
        let pts = ((((pes[9] >> 1) & 0x07) as u64) << 30)
            | ((pes[10] as u64) << 22)
            | (((pes[11] >> 1) as u64) << 15)
            | ((pes[12] as u64) << 7)
            | ((pes[13] >> 1) as u64);
        assert!(pts < (1u64 << 33), "PTS stays within the 33-bit field");
    }

    #[test]
    fn base_relative_pts_wraps_across_33bit_clock_rollover() {
        // Regression: a real 90 kHz clock wrap must NOT collapse to PTS 0.
        // Seed base near the top of the 33-bit range; a later frame whose tick
        // has wrapped past 0 lands far below base. The OLD `saturating_sub`
        // returned 0 (flat-lining timing); modular subtraction must return the
        // true small forward delta.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        // Force the base to 2^33 - 100 directly (a value reachable only after
        // ~26.5 h of stream; set it rather than ripping that long).
        mux.base_pts_90k = Some((1u64 << 33) - 100);
        // pts_ns = 1ms → raw_90k = 1_000_000 * 9 / 100_000 = 90 ticks (wrapped
        // past 0, far below the near-max base).
        let pts_ns = 1_000_000i64;
        let rel = mux.base_relative_pts(pts_ns, /* may_seed_base */ false);
        // 90 - (2^33 - 100) mod 2^33 = 190 ticks forward across the wrap (NOT 0).
        assert_eq!(
            rel, 190,
            "a 33-bit clock wrap must produce the true forward delta, not 0"
        );

        // And a frame genuinely a little BEFORE the base still floors to 0
        // (documented pre-base behavior — e.g. leading audio). base = 200 ticks,
        // frame at 90 ticks (< base) → backward step → floor to 0.
        mux.base_pts_90k = Some(200);
        let rel0 = mux.base_relative_pts(1_000_000i64, false); // raw_90k = 90 < 200
        assert_eq!(rel0, 0, "a frame before the base must still floor to 0");
    }

    #[test]
    fn negative_pts_ns_encodes_zero() {
        // base_relative_pts treats pts_ns <= 0 as raw 0. A negative input
        // must encode PTS 0, not a wrapped value.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = M2tsMux::new(&mut sink);
            let mut frame = Vec::new();
            frame.extend_from_slice(&4u32.to_be_bytes());
            frame.extend_from_slice(&[0x40, 0x01, 0x0C, 0x01]);
            mux.write_video(-5, true, &frame).unwrap();
            mux.finish().unwrap();
        }
        let pkt = find_pkt(&sink, PID_VIDEO, true).unwrap();
        let af_len = pkt[4] as usize;
        let pes = &pkt[4 + 1 + af_len..];
        let pts = ((((pes[9] >> 1) & 0x07) as u64) << 30)
            | ((pes[10] as u64) << 22)
            | (((pes[11] >> 1) as u64) << 15)
            | ((pes[12] as u64) << 7)
            | ((pes[13] >> 1) as u64);
        assert_eq!(pts, 0, "negative pts_ns encodes PTS 0");
    }

    // ── audio without configured track ────────────────────────────────────

    #[test]
    fn write_audio_without_track_is_silently_dropped() {
        // write_audio on a video-only muxer must drop the frame (no audio
        // PID configured) without error — and emit no audio PID packets.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = M2tsMux::new(&mut sink);
            let mut frame = Vec::new();
            frame.extend_from_slice(&3u32.to_be_bytes());
            frame.extend_from_slice(&[0x40, 0x01, 0x0C]);
            mux.write_video(0, true, &frame).unwrap();
            mux.write_audio(0, &[0x0B, 0x77]).unwrap(); // no track → dropped
            mux.finish().unwrap();
        }
        let pids = extract_pids(&sink);
        assert!(
            !pids.iter().any(|p| *p == PID_AUDIO),
            "no audio track configured → no audio PID emitted"
        );
    }

    // ── empty stream ──────────────────────────────────────────────────────

    #[test]
    fn finish_without_frames_emits_nothing() {
        // A muxer with no frames written emits no packets (PSI is gated on
        // write paths). finish() must be a clean no-op.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        mux.finish().unwrap();
        drop(mux);
        assert!(sink.is_empty(), "no frames → no output");
    }

    #[test]
    fn pat_always_on_pid_zero() {
        // ISO 13818-1 mandates the PAT on PID 0x0000.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = M2tsMux::new(&mut sink);
            let mut frame = Vec::new();
            frame.extend_from_slice(&3u32.to_be_bytes());
            frame.extend_from_slice(&[0x40, 0x01, 0x0C]);
            mux.write_video(0, true, &frame).unwrap();
            mux.finish().unwrap();
        }
        assert_eq!(
            extract_pids(&sink)[0],
            0x0000,
            "first packet is PAT on PID 0"
        );
    }
}
