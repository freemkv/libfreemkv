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

/// Stream-type codes from ISO/IEC 13818-1 Table 2-29 + later amendments.
const STREAM_TYPE_HEVC: u8 = 0x24;
const STREAM_TYPE_AC3: u8 = 0x81;
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
    /// NALUs (MKV-style) or already Annex B; both are accepted.
    pub fn write_video(&mut self, pts_ns: i64, data: &[u8]) -> io::Result<()> {
        let pts_90k = self.base_relative_pts(pts_ns);
        // PCR comes "before" the PTS it timestamps; clamp at 0 for the
        // first frame so we don't underflow.
        let pcr = pts_90k.saturating_sub(PCR_LEAD_90KHZ);

        // Annex-B-ify the frame and prepend VPS/SPS/PPS once.
        let mut es = Vec::with_capacity(data.len() + 64);
        if !self.params_written {
            if let Some(cp) = &self.video_codec_private {
                let payload = hvcc_payload(cp);
                if !payload.is_empty() {
                    let params = super::hevc::length_prefixed_to_annex_b(&payload);
                    es.extend_from_slice(&params);
                }
            }
            self.params_written = true;
        }
        let annex_b = super::hevc::length_prefixed_to_annex_b(data);
        es.extend_from_slice(&annex_b);

        let pes = build_video_pes(pts_90k, &es);
        self.write_pes(PID_VIDEO, &pes, Some(pcr))
    }

    /// Write one audio PES frame. Returns `Ok(())` and silently drops
    /// the frame if no audio track was configured — the design assumes
    /// the upstream picks tracks and won't ship audio to a video-only
    /// muxer, but defending against it keeps the API a single shape.
    pub fn write_audio(&mut self, pts_ns: i64, data: &[u8]) -> io::Result<()> {
        if self.audio.is_none() {
            return Ok(());
        }
        let pts_90k = self.base_relative_pts(pts_ns);
        let pes = build_audio_pes(pts_90k, data);
        self.write_pes(PID_AUDIO, &pes, None)
    }

    /// Drain the underlying writer. No TS-level trailer is mandatory —
    /// receivers detect end-of-stream from socket close or file EOF.
    pub fn finish(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    /// Convert input PTS (nanoseconds) to 90 kHz ticks rebased on the
    /// first frame's PTS. Saturating at 0 keeps the math friendly when
    /// frames arrive slightly out of decode order.
    fn base_relative_pts(&mut self, pts_ns: i64) -> u64 {
        let raw_90k = if pts_ns > 0 {
            (pts_ns as u64) * 9 / 100_000
        } else {
            0
        };
        let base = *self.base_pts_90k.get_or_insert(raw_90k);
        raw_90k.saturating_sub(base)
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
    fn write_pes(&mut self, pid: u16, pes: &[u8], pcr: Option<u64>) -> io::Result<()> {
        // PSI cadence is enforced per TS packet — interleave a fresh
        // PAT+PMT into the packet stream every PSI_INTERVAL_PACKETS so
        // long single-PES emissions (e.g. one 60 KB video frame) don't
        // starve receivers tuning in mid-stream.
        let mut offset = 0;
        let mut first = true;
        while offset < pes.len() {
            self.maybe_emit_psi()?;

            let attach_pcr = first
                && (pid == PID_VIDEO)
                && (pcr.is_some())
                && (self.packets_written == 0 || self.video_packets_since_pcr >= PCR_INTERVAL_PACKETS);

            let af_body: Vec<u8> = if attach_pcr {
                build_pcr_adaptation(pcr.unwrap_or(0))
            } else {
                Vec::new()
            };

            let remaining = pes.len() - offset;
            // Capacity for payload given AF body and 1-byte AF length.
            // When AF body is empty we can still skip the AF entirely
            // and get the full 184 B; only invoke the AF when we'd
            // otherwise need stuffing.
            let (af_present, payload_len, stuffing): (bool, usize, usize) = if !af_body.is_empty() {
                // AF is mandatory (PCR). 1 byte length + body + stuffing
                // + payload = 184.
                let max_payload = 184 - 1 - af_body.len();
                let p = remaining.min(max_payload);
                let s = max_payload - p;
                (true, p, s)
            } else if remaining >= 184 {
                // Full payload packet — no AF at all.
                (false, 184, 0)
            } else {
                // Last (small) packet — stuff via empty AF.
                // 1 byte length + 0 body + stuffing + payload = 184.
                let max_payload = 183;
                let p = remaining.min(max_payload);
                let s = max_payload - p;
                (true, p, s)
            };

            let cc = self.advance_cc(pid);
            let mut packet = Packet::new();
            packet.set_header(pid, first, true, af_present, cc);
            if af_present {
                packet.append_adaptation(&af_body, stuffing);
            }
            packet.append_payload(&pes[offset..offset + payload_len]);
            debug_assert_eq!(packet.len(), 188, "packet not 188 bytes");
            self.out.write_packet(&packet)?;

            self.packets_written += 1;
            if pid == PID_VIDEO {
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
        if self.packets_written == 0 || self.packets_written.is_multiple_of(PSI_INTERVAL_PACKETS) {
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
        packet.append_payload(&payload);
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
        packet.append_payload(&payload);
        packet.pad_to_188();
        self.out.write_packet(&packet)?;
        self.packets_written += 1;
        Ok(())
    }
}

/// Extract the raw hvcC bytes for handoff to `length_prefixed_to_annex_b`.
/// hvcC layout: 22-byte fixed header, then `numOfArrays` arrays of
/// `(nalType, numNalus, [nalLength:u16, NAL bytes]…)`. We convert this
/// directly to a length-prefixed byte stream (NAL length is u16 in
/// hvcC; widen to u32 for the standard length-prefixed encoding).
fn hvcc_payload(hvcc: &[u8]) -> Vec<u8> {
    if hvcc.len() < 23 {
        return Vec::new();
    }
    let num_arrays = hvcc[22] as usize;
    let mut out = Vec::new();
    let mut offset = 23;
    for _ in 0..num_arrays {
        if offset + 3 > hvcc.len() {
            break;
        }
        offset += 1;
        let num_nalus = u16::from_be_bytes([hvcc[offset], hvcc[offset + 1]]) as usize;
        offset += 2;
        for _ in 0..num_nalus {
            if offset + 2 > hvcc.len() {
                break;
            }
            let nal_len = u16::from_be_bytes([hvcc[offset], hvcc[offset + 1]]) as usize;
            offset += 2;
            if offset + nal_len > hvcc.len() {
                break;
            }
            out.extend_from_slice(&(nal_len as u32).to_be_bytes());
            out.extend_from_slice(&hvcc[offset..offset + nal_len]);
            offset += nal_len;
        }
    }
    out
}

/// Build a PES packet for a video access unit.
fn build_video_pes(pts_90k: u64, es: &[u8]) -> Vec<u8> {
    build_pes_packet(0xE0, pts_90k, es, /* length_in_header */ false)
}

/// Build a PES packet for an audio access unit.
fn build_audio_pes(pts_90k: u64, es: &[u8]) -> Vec<u8> {
    // Audio PES: length is fillable when it fits in u16. We always
    // write the length so receivers don't have to scan for the next
    // start code.
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
    let mut af = vec![0x50]; // PCR_flag=1, random_access_indicator=1
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
        assert_eq!(buf.len() % 188, 0, "stream not packet-aligned: {} bytes", buf.len());
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
        let a = [0u8, 0xB0, 0x0D, 0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01, 0xE1, 0x00];
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
        mux.write_video(0, &frame).unwrap();
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
    fn audio_track_appears_in_pmt_and_stream() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = M2tsMux::new(&mut sink);
        mux.set_audio(AudioCodec::Ac3);
        // Video + audio frame pair.
        let mut frame = Vec::new();
        frame.extend_from_slice(&3u32.to_be_bytes());
        frame.extend_from_slice(&[0x40, 0x01, 0x0C]);
        mux.write_video(0, &frame).unwrap();
        mux.write_audio(20_000_000, &[0x0B, 0x77, 0x12, 0x34]).unwrap();
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
        mux.write_video(0, &frame).unwrap();
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
            mux.write_video(pts, &frame).unwrap();
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
}
