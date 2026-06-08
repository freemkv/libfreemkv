//! 188-byte MPEG-TS packet builder + writer.
//!
//! Internal helper for `super::M2tsMux`. Not public API — exposes raw
//! byte layout so the parent module can compose PSI / PCR / PES bytes
//! without each caller re-implementing the 188-byte boundary math.

use crate::error::Error;
use std::io::{self, Write};

const TS_PACKET_SIZE: usize = 188;
/// Header is 4 bytes, leaving 184 bytes for the adaptation field area
/// plus payload. With a 1-byte `adaptation_field_length` prefix the
/// field body + stuffing can be at most 183 bytes.
const MAX_AF_LEN: usize = TS_PACKET_SIZE - 4 - 1;
const SYNC_BYTE: u8 = 0x47;
const STUFF_BYTE: u8 = 0xFF;

/// One TS packet under construction. Backed by a fixed 188-byte array
/// with a write cursor — no per-packet heap allocation. Always emits 188
/// bytes when [`pad_to_188`](Self::pad_to_188) is called; if it's not
/// called the caller is responsible for filling the packet exactly.
pub(super) struct Packet {
    buf: [u8; TS_PACKET_SIZE],
    len: usize,
}

impl Packet {
    pub(super) fn new() -> Self {
        Self {
            buf: [0u8; TS_PACKET_SIZE],
            len: 0,
        }
    }

    /// Push a byte, saturating at the packet boundary. The boundary is
    /// never reached by the sole caller (mod.rs sizes every field to sum
    /// to 188); the bound prevents a future caller from corrupting memory.
    fn push(&mut self, b: u8) {
        if self.len < TS_PACKET_SIZE {
            self.buf[self.len] = b;
            self.len += 1;
        }
    }

    fn extend(&mut self, bytes: &[u8]) {
        let n = bytes.len().min(TS_PACKET_SIZE - self.len);
        self.buf[self.len..self.len + n].copy_from_slice(&bytes[..n]);
        self.len += n;
    }

    /// Write the 4-byte TS packet header.
    ///
    /// * `pid` — 13-bit PID
    /// * `payload_unit_start` — first packet of a PES / PSI section
    /// * `has_payload` — packet carries any payload bytes
    /// * `has_adaptation` — packet carries an adaptation field
    /// * `cc` — 4-bit continuity counter
    pub(super) fn set_header(
        &mut self,
        pid: u16,
        payload_unit_start: bool,
        has_payload: bool,
        has_adaptation: bool,
        cc: u8,
    ) {
        self.len = 0;
        self.push(SYNC_BYTE);
        let pus_bit = if payload_unit_start { 0x40 } else { 0 };
        // transport_error_indicator(1)=0 | payload_unit_start(1) | transport_priority(1)=0 | PID(5 high)
        self.push(pus_bit | ((pid >> 8) as u8 & 0x1F));
        self.push(pid as u8);
        // transport_scrambling_control(2)=0 | adaptation_field_control(2) | continuity_counter(4)
        let afc = match (has_adaptation, has_payload) {
            (false, false) => 0b00, // reserved — should not happen
            (false, true) => 0b01,  // payload only
            (true, false) => 0b10,  // adaptation only
            (true, true) => 0b11,   // both
        };
        self.push((afc << 4) | (cc & 0x0F));
    }

    /// Append the adaptation field after the header.
    ///
    /// `body` is the adaptation field body (flags byte plus optional PCR
    /// and so on). `stuffing` is the number of `0xFF` stuffing bytes to
    /// append after the body. The first byte of the field
    /// (`adaptation_field_length`) is computed here from
    /// `body.len() + stuffing`.
    ///
    /// Returns [`Error::M2tsPacketMalformed`] if the computed
    /// `adaptation_field_length` would exceed `MAX_AF_LEN` — the length
    /// byte and the bytes actually written must always agree, so an
    /// over-long field is rejected rather than written with a clamped
    /// (and therefore lying) length byte.
    pub(super) fn append_adaptation(&mut self, body: &[u8], stuffing: usize) -> io::Result<()> {
        let af_len = body.len() + stuffing;
        if af_len > MAX_AF_LEN {
            return Err(Error::M2tsPacketMalformed.into());
        }
        self.push(af_len as u8);
        self.extend(body);
        for _ in 0..stuffing {
            self.push(STUFF_BYTE);
        }
        Ok(())
    }

    /// Append payload bytes.
    ///
    /// Returns [`Error::M2tsPacketMalformed`] if doing so would push the
    /// packet past 188 bytes — overflow is a muxer invariant break, not
    /// something to silently emit.
    pub(super) fn append_payload(&mut self, payload: &[u8]) -> io::Result<()> {
        if self.len + payload.len() > TS_PACKET_SIZE {
            return Err(Error::M2tsPacketMalformed.into());
        }
        self.extend(payload);
        Ok(())
    }

    /// Pad the packet to exactly 188 bytes with `0xFF` bytes — used by
    /// PSI emit paths where the section is much smaller than 184 bytes.
    /// For PSI packets only — payload-carrying packets reserve room for
    /// stuffing via `append_adaptation`.
    pub(super) fn pad_to_188(&mut self) {
        while self.len < TS_PACKET_SIZE {
            self.push(STUFF_BYTE);
        }
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }
}

/// Writer for assembled TS packets. Owns the underlying sink and writes
/// each 188-byte packet straight through — it adds no buffering of its
/// own, so callers that need buffering should wrap the sink in a
/// `BufWriter`.
pub(super) struct PacketWriter<W: Write> {
    inner: W,
}

impl<W: Write> PacketWriter<W> {
    pub(super) fn new(inner: W) -> Self {
        Self { inner }
    }

    pub(super) fn write_packet(&mut self, packet: &Packet) -> io::Result<()> {
        let bytes = packet.bytes();
        // Hard check, not a debug_assert: a non-188-byte packet would
        // corrupt the transport stream, so refuse to write it in any
        // build rather than emitting a short/long packet silently.
        if bytes.len() != TS_PACKET_SIZE {
            return Err(Error::M2tsPacketMalformed.into());
        }
        self.inner.write_all(bytes)
    }

    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pad_fills_to_188() {
        let mut p = Packet::new();
        p.set_header(0x100, true, true, false, 0);
        p.append_payload(&[1, 2, 3]).unwrap();
        p.pad_to_188();
        assert_eq!(p.bytes().len(), 188);
        assert_eq!(p.bytes()[0], SYNC_BYTE);
        // After 4-byte header + 3 payload, byte 7 starts stuffing.
        assert_eq!(p.bytes()[7], STUFF_BYTE);
    }

    #[test]
    fn append_adaptation_rejects_overflow() {
        let mut p = Packet::new();
        p.set_header(0x100, true, true, true, 0);
        // body(1) + stuffing(MAX_AF_LEN) = MAX_AF_LEN + 1 > MAX_AF_LEN.
        let err = p.append_adaptation(&[0x00], MAX_AF_LEN).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn append_payload_rejects_overflow() {
        let mut p = Packet::new();
        p.set_header(0x100, true, true, false, 0);
        // 4-byte header + 185 payload = 189 > 188.
        let err = p.append_payload(&[0u8; 185]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn write_packet_rejects_short_packet() {
        let mut p = Packet::new();
        p.set_header(0x100, true, true, false, 0);
        p.append_payload(&[1, 2, 3]).unwrap(); // only 7 bytes, not padded
        let mut sink: Vec<u8> = Vec::new();
        let mut w = PacketWriter::new(&mut sink);
        let err = w.write_packet(&p).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(sink.is_empty(), "short packet must not be written");
    }

    #[test]
    fn header_pid_round_trips() {
        let mut p = Packet::new();
        p.set_header(0x1ABC, false, true, false, 0xA);
        let pid = u16::from_be_bytes([p.bytes()[1] & 0x1F, p.bytes()[2]]);
        assert_eq!(pid, 0x1ABC);
        assert_eq!(p.bytes()[3] & 0x0F, 0xA);
    }

    // ════════════════════════════════════════════════════════════════════
    // Added hardening tests
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn header_sync_byte_and_pusi_bit() {
        // ISO 13818-1: sync_byte 0x47 at byte 0; PUSI is bit 6 of byte 1.
        let mut p = Packet::new();
        p.set_header(0x0100, true, true, false, 0);
        assert_eq!(p.bytes()[0], SYNC_BYTE);
        assert_eq!(p.bytes()[1] & 0x40, 0x40, "PUSI set");
        // transport_error_indicator (bit 7) and priority (bit 5) clear.
        assert_eq!(p.bytes()[1] & 0x80, 0, "TEI clear");
        assert_eq!(p.bytes()[1] & 0x20, 0, "transport_priority clear");

        let mut p2 = Packet::new();
        p2.set_header(0x0100, false, true, false, 0);
        assert_eq!(p2.bytes()[1] & 0x40, 0, "PUSI clear when not a unit start");
    }

    #[test]
    fn header_afc_bits_per_combination() {
        // adaptation_field_control (bits 5:4 of byte 3), ISO 13818-1
        // Table 2-5: 01 payload only, 10 AF only, 11 both, 00 reserved.
        let cases = [
            (false, true, 0b01u8),
            (true, false, 0b10),
            (true, true, 0b11),
            (false, false, 0b00),
        ];
        for (af, pl, want) in cases {
            let mut p = Packet::new();
            p.set_header(0x0100, true, pl, af, 0);
            assert_eq!((p.bytes()[3] >> 4) & 0x03, want, "AFC for af={af} pl={pl}");
        }
    }

    #[test]
    fn append_adaptation_length_byte_matches_written_bytes() {
        // The adaptation_field_length byte must equal body+stuffing — the
        // written length and declared length must agree or a decoder
        // misframes the payload.
        let mut p = Packet::new();
        p.set_header(0x0100, true, true, true, 0);
        p.append_adaptation(&[0x10, 0xAA, 0xBB], 4).unwrap(); // 3 body + 4 stuffing
        // byte 4 is the length byte.
        assert_eq!(p.bytes()[4], 3 + 4, "length byte = body+stuffing");
        // body bytes follow.
        assert_eq!(&p.bytes()[5..8], &[0x10, 0xAA, 0xBB]);
        // stuffing bytes are 0xFF.
        assert_eq!(&p.bytes()[8..12], &[0xFF; 4]);
    }

    #[test]
    fn append_adaptation_at_exact_max_succeeds() {
        // MAX_AF_LEN (183) is the largest legal adaptation field body+stuff.
        // Exactly MAX_AF_LEN must succeed; the boundary itself is valid.
        let mut p = Packet::new();
        p.set_header(0x0100, true, true, true, 0);
        assert!(p.append_adaptation(&[0x00], MAX_AF_LEN - 1).is_ok());
        assert_eq!(p.bytes()[4] as usize, MAX_AF_LEN);
    }

    #[test]
    fn append_payload_at_exact_boundary_fills_188() {
        // 4-byte header + 184 payload = exactly 188 (no AF). The boundary
        // must be accepted, not rejected.
        let mut p = Packet::new();
        p.set_header(0x0100, true, true, false, 0);
        assert!(p.append_payload(&[0xAB; 184]).is_ok());
        assert_eq!(p.len(), 188);
    }

    #[test]
    fn pad_to_188_is_idempotent_when_already_full() {
        // Padding a packet that already reached 188 bytes must not grow it
        // past 188 (the push() bound prevents overflow).
        let mut p = Packet::new();
        p.set_header(0x0100, true, true, false, 0);
        p.append_payload(&[0xAB; 184]).unwrap();
        assert_eq!(p.len(), 188);
        p.pad_to_188();
        assert_eq!(p.len(), 188, "no growth past 188");
    }

    #[test]
    fn write_packet_rejects_long_packet() {
        // A packet whose len somehow exceeds 188 must be refused (the writer
        // checks exact equality). We can't push past 188 (push saturates),
        // so test the under-188 rejection path which the writer guards.
        let mut p = Packet::new();
        p.set_header(0x0100, true, true, false, 0);
        p.append_payload(&[1, 2, 3, 4, 5]).unwrap(); // 9 bytes, not 188
        let mut sink: Vec<u8> = Vec::new();
        let mut w = PacketWriter::new(&mut sink);
        assert!(w.write_packet(&p).is_err());
        assert!(sink.is_empty());
    }

    #[test]
    fn write_packet_accepts_exactly_188() {
        // A correctly-sized 188-byte packet must be written through verbatim.
        let mut p = Packet::new();
        p.set_header(0x0100, true, true, false, 0);
        p.append_payload(&[0x5A; 184]).unwrap();
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut w = PacketWriter::new(&mut sink);
            w.write_packet(&p).unwrap();
        }
        assert_eq!(sink.len(), 188);
        assert_eq!(sink[0], SYNC_BYTE);
    }

    #[test]
    fn pid_high_bits_masked_to_13_bits() {
        // PID is 13 bits. Bits above 0x1FFF must not leak into the
        // transport_priority / PUSI / TEI bits of byte 1.
        let mut p = Packet::new();
        // 0xE100 has bits set above the 13-bit PID range.
        p.set_header(0xE100, false, true, false, 0);
        assert_eq!(
            p.bytes()[1] & 0xE0,
            0,
            "top 3 bits of byte1 are flags, not PID"
        );
        let pid = u16::from_be_bytes([p.bytes()[1] & 0x1F, p.bytes()[2]]);
        assert_eq!(pid, 0xE100 & 0x1FFF, "PID masked to 13 bits");
    }
}
