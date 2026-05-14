//! 188-byte MPEG-TS packet builder + writer.
//!
//! Internal helper for `super::M2tsMux`. Not public API — exposes raw
//! byte layout so the parent module can compose PSI / PCR / PES bytes
//! without each caller re-implementing the 188-byte boundary math.

use std::io::{self, Write};

const TS_PACKET_SIZE: usize = 188;
const SYNC_BYTE: u8 = 0x47;
const STUFF_BYTE: u8 = 0xFF;

/// One TS packet under construction. Always emits 188 bytes when
/// [`pad_to_188`](Self::pad_to_188) is called; if it's not called the
/// caller is responsible for filling the packet exactly.
pub(super) struct Packet {
    buf: Vec<u8>,
}

impl Packet {
    pub(super) fn new() -> Self {
        Self {
            buf: Vec::with_capacity(TS_PACKET_SIZE),
        }
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
        self.buf.clear();
        self.buf.push(SYNC_BYTE);
        let pus_bit = if payload_unit_start { 0x40 } else { 0 };
        // transport_error_indicator(1)=0 | payload_unit_start(1) | transport_priority(1)=0 | PID(5 high)
        self.buf.push(pus_bit | ((pid >> 8) as u8 & 0x1F));
        self.buf.push(pid as u8);
        // transport_scrambling_control(2)=0 | adaptation_field_control(2) | continuity_counter(4)
        let afc = match (has_adaptation, has_payload) {
            (false, false) => 0b00, // reserved — should not happen
            (false, true) => 0b01,  // payload only
            (true, false) => 0b10,  // adaptation only
            (true, true) => 0b11,   // both
        };
        self.buf.push((afc << 4) | (cc & 0x0F));
    }

    /// Append the adaptation field after the header.
    ///
    /// `body` is the adaptation field body (flags byte + optional PCR
    /// + …). `stuffing` is the number of `0xFF` stuffing bytes to
    /// append after the body. The first byte of the field
    /// (`adaptation_field_length`) is computed here from `body.len() +
    /// stuffing`.
    pub(super) fn append_adaptation(&mut self, body: &[u8], stuffing: usize) {
        let af_len = body.len() + stuffing;
        debug_assert!(af_len <= 183, "adaptation field overflow");
        self.buf.push(af_len as u8);
        self.buf.extend_from_slice(body);
        for _ in 0..stuffing {
            self.buf.push(STUFF_BYTE);
        }
    }

    /// Append payload bytes.
    pub(super) fn append_payload(&mut self, payload: &[u8]) {
        self.buf.extend_from_slice(payload);
        debug_assert!(self.buf.len() <= TS_PACKET_SIZE, "packet overflow");
    }

    /// Pad the packet to exactly 188 bytes with `0xFF` bytes — used by
    /// PSI emit paths where the section is much smaller than 184 bytes.
    /// For PSI packets only — payload-carrying packets reserve room for
    /// stuffing via `append_adaptation`.
    pub(super) fn pad_to_188(&mut self) {
        while self.buf.len() < TS_PACKET_SIZE {
            self.buf.push(STUFF_BYTE);
        }
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.buf
    }

    pub(super) fn len(&self) -> usize {
        self.buf.len()
    }
}

/// Buffered writer for assembled TS packets. Owns the underlying sink.
pub(super) struct PacketWriter<W: Write> {
    inner: W,
}

impl<W: Write> PacketWriter<W> {
    pub(super) fn new(inner: W) -> Self {
        Self { inner }
    }

    pub(super) fn write_packet(&mut self, packet: &Packet) -> io::Result<()> {
        let bytes = packet.bytes();
        debug_assert_eq!(bytes.len(), TS_PACKET_SIZE);
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
        p.append_payload(&[1, 2, 3]);
        p.pad_to_188();
        assert_eq!(p.bytes().len(), 188);
        assert_eq!(p.bytes()[0], SYNC_BYTE);
        // After 4-byte header + 3 payload, byte 7 starts stuffing.
        assert_eq!(p.bytes()[7], STUFF_BYTE);
    }

    #[test]
    fn header_pid_round_trips() {
        let mut p = Packet::new();
        p.set_header(0x1ABC, false, true, false, 0xA);
        let pid = u16::from_be_bytes([p.bytes()[1] & 0x1F, p.bytes()[2]]);
        assert_eq!(pid, 0x1ABC);
        assert_eq!(p.bytes()[3] & 0x0F, 0xA);
    }
}
