//! EBML (Extensible Binary Meta Language) write primitives for Matroska.
//!
//! EBML uses variable-length integers for element IDs and sizes.
//! This module provides low-level writers for constructing MKV files.

use std::io::{self, Read, Seek, SeekFrom, Write};

/// Write an EBML element ID (1-4 bytes, already encoded).
/// Element IDs are predefined constants — we write them verbatim.
pub fn write_id(w: &mut impl Write, id: u32) -> io::Result<()> {
    if id <= 0xFF {
        w.write_all(&[id as u8])
    } else if id <= 0xFFFF {
        w.write_all(&[(id >> 8) as u8, id as u8])
    } else if id <= 0xFF_FFFF {
        w.write_all(&[(id >> 16) as u8, (id >> 8) as u8, id as u8])
    } else {
        w.write_all(&[
            (id >> 24) as u8,
            (id >> 16) as u8,
            (id >> 8) as u8,
            id as u8,
        ])
    }
}

/// Write an EBML variable-length size (1-8 bytes).
/// Uses the EBML VINT encoding: leading bits indicate width.
pub fn write_size(w: &mut impl Write, size: u64) -> io::Result<()> {
    if size < 0x7F {
        w.write_all(&[(size as u8) | 0x80])
    } else if size < 0x3FFF {
        w.write_all(&[((size >> 8) as u8) | 0x40, size as u8])
    } else if size < 0x1F_FFFF {
        w.write_all(&[((size >> 16) as u8) | 0x20, (size >> 8) as u8, size as u8])
    } else if size < 0x0FFF_FFFF {
        w.write_all(&[
            ((size >> 24) as u8) | 0x10,
            (size >> 16) as u8,
            (size >> 8) as u8,
            size as u8,
        ])
    } else if size >= 0x00FF_FFFF_FFFF_FFFF {
        // 0x00FF_FFFF_FFFF_FFFF (max 56-bit) encodes byte-for-byte
        // identical to write_unknown_size (the EBML all-ones
        // "unknown/open-ended" sentinel), and anything larger doesn't fit
        // the 7-byte payload. Reject so a finite size can never be emitted
        // as the unknown-size marker.
        Err(crate::error::Error::MkvInvalid.into())
    } else {
        // 8-byte size for large elements
        w.write_all(&[
            0x01,
            (size >> 48) as u8,
            (size >> 40) as u8,
            (size >> 32) as u8,
            (size >> 24) as u8,
            (size >> 16) as u8,
            (size >> 8) as u8,
            size as u8,
        ])
    }
}

/// Write an EBML "unknown size" marker (all 1s in VINT, 8 bytes).
/// Used for the Segment element when total size isn't known upfront.
pub fn write_unknown_size(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
}

/// Write a complete EBML unsigned integer element.
pub fn write_uint(w: &mut impl Write, id: u32, val: u64) -> io::Result<()> {
    write_id(w, id)?;
    if val <= 0xFF {
        write_size(w, 1)?;
        w.write_all(&[val as u8])
    } else if val <= 0xFFFF {
        write_size(w, 2)?;
        w.write_all(&[(val >> 8) as u8, val as u8])
    } else if val <= 0xFF_FFFF {
        write_size(w, 3)?;
        w.write_all(&[(val >> 16) as u8, (val >> 8) as u8, val as u8])
    } else if val <= 0xFFFF_FFFF {
        write_size(w, 4)?;
        w.write_all(&[
            (val >> 24) as u8,
            (val >> 16) as u8,
            (val >> 8) as u8,
            val as u8,
        ])
    } else {
        write_size(w, 8)?;
        w.write_all(&val.to_be_bytes())
    }
}

/// Write a complete EBML float element (8-byte double).
pub fn write_float(w: &mut impl Write, id: u32, val: f64) -> io::Result<()> {
    write_id(w, id)?;
    write_size(w, 8)?;
    w.write_all(&val.to_be_bytes())
}

/// Write a complete EBML UTF-8 string element.
pub fn write_string(w: &mut impl Write, id: u32, val: &str) -> io::Result<()> {
    write_id(w, id)?;
    write_size(w, val.len() as u64)?;
    w.write_all(val.as_bytes())
}

/// Write a complete EBML binary element.
pub fn write_binary(w: &mut impl Write, id: u32, data: &[u8]) -> io::Result<()> {
    write_id(w, id)?;
    write_size(w, data.len() as u64)?;
    w.write_all(data)
}

/// Start a master element: write ID + placeholder size.
/// Returns the file offset of the size field for later fixup.
pub fn start_master<W: Write + Seek>(w: &mut W, id: u32) -> io::Result<u64> {
    write_id(w, id)?;
    let size_pos = w.stream_position()?;
    // 8-byte size placeholder (will be overwritten by end_master)
    w.write_all(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])?;
    Ok(size_pos)
}

/// End a master element: seek back and write the actual size.
///
/// `size_pos` must be the offset returned by [`start_master`], which always
/// writes the 8-byte size placeholder before any body bytes. Therefore
/// `end_pos >= size_pos + 8` always holds, and the resulting `data_size`
/// fits the 7-byte VINT payload (a single MKV element exceeding 2^56 bytes
/// is not representable and never produced here).
pub fn end_master<W: Write + Seek>(w: &mut W, size_pos: u64) -> io::Result<()> {
    let end_pos = w.stream_position()?;
    debug_assert!(
        end_pos >= size_pos + 8,
        "end_master: end_pos {end_pos} < size_pos {size_pos} + 8 (placeholder not written?)"
    );
    let data_size = end_pos - size_pos - 8; // subtract the 8-byte size field itself
    debug_assert!(
        data_size < 0x0100_0000_0000_0000,
        "end_master: data_size {data_size} exceeds the 7-byte VINT payload"
    );
    w.seek(SeekFrom::Start(size_pos))?;
    // Write as 8-byte VINT: 0x01 followed by 7 bytes of size
    w.write_all(&[
        0x01,
        (data_size >> 48) as u8,
        (data_size >> 40) as u8,
        (data_size >> 32) as u8,
        (data_size >> 24) as u8,
        (data_size >> 16) as u8,
        (data_size >> 8) as u8,
        data_size as u8,
    ])?;
    w.seek(SeekFrom::Start(end_pos))?;
    Ok(())
}

// ============================================================
// EBML Read primitives
// ============================================================

/// Read an EBML element ID. Returns (id, bytes_consumed).
pub fn read_id(r: &mut impl Read) -> io::Result<(u32, usize)> {
    let mut first = [0u8; 1];
    r.read_exact(&mut first)?;
    let b0 = first[0];

    if b0 & 0x80 != 0 {
        Ok((b0 as u32, 1))
    } else if b0 & 0x40 != 0 {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        Ok((((b0 as u32) << 8) | b[0] as u32, 2))
    } else if b0 & 0x20 != 0 {
        let mut b = [0u8; 2];
        r.read_exact(&mut b)?;
        Ok((((b0 as u32) << 16) | (b[0] as u32) << 8 | b[1] as u32, 3))
    } else if b0 & 0x10 != 0 {
        let mut b = [0u8; 3];
        r.read_exact(&mut b)?;
        Ok((
            ((b0 as u32) << 24) | (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32,
            4,
        ))
    } else {
        Err(crate::error::Error::MkvInvalid.into())
    }
}

/// Read an EBML variable-length size. Returns (size, bytes_consumed).
/// Size of u64::MAX means "unknown size".
pub fn read_size(r: &mut impl Read) -> io::Result<(u64, usize)> {
    let mut first = [0u8; 1];
    r.read_exact(&mut first)?;
    let b0 = first[0];

    if b0 & 0x80 != 0 {
        let val = (b0 & 0x7F) as u64;
        if val == 0x7F {
            return Ok((u64::MAX, 1));
        } // unknown
        Ok((val, 1))
    } else if b0 & 0x40 != 0 {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        let val = (((b0 & 0x3F) as u64) << 8) | b[0] as u64;
        if val == 0x3FFF {
            return Ok((u64::MAX, 2));
        }
        Ok((val, 2))
    } else if b0 & 0x20 != 0 {
        let mut b = [0u8; 2];
        r.read_exact(&mut b)?;
        let val = (((b0 & 0x1F) as u64) << 16) | (b[0] as u64) << 8 | b[1] as u64;
        if val == 0x1F_FFFF {
            return Ok((u64::MAX, 3));
        }
        Ok((val, 3))
    } else if b0 & 0x10 != 0 {
        let mut b = [0u8; 3];
        r.read_exact(&mut b)?;
        let val =
            (((b0 & 0x0F) as u64) << 24) | (b[0] as u64) << 16 | (b[1] as u64) << 8 | b[2] as u64;
        if val == 0x0FFF_FFFF {
            return Ok((u64::MAX, 4));
        }
        Ok((val, 4))
    } else if b0 & 0x08 != 0 {
        let mut b = [0u8; 4];
        r.read_exact(&mut b)?;
        let val = (((b0 & 0x07) as u64) << 32)
            | (b[0] as u64) << 24
            | (b[1] as u64) << 16
            | (b[2] as u64) << 8
            | b[3] as u64;
        if val == 0x07_FFFF_FFFF {
            return Ok((u64::MAX, 5));
        }
        Ok((val, 5))
    } else if b0 & 0x04 != 0 {
        let mut b = [0u8; 5];
        r.read_exact(&mut b)?;
        let val = (((b0 & 0x03) as u64) << 40)
            | (b[0] as u64) << 32
            | (b[1] as u64) << 24
            | (b[2] as u64) << 16
            | (b[3] as u64) << 8
            | b[4] as u64;
        if val == 0x3FF_FFFF_FFFF {
            return Ok((u64::MAX, 6));
        }
        Ok((val, 6))
    } else if b0 & 0x02 != 0 {
        let mut b = [0u8; 6];
        r.read_exact(&mut b)?;
        let val = (((b0 & 0x01) as u64) << 48)
            | (b[0] as u64) << 40
            | (b[1] as u64) << 32
            | (b[2] as u64) << 24
            | (b[3] as u64) << 16
            | (b[4] as u64) << 8
            | b[5] as u64;
        if val == 0x01_FFFF_FFFF_FFFF {
            return Ok((u64::MAX, 7));
        }
        Ok((val, 7))
    } else if b0 & 0x01 != 0 {
        let mut b = [0u8; 7];
        r.read_exact(&mut b)?;
        let val = (b[0] as u64) << 48
            | (b[1] as u64) << 40
            | (b[2] as u64) << 32
            | (b[3] as u64) << 24
            | (b[4] as u64) << 16
            | (b[5] as u64) << 8
            | b[6] as u64;
        if val == 0x00FF_FFFF_FFFF_FFFF {
            return Ok((u64::MAX, 8));
        }
        Ok((val, 8))
    } else {
        // b0 == 0x00: no length marker in the first byte. A VINT wider than
        // 8 bytes is not representable by Matroska's size encoding, so this
        // is a malformed/over-long size field rather than a valid 8-byte
        // length. Reject it instead of silently building a size from the
        // following 7 bytes (which would desync the parse).
        Err(crate::error::Error::MkvInvalid.into())
    }
}

/// Read an EBML element header (ID + size). Returns (id, data_size, header_bytes).
pub fn read_element_header(r: &mut impl Read) -> io::Result<(u32, u64, usize)> {
    let (id, id_len) = read_id(r)?;
    let (size, size_len) = read_size(r)?;
    Ok((id, size, id_len + size_len))
}

/// Read an unsigned integer value of `len` bytes.
pub fn read_uint_val(r: &mut impl Read, len: usize) -> io::Result<u64> {
    // An EBML unsigned integer is at most 8 bytes. A malformed element
    // claiming `len > 8` would index past this stack buffer and panic
    // (DoS on untrusted input) — reject it at the source so every caller
    // is safe, not just the ones that pre-check.
    if len > 8 {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf[..len])?;
    let mut val = 0u64;
    for &b in &buf[..len] {
        val = (val << 8) | b as u64;
    }
    Ok(val)
}

/// Read a float value. EBML floats are exactly 0, 4, or 8 bytes; any other
/// length is rejected as [`Error::MkvInvalid`] and exactly the float width is
/// consumed (so a malformed element never under- or over-reads and desyncs the
/// rest of the parent element).
pub fn read_float_val(r: &mut impl Read, len: usize) -> io::Result<f64> {
    match len {
        0 => Ok(0.0),
        4 => {
            let mut buf = [0u8; 4];
            r.read_exact(&mut buf)?;
            Ok(f32::from_be_bytes(buf) as f64)
        }
        8 => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf)?;
            Ok(f64::from_be_bytes(buf))
        }
        _ => Err(crate::error::Error::MkvInvalid.into()),
    }
}

/// Read a UTF-8 string value of `len` bytes.
pub fn read_string_val(r: &mut impl Read, len: usize) -> io::Result<String> {
    let mut buf = read_exact_bounded(r, len)?;
    // Strip trailing nulls
    while buf.last() == Some(&0) {
        buf.pop();
    }
    // Library rule: errors are numeric variants, never English strings.
    // A non-UTF-8 string element is malformed input → MkvInvalid.
    String::from_utf8(buf).map_err(|_| crate::error::Error::MkvInvalid.into())
}

/// Read binary data of `len` bytes.
pub fn read_binary_val(r: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    read_exact_bounded(r, len)
}

/// Read exactly `len` bytes WITHOUT trusting `len` to size the allocation.
///
/// `vec![0u8; len]` on an attacker-controlled EBML size would allocate
/// gigabytes before the read fails. Instead we cap the reader to `len`
/// and grow the buffer as bytes actually arrive: a malformed element that
/// claims a huge length but supplies few bytes allocates only what it
/// delivers, then errors on the short read.
fn read_exact_bounded(r: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let got = r.take(len as u64).read_to_end(&mut buf)?;
    if got != len {
        // A truncated element is malformed input. Use the typed crate error
        // so callers matching on Error::MkvInvalid catch short reads rather
        // than a bare io::ErrorKind that bypasses the numeric-code identity.
        return Err(crate::error::Error::MkvInvalid.into());
    }
    Ok(buf)
}

// ============================================================
// Matroska Element IDs
// ============================================================

// EBML Header
pub const EBML: u32 = 0x1A45_DFA3;
pub const EBML_VERSION: u32 = 0x4286;
pub const EBML_READ_VERSION: u32 = 0x42F7;
pub const EBML_MAX_ID_LENGTH: u32 = 0x42F2;
pub const EBML_MAX_SIZE_LENGTH: u32 = 0x42F3;
pub const EBML_DOC_TYPE: u32 = 0x4282;
pub const EBML_DOC_TYPE_VERSION: u32 = 0x4287;
pub const EBML_DOC_TYPE_READ_VERSION: u32 = 0x4285;

// Segment
pub const SEGMENT: u32 = 0x1853_8067;

// SeekHead
pub const SEEK_HEAD: u32 = 0x114D_9B74;
pub const SEEK: u32 = 0x4DBB;
/// Void — RFC 9559 (Matroska) §EBML global element 0xEC. Used to neutralise a
/// reserved-but-unused region (e.g. the CUES SeekHead entry when no Cues element
/// is written) so it carries no meaning to a parser.
pub const VOID: u32 = 0xEC;
pub const SEEK_ID: u32 = 0x53AB;
pub const SEEK_POSITION: u32 = 0x53AC;

// Segment Info
pub const INFO: u32 = 0x1549_A966;
pub const TIMESTAMP_SCALE: u32 = 0x2A_D7B1;
pub const DURATION: u32 = 0x4489;
pub const MUXING_APP: u32 = 0x4D80;
pub const WRITING_APP: u32 = 0x5741;
pub const TITLE: u32 = 0x7BA9;

// Tracks
pub const TRACKS: u32 = 0x1654_AE6B;
pub const TRACK_ENTRY: u32 = 0xAE;
pub const TRACK_NUMBER: u32 = 0xD7;
pub const TRACK_UID: u32 = 0x73C5;
pub const TRACK_TYPE: u32 = 0x83;
pub const FLAG_LACING: u32 = 0x9C;
pub const FLAG_DEFAULT: u32 = 0x88;
pub const FLAG_FORCED: u32 = 0x55AA;
pub const LANGUAGE: u32 = 0x22_B59C;
pub const CODEC_ID: u32 = 0x86;
pub const CODEC_PRIVATE: u32 = 0x63A2;
pub const TRACK_NAME: u32 = 0x536E;
pub const DEFAULT_DURATION: u32 = 0x23_E383;
/// DefaultDecodedFieldDuration — nanoseconds per FIELD (half a frame for
/// interlaced content). Emitting it on an interlaced track tells a reader the
/// field rate so it stops halving the frame rate (Windows shell shows 12.5 fps
/// for a 25 fps 576i stream without it). RFC 9559 / Matroska v4.
pub const DEFAULT_DECODED_FIELD_DURATION: u32 = 0x23_4E7A;

// Video
pub const VIDEO: u32 = 0xE0;
pub const PIXEL_WIDTH: u32 = 0xB0;
pub const PIXEL_HEIGHT: u32 = 0xBA;
// Scan type (children of Video).
pub const FLAG_INTERLACED: u32 = 0x9A;
pub const FIELD_ORDER: u32 = 0x9D;
// FlagInterlaced values: 1 = interlaced, 2 = progressive (0 = undetermined).
pub const INTERLACED_INTERLACED: u64 = 1;
pub const INTERLACED_PROGRESSIVE: u64 = 2;
// FieldOrder values (Matroska / RFC 9559, element 0x9D): 1 = top-field-first,
// 6 = bottom-field-first, 0 = progressive. The muxer derives TFF vs BFF from the
// bitstream's measured top_field_first when available, falling back to TFF for
// interlaced content (NTSC 480i / PAL 576i / HD 1080i are overwhelmingly TFF).
// 0xFF is our sentinel for "undetermined / omit the element".
pub const FIELD_ORDER_TFF: u8 = 1;
/// Bottom-field-first (RFC 9559 element 0x9D = 6). Emitted when the bitstream's
/// measured top_field_first is false.
pub const FIELD_ORDER_BFF: u8 = 6;
pub const FIELD_ORDER_UNDETERMINED: u8 = 0xFF;
pub const DISPLAY_WIDTH: u32 = 0x54B0;
pub const DISPLAY_HEIGHT: u32 = 0x54BA;
pub const COLOUR: u32 = 0x55B0;
pub const TRANSFER_CHARACTERISTICS: u32 = 0x55BA;
pub const MATRIX_COEFFICIENTS: u32 = 0x55B1;
pub const PRIMARIES: u32 = 0x55BB;
pub const RANGE: u32 = 0x55B9;

// Dolby Vision — BlockAdditionMapping carries the DOVIDecoderConfigurationRecord
// (dvcC) so players / mediainfo recognise the track as Dolby Vision.
pub const BLOCK_ADDITION_MAPPING: u32 = 0x41E4;
pub const BLOCK_ADD_ID_TYPE: u32 = 0x41E7;
pub const BLOCK_ADD_ID_EXTRA_DATA: u32 = 0x41ED;

// Audio
pub const AUDIO: u32 = 0xE1;
pub const SAMPLING_FREQUENCY: u32 = 0xB5;
pub const CHANNELS: u32 = 0x9F;
pub const BIT_DEPTH: u32 = 0x6264;

// Cluster
pub const CLUSTER: u32 = 0x1F43_B675;
pub const CLUSTER_TIMESTAMP: u32 = 0xE7;
pub const SIMPLE_BLOCK: u32 = 0xA3;
pub const BLOCK_GROUP: u32 = 0xA0;
pub const BLOCK: u32 = 0xA1;
pub const BLOCK_DURATION: u32 = 0x9B;

// Cues
pub const CUES: u32 = 0x1C53_BB6B;
pub const CUE_POINT: u32 = 0xBB;
pub const CUE_TIME: u32 = 0xB3;
pub const CUE_TRACK_POSITIONS: u32 = 0xB7;
pub const CUE_TRACK: u32 = 0xF7;
pub const CUE_CLUSTER_POSITION: u32 = 0xF1;

// Tags — per-track statistics tags. mkvmerge convention: a `BPS` SimpleTag
// per track carries the bits-per-second so readers (Windows Explorer's MKV
// property handler) that read the container tag rather than computing from
// stream size show a bitrate for every track, not just CBR audio.
pub const TAGS: u32 = 0x1254_C367;
pub const TAG: u32 = 0x7373;
pub const TARGETS: u32 = 0x63C0;
pub const TAG_TRACK_UID: u32 = 0x63C5;
pub const SIMPLE_TAG: u32 = 0x67C8;
pub const TAG_NAME: u32 = 0x45A3;
pub const TAG_STRING: u32 = 0x4487;

// Chapters
pub const CHAPTERS: u32 = 0x1043_A770;
pub const EDITION_ENTRY: u32 = 0x45B9;
pub const CHAPTER_ATOM: u32 = 0xB6;
pub const CHAPTER_UID: u32 = 0x73C4;
pub const CHAPTER_TIME_START: u32 = 0x91;
pub const CHAPTER_DISPLAY: u32 = 0x80;
pub const CHAP_STRING: u32 = 0x85;
pub const CHAP_LANGUAGE: u32 = 0x437C;

// Track types
pub const TRACK_TYPE_VIDEO: u64 = 1;
pub const TRACK_TYPE_AUDIO: u64 = 2;
pub const TRACK_TYPE_SUBTITLE: u64 = 17;

// Matroska CodecID strings (the `CodecID` element value per the Matroska codec
// registry). Single source of truth for both the muxer (Codec -> string) and
// the demuxer (string -> Codec), so the two can never drift.
pub const CODEC_HEVC: &str = "V_MPEGH/ISO/HEVC";
pub const CODEC_H264: &str = "V_MPEG4/ISO/AVC";
pub const CODEC_VC1: &str = "V_MS/VFW/FOURCC";
pub const CODEC_MPEG2: &str = "V_MPEG2";
pub const CODEC_AC3: &str = "A_AC3";
pub const CODEC_EAC3: &str = "A_EAC3";
pub const CODEC_TRUEHD: &str = "A_TRUEHD";
pub const CODEC_DTS: &str = "A_DTS";
pub const CODEC_PCM_BE: &str = "A_PCM/INT/BIG";
pub const CODEC_PGS: &str = "S_HDMV/PGS";
pub const CODEC_VOBSUB: &str = "S_VOBSUB";

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_write_size() {
        let mut buf = Vec::new();
        write_size(&mut buf, 0).unwrap();
        assert_eq!(buf, [0x80]);

        buf.clear();
        write_size(&mut buf, 127).unwrap();
        assert_eq!(buf, [0x40, 127]); // 127 >= 0x7F, uses 2 bytes: (0>>8)|0x40, 127

        buf.clear();
        write_size(&mut buf, 126).unwrap();
        assert_eq!(buf, [126 | 0x80]); // 126 < 0x7F, uses 1 byte
    }

    #[test]
    fn write_size_rejects_unknown_size_sentinel() {
        // 0x00FF_FFFF_FFFF_FFFF would encode byte-for-byte identical to the
        // EBML unknown-size marker; it must be rejected, not silently emitted.
        let mut buf = Vec::new();
        let e = write_size(&mut buf, 0x00FF_FFFF_FFFF_FFFF).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(buf.is_empty(), "no bytes should be written on rejection");

        // One below the boundary still encodes as a normal 8-byte size whose
        // payload is NOT all-ones, so read_size yields the finite value back.
        buf.clear();
        let v = 0x00FF_FFFF_FFFF_FFFE;
        write_size(&mut buf, v).unwrap();
        let (back, consumed) = read_size(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(consumed, 8);
        assert_eq!(back, v);
    }

    #[test]
    fn test_write_uint() {
        let mut buf = Vec::new();
        write_uint(&mut buf, 0x4286, 1).unwrap(); // EBML_VERSION = 1
        // ID: 42 86, Size: 81 (1 byte), Data: 01
        assert_eq!(buf, [0x42, 0x86, 0x81, 0x01]);
    }

    #[test]
    fn test_write_string() {
        let mut buf = Vec::new();
        write_string(&mut buf, 0x4282, "matroska").unwrap();
        // ID: 42 82, Size: 88 (8 bytes), Data: "matroska"
        assert_eq!(&buf[0..2], &[0x42, 0x82]);
        assert_eq!(buf[2], 0x88); // size = 8
        assert_eq!(&buf[3..], b"matroska");
    }

    #[test]
    fn test_master_element() {
        let mut buf = Cursor::new(Vec::new());
        let pos = start_master(&mut buf, EBML).unwrap();
        write_uint(&mut buf, EBML_VERSION, 1).unwrap();
        end_master(&mut buf, pos).unwrap();
        let data = buf.into_inner();
        // EBML header: 1A 45 DF A3, then 8-byte size, then content
        assert_eq!(&data[0..4], &[0x1A, 0x45, 0xDF, 0xA3]);
    }

    #[test]
    fn write_read_id_roundtrip() {
        // 1-byte IDs have high bit set (0x80..=0xFF)
        for &id in &[0x80u32, 0xA3, 0xFF] {
            let mut buf = Vec::new();
            write_id(&mut buf, id).unwrap();
            assert_eq!(buf.len(), 1);
            let mut cursor = Cursor::new(&buf);
            let (read_back, consumed) = read_id(&mut cursor).unwrap();
            assert_eq!(read_back, id, "1-byte ID roundtrip failed for 0x{:X}", id);
            assert_eq!(consumed, 1);
        }
        // 2-byte IDs (0x4000..=0x7FFF)
        for &id in &[0x4286u32, 0x4282, 0x7FFF] {
            let mut buf = Vec::new();
            write_id(&mut buf, id).unwrap();
            assert_eq!(buf.len(), 2);
            let mut cursor = Cursor::new(&buf);
            let (read_back, consumed) = read_id(&mut cursor).unwrap();
            assert_eq!(read_back, id, "2-byte ID roundtrip failed for 0x{:X}", id);
            assert_eq!(consumed, 2);
        }
        // 3-byte IDs (0x200000..=0x3FFFFF)
        for &id in &[0x22B59Cu32, 0x23E383] {
            let mut buf = Vec::new();
            write_id(&mut buf, id).unwrap();
            assert_eq!(buf.len(), 3);
            let mut cursor = Cursor::new(&buf);
            let (read_back, consumed) = read_id(&mut cursor).unwrap();
            assert_eq!(read_back, id, "3-byte ID roundtrip failed for 0x{:X}", id);
            assert_eq!(consumed, 3);
        }
        // 4-byte IDs (0x10000000..=0x1FFFFFFF)
        for &id in &[EBML, SEGMENT, TRACKS, CLUSTER] {
            let mut buf = Vec::new();
            write_id(&mut buf, id).unwrap();
            assert_eq!(buf.len(), 4);
            let mut cursor = Cursor::new(&buf);
            let (read_back, consumed) = read_id(&mut cursor).unwrap();
            assert_eq!(read_back, id, "4-byte ID roundtrip failed for 0x{:X}", id);
            assert_eq!(consumed, 4);
        }
    }

    #[test]
    fn write_read_size_roundtrip() {
        let test_sizes: &[u64] = &[
            0,
            1,
            0x7E,
            127,
            128,
            0x3FFE,
            16383,
            16384,
            0x1FFFFE,
            0x0FFFFFFE,
            0x1_0000_0000,
        ];
        for &size in test_sizes {
            let mut buf = Vec::new();
            write_size(&mut buf, size).unwrap();
            let mut cursor = Cursor::new(&buf);
            let (read_back, _consumed) = read_size(&mut cursor).unwrap();
            assert_eq!(read_back, size, "size roundtrip failed for {}", size);
        }
    }

    #[test]
    fn write_read_uint_roundtrip() {
        let test_vals: &[u64] = &[
            0,
            1,
            127,
            255,
            256,
            0xFFFF,
            0xFF_FFFF,
            0xFFFF_FFFF,
            1_000_000_000_000,
        ];
        let test_id = EBML_VERSION;
        for &val in test_vals {
            let mut buf = Vec::new();
            write_uint(&mut buf, test_id, val).unwrap();
            let mut cursor = Cursor::new(&buf);
            let (id, _id_len) = read_id(&mut cursor).unwrap();
            assert_eq!(id, test_id);
            let (size, _) = read_size(&mut cursor).unwrap();
            let read_val = read_uint_val(&mut cursor, size as usize).unwrap();
            assert_eq!(read_val, val, "uint roundtrip failed for {}", val);
        }
    }

    #[test]
    fn write_read_string_roundtrip() {
        let test_strings = &[
            "",
            "matroska",
            "freemkv",
            "Hello, World!",
            "unicode: \u{1F600}",
        ];
        let test_id = EBML_DOC_TYPE;
        for &s in test_strings {
            let mut buf = Vec::new();
            write_string(&mut buf, test_id, s).unwrap();
            let mut cursor = Cursor::new(&buf);
            let (id, _) = read_id(&mut cursor).unwrap();
            assert_eq!(id, test_id);
            let (size, _) = read_size(&mut cursor).unwrap();
            let read_s = read_string_val(&mut cursor, size as usize).unwrap();
            assert_eq!(read_s, s, "string roundtrip failed for {:?}", s);
        }
    }

    #[test]
    fn write_read_float_roundtrip() {
        let test_vals: &[f64] = &[
            0.0,
            1.0,
            -1.0,
            std::f64::consts::PI,
            48000.0,
            7200000.0,
            f64::MIN,
            f64::MAX,
        ];
        let test_id = DURATION;
        for &val in test_vals {
            let mut buf = Vec::new();
            write_float(&mut buf, test_id, val).unwrap();
            let mut cursor = Cursor::new(&buf);
            let (id, _) = read_id(&mut cursor).unwrap();
            assert_eq!(id, test_id);
            let (size, _) = read_size(&mut cursor).unwrap();
            assert_eq!(size, 8);
            let read_val = read_float_val(&mut cursor, size as usize).unwrap();
            assert_eq!(
                read_val.to_bits(),
                val.to_bits(),
                "float roundtrip failed for {}",
                val
            );
        }
    }

    #[test]
    fn read_size_unknown_sentinel_all_widths() {
        // The all-ones VINT of each width is the EBML "unknown size" marker
        // and must read back as u64::MAX. write_size never emits the 5/6/7-byte
        // widths, so these are hand-crafted. Each entry is (bytes, expected_len).
        let cases: &[(&[u8], usize)] = &[
            // 1-byte: 0x80 | 0x7F
            (&[0xFF], 1),
            // 2-byte: 0x40 marker, value bits all 1
            (&[0x7F, 0xFF], 2),
            // 3-byte
            (&[0x3F, 0xFF, 0xFF], 3),
            // 4-byte
            (&[0x1F, 0xFF, 0xFF, 0xFF], 4),
            // 5-byte (0x08 marker)
            (&[0x0F, 0xFF, 0xFF, 0xFF, 0xFF], 5),
            // 6-byte (0x04 marker)
            (&[0x07, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 6),
            // 7-byte (0x02 marker)
            (&[0x03, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 7),
            // 8-byte (0x01 marker)
            (&[0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 8),
        ];
        for (bytes, expected_len) in cases {
            let mut cursor = Cursor::new(*bytes);
            let (size, consumed) = read_size(&mut cursor).unwrap();
            assert_eq!(
                size,
                u64::MAX,
                "all-ones {}-byte VINT should be unknown-size",
                expected_len
            );
            assert_eq!(consumed, *expected_len);
        }
    }

    #[test]
    fn read_size_concrete_5_6_7_byte_values() {
        // A non-sentinel 5/6/7-byte size must read back as its concrete value,
        // not be mistaken for unknown-size.
        // 5-byte: marker 0x08, value 0x01 (0x0800000001 with width bit only).
        let mut c = Cursor::new(&[0x08u8, 0x00, 0x00, 0x00, 0x01]);
        assert_eq!(read_size(&mut c).unwrap(), (1, 5));
        // 6-byte
        let mut c = Cursor::new(&[0x04u8, 0x00, 0x00, 0x00, 0x00, 0x05]);
        assert_eq!(read_size(&mut c).unwrap(), (5, 6));
        // 7-byte
        let mut c = Cursor::new(&[0x02u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09]);
        assert_eq!(read_size(&mut c).unwrap(), (9, 7));
    }

    #[test]
    fn read_size_rejects_zero_first_byte() {
        // b0 == 0x00 has no width marker — an over-long/invalid VINT. It must
        // be rejected, not silently treated as an 8-byte size.
        let mut c = Cursor::new(&[0x00u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        let e = read_size(&mut c).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn write_size_rejects_at_or_above_2_56() {
        // 2^56 cannot be encoded in the 7-payload-byte 8-byte VINT and must
        // error rather than silently truncate.
        let mut buf = Vec::new();
        let e = write_size(&mut buf, 0x0100_0000_0000_0000).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        // The largest encodable size still succeeds.
        let mut buf = Vec::new();
        write_size(&mut buf, 0x00FF_FFFF_FFFF_FFFE).unwrap();
        assert_eq!(buf.len(), 8);
    }

    #[test]
    fn unknown_size() {
        let mut buf = Vec::new();
        write_unknown_size(&mut buf).unwrap();
        assert_eq!(buf.len(), 8);
        assert_eq!(buf[0], 0x01);
        for &b in &buf[1..] {
            assert_eq!(
                b, 0xFF,
                "unknown size bytes should all be 0xFF after first byte"
            );
        }
        // Reading it back should yield u64::MAX
        let mut cursor = Cursor::new(&buf);
        let (size, consumed) = read_size(&mut cursor).unwrap();
        assert_eq!(size, u64::MAX);
        assert_eq!(consumed, 8);
    }

    // ============================================================
    // write_id — exact width selection per EBML element-ID ranges
    // (Matroska/EBML spec: an element ID is written verbatim; its
    // declared width is implied by the position of the leading 1 bit.
    // write_id must pick the minimal whole-byte encoding so the ID
    // round-trips and parsers see the same width.)
    // ============================================================

    #[test]
    fn write_id_exact_bytes_per_width() {
        // 1-byte ID (high bit set): emitted as a single byte verbatim.
        let mut b = Vec::new();
        write_id(&mut b, 0xA3).unwrap(); // SimpleBlock
        assert_eq!(b, [0xA3]);

        // The boundary just above 1 byte: 0x100 must be a 2-byte ID. A
        // mutation that widened the 1-byte branch (id <= 0x1FF) would drop
        // the high byte here.
        let mut b = Vec::new();
        write_id(&mut b, 0x0100).unwrap();
        assert_eq!(b, [0x01, 0x00]);

        // 2-byte ID written MSB-first.
        let mut b = Vec::new();
        write_id(&mut b, 0x4286).unwrap(); // EBMLVersion
        assert_eq!(b, [0x42, 0x86]);

        // 3-byte boundary: 0x1_0000 must be 3 bytes.
        let mut b = Vec::new();
        write_id(&mut b, 0x01_0000).unwrap();
        assert_eq!(b, [0x01, 0x00, 0x00]);

        // 3-byte ID (Language = 0x22B59C).
        let mut b = Vec::new();
        write_id(&mut b, 0x22_B59C).unwrap();
        assert_eq!(b, [0x22, 0xB5, 0x9C]);

        // 4-byte boundary: 0x100_0000 must be 4 bytes.
        let mut b = Vec::new();
        write_id(&mut b, 0x0100_0000).unwrap();
        assert_eq!(b, [0x01, 0x00, 0x00, 0x00]);

        // 4-byte ID (Segment = 0x18538067) MSB-first.
        let mut b = Vec::new();
        write_id(&mut b, 0x1853_8067).unwrap();
        assert_eq!(b, [0x18, 0x53, 0x80, 0x67]);
    }

    #[test]
    fn read_id_rejects_zero_first_byte() {
        // A first byte of 0x00 has no length marker in any of bits 7..4, so
        // read_id falls through to the else branch and must reject it (an
        // EBML ID wider than 4 bytes is not representable here). Otherwise the
        // parser would desync.
        let mut c = Cursor::new(&[0x00u8, 0x11, 0x22, 0x33]);
        let e = read_id(&mut c).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    }

    // ============================================================
    // write_uint — the SIZE byte must reflect the minimal big-endian
    // value width (1/2/3/4/8). The Matroska spec stores unsigned ints
    // big-endian with no leading-zero bytes; the declared element size
    // is exactly that width. A boundary bug would write the wrong size
    // and desync every following element.
    // ============================================================

    #[test]
    fn write_uint_size_byte_matches_value_width() {
        // (value, expected_size_byte, expected_payload)
        // size byte is a 1-byte VINT: 0x80 | len.
        let cases: &[(u64, u8, &[u8])] = &[
            (0x00, 0x81, &[0x00]),                          // 1 byte
            (0xFF, 0x81, &[0xFF]),                          // 1 byte (boundary high)
            (0x0100, 0x82, &[0x01, 0x00]),                  // 2 bytes (just over u8)
            (0xFFFF, 0x82, &[0xFF, 0xFF]),                  // 2 bytes (boundary high)
            (0x01_0000, 0x83, &[0x01, 0x00, 0x00]),         // 3 bytes
            (0xFF_FFFF, 0x83, &[0xFF, 0xFF, 0xFF]),         // 3 bytes (boundary high)
            (0x0100_0000, 0x84, &[0x01, 0x00, 0x00, 0x00]), // 4 bytes
            (0xFFFF_FFFF, 0x84, &[0xFF, 0xFF, 0xFF, 0xFF]), // 4 bytes (boundary high)
            // Just over u32 → jumps straight to 8 bytes (no 5/6/7 path).
            (
                0x1_0000_0000,
                0x88,
                &[0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
        ];
        let id = EBML_VERSION; // 2-byte ID 0x4286
        for (val, size_byte, payload) in cases {
            let mut buf = Vec::new();
            write_uint(&mut buf, id, *val).unwrap();
            assert_eq!(&buf[0..2], &[0x42, 0x86], "ID prefix for val {val:#x}");
            assert_eq!(buf[2], *size_byte, "size byte for val {val:#x}");
            assert_eq!(&buf[3..], *payload, "payload for val {val:#x}");
        }
    }

    #[test]
    fn write_uint_zero_is_one_byte_not_zero_length() {
        // EBML stores 0 as a single 0x00 byte (size 1), NOT a zero-length
        // element. A muxer reader expects to consume exactly one payload byte.
        let mut buf = Vec::new();
        write_uint(&mut buf, EBML_VERSION, 0).unwrap();
        // ID(2) + size(1=0x81) + one payload byte 0x00.
        assert_eq!(buf, [0x42, 0x86, 0x81, 0x00]);
    }

    // ============================================================
    // write_float — EBML floats here are always 8-byte IEEE-754 doubles,
    // big-endian (Matroska SamplingFrequency/Duration). size byte = 0x88.
    // ============================================================

    #[test]
    fn write_float_is_8_byte_big_endian_double() {
        let mut buf = Vec::new();
        write_float(&mut buf, DURATION, 48000.0).unwrap();
        // ID DURATION = 0x4489 (2 bytes), size = 0x88 (8), then BE f64.
        assert_eq!(&buf[0..2], &[0x44, 0x89]);
        assert_eq!(buf[2], 0x88, "float element must declare 8-byte size");
        assert_eq!(&buf[3..11], &48000.0f64.to_be_bytes());
        // The reader (4-byte path) must yield an f32-promoted value, while the
        // 8-byte path yields the exact double.
        let got = read_float_val(&mut Cursor::new(&buf[3..11]), 8).unwrap();
        assert_eq!(got.to_bits(), 48000.0f64.to_bits());
    }

    // ============================================================
    // write_string / write_binary — declared size must equal the byte
    // length (UTF-8 byte count, not char count) so the reader consumes
    // exactly the payload and no more.
    // ============================================================

    #[test]
    fn write_string_size_is_utf8_byte_count_not_char_count() {
        // "é" is 2 UTF-8 bytes; the size field must be 2, not 1.
        let mut buf = Vec::new();
        write_string(&mut buf, EBML_DOC_TYPE, "é").unwrap();
        assert_eq!(&buf[0..2], &[0x42, 0x82]); // DocType ID
        assert_eq!(buf[2], 0x80 | 2, "size must be UTF-8 byte length (2)");
        assert_eq!(&buf[3..], "é".as_bytes());
    }

    #[test]
    fn write_binary_declares_exact_length() {
        let data = [0xDE, 0xAD, 0xBE, 0xEF, 0x00];
        let mut buf = Vec::new();
        write_binary(&mut buf, CODEC_PRIVATE, &data).unwrap();
        // CODEC_PRIVATE id 0x63A2 (2 bytes), size 0x85 (len 5), then data.
        assert_eq!(&buf[0..2], &[0x63, 0xA2]);
        assert_eq!(buf[2], 0x80 | 5);
        assert_eq!(&buf[3..], &data);
    }

    // ============================================================
    // read_string_val — Matroska strings may be null-padded; the reader
    // strips trailing NULs but must preserve interior content and the
    // payload byte-count consumed.
    // ============================================================

    #[test]
    fn read_string_val_strips_only_trailing_nulls() {
        // "ab\0\0" → "ab"; interior content must not be touched.
        let raw = b"ab\0\0";
        let s = read_string_val(&mut Cursor::new(raw), raw.len()).unwrap();
        assert_eq!(s, "ab");
        // A string that is ALL nulls collapses to empty (every byte popped).
        let raw = b"\0\0\0";
        let s = read_string_val(&mut Cursor::new(raw), raw.len()).unwrap();
        assert_eq!(s, "");
        // An interior NUL is NOT a terminator for the strip loop (it only pops
        // from the tail), so "a\0b" keeps the interior NUL.
        let raw = b"a\0b";
        let s = read_string_val(&mut Cursor::new(raw), raw.len()).unwrap();
        assert_eq!(s.as_bytes(), b"a\0b");
    }

    // ============================================================
    // read_uint_val — big-endian assembly; an EBML uint never exceeds 8
    // bytes (the reader rejects len>8 to avoid a stack OOB).
    // ============================================================

    #[test]
    fn read_uint_val_big_endian_and_len_zero() {
        // Big-endian: 0x01 0x02 0x03 → 0x010203.
        let v = read_uint_val(&mut Cursor::new(&[0x01u8, 0x02, 0x03]), 3).unwrap();
        assert_eq!(v, 0x01_0203);
        // len 0 yields 0 with no read.
        let v = read_uint_val(&mut Cursor::new(&[] as &[u8]), 0).unwrap();
        assert_eq!(v, 0);
        // Full 8-byte width assembles correctly (no truncation).
        let bytes = [0x12u8, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];
        let v = read_uint_val(&mut Cursor::new(&bytes), 8).unwrap();
        assert_eq!(v, 0x1234_5678_9ABC_DEF0);
    }

    #[test]
    fn read_uint_val_rejects_len_above_8() {
        // len 9 would index past the [0u8; 8] buffer → OOB/DoS on untrusted
        // input. Must be a clean MkvInvalid.
        let e = read_uint_val(&mut Cursor::new(&[0u8; 16]), 9).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    }

    // ============================================================
    // read_float_val — exactly 0/4/8 byte widths; 4-byte is an f32
    // promoted to f64, 8-byte is an exact f64.
    // ============================================================

    #[test]
    fn read_float_val_4_byte_is_f32_promoted() {
        // 1.5 as a 32-bit float → 0x3FC00000.
        let bytes = 1.5f32.to_be_bytes();
        let v = read_float_val(&mut Cursor::new(&bytes), 4).unwrap();
        assert_eq!(v, 1.5f64);
        // A value with no exact f32 representation loses precision exactly as
        // f32→f64 would (proves the 4-byte branch uses f32, not f64).
        let bytes = 0.1f32.to_be_bytes();
        let v = read_float_val(&mut Cursor::new(&bytes), 4).unwrap();
        assert_eq!(v, 0.1f32 as f64);
        assert_ne!(v, 0.1f64, "4-byte path must be f32, losing f64 precision");
    }

    #[test]
    fn read_float_val_rejects_odd_widths() {
        // Only 0/4/8 are valid; 1,2,3,5,6,7 must error (never over/under-read).
        for len in [1usize, 2, 3, 5, 6, 7] {
            let e = read_float_val(&mut Cursor::new(&[0u8; 8]), len).unwrap_err();
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "len {len}");
        }
    }

    // ============================================================
    // read_binary_val / read_exact_bounded — a declared length that
    // exceeds the bytes actually present is a truncated (malformed)
    // element and must error without allocating the full declared size.
    // ============================================================

    #[test]
    fn read_binary_val_short_read_errors() {
        // Declare 100 bytes but supply 4 → MkvInvalid (truncated element).
        let e = read_binary_val(&mut Cursor::new(&[1u8, 2, 3, 4]), 100).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        // Exact-length read returns the bytes verbatim.
        let v = read_binary_val(&mut Cursor::new(&[1u8, 2, 3, 4]), 4).unwrap();
        assert_eq!(v, vec![1, 2, 3, 4]);
    }

    // ============================================================
    // read_element_header — header_bytes is id_len + size_len, and a
    // truncated header (EOF mid-size) surfaces as an error.
    // ============================================================

    #[test]
    fn read_element_header_reports_total_header_len() {
        // 4-byte ID (Segment) + 8-byte unknown size = 12 header bytes.
        let mut buf = Vec::new();
        write_id(&mut buf, SEGMENT).unwrap();
        write_unknown_size(&mut buf).unwrap();
        let (id, size, hdr) = read_element_header(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(id, SEGMENT);
        assert_eq!(size, u64::MAX);
        assert_eq!(hdr, 12, "4-byte id + 8-byte size = 12 header bytes");

        // 1-byte ID (SimpleBlock 0xA3) + 1-byte size = 2 header bytes.
        let mut buf = Vec::new();
        write_id(&mut buf, SIMPLE_BLOCK).unwrap();
        write_size(&mut buf, 10).unwrap();
        let (id, size, hdr) = read_element_header(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(id, SIMPLE_BLOCK);
        assert_eq!(size, 10);
        assert_eq!(hdr, 2);
    }

    #[test]
    fn read_id_truncated_after_marker_errors() {
        // First byte 0x40 promises a 2-byte ID but the second byte is missing.
        // read_exact must surface EOF, never silently produce a 1-byte ID.
        let e = read_id(&mut Cursor::new(&[0x40u8])).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
    }

    // ============================================================
    // write_size — every declared width-boundary, asserting the exact
    // VINT bytes (length marker + payload). Grounded in the EBML VINT
    // spec: width W encodes 7*W payload bits, the highest value of each
    // width being reserved as the unknown-size sentinel.
    // ============================================================

    #[test]
    fn write_size_exact_bytes_at_width_boundaries() {
        // Largest 1-byte value (126 = 0x7E): marker 0x80 | value.
        let mut b = Vec::new();
        write_size(&mut b, 0x7E).unwrap();
        assert_eq!(b, [0x80 | 0x7E]);
        // 0x7F is NOT 1-byte here (reserved sentinel region) → 2 bytes.
        let mut b = Vec::new();
        write_size(&mut b, 0x7F).unwrap();
        assert_eq!(b, [0x40, 0x7F]);
        // Largest 2-byte value below the 0x3FFF sentinel.
        let mut b = Vec::new();
        write_size(&mut b, 0x3FFE).unwrap();
        assert_eq!(b, [0x40 | 0x3F, 0xFE]);
        // First 3-byte value (0x3FFF goes 3-byte because `< 0x3FFF` is false).
        let mut b = Vec::new();
        write_size(&mut b, 0x3FFF).unwrap();
        assert_eq!(b, [0x20, 0x3F, 0xFF]);
        // First 4-byte value: 0x1F_FFFF is not < 0x1F_FFFF.
        let mut b = Vec::new();
        write_size(&mut b, 0x1F_FFFF).unwrap();
        assert_eq!(b, [0x10, 0x1F, 0xFF, 0xFF]);
        // First 8-byte value: 0x0FFF_FFFF is not < 0x0FFF_FFFF.
        let mut b = Vec::new();
        write_size(&mut b, 0x0FFF_FFFF).unwrap();
        assert_eq!(b, [0x01, 0, 0, 0, 0x0F, 0xFF, 0xFF, 0xFF]);
    }

    // ============================================================
    // start_master / end_master — the size placeholder is an 8-byte VINT
    // (0x01 + 7 payload bytes), and end_master must back-patch the exact
    // body byte count (end - start - 8). This is the core of every nested
    // Matroska master element; a wrong subtraction silently corrupts the
    // declared size of EVERY master element in the file.
    // ============================================================

    #[test]
    fn end_master_backpatches_exact_body_size() {
        let mut c = Cursor::new(Vec::new());
        let pos = start_master(&mut c, SEGMENT).unwrap();
        // Body: a 4-byte uint element (ID 0x4286, size 0x81, payload 0x01).
        write_uint(&mut c, EBML_VERSION, 1).unwrap();
        end_master(&mut c, pos).unwrap();
        let data = c.into_inner();
        // Layout: SEGMENT id (4 bytes) | 8-byte size VINT | body (4 bytes).
        assert_eq!(&data[0..4], &SEGMENT.to_be_bytes());
        // The size field is an 8-byte VINT; its payload must equal the body
        // length (4). 0x01 marker then 7 payload bytes ending in 0x04.
        assert_eq!(data[4], 0x01);
        assert_eq!(&data[5..12], &[0, 0, 0, 0, 0, 0, 4]);
        // Read it back: the header parser sees the exact body size.
        let (id, size, hdr) = read_element_header(&mut Cursor::new(&data)).unwrap();
        assert_eq!(id, SEGMENT);
        assert_eq!(size, 4, "back-patched size must equal body byte count");
        assert_eq!(hdr, 12);
        assert_eq!(data.len() as u64, hdr as u64 + size);
    }

    #[test]
    fn end_master_empty_body_is_zero_size() {
        // A master with no body must declare size 0 (end == start + 8).
        let mut c = Cursor::new(Vec::new());
        let pos = start_master(&mut c, INFO).unwrap();
        end_master(&mut c, pos).unwrap();
        let data = c.into_inner();
        let (id, size, _) = read_element_header(&mut Cursor::new(&data)).unwrap();
        assert_eq!(id, INFO);
        assert_eq!(size, 0);
    }

    #[test]
    fn nested_masters_each_get_correct_size() {
        // Outer master containing an inner master + a sibling uint. Each
        // declared size must bound exactly its own body. This is the nested
        // sizing that mkv.rs relies on for Segment→Tracks→TrackEntry.
        let mut c = Cursor::new(Vec::new());
        let outer = start_master(&mut c, TRACKS).unwrap();
        let inner = start_master(&mut c, TRACK_ENTRY).unwrap();
        write_uint(&mut c, TRACK_NUMBER, 1).unwrap();
        end_master(&mut c, inner).unwrap();
        write_uint(&mut c, TRACK_NUMBER, 2).unwrap();
        end_master(&mut c, outer).unwrap();
        let data = c.into_inner();

        let mut cur = Cursor::new(&data);
        let (oid, osize, _) = read_element_header(&mut cur).unwrap();
        assert_eq!(oid, TRACKS);
        let outer_body_start = cur.position();
        // First child of TRACKS is TRACK_ENTRY.
        let (iid, isize, _) = read_element_header(&mut cur).unwrap();
        assert_eq!(iid, TRACK_ENTRY);
        // Skip TRACK_ENTRY body; the next element must be the sibling uint.
        cur.set_position(cur.position() + isize);
        let (sid, ssize, _) = read_element_header(&mut cur).unwrap();
        assert_eq!(sid, TRACK_NUMBER, "sibling after inner master");
        // Skip the sibling's body too, then total bytes consumed inside the
        // outer master must exactly equal its declared size.
        cur.set_position(cur.position() + ssize);
        let consumed = cur.position() - outer_body_start;
        assert_eq!(consumed, osize, "outer size must bound both children");
        // And the whole buffer is exactly the outer element.
        assert_eq!(data.len() as u64, outer_body_start + osize);
    }
}
