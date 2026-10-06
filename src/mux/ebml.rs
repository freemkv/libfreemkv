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
        // Max 56-bit value encodes identical to write_unknown_size's all-ones
        // sentinel; reject so a finite size can never be mistaken for it.
        Err(crate::error::Error::MkvUnencodable.into())
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

/// Write a complete EBML signed-integer element (two's-complement, big-endian,
/// minimal width). Used for `ReferenceBlock` (0xFB), whose value is a signed
/// tick offset relative to the current block's timestamp.
pub fn write_int(w: &mut impl Write, id: u32, val: i64) -> io::Result<()> {
    write_id(w, id)?;
    // Minimal two's-complement width: shrink while the top byte is pure sign
    // extension of the next byte's MSB.
    let be = val.to_be_bytes();
    let mut start = 0usize;
    while start < 7 {
        let sign_ext = if be[start + 1] & 0x80 != 0 {
            0xFF
        } else {
            0x00
        };
        if be[start] != sign_ext {
            break;
        }
        start += 1;
    }
    let bytes = &be[start..];
    write_size(w, bytes.len() as u64)?;
    w.write_all(bytes)
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

// Fixed-width 8-octet EBML VINT (0x01 marker + 56-bit big-endian payload, RFC 8794 §4.4) used
// to back-patch a master element's size field.
fn fixed_width_vint8(data_size: u64) -> [u8; 8] {
    debug_assert!(data_size < 0x0100_0000_0000_0000);
    [
        0x01,
        (data_size >> 48) as u8,
        (data_size >> 40) as u8,
        (data_size >> 32) as u8,
        (data_size >> 24) as u8,
        (data_size >> 16) as u8,
        (data_size >> 8) as u8,
        data_size as u8,
    ]
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
    w.write_all(&fixed_width_vint8(data_size))?;
    w.seek(SeekFrom::Start(end_pos))?;
    Ok(())
}

/// Start a master element **in an in-memory buffer**: append ID + the same
/// 8-byte size placeholder [`start_master`] writes. Returns the buffer index of
/// the size field, for [`end_master_buf`].
///
/// This is the seek-free twin of [`start_master`]/[`end_master`], producing byte-for-byte
/// identical output.
pub fn start_master_buf(buf: &mut Vec<u8>, id: u32) -> io::Result<usize> {
    write_id(buf, id)?;
    let size_pos = buf.len();
    // 8-byte size placeholder (overwritten by end_master_buf)
    buf.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    Ok(size_pos)
}

/// End a master element in an in-memory buffer: patch the 8-byte size field at
/// `size_pos` (as returned by [`start_master_buf`]) with the body length.
///
/// Errors rather than panicking if `size_pos` does not name a placeholder that
/// is still inside the buffer, or if the body exceeds the 7-byte VINT payload.
pub fn end_master_buf(buf: &mut [u8], size_pos: usize) -> io::Result<()> {
    let end = buf.len();
    let Some(body_start) = size_pos.checked_add(8) else {
        return Err(crate::error::Error::MkvUnencodable.into());
    };
    if end < body_start {
        return Err(crate::error::Error::MkvUnencodable.into());
    }
    let data_size = (end - body_start) as u64;
    if data_size >= 0x0100_0000_0000_0000 {
        return Err(crate::error::Error::MkvUnencodable.into());
    }
    buf[size_pos..body_start].copy_from_slice(&fixed_width_vint8(data_size));
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
        Err(crate::error::Error::MkvSourceInvalid.into())
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
        // b0 == 0x00: no length marker, meaning a VINT wider than 8 bytes, which
        // Matroska can't represent. Reject rather than desync the parse.
        Err(crate::error::Error::MkvSourceInvalid.into())
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
    // len > 8 would index past this stack buffer and panic (DoS on untrusted
    // input); reject at the source so every caller is safe, not just pre-checked ones.
    if len > 8 {
        return Err(crate::error::Error::MkvSourceInvalid.into());
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
/// length is rejected as [`Error::MkvSourceInvalid`](crate::Error::MkvSourceInvalid) and exactly the float width is
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
        _ => Err(crate::error::Error::MkvSourceInvalid.into()),
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
    // A non-UTF-8 string element is malformed input → MkvSourceInvalid.
    String::from_utf8(buf).map_err(|_| crate::error::Error::MkvSourceInvalid.into())
}

/// Read binary data of `len` bytes.
pub fn read_binary_val(r: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    read_exact_bounded(r, len)
}

// Reads exactly `len` bytes without trusting `len` to size the allocation up front (an
// attacker-controlled EBML size could claim gigabytes).
fn read_exact_bounded(r: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let got = r.take(len as u64).read_to_end(&mut buf)?;
    if got != len {
        // A truncated element is malformed input. Use the typed crate error
        // so callers matching on Error::MkvSourceInvalid catch short reads rather
        // than a bare io::ErrorKind that bypasses the numeric-code identity.
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    Ok(buf)
}

// Matroska Element IDs

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

/// A Void element (0xEC) that occupies EXACTLY `total_len` bytes in place: 1-byte
/// id + 1-byte size VINT + zeroed payload. Used to neutralise a fixed element
/// without shifting the bytes after it. `total_len` must be in `2..=128`: a
/// 127-byte payload would need size byte 0xFF, the reserved unknown-size VINT.
pub fn void_element(total_len: usize) -> io::Result<Vec<u8>> {
    if !(2..=128).contains(&total_len) {
        return Err(crate::error::Error::MkvUnencodable.into());
    }
    let mut v = vec![VOID as u8, 0x80 | (total_len - 2) as u8];
    v.resize(total_len, 0);
    Ok(v)
}
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
// FieldOrder (RFC 9559 element 0x9D): 1 = TFF, 6 = BFF, 0 = progressive. Falls
// back to TFF when top_field_first isn't measured (most interlaced content is TFF).
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
// HDR10 static metadata — children of COLOUR (RFC 9559 / Matroska spec).
//
// MaxCLL / MaxFALL are direct children of Colour and are UINTs (cd/m²).
pub const MAX_CLL: u32 = 0x55BC;
pub const MAX_FALL: u32 = 0x55BD;
// MasteringMetadata is a master child of Colour; its chromaticity / luminance
// children are EBML FLOATs. Chromaticity values are in the 0..1 range; the
// luminance values are in cd/m².
pub const MASTERING_METADATA: u32 = 0x55D0;
pub const PRIMARY_R_CHROMATICITY_X: u32 = 0x55D1;
pub const PRIMARY_R_CHROMATICITY_Y: u32 = 0x55D2;
pub const PRIMARY_G_CHROMATICITY_X: u32 = 0x55D3;
pub const PRIMARY_G_CHROMATICITY_Y: u32 = 0x55D4;
pub const PRIMARY_B_CHROMATICITY_X: u32 = 0x55D5;
pub const PRIMARY_B_CHROMATICITY_Y: u32 = 0x55D6;
pub const WHITE_POINT_CHROMATICITY_X: u32 = 0x55D7;
pub const WHITE_POINT_CHROMATICITY_Y: u32 = 0x55D8;
pub const LUMINANCE_MAX: u32 = 0x55D9;
pub const LUMINANCE_MIN: u32 = 0x55DA;

// Dolby Vision — BlockAdditionMapping carries the DOVIDecoderConfigurationRecord
// (dvcC) so players / mediainfo recognise the track as Dolby Vision.
pub const BLOCK_ADDITION_MAPPING: u32 = 0x41E4;
pub const BLOCK_ADD_ID_TYPE: u32 = 0x41E7;
pub const BLOCK_ADD_ID_EXTRA_DATA: u32 = 0x41ED;
/// BlockAddIDValue (RFC 9559) — the value a per-frame `BlockAddID` references
/// to select this BlockAdditionMapping. Values ≥ 2 (1 is the default plain
/// BlockAdditional). Used by the MVC (`mvcC`) mapping for Blu-ray 3D.
pub const BLOCK_ADD_ID_VALUE: u32 = 0x41F0;
/// MaxBlockAdditionID (RFC 9559 5.1.4.1.16): highest BlockAddID used on the track.
pub const MAX_BLOCK_ADDITION_ID: u32 = 0x55EE;

// Block additions inside a BlockGroup — per-frame side data. For Blu-ray 3D (MVC),
// dependent (right-eye) NAL units ride as a BlockAdditional under `mvcC` (RFC 9559
// §5.1.4.1.4; Matroska Codec Specifications §4.1.5).
pub const BLOCK_ADDITIONS: u32 = 0x75A1;
pub const BLOCK_MORE: u32 = 0xA6;
pub const BLOCK_ADDITIONAL: u32 = 0xA5;
pub const BLOCK_ADD_ID: u32 = 0xEE;
/// ReferenceBlock (RFC 9559 element 0xFB, child of BlockGroup) — signed
/// timestamp (in TimestampScale ticks) of a block this one references, relative
/// to this block's own timestamp. Its PRESENCE marks the Block as non-keyframe
/// (a keyframe Block in a BlockGroup carries none). Written for non-keyframe
/// video frames that must live in a BlockGroup to carry an MVC BlockAdditional.
pub const REFERENCE_BLOCK: u32 = 0xFB;
/// DiscardPadding (RFC 9559 §5.1.3.5.7): signed ns of decoded audio to drop.
pub const DISCARD_PADDING: u32 = 0x75A2;
/// CodecDelay / SeekPreRoll (RFC 9559 §5.1.4.1.18/19), TrackEntry children, ns.
pub const CODEC_DELAY: u32 = 0x56AA;
pub const SEEK_PRE_ROLL: u32 = 0x56BB;

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

// Tags — mkvmerge convention: a `BPS` SimpleTag per track carries bits-per-second
// so readers relying on the container tag (e.g. Explorer's MKV handler) show a
// bitrate for every track, not just CBR audio.
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
pub const EDITION_FLAG_DEFAULT: u32 = 0x45DB;
pub const CHAPTER_FLAG_HIDDEN: u32 = 0x98;
pub const CHAPTER_FLAG_ENABLED: u32 = 0x4598;
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
/// MPEG-1 Video. Distinct from V_MPEG2: a decoder selects its bitstream
/// parser from this ID.
pub const CODEC_MPEG1: &str = "V_MPEG1";
/// AV1. CodecPrivate carries the AV1CodecConfigurationRecord.
pub const CODEC_AV1: &str = "V_AV1";
pub const CODEC_AC3: &str = "A_AC3";
pub const CODEC_EAC3: &str = "A_EAC3";
pub const CODEC_TRUEHD: &str = "A_TRUEHD";
pub const CODEC_DTS: &str = "A_DTS";
pub const CODEC_PCM_BE: &str = "A_PCM/INT/BIG";
/// Little-endian integer PCM (read side only; converted to big-endian).
pub const CODEC_PCM_LE: &str = "A_PCM/INT/LIT";
/// AAC. The generic registered ID; the AudioSpecificConfig travels in
/// CodecPrivate, so no profile suffix is needed (and the `A_AAC/MPEG4/*`
/// suffixed forms are legacy).
pub const CODEC_AAC: &str = "A_AAC";
/// MPEG-1/2 Audio Layer II — DVD audio_coding_mode 3.
pub const CODEC_MP2: &str = "A_MPEG/L2";
/// MPEG-1/2 Audio Layer III.
pub const CODEC_MP3: &str = "A_MPEG/L3";
/// FLAC. CodecPrivate carries the STREAMINFO metadata block.
pub const CODEC_FLAC: &str = "A_FLAC";
/// Opus. CodecPrivate carries the OpusHead identification header.
pub const CODEC_OPUS: &str = "A_OPUS";
pub const CODEC_PGS: &str = "S_HDMV/PGS";
pub const CODEC_VOBSUB: &str = "S_VOBSUB";

#[cfg(test)]
#[path = "ebml_tests.rs"]
mod tests;
