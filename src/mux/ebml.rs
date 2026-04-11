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

/// Write a complete EBML signed integer element.
pub fn write_int(w: &mut impl Write, id: u32, val: i64) -> io::Result<()> {
    write_uint(w, id, val as u64)
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
pub fn end_master<W: Write + Seek>(w: &mut W, size_pos: u64) -> io::Result<()> {
    let end_pos = w.stream_position()?;
    let data_size = end_pos - size_pos - 8; // subtract the 8-byte size field itself
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
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid EBML ID",
        ))
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
        if val == 0x1FFFFF {
            return Ok((u64::MAX, 3));
        }
        Ok((val, 3))
    } else if b0 & 0x10 != 0 {
        let mut b = [0u8; 3];
        r.read_exact(&mut b)?;
        let val =
            (((b0 & 0x0F) as u64) << 24) | (b[0] as u64) << 16 | (b[1] as u64) << 8 | b[2] as u64;
        if val == 0x0FFFFFFF {
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
        Ok((val, 7))
    } else {
        let mut b = [0u8; 7];
        r.read_exact(&mut b)?;
        let val = (b[0] as u64) << 48
            | (b[1] as u64) << 40
            | (b[2] as u64) << 32
            | (b[3] as u64) << 24
            | (b[4] as u64) << 16
            | (b[5] as u64) << 8
            | b[6] as u64;
        if val == 0x00FFFFFFFFFFFFFF {
            return Ok((u64::MAX, 8));
        }
        Ok((val, 8))
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
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf[..len])?;
    let mut val = 0u64;
    for &b in &buf[..len] {
        val = (val << 8) | b as u64;
    }
    Ok(val)
}

/// Read a float value (4 or 8 bytes).
pub fn read_float_val(r: &mut impl Read, len: usize) -> io::Result<f64> {
    if len == 4 {
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf)?;
        Ok(f32::from_be_bytes(buf) as f64)
    } else {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }
}

/// Read a UTF-8 string value of `len` bytes.
pub fn read_string_val(r: &mut impl Read, len: usize) -> io::Result<String> {
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    // Strip trailing nulls
    while buf.last() == Some(&0) {
        buf.pop();
    }
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read binary data of `len` bytes.
pub fn read_binary_val(r: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read a VINT (track number) from a SimpleBlock. Returns (value, bytes_consumed).
pub fn read_vint(r: &mut impl Read) -> io::Result<(u64, usize)> {
    let mut first = [0u8; 1];
    r.read_exact(&mut first)?;
    let b0 = first[0];
    if b0 & 0x80 != 0 {
        return Ok(((b0 & 0x7F) as u64, 1));
    }
    if b0 & 0x40 != 0 {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        return Ok(((((b0 & 0x3F) as u64) << 8) | b[0] as u64, 2));
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "unsupported VINT width",
    ))
}

// ============================================================
// Matroska Element IDs
// ============================================================

// EBML Header
pub const EBML: u32 = 0x1A45DFA3;
pub const EBML_VERSION: u32 = 0x4286;
pub const EBML_READ_VERSION: u32 = 0x42F7;
pub const EBML_MAX_ID_LENGTH: u32 = 0x42F2;
pub const EBML_MAX_SIZE_LENGTH: u32 = 0x42F3;
pub const EBML_DOC_TYPE: u32 = 0x4282;
pub const EBML_DOC_TYPE_VERSION: u32 = 0x4287;
pub const EBML_DOC_TYPE_READ_VERSION: u32 = 0x4285;

// Segment
pub const SEGMENT: u32 = 0x18538067;

// Seek Head
pub const SEEK_HEAD: u32 = 0x114D9B74;
pub const SEEK: u32 = 0x4DBB;
pub const SEEK_ID: u32 = 0x53AB;
pub const SEEK_POSITION: u32 = 0x53AC;

// Segment Info
pub const INFO: u32 = 0x1549A966;
pub const TIMESTAMP_SCALE: u32 = 0x2AD7B1;
pub const DURATION: u32 = 0x4489;
pub const MUXING_APP: u32 = 0x4D80;
pub const WRITING_APP: u32 = 0x5741;
pub const TITLE: u32 = 0x7BA9;

// Tracks
pub const TRACKS: u32 = 0x1654AE6B;
pub const TRACK_ENTRY: u32 = 0xAE;
pub const TRACK_NUMBER: u32 = 0xD7;
pub const TRACK_UID: u32 = 0x73C5;
pub const TRACK_TYPE: u32 = 0x83;
pub const FLAG_LACING: u32 = 0x9C;
pub const FLAG_DEFAULT: u32 = 0x88;
pub const FLAG_FORCED: u32 = 0x55AA;
pub const LANGUAGE: u32 = 0x22B59C;
pub const CODEC_ID: u32 = 0x86;
pub const CODEC_PRIVATE: u32 = 0x63A2;
pub const TRACK_NAME: u32 = 0x536E;
pub const DEFAULT_DURATION: u32 = 0x23E383;

// Video
pub const VIDEO: u32 = 0xE0;
pub const PIXEL_WIDTH: u32 = 0xB0;
pub const PIXEL_HEIGHT: u32 = 0xBA;
pub const DISPLAY_WIDTH: u32 = 0x54B0;
pub const DISPLAY_HEIGHT: u32 = 0x54BA;
pub const COLOUR: u32 = 0x55B0;
pub const TRANSFER_CHARACTERISTICS: u32 = 0x55BA;
pub const MATRIX_COEFFICIENTS: u32 = 0x55B1;
pub const PRIMARIES: u32 = 0x55BB;
pub const RANGE: u32 = 0x55B9;

// Audio
pub const AUDIO: u32 = 0xE1;
pub const SAMPLING_FREQUENCY: u32 = 0xB5;
pub const CHANNELS: u32 = 0x9F;
pub const BIT_DEPTH: u32 = 0x6264;

// Cluster
pub const CLUSTER: u32 = 0x1F43B675;
pub const CLUSTER_TIMESTAMP: u32 = 0xE7;
pub const SIMPLE_BLOCK: u32 = 0xA3;

// Cues
pub const CUES: u32 = 0x1C53BB6B;
pub const CUE_POINT: u32 = 0xBB;
pub const CUE_TIME: u32 = 0xB3;
pub const CUE_TRACK_POSITIONS: u32 = 0xB7;
pub const CUE_TRACK: u32 = 0xF7;
pub const CUE_CLUSTER_POSITION: u32 = 0xF1;

// Chapters
pub const CHAPTERS: u32 = 0x1043A770;
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
}
