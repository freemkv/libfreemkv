//! EBML (Extensible Binary Meta Language) write primitives for Matroska.
//!
//! EBML uses variable-length integers for element IDs and sizes.
//! This module provides low-level writers for constructing MKV files.

use std::io::{self, Write, Seek, SeekFrom};

/// Write an EBML element ID (1-4 bytes, already encoded).
/// Element IDs are predefined constants — we write them verbatim.
pub fn write_id(w: &mut impl Write, id: u32) -> io::Result<()> {
    if id <= 0x7F {
        w.write_all(&[id as u8])
    } else if id <= 0x7FFF {
        w.write_all(&[(id >> 8) as u8, id as u8])
    } else if id <= 0x7F_FFFF {
        w.write_all(&[(id >> 16) as u8, (id >> 8) as u8, id as u8])
    } else {
        w.write_all(&[(id >> 24) as u8, (id >> 16) as u8, (id >> 8) as u8, id as u8])
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
        w.write_all(&[
            ((size >> 16) as u8) | 0x20,
            (size >> 8) as u8,
            size as u8,
        ])
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
            (val >> 24) as u8, (val >> 16) as u8,
            (val >> 8) as u8, val as u8,
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
}
