//! ISO-BMFF box primitives: `[size:u32-BE][type:4][body]` (ISO/IEC 14496-12
//! §4.2), and the FullBox variant that prefixes a 1-byte version + 3-byte flags.

/// Wrap a body in a plain box `[size][type][body]`. `size` counts the 8-byte
/// header. All `moov`-tree boxes are small (the large `mdat` is written directly
/// with a 64-bit size, not through here), so a `u32` size never overflows.
pub(super) fn bx(box_type: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let total = body.len() + 8;
    debug_assert!(
        total <= u32::MAX as usize,
        "mp4 box {box_type:?} exceeds u32"
    );
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&(total as u32).to_be_bytes());
    out.extend_from_slice(box_type);
    out.extend_from_slice(body);
    out
}

/// Wrap a body in a FullBox: `[size][type][version:1][flags:3][body]`.
pub(super) fn fullbox(box_type: &[u8; 4], version: u8, flags: u32, body: &[u8]) -> Vec<u8> {
    let mut full = Vec::with_capacity(body.len() + 4);
    full.push(version);
    full.extend_from_slice(&flags.to_be_bytes()[1..]); // low 3 bytes
    full.extend_from_slice(body);
    bx(box_type, &full)
}
