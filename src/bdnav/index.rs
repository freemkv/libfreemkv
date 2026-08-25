//! Parse `/BDMV/index.bdmv` — the BD navigation index (First Play, Top Menu,
//! and the title table), per its documented binary layout. This is read as a
//! documented binary format, never executed: every field is bounds-checked and
//! any malformed input yields `None` (the nav resolver then abstains).
//!
//! Layout: `"INDX"` + version(4) + `indexes_start`(u32 @8) + … ; at
//! `indexes_start`: `index_len`(u32), First-Play object(12), Top-Menu object(12),
//! `num_titles`(u16), then `num_titles` title objects(12 each). Every object's
//! `object_type` is the top two bits of its first byte; for an HDMV object the
//! `id_ref` into `MovieObject.bdmv` is a big-endian u16 at object offset 6.

/// One playback/title object in `index.bdmv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaybackObj {
    /// HDMV title. `id_ref` indexes `MovieObject.bdmv`; `0xffff` means "no
    /// object".
    Hdmv { id_ref: u16 },
    /// BD-J title (a Java Xlet chooses what plays — the nav VM cannot resolve
    /// it and must abstain).
    BdJ,
    /// Unrecognised object type.
    Unknown,
}

/// The parsed index: the two entry objects plus the title table (title numbers
/// `1..=titles.len()` map to `titles[i-1]`; title 0 is the Top Menu).
#[derive(Debug, Clone)]
pub(crate) struct Index {
    pub first_play: PlaybackObj,
    pub top_menu: PlaybackObj,
    pub titles: Vec<PlaybackObj>,
}

const OBJ_LEN: usize = 12;
/// Sanity cap on the title count (real discs have well under this).
const MAX_TITLES: usize = 4096;

fn be_u16(d: &[u8], o: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*d.get(o)?, *d.get(o + 1)?]))
}
fn be_u32(d: &[u8], o: usize) -> Option<u32> {
    Some(u32::from_be_bytes([
        *d.get(o)?,
        *d.get(o + 1)?,
        *d.get(o + 2)?,
        *d.get(o + 3)?,
    ]))
}

/// Parse one 12-byte object starting at `o`. `object_type` is the top two bits
/// of byte 0; the object body starts at byte 4, and an HDMV `id_ref` is the
/// big-endian u16 at body offset +2 (object offset +6).
fn parse_obj(d: &[u8], o: usize) -> Option<PlaybackObj> {
    let b0 = *d.get(o)?;
    d.get(o + OBJ_LEN - 1)?; // require the whole 12-byte record
    Some(match (b0 >> 6) & 0x3 {
        1 => PlaybackObj::Hdmv {
            id_ref: be_u16(d, o + 6)?,
        },
        2 => PlaybackObj::BdJ,
        _ => PlaybackObj::Unknown,
    })
}

/// Parse `index.bdmv`. Returns `None` on any structural problem.
pub(crate) fn parse(d: &[u8]) -> Option<Index> {
    if d.get(0..4)? != b"INDX" {
        return None;
    }
    // version at 4..8 ("0100"/"0200"/"0300") is not load-bearing for resolution.
    let indexes_start = be_u32(d, 8)? as usize;
    // At indexes_start: u32 index_len, First-Play(12), Top-Menu(12), u16 titles.
    let mut o = indexes_start.checked_add(4)?;
    let first_play = parse_obj(d, o)?;
    o = o.checked_add(OBJ_LEN)?;
    let top_menu = parse_obj(d, o)?;
    o = o.checked_add(OBJ_LEN)?;
    let num_titles = be_u16(d, o)? as usize;
    o = o.checked_add(2)?;
    if num_titles == 0 || num_titles > MAX_TITLES {
        return None;
    }
    let span = num_titles.checked_mul(OBJ_LEN)?;
    if o.checked_add(span)? > d.len() {
        return None;
    }
    let mut titles = Vec::with_capacity(num_titles);
    for i in 0..num_titles {
        titles.push(parse_obj(d, o + i * OBJ_LEN)?);
    }
    Some(Index {
        first_play,
        top_menu,
        titles,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a 12-byte object record: `object_type` in the top two bits of
    /// byte 0, HDMV `id_ref` big-endian at offset 6.
    fn hdmv_obj(id_ref: u16) -> [u8; 12] {
        let mut b = [0u8; 12];
        b[0] = 1 << 6;
        b[6..8].copy_from_slice(&id_ref.to_be_bytes());
        b
    }
    fn bdj_obj() -> [u8; 12] {
        let mut b = [0u8; 12];
        b[0] = 2 << 6;
        b
    }

    fn build(first: [u8; 12], top: [u8; 12], titles: &[[u8; 12]]) -> Vec<u8> {
        let indexes_start = 48u32;
        let mut d = vec![0u8; indexes_start as usize];
        d[0..4].copy_from_slice(b"INDX");
        d[4..8].copy_from_slice(b"0300");
        d[8..12].copy_from_slice(&indexes_start.to_be_bytes());
        d.extend_from_slice(&0u32.to_be_bytes()); // index_len (unused by parser)
        d.extend_from_slice(&first);
        d.extend_from_slice(&top);
        d.extend_from_slice(&(titles.len() as u16).to_be_bytes());
        for t in titles {
            d.extend_from_slice(t);
        }
        d
    }

    #[test]
    fn parses_hdmv_first_play_and_titles() {
        let d = build(hdmv_obj(0), bdj_obj(), &[bdj_obj(), bdj_obj()]);
        let idx = parse(&d).expect("parses");
        assert_eq!(idx.first_play, PlaybackObj::Hdmv { id_ref: 0 });
        assert_eq!(idx.top_menu, PlaybackObj::BdJ);
        assert_eq!(idx.titles.len(), 2);
        assert_eq!(idx.titles[0], PlaybackObj::BdJ);
    }

    #[test]
    fn rejects_bad_magic_and_truncation() {
        assert!(parse(b"NOPE").is_none());
        let d = build(hdmv_obj(3), hdmv_obj(0), &[hdmv_obj(1)]);
        // Truncating below the declared title span must yield None, never panic.
        assert!(parse(&d[..d.len() - 4]).is_none());
    }
}
