//! Parse `/BDMV/index.bdmv` — the BD navigation index (First Play, Top Menu,
//! and the title table), per its documented binary layout. This is read as a
//! documented binary format, never executed: every field is bounds-checked and
//! any malformed input yields `None` (the nav resolver then abstains).

use super::be_u16;

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
    // version at 4..8 ("0100"/"0200"/"0240"/"0300") is not load-bearing for resolution.
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
#[path = "index_tests.rs"]
mod tests;
