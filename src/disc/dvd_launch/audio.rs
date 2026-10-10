//! Resolve a navigation-selected logical audio slot without positional fallback.

use super::super::u16_at;
use super::ifo::{Result, required};
use crate::disc::{DiscTitle, DvdLaunchReviewReason as Reason};

pub(super) fn selected(
    vts: &[u8],
    pgc: &[u8],
    logical: u8,
    title: &DiscTitle,
) -> Result<(u16, String)> {
    let count = required(u16_at(vts, 0x202))?;
    if count > 8 || usize::from(logical) >= count {
        return Err(Reason::IncompleteNavigation);
    }
    // Language bytes are evidence only when the attribute declares a language code.
    if required(vts.get(0x204 + usize::from(logical) * 8))? & 0x0c != 4 {
        return Err(Reason::UnprovenPresentation);
    }
    let route = |slot: usize| -> Result<Option<(u16, crate::ifo::DvdAudioAttr)>> {
        let ctl = required(u16_at(pgc, 12 + slot * 2))? as u16;
        let Some(physical) = crate::ifo::audio_stream_number(ctl) else {
            return Ok(None);
        };
        let attr = crate::ifo::parse_audio_attr(vts, 0x204 + slot * 8)
            .map_err(|_| Reason::IncompleteNavigation)?;
        let pid =
            crate::ifo::audio_pid(attr.codec, physical).ok_or(Reason::UnsupportedNavigation)?;
        Ok(Some((pid, attr)))
    };
    let (pid, attr) = required(route(usize::from(logical))?)?;
    for slot in 0..count {
        if let Some((other_pid, other)) = route(slot)?
            && other_pid == pid
            && (other.language != attr.language || other.codec != attr.codec)
        {
            return Err(Reason::AmbiguousNavigation);
        }
    }
    let matches: Vec<_> = title
        .audio_streams()
        .filter(|audio| audio.pid == pid)
        .collect();
    if matches.len() != 1 || matches[0].language != attr.language || matches[0].codec != attr.codec
    {
        return Err(Reason::UnprovenPresentation);
    }
    Ok((pid, attr.language))
}
