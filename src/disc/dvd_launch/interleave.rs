//! Forward ILVU walks; oversized terminal units need an exact VOBU boundary proof.
use super::super::u32_at;
use super::{ifo::required, produce::Failure};
use crate::{
    disc::{DvdLaunchReviewReason as Reason, Extent},
    sector::SectorSource,
};

fn read_nav(
    reader: &mut dyn SectorSource,
    base: u32,
    at: u32,
    position: &[u8],
    budget: &mut usize,
) -> Result<[u8; 2048], Failure> {
    *budget = budget.checked_sub(1).ok_or(Reason::BudgetExceeded)?;
    let lba = base.checked_add(at).ok_or(Reason::UnprovenPresentation)?;
    let mut pack = [0; 2048];
    if reader.read_sectors(lba, 1, &mut pack, false)? != 2048 {
        return Err(Reason::IncompleteNavigation.into());
    }
    if pack[..4] != [0, 0, 1, 0xba]
        || pack[4] & 0xc0 != 0x40
        || pack[0x400..0x407] != [0, 0, 1, 0xbf, 3, 0xfa, 1]
    {
        return Err(Reason::UnprovenPresentation.into());
    }
    let d = &pack[0x407..];
    if u32_at(d, 4) != Some(at as usize) || d[24..26] != position[..2] || d[27] != position[3] {
        return Err(Reason::UnprovenPresentation.into());
    }
    Ok(pack)
}

// A terminal cell may end within a physical ILVU. Require a contiguous chain of
// same-cell VOBUs ending exactly there, corroborated by the SRI end-of-cell marker.
fn terminal(
    reader: &mut dyn SectorSource,
    base: u32,
    mut at: u32,
    last: u32,
    position: &[u8],
    budget: &mut usize,
    mut pack: [u8; 2048],
) -> Result<(), Failure> {
    loop {
        let d = &pack[0x407..];
        let end = at
            .checked_add(required(u32_at(d, 8))? as u32)
            .ok_or(Reason::UnprovenPresentation)?;
        let next = required(u32_at(d, 314))? as u32;
        if end > last {
            return Err(Reason::UnprovenPresentation.into());
        }
        if end == last {
            return if next == 0x3fff_ffff {
                Ok(())
            } else {
                Err(Reason::UnprovenPresentation.into())
            };
        }
        if next == 0x3fff_ffff || next & 0x4000_0000 != 0 {
            return Err(Reason::UnprovenPresentation.into());
        }
        let distance = next & 0x3fff_ffff;
        let linked = at
            .checked_add(distance)
            .ok_or(Reason::UnprovenPresentation)?;
        if distance == 0 || Some(linked) != end.checked_add(1) {
            return Err(Reason::UnprovenPresentation.into());
        }
        at = linked;
        pack = read_nav(reader, base, at, position, budget)?;
    }
}

pub(super) fn walk(
    reader: &mut dyn SectorSource,
    base: u32,
    first: u32,
    last: u32,
    position: &[u8],
    budget: &mut usize,
) -> Result<Vec<Extent>, Failure> {
    if first >= last || position.len() != 4 {
        return Err(Reason::UnprovenPresentation.into());
    }
    let mut at = first;
    let mut out = Vec::new();
    loop {
        let lba = base.checked_add(at).ok_or(Reason::UnprovenPresentation)?;
        let pack = read_nav(reader, base, at, position, budget)?;
        let d = &pack[0x407..];
        let end_rel = required(u32_at(d, 34))? as u32;
        let next_rel = required(u32_at(d, 38))? as u32;
        let end = at
            .checked_add(end_rel)
            .ok_or(Reason::UnprovenPresentation)?;
        if end_rel == 0 || required(u32_at(d, 8))? > end_rel as usize {
            return Err(Reason::UnprovenPresentation.into());
        }
        if end > last {
            if !matches!(next_rel, 0 | 0x7fff_ffff | 0xffff_ffff)
                && at.checked_add(next_rel).is_none_or(|next| next <= end)
            {
                return Err(Reason::UnprovenPresentation.into());
            }
            terminal(reader, base, at, last, position, budget, pack)?;
            out.push(Extent {
                start_lba: lba,
                sector_count: last
                    .checked_sub(at)
                    .and_then(|n| n.checked_add(1))
                    .ok_or(Reason::UnprovenPresentation)?,
            });
            return Ok(out);
        }
        out.push(Extent {
            start_lba: lba,
            sector_count: end_rel.checked_add(1).ok_or(Reason::UnprovenPresentation)?,
        });
        if matches!(next_rel, 0 | 0x7fff_ffff | 0xffff_ffff) {
            if end != last {
                return Err(Reason::UnprovenPresentation.into());
            }
            return Ok(out);
        }
        let next = at
            .checked_add(next_rel)
            .ok_or(Reason::UnprovenPresentation)?;
        // A completely covered cell may link to the next cell's unit. Never
        // require that unit to belong to this presentation, or clip it into it.
        if end == last && next > last {
            return Ok(out);
        }
        if next <= end || next > last {
            return Err(Reason::UnprovenPresentation.into());
        }
        at = next;
    }
}
