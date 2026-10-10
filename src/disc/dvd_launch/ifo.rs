//! Checked slices and command locations; no forgiving IFO fallbacks.

use super::super::{pgcs, sector_table, table, u16_at, u32_at};
use crate::disc::{DvdLaunchReviewReason as Reason, DvdLaunchStep};

pub(super) type Result<T> = std::result::Result<T, Reason>;
pub(super) fn required<T>(value: Option<T>) -> Result<T> {
    value.ok_or(Reason::IncompleteNavigation)
}

pub(super) struct Pgc<'a> {
    pub bytes: &'a [u8],
    pub pre: Vec<DvdLaunchStep>,
    pub post: Vec<DvdLaunchStep>,
    pub cell: Vec<DvdLaunchStep>,
    pub entry: u8,
}

pub(super) fn programs(bytes: &[u8], pointer: usize, vts: u8, menu: bool) -> Result<Vec<Pgc<'_>>> {
    let mut t = required(sector_table(bytes, pointer))?;
    if menu {
        if u16_at(t, 0) != Some(1) || t.get(10) != Some(&0) {
            return Err(Reason::UnsupportedNavigation);
        }
        t = required(table(t, required(u32_at(t, 12))?))?;
    }
    let bodies = required(pgcs(t))?;
    bodies
        .iter()
        .enumerate()
        .map(|(i, &b)| {
            let base = b.as_ptr() as usize - bytes.as_ptr() as usize;
            let at = required(u16_at(b, 0xe4))?;
            let mut lists = [Vec::new(), Vec::new(), Vec::new()];
            if at != 0 {
                if at < 236 {
                    return Err(Reason::IncompleteNavigation);
                }
                let counts = [
                    required(u16_at(b, at))?,
                    required(u16_at(b, at + 2))?,
                    required(u16_at(b, at + 4))?,
                ];
                let total: usize = counts.iter().sum();
                if total > 128 || u16_at(b, at + 6) != Some(7 + total * 8) {
                    return Err(Reason::IncompleteNavigation);
                }
                let mut pos = at + 8;
                for (list, count) in lists.iter_mut().zip(counts) {
                    for _ in 0..count {
                        list.push(DvdLaunchStep {
                            vts,
                            menu_vob: false,
                            byte_offset: u32::try_from(base + pos)
                                .map_err(|_| Reason::BudgetExceeded)?,
                            command: required(b.get(pos..pos + 8).and_then(|s| s.try_into().ok()))?,
                        });
                        pos += 8;
                    }
                }
            }
            let [pre, post, cell] = lists;
            Ok(Pgc {
                bytes: b,
                pre,
                post,
                cell,
                entry: t[8 + i * 8],
            })
        })
        .collect()
}

pub(super) fn entry(pgcs: &[Pgc<'_>], id: u8) -> Result<usize> {
    let found: Vec<_> = pgcs
        .iter()
        .enumerate()
        .filter(|(_, p)| p.entry == 0x80 | id)
        .map(|(i, _)| i)
        .collect();
    if found.len() != 1 {
        return Err(Reason::AmbiguousNavigation);
    }
    Ok(found[0])
}

pub(super) fn title_pgc(bytes: &[u8], title: u8, pgcs: &[Pgc<'_>]) -> Result<usize> {
    let t = required(sector_table(bytes, 0xc8))?;
    let count = required(u16_at(t, 0))?;
    let index = required(usize::from(title).checked_sub(1))?;
    if count > 99 || index >= count {
        return Err(Reason::IncompleteNavigation);
    }
    let start = required(u32_at(t, 8 + index * 4))?;
    let end = if index + 1 < count {
        required(u32_at(t, 12 + index * 4))?
    } else {
        t.len()
    };
    if start < 8 + count * 4 || end <= start || (end - start) % 4 != 0 {
        return Err(Reason::IncompleteNavigation);
    }
    let parts = required(t.get(start..end))?;
    let pgcn = required(required(u16_at(parts, 0))?.checked_sub(1))?;
    let pgc = required(pgcs.get(pgcn))?;
    let mut prev = 0;
    for p in parts.as_chunks::<4>().0 {
        let n = required(u16_at(p, 2))?;
        if u16_at(p, 0) != Some(pgcn + 1)
            || n <= prev
            || n > usize::from(pgc.bytes[2])
            || (prev == 0 && n != 1)
        {
            return Err(Reason::UnprovenPresentation);
        }
        prev = n;
    }
    Ok(pgcn)
}
