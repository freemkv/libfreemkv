//! Stable, connected buttons across explicitly equivalent display groups.

use super::super::{u16_at, u32_at};
use super::ifo::{Result, required};
use crate::disc::DvdLaunchReviewReason as Reason;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Buttons {
    pub masks: Vec<u8>,
    pub commands: Vec<[u8; 8]>,
    pub arrows: Vec<[u8; 4]>,
}

pub(super) fn parse(
    sector: &[u8],
    sector_number: usize,
    previous: Option<&Buttons>,
) -> Result<Option<Buttons>> {
    if sector.len() != 2048 || sector[..4] != [0, 0, 1, 0xba] || sector[4] & 0xc0 != 0x40 {
        return Err(Reason::IncompleteNavigation);
    }
    let next = 14 + usize::from(sector[13] & 7);
    if sector.get(next..next + 4) != Some(&[0, 0, 1, 0xbb]) {
        return Ok(None);
    }
    if next != 14
        || u16_at(sector, 18) != Some(18)
        || sector[38..42] != [0, 0, 1, 0xbf]
        || u16_at(sector, 42) != Some(0x3d4)
        || sector[44] != 0
        || sector[1024..1028] != [0, 0, 1, 0xbf]
        || u16_at(sector, 1028) != Some(0x3fa)
        || sector[1030] != 1
        || u32_at(sector, 45) != Some(sector_number)
    {
        return Err(Reason::IncompleteNavigation);
    }
    let pci = &sector[45..1024];
    let h = &pci[96..];
    if u16_at(h, 0) == Some(0) {
        return Ok(None);
    }
    let groups = usize::from((h[14] >> 4) & 3);
    let count = usize::from(h[17]);
    let status = required(u16_at(h, 0))?;
    if !matches!(status, 1 | 2)
        || (status == 2 && previous.is_none())
        || groups == 0
        || count == 0
        || count > 36 / groups
        || h[16] != 0
        || usize::from(h[18]) > count
        || h[20] != 0
        || h[21] != 0
        || required(u32_at(pci, 8))? & (1 << 17) != 0
    {
        return Err(Reason::UnsupportedNavigation);
    }
    let start = required(u32_at(h, 2))?;
    if required(u32_at(h, 6))? <= start
        || required(u32_at(h, 10))? <= start
        || start > required(u32_at(pci, 16))?
        || required(u32_at(h, 6))? < required(u32_at(pci, 12))?
    {
        return Err(Reason::IncompleteNavigation);
    }
    let masks = [h[14] & 7, (h[15] >> 4) & 7, h[15] & 7][..groups].to_vec();
    if groups > 1
        && (masks.contains(&0)
            || masks
                .iter()
                .enumerate()
                .any(|(i, m)| masks[..i].iter().any(|n| m & n != 0)))
    {
        return Err(Reason::AmbiguousNavigation);
    }
    let mut result = Buttons {
        masks,
        commands: Vec::new(),
        arrows: Vec::new(),
    };
    for group in 0..groups {
        for i in 0..count {
            let at = 46 + (group * (36 / groups) + i) * 18;
            let b = required(h.get(at..at + 18))?;
            let x0 = (u16::from(b[0] & 63) << 4) | u16::from(b[1] >> 4);
            let x1 = (u16::from(b[1] & 3) << 8) | u16::from(b[2]);
            let y0 = (u16::from(b[3] & 63) << 4) | u16::from(b[4] >> 4);
            let y1 = (u16::from(b[4] & 3) << 8) | u16::from(b[5]);
            let arrows: [u8; 4] = required(b[6..10].try_into().ok())?;
            let command = required(b[10..18].try_into().ok())?;
            if b[3] & 0xc0 != 0
                || x0 >= x1
                || y0 >= y1
                || arrows.iter().any(|&n| n == 0 || usize::from(n) > count)
            {
                return Err(Reason::UnsupportedNavigation);
            }
            if group == 0 {
                result.commands.push(command);
                result.arrows.push(arrows);
            } else if result.commands[i] != command || result.arrows[i] != arrows {
                return Err(Reason::AmbiguousNavigation);
            }
        }
    }
    for start in 0..count {
        let mut seen = vec![false; count];
        let mut todo = vec![start];
        while let Some(i) = todo.pop() {
            if seen[i] {
                continue;
            }
            seen[i] = true;
            todo.extend(
                result.arrows[i]
                    .iter()
                    .map(|n| usize::from(*n) - 1)
                    .filter(|&n| !seen[n]),
            );
        }
        if seen.contains(&false) {
            return Err(Reason::IncompleteNavigation);
        }
    }
    // Equal-HLI continuation is accepted only after a complete initial HLI and
    // only when the repeated command/arrow evidence agrees with it.
    if status == 2 && previous != Some(&result) {
        return Err(Reason::AmbiguousNavigation);
    }
    Ok(Some(result))
}
