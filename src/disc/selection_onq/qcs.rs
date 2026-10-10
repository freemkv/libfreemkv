use super::binary::{MAX_CELLS, MAX_RECORDS, Reader, count};
use super::{Reject, Result};

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Cell<'a> {
    Integer(i32),
    String(&'a [u8]),
}

#[derive(Debug)]
pub(super) struct Table<'a> {
    pub rows: Vec<Vec<Cell<'a>>>,
    pub program: Option<&'a [u8]>,
    /// Present only when the runtime dispatches column targets (flags 0).
    pub targets: Vec<(u16, u16)>,
}

pub(super) fn parse(data: &[u8]) -> Result<Table<'_>> {
    let mut r = Reader::new(data)?;
    r.magic(b"QCSF", 1)?;
    let rows = count(r.u32()? as usize, MAX_RECORDS)?;
    let cols = count(r.u32()? as usize, 256)?;
    let types_at = r.u32()? as usize;
    let offsets_at = r.u32()? as usize;
    let data_at = r.u32()? as usize;
    let width = r.u16()? as i16;
    if width < 0 {
        return Err(Reject::Invalid);
    }
    let width = width as usize;
    let flags = r.u16()?;
    let metadata_at = r.u16()? as i16;
    if flags & !2 != 0 {
        return Err(Reject::Unsupported);
    }
    if metadata_at < 0 {
        return Err(Reject::Invalid);
    }
    let metadata_at = metadata_at as usize;
    // afl reads this descriptor extent even when fi subsequently executes the
    // embedded program instead. Do not treat flags 0 as an inert table.
    let mut metadata = r.at(metadata_at)?;
    metadata.take(cols * 8)?;
    metadata.pos = metadata_at;
    let mut targets = Vec::new();
    let program = if flags & 2 != 0 {
        Some(r.data.get(metadata_at..).ok_or(Reject::Truncated)?)
    } else {
        for _ in 0..cols {
            metadata.take(4)?;
            targets.push((metadata.u16()?, metadata.u16()?));
        }
        if metadata.pos != data.len() {
            return Err(Reject::Unsupported);
        }
        None
    };
    count(rows.checked_mul(cols).ok_or(Reject::Budget)?, MAX_CELLS)?;
    if offsets_at < 40
        || offsets_at.checked_add(cols * 2) != Some(types_at)
        || types_at.checked_add(cols) != Some(data_at)
    {
        return Err(Reject::Unsupported);
    }
    let types = r.at(types_at)?.take(cols)?;
    let mut offsets = r.at(offsets_at)?;
    let mut layout = Vec::with_capacity(cols);
    let mut next = 0;
    for &ty in types {
        let offset = offsets.u16()? as usize;
        let size = match ty {
            17 => 1,
            18 | 22 => 2,
            _ => return Err(Reject::Unsupported),
        };
        if offset != next {
            return Err(Reject::Unsupported);
        }
        next += size;
        layout.push((ty, offset));
    }
    if next != width {
        return Err(Reject::Invalid);
    }
    let payload_len = rows.checked_mul(width).ok_or(Reject::Budget)?;
    r.at(data_at)?.take(payload_len)?;
    let strings_at = data_at.checked_add(payload_len).ok_or(Reject::Budget)?;
    let strings_end = metadata_at;
    if strings_end < strings_at {
        return Err(Reject::Invalid);
    }
    let string_reader = Reader::new(&data[..strings_end])?;
    let mut string_starts = std::collections::BTreeSet::new();
    let mut cursor = strings_at;
    while cursor < strings_end {
        string_starts.insert(cursor);
        cursor += string_reader.string(cursor)?.len() + 1;
    }
    let mut result = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut cells = Vec::with_capacity(cols);
        for &(ty, offset) in &layout {
            let mut cell = r.at(data_at + row * width + offset)?;
            cells.push(match ty {
                17 => Cell::Integer(i32::from(cell.u8()?)),
                18 => Cell::Integer(i32::from(cell.u16()? as i16)),
                22 => {
                    let pointer = cell.u16()? as i16;
                    if pointer < 0 || (pointer != 0 && !string_starts.contains(&(pointer as usize)))
                    {
                        return Err(Reject::Invalid);
                    }
                    Cell::String(string_reader.string(pointer as usize)?)
                }
                _ => return Err(Reject::Unsupported),
            });
        }
        result.push(cells);
    }
    Ok(Table {
        rows: result,
        program,
        targets,
    })
}
