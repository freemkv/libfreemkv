use super::binary::{MAX_CELLS, MAX_RECORDS, Reader, count};
use super::{Reject, Result};
use std::collections::BTreeSet;
#[cfg(test)]
use std::ops::Range;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Value<'a> {
    Integer(i32),
    String(&'a [u8]),
    Integers {
        rows: usize,
        cols: usize,
        values: Vec<i32>,
    },
    Strings {
        rows: usize,
        cols: usize,
        values: Vec<&'a [u8]>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Event {
    pub event: u16,
    pub target: u16,
    pub function: u16,
}

#[derive(Debug)]
pub(super) struct Object<'a> {
    #[cfg(test)]
    pub span: Range<usize>,
    pub id: u16,
    pub parent: u16,
    pub kind: u8,
    pub payload: &'a [u8],
    pub events: Vec<Event>,
}

#[derive(Debug)]
pub(super) struct Group<'a> {
    #[cfg(test)]
    pub span: Range<usize>,
    #[cfg(test)]
    pub function_spans: Vec<Range<usize>>,
    #[cfg(test)]
    pub variable_spans: Vec<Range<usize>>,
    string_references: Vec<usize>,
    pub id: u16,
    pub variables: Vec<Value<'a>>,
    pub variable_types: &'a [u8],
    pub functions: Vec<&'a [u8]>,
    pub objects: Vec<Object<'a>>,
    pub end: usize,
}

#[derive(Debug)]
pub(super) struct Program<'a> {
    pub global: Group<'a>,
    pub screen: Group<'a>,
}

fn object<'a>(r: &mut Reader<'a>) -> Result<Object<'a>> {
    #[cfg(test)]
    let object_start = r.pos;
    let kind = r.u8()?;
    let id = r.u16()?;
    let parent = r.u16()?;
    let start = r.pos;
    let remaining = match kind {
        1 => 35,
        2 | 11 => 3,
        3 | 4 => 7,
        5 => 35,
        6 => {
            if r.u8()? & 16 != 0 {
                51
            } else {
                41
            }
        }
        7 => 9,
        8 => {
            if r.u8()? & 2 != 0 {
                4
            } else {
                2
            }
        }
        9 => 5,
        10 => 19,
        12 => 21,
        15 => 1,
        _ => return Err(Reject::Unsupported),
    };
    r.take(remaining)?;
    let payload = &r.data[start..r.pos];
    let count = r.u8()?;
    let mut events = Vec::with_capacity(count as usize);
    let mut seen = BTreeSet::new();
    for _ in 0..count {
        let event = r.u16()?;
        if !seen.insert(event) {
            return Err(Reject::Invalid);
        }
        events.push(Event {
            event,
            target: r.u16()?,
            function: r.u16()?,
        });
    }
    Ok(Object {
        #[cfg(test)]
        span: object_start..r.pos,
        id,
        parent,
        kind,
        payload,
        events,
    })
}

fn group<'a>(r: &mut Reader<'a>, kind: u8) -> Result<Group<'a>> {
    #[cfg(test)]
    let group_start = r.pos;
    let root = object(r)?;
    if root.kind != kind || root.parent != 0 {
        return Err(Reject::Invalid);
    }
    let id = r.u16()?;
    let children = count(r.u16()? as usize, MAX_RECORDS)?;
    let function_count = count(r.u16()? as usize, MAX_RECORDS)?;
    let vars = r.u16()? as i16;
    if vars < 0 {
        return Err(Reject::Invalid);
    }
    let vars = count(vars as usize, MAX_RECORDS)?;
    let types = r.take(vars)?;
    #[cfg(test)]
    let words_start = r.pos;
    let mut words = Vec::with_capacity(vars);
    for _ in 0..vars {
        words.push(r.u32()?);
    }
    r.u16()?;
    let mut variables = Vec::with_capacity(vars);
    #[cfg(test)]
    let mut variable_spans = Vec::with_capacity(vars);
    let mut string_references = Vec::new();
    let mut budget = MAX_CELLS;
    for (&ty, word) in types.iter().zip(words) {
        #[cfg(test)]
        let start = if matches!(ty, 28 | 29) {
            r.pos
        } else {
            words_start + variables.len() * 4
        };
        variables.push(match ty {
            21 | 36 => Value::Integer(word as i32),
            22 => {
                string_references.push(word as usize);
                Value::String(r.string(word as usize)?)
            }
            28 | 29 => {
                let rows = r.u16()? as i16;
                let cols = r.u16()? as i16;
                if rows < 0 || cols < 0 {
                    return Err(Reject::Unsupported);
                }
                let rows = rows.max(1) as usize;
                let cols = cols.max(1) as usize;
                let initialized = r.u16()? != 0;
                let cells = rows.checked_mul(cols).ok_or(Reject::Budget)?;
                budget = budget.checked_sub(cells).ok_or(Reject::Budget)?;
                if ty == 28 {
                    let mut values = vec![0; cells];
                    if initialized {
                        for value in &mut values {
                            *value = r.u32()? as i32;
                        }
                    }
                    Value::Integers { rows, cols, values }
                } else {
                    let mut values = vec![&[][..]; cells];
                    if initialized {
                        for value in &mut values {
                            let p = r.u32()? as usize;
                            string_references.push(p);
                            *value = r.string(p)?;
                        }
                    }
                    Value::Strings { rows, cols, values }
                }
            }
            _ => return Err(Reject::Unsupported),
        });
        #[cfg(test)]
        variable_spans.push(
            start..if matches!(ty, 28 | 29) {
                r.pos
            } else {
                start + 4
            },
        );
    }
    let mut functions = Vec::with_capacity(function_count);
    #[cfg(test)]
    let mut function_spans = Vec::with_capacity(function_count);
    if function_count != 0 {
        let length = r.u32()? as usize;
        #[cfg(test)]
        let blob_start = r.pos;
        let blob = r.take(length)?;
        let mut table = Reader::new(blob)?;
        table.take(function_count * 4)?;
        table.pos = 0;
        let mut offsets = Vec::with_capacity(function_count + 1);
        for i in 0..function_count {
            let offset = table.u32()? as usize;
            if offset < function_count * 4
                || offset >= length
                || (i == 0 && offset != function_count * 4)
                || offsets.last().is_some_and(|v| *v >= offset)
            {
                return Err(Reject::Invalid);
            }
            offsets.push(offset);
        }
        offsets.push(length);
        for pair in offsets.windows(2) {
            let body = &blob[pair[0]..pair[1]];
            if body.len() < 4 {
                return Err(Reject::Invalid);
            }
            // Runtime skips the u16 after local count; it is not a byte extent.
            // Table offsets own boundaries; exact templates validate their bytes.
            if body.last() != Some(&63) {
                return Err(Reject::Unsupported);
            }
            functions.push(body);
            #[cfg(test)]
            function_spans.push(blob_start + pair[0]..blob_start + pair[1]);
        }
    }
    let root_id = root.id;
    let mut ids = BTreeSet::from([root_id]);
    let mut objects = vec![root];
    for _ in 0..children {
        let child = object(r)?;
        if matches!(child.kind, 1 | 15) || !ids.contains(&child.parent) || !ids.insert(child.id) {
            return Err(Reject::Invalid);
        }
        objects.push(child);
    }
    Ok(Group {
        #[cfg(test)]
        span: group_start..r.pos,
        #[cfg(test)]
        function_spans,
        #[cfg(test)]
        variable_spans,
        string_references,
        id,
        variables,
        variable_types: types,
        functions,
        objects,
        end: r.pos,
    })
}

pub(super) fn parse(data: &[u8]) -> Result<Program<'_>> {
    let mut r = Reader::new(data)?;
    r.magic(b"QCOF", 5)?;
    let screen_at = r.u32()? as usize;
    let root_at = r.u8()? as usize;
    if root_at != 13 || screen_at <= root_at {
        return Err(Reject::Unsupported);
    }
    let global = group(&mut r, 15)?;
    if global.end != screen_at {
        return Err(Reject::Invalid);
    }
    let screen = group(&mut r, 1)?;
    // Classify the entire trailing pool; only entry starts are valid pointers.
    // No header, code, object bytes or mid-string alias can masquerade as data.
    let mut starts = BTreeSet::new();
    let mut cursor = screen.end;
    while cursor < data.len() {
        count(starts.len() + 1, MAX_RECORDS)?;
        starts.insert(cursor);
        let size = r.string(cursor)?.len();
        cursor = cursor.checked_add(size + 1).ok_or(Reject::Budget)?;
    }
    for pointer in global
        .string_references
        .iter()
        .chain(&screen.string_references)
    {
        if *pointer != 0 && !starts.contains(pointer) {
            return Err(Reject::Invalid);
        }
    }
    Ok(Program { global, screen })
}
