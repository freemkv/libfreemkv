//! Data preconditions for the reviewed compiler layout's startup timing code.
//! These are not an independent program-version or execution certificate.
use super::{
    Reject, Result,
    qco::{Group, Value},
};

fn value<'a, 'b>(group: &'a Group<'b>, slot: usize) -> Result<&'a Value<'b>> {
    group
        .variables
        .get(slot.checked_sub(1).ok_or(Reject::Invalid)?)
        .ok_or(Reject::Invalid)
}

fn integer(group: &Group<'_>, slot: usize) -> Result<i32> {
    match value(group, slot)? {
        Value::Integer(value) => Ok(*value),
        _ => Err(Reject::Invalid),
    }
}

fn array<'a, 'b>(group: &'a Group<'b>, slot: usize) -> Result<(usize, usize, &'a [i32])> {
    match value(group, slot)? {
        Value::Integers { rows, cols, values } if rows.checked_mul(*cols) == Some(values.len()) => {
            Ok((*rows, *cols, values))
        }
        _ => Err(Reject::Invalid),
    }
}

fn timing(start: i32, end: i32, rate: i32) -> Result<()> {
    let delta = end.checked_sub(start).ok_or(Reject::Invalid)?;
    if delta < 0 || rate <= 0 || (delta / 1000).checked_mul(rate).is_none() {
        return Err(Reject::Invalid);
    }
    Ok(())
}

pub(super) fn timing_tables(group: &Group<'_>) -> Result<()> {
    let rate = integer(group, 643)?;
    let duration = integer(group, 238)?;
    if duration < 0 || duration.checked_mul(1000).is_none() {
        return Err(Reject::Invalid);
    }
    let (rows, cols, source) = array(group, 652)?;
    let (out_rows, out_cols, _) = array(group, 11)?;
    if rows < 2 || cols < 2 || out_rows == 0 || out_cols < rows - 1 {
        return Err(Reject::Invalid);
    }
    for row in 0..rows - 1 {
        timing(source[1], source[row * cols + 1], rate)?;
    }
    let (rows, cols, source) = array(group, 654)?;
    let (out_rows, out_cols, _) = array(group, 177)?;
    if rows == 0 || cols < 3 || out_rows == 0 || out_cols == 0 {
        return Err(Reject::Invalid);
    }
    timing(source[1], source[(rows - 1) * cols + 2], rate)
}
