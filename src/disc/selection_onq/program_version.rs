//! Fixed compiler-declaration-layout identity, separate from authored data.
//! Declaration reordering is unsupported. This gate identifies code, not
//! a proved execution: data, lifecycle, native and frame gates remain required.
use super::{
    Reject, Result,
    qco::{Group, Program, Value},
    template,
};
use sha2::{Digest, Sha256};

// Reviewed parsed compiler code/layout/events, excluding authored data.
const ONQ_UHD_V1: [u8; 32] = [
    0x7b, 0x63, 0x2d, 0x9f, 0x79, 0x56, 0x2a, 0xd1, 0xfc, 0xd3, 0xa3, 0x3f, 0xef, 0xf2, 0xa2, 0x6c,
    0xad, 0x38, 0xb9, 0x34, 0xa5, 0xc6, 0x85, 0x7e, 0xa6, 0x2a, 0xe7, 0x27, 0x24, 0xbc, 0x55, 0xa3,
];

pub(super) fn recognize(program: &Program<'_>, playback: u16, table_rows: usize) -> Result<()> {
    if fingerprint(program, playback, table_rows)? != ONQ_UHD_V1 {
        return Err(Reject::Unsupported);
    }
    Ok(())
}

fn number(hash: &mut Sha256, value: usize) {
    hash.update((value as u64).to_be_bytes());
}

fn group(
    hash: &mut Sha256,
    group: &Group<'_>,
    playback: Option<u16>,
    table_rows: usize,
) -> Result<()> {
    hash.update(group.id.to_be_bytes());
    number(hash, group.functions.len());
    for body in &group.functions {
        number(hash, body.len());
        let thunk = template::playback_thunk(body)
            .ok()
            .filter(|thunk| Some(thunk.callee) == playback);
        if let Some(thunk) = thunk {
            if usize::from(thunk.row) >= table_rows {
                return Err(Reject::Invalid);
            }
            hash.update([1]);
            hash.update(&body[..4]);
            hash.update([0]);
            hash.update(&body[5..]);
        } else {
            hash.update([0]);
            hash.update(body);
        }
    }
    number(hash, group.variables.len());
    if group.variable_types.len() != group.variables.len() {
        return Err(Reject::Invalid);
    }
    hash.update(group.variable_types);
    for value in &group.variables {
        let (kind, rows, cols) = match value {
            Value::Integer(_) => (0, 0, 0),
            Value::String(_) => (1, 0, 0),
            Value::Integers { rows, cols, .. } => (2, *rows, *cols),
            Value::Strings { rows, cols, .. } => (3, *rows, *cols),
        };
        hash.update([kind]);
        number(hash, rows);
        number(hash, cols);
    }
    number(hash, group.objects.len());
    for object in &group.objects {
        hash.update([object.kind]);
        hash.update(object.id.to_be_bytes());
        hash.update(object.parent.to_be_bytes());
        number(hash, object.events.len());
        for event in &object.events {
            hash.update(event.event.to_be_bytes());
            hash.update(event.target.to_be_bytes());
            hash.update(event.function.to_be_bytes());
        }
    }
    Ok(())
}

pub(super) fn fingerprint(
    program: &Program<'_>,
    playback: u16,
    table_rows: usize,
) -> Result<[u8; 32]> {
    if playback == 0 || playback > 0x3fff || table_rows == 0 || table_rows > 4096 {
        return Err(Reject::Invalid);
    }
    let mut hash = Sha256::new();
    hash.update(b"freemkv/onq/compiler-program-layout/row-roles/v1\0");
    group(&mut hash, &program.global, None, table_rows)?;
    group(&mut hash, &program.screen, Some(playback), table_rows)?;
    Ok(hash.finalize().into())
}

#[cfg(test)]
#[path = "program_version_tests.rs"]
mod tests;
