//! Syntactic write candidates for a finite, typed onQ subset. Runtime
//! identity, table-program effects and screen-resource loading remain required
//! independent obligations; this result is not authored selection evidence.
use super::bytecode;
use super::qco::{Program, Value};
use super::{Reject, Result, template};
use std::collections::BTreeSet;

#[derive(Debug)]
pub(super) struct ArrayWriteCandidates {
    /// Table-program callback cells must never reference these mutating helpers.
    pub fills: BTreeSet<u16>,
    /// Every screen activation still requires the single verified FS resource
    /// loading contract; recording it does not discharge that obligation.
    pub screen_loads: Vec<(u16, usize)>,
    pub callbacks: Vec<(u16, u8, u16)>,
}

fn variable<'a>(program: &'a Program<'_>, reference: u16) -> Result<&'a Value<'a>> {
    let variables = match reference & 0xc000 {
        0x4000 => &program.screen.variables,
        0xc000 => &program.global.variables,
        _ => return Err(Reject::Invalid),
    };
    variables
        .get(
            usize::from(reference & 0x3fff)
                .checked_sub(1)
                .ok_or(Reject::Invalid)?,
        )
        .ok_or(Reject::Invalid)
}

fn function_exists(program: &Program<'_>, reference: u16) -> bool {
    let functions = match reference & 0xc000 {
        0x4000 => &program.screen.functions,
        0xc000 => &program.global.functions,
        _ => return false,
    };
    let index = reference & 0x3fff;
    index != 0 && usize::from(index) <= functions.len()
}

pub(super) fn array_candidates(
    program: &Program<'_>,
    protected: &[u16],
) -> Result<ArrayWriteCandidates> {
    array_candidates_with_literal_writes(program, protected, &BTreeSet::new())
}

// Each exception requires a separately checked literal-cell/value contract.
pub(super) fn array_candidates_with_literal_writes(
    program: &Program<'_>,
    protected: &[u16],
    allowed: &BTreeSet<(u16, usize)>,
) -> Result<ArrayWriteCandidates> {
    array_candidates_with_exceptions(program, protected, allowed, &BTreeSet::new())
}

/// Caller must validate immutable typed fill sources and all three destination
/// arrays. This allowance exempts only the exact reviewed wrapper calls, never
/// direct writes, backing-slot aliases, or opaque calls to the fill helper.
pub(super) fn array_candidates_with_typed_fills(
    program: &Program<'_>,
    protected: &[u16],
    allowed_wrapper_refs: &BTreeSet<u16>,
) -> Result<ArrayWriteCandidates> {
    array_candidates_with_exceptions(program, protected, &BTreeSet::new(), allowed_wrapper_refs)
}

fn array_candidates_with_exceptions(
    program: &Program<'_>,
    protected: &[u16],
    allowed: &BTreeSet<(u16, usize)>,
    allowed_wrapper_refs: &BTreeSet<u16>,
) -> Result<ArrayWriteCandidates> {
    let protected: BTreeSet<_> = protected.iter().copied().collect();
    if protected.is_empty() {
        return Err(Reject::Invalid);
    }
    for &reference in &protected {
        if !matches!(variable(program, reference)?, Value::Integers { .. }) {
            return Err(Reject::Invalid);
        }
    }
    let mut bodies = Vec::new();
    let mut fills = BTreeSet::new();
    let mut total = 0usize;
    for (prefix, group) in [(0xc000, &program.global), (0x4000, &program.screen)] {
        for (index, body) in group.functions.iter().enumerate() {
            let reference = prefix | (index as u16 + 1);
            let code = bytecode::decode(body)?;
            total += code.len();
            if total > 65536 {
                return Err(Reject::Budget);
            }
            if code
                .iter()
                .any(|i| matches!(i.opcode, 0x1a | 0x1b | 0x1e | 0x1f | 0x3a | 0x3b))
            {
                // Only the complete reviewed fill helper may mutate an alias.
                // It has no callbacks, native calls or other hidden effects.
                if prefix != 0xc000 {
                    return Err(Reject::Unsupported);
                }
                template::array_fill(body)?;
                fills.insert(reference);
            }
            bodies.push((reference, *body, code));
        }
        for object in &group.objects {
            for event in &object.events {
                if event.target != 0 || !function_exists(program, event.function) {
                    return Err(Reject::Unsupported);
                }
            }
        }
    }
    // Events cannot call an alias-writing helper with opaque runtime arguments.
    if [&program.global, &program.screen]
        .iter()
        .flat_map(|g| &g.objects)
        .flat_map(|o| &o.events)
        .any(|e| fills.contains(&e.function))
    {
        return Err(Reject::Unsupported);
    }
    let mut screen_loads = Vec::new();
    let mut callbacks = Vec::new();
    let mut used_wrappers = BTreeSet::new();
    for (reference, body, code) in bodies {
        for write in bytecode::writes(&code) {
            if matches!(write.opcode, 0x26 | 0x27 | 0x2a | 0x2b | 0x2e | 0x2f | 0x42) {
                // Destination hints cover normal predecessors only. Because
                // lj resumes after exceptions, authored authority additionally
                // requires a stack/type proof that the literal push succeeds.
                let destination = u16::try_from(write.literal_destination.ok_or(Reject::Unproven)?)
                    .map_err(|_| Reject::Invalid)?;
                variable(program, destination)?;
                // afw's scalar bank also stores the backing-array index.
                // A scalar write to ANY array slot can manufacture an alias
                // from an otherwise disjoint wrapper argument to protected data.
                if matches!(write.opcode, 0x2e | 0x2f | 0x42)
                    && matches!(
                        variable(program, destination)?,
                        Value::Integers { .. } | Value::Strings { .. }
                    )
                {
                    return Err(Reject::Unsupported);
                }
                if matches!(write.opcode, 0x26 | 0x27 | 0x2a | 0x2b)
                    && !matches!(
                        variable(program, destination)?,
                        Value::Integers { .. } | Value::Strings { .. }
                    )
                {
                    return Err(Reject::Unsupported);
                }
                if protected.contains(&destination)
                    && !(write.opcode == 0x26 && allowed.contains(&(reference, write.pc)))
                {
                    return Err(Reject::Unsupported);
                }
            }
        }
        for (index, instruction) in code.iter().enumerate() {
            match instruction.opcode {
                0x16 => {
                    if instruction.branch_target {
                        return Err(Reject::Unproven);
                    }
                    let target = index
                        .checked_sub(1)
                        .and_then(|i| code[i].integer())
                        .and_then(|v| u16::try_from(v).ok())
                        .ok_or(Reject::Unproven)?;
                    if !function_exists(program, target) {
                        return Err(Reject::Invalid);
                    }
                    if fills.contains(&target) {
                        // Every call of the alias-mutator has a complete wrapper
                        // with explicit distinct array arguments, never a copied
                        // incoming reference. No branch can bypass those pushes.
                        let bindings = template::array_fill_wrapper(body)?;
                        if bindings["g:f_fill"] != target {
                            return Err(Reject::Invalid);
                        }
                        for role in ["s:v_array_a", "s:v_array_b", "s:v_array_c"] {
                            let array = bindings[role];
                            if (protected.contains(&array)
                                && !allowed_wrapper_refs.contains(&reference))
                                || !matches!(variable(program, array)?, Value::Integers { .. })
                            {
                                return Err(Reject::Unsupported);
                            }
                            if protected.contains(&array) {
                                used_wrappers.insert(reference);
                            }
                        }
                    }
                }
                0x49 => {
                    // The reviewed setter pops object then function. Require
                    // both literal pushes and no entry bypassing either one.
                    let object = index
                        .checked_sub(1)
                        .and_then(|i| code.get(i))
                        .ok_or(Reject::Unproven)?;
                    let function = index
                        .checked_sub(2)
                        .and_then(|i| code.get(i))
                        .ok_or(Reject::Unproven)?;
                    if instruction.branch_target || object.branch_target {
                        return Err(Reject::Unproven);
                    }
                    let object = object
                        .integer()
                        .and_then(|v| u16::try_from(v).ok())
                        .ok_or(Reject::Unproven)?;
                    let function = function
                        .integer()
                        .and_then(|v| u16::try_from(v).ok())
                        .ok_or(Reject::Unproven)?;
                    if !program.screen.objects.iter().any(|o| o.id == object)
                        || fills.contains(&function)
                        || (function != 0 && !function_exists(program, function))
                    {
                        return Err(Reject::Unsupported);
                    }
                    callbacks.push((object, instruction.operands[0], function));
                }
                0x40 => screen_loads.push((reference, instruction.pc)),
                _ => {}
            }
        }
    }
    if &used_wrappers != allowed_wrapper_refs {
        return Err(Reject::Invalid);
    }
    Ok(ArrayWriteCandidates {
        fills,
        screen_loads,
        callbacks,
    })
}
