//! Finite byte-exact roles, not an interpreter. Recognition is only one local
//! proof obligation and never authorizes a roster or bypasses writer checks.
use super::qco::{Group, Object, Value};
use super::{Reject, Result};
use std::collections::BTreeMap;

// All literals are opcodes, frame extents, branch displacements or argument
// counts. Every authored reference is a typed symbolic operand, not a disc ID.
const SELECT: &str = "03 00 8f 02 s:v_selected 2c 02 s:v_animation 24 22 01 02 s:v_selected 2c 02 s:v_buttons 24 22 02 02 s:v_selected 2c 02 s:v_submenus 24 22 03 03 g:v_sound 2c 03 g:f_sound 16 01 20 03 02 s:v_pressed 2c 02 s:v_style 2c 02 o:o_highlight 02 o:o_cursor 02 s:v_style_c 47 02 s:v_style_b 47 02 s:v_style_d 47 02 s:v_style_a 47 02 s:v_style_base 47 02 s:v_animation 47 02 s:v_transition 47 02 s:v_buttons 47 02 s:v_selected 2c 02 s:f_transition 16 0e 02 s:v_selected 2c 02 s:v_submenus 24 45 00 12 02 s:v_selected 2c 02 s:v_actions 24 02 s:f_submenu 16 01 14 00 13 02 s:v_selected 2c 02 s:v_actions 24 02 s:v_selected 2c 02 s:f_play 16 02 3f";

/// Recognizes the entire finite selection handler, including both branches.
/// Its call effects and state invariants remain separate required obligations.
pub(super) fn selection_handler(body: &[u8]) -> Result<BTreeMap<&'static str, u16>> {
    match_template(SELECT, body)
}

pub(super) fn array_fill(body: &[u8]) -> Result<()> {
    match_template(
        "01 00 19 20 01 46 03 4b 01 0f 45 00 0e 20 02 20 01 1a 03 41 01 01 14 ff ec 3f",
        body,
    )
    .map(|_| ())
}

/// The reviewed count routine returns the index of the first zero, or the
/// array's length when every entry is nonzero. It does not inspect resources,
/// labels, durations or inactive action templates.
pub(super) fn first_zero_count(body: &[u8]) -> Result<()> {
    match_template(
        "02 00 2a 20 01 46 03 4b 01 0f 45 00 1c 20 01 18 03 22 02
20 02 01 00 0c 45 00 08 20 01 36 14 00 0b 41 01 01 14 ff de 20 01 36 3f",
        body,
    )
    .map(|_| ())
}

pub(super) fn count_setup(body: &[u8]) -> Result<BTreeMap<&'static str, u16>> {
    match_template(
        "00 00 16 02 s:v_buttons 47 02 s:f_count 16 01 34
02 s:v_count 2e 02 s:f_refresh 16 00 3f",
        body,
    )
}

/// Reset an out-of-range menu index to zero, otherwise return it unchanged.
#[cfg(test)]
pub(super) fn index_clamp(body: &[u8]) -> Result<()> {
    match_template(
        "00 00 1c 20 01 01 00 0f 44 00 0a 20 01 20 02 0e
45 00 08 01 00 36 14 00 05 20 01 36 3f",
        body,
    )
    .map(|_| ())
}

/// Both directions pass the same immutable roster/count and current index to
/// the reviewed step helper; only the literal increment/decrement differs.
pub(super) fn navigation_handler(body: &[u8], next: bool) -> Result<BTreeMap<&'static str, u16>> {
    if body.len() != 80 {
        return Err(Reject::Unsupported);
    }
    const PREFIX: &str = "00 00 4f 02 s:v_pressed 2c 02 s:v_style 2c
02 o:o_highlight 02 o:o_cursor 02 s:v_style_c 47 02 s:v_style_b 47
02 s:v_style_d 47 02 s:v_style_a 47 02 s:v_style_base 47
02 s:v_animation 47 02 s:v_transition 47 02 s:v_count 2c
02 s:v_buttons 47 02 s:v_selected 2c 02 s:v_selected 2c 01 01";
    const NEXT: &str = "00 00 4f 02 s:v_pressed 2c 02 s:v_style 2c
02 o:o_highlight 02 o:o_cursor 02 s:v_style_c 47 02 s:v_style_b 47
02 s:v_style_d 47 02 s:v_style_a 47 02 s:v_style_base 47
02 s:v_animation 47 02 s:v_transition 47 02 s:v_count 2c
02 s:v_buttons 47 02 s:v_selected 2c 02 s:v_selected 2c 01 01
06 02 s:f_step 16 0f 34 02 s:v_selected 2e 02 s:f_refresh 16 00 3f";
    // Keep one symbolic template; normalize only the reviewed arithmetic byte.
    let mut normalized = body.to_vec();
    let arithmetic = PREFIX
        .split_ascii_whitespace()
        .map(|token| if token.contains(':') { 2 } else { 1 })
        .sum::<usize>();
    if !next {
        if normalized.get(arithmetic) != Some(&0x0a) {
            return Err(Reject::Unsupported);
        }
        normalized[arithmetic] = 0x06;
    }
    match_template(NEXT, &normalized)
}

/// Connect the initialization count to the same roster consumed by selection.
/// This is a local consistency proof, not proof that initialization is reached
/// or that later writers preserve the count; both are separate obligations.
pub(super) fn selection_count(
    group: &Group<'_>,
    selection: usize,
    setup: usize,
) -> Result<(u16, usize)> {
    let function = |index: usize| {
        group
            .functions
            .get(index.checked_sub(1).ok_or(Reject::Invalid)?)
            .copied()
            .ok_or(Reject::Invalid)
    };
    let selected = selection_handler(function(selection)?)?;
    let setup = count_setup(function(setup)?)?;
    if selected["s:v_buttons"] != setup["s:v_buttons"] {
        return Err(Reject::Invalid);
    }
    first_zero_count(function(usize::from(setup["s:f_count"] & 0x3fff))?)?;
    function(usize::from(setup["s:f_refresh"] & 0x3fff))?;
    let count_ref = setup["s:v_count"];
    if !matches!(
        group.variables.get(usize::from(count_ref & 0x3fff) - 1),
        Some(Value::Integer(_))
    ) {
        return Err(Reject::Invalid);
    }
    Ok((
        count_ref,
        selected_action_rows(group, selection)?.rows.len(),
    ))
}

pub(super) fn array_fill_wrapper(body: &[u8]) -> Result<BTreeMap<&'static str, u16>> {
    match_template(
        "00 00 2a
02 s:v_array_a 47 03 g:v_value_a 2c 03 g:f_fill 16 02
02 s:v_array_b 47 03 g:v_value_b 2c 03 g:f_fill 16 02
02 s:v_array_c 47 03 g:v_value_c 2c 03 g:f_fill 16 02 3f",
        body,
    )
}

/// Paired animation helpers: every dynamic property destination is the first
/// argument (local1 with zero locals), which neither helper overwrites. Called
/// function/native effects are deliberately not discharged by recognition.
#[cfg(test)]
pub(super) fn visibility_helper(body: &[u8], show: bool) -> Result<BTreeMap<&'static str, u16>> {
    let pattern = if show {
        "00 00 63 01 00 04 r:g_player_group o:o_player_root 32 01
20 05 20 04 20 03 20 02 20 01 03 g:f_animation 16 05 34 3d 01
01 00 20 05 0a 20 01 32 02 01 00 20 01 32 03 01 01 20 01 32 01
20 01 03 g:f_begin 16 01 34 3d 01
01 00 03 g:v_duration 2c 01 00 01 00 20 01 02 s:f_transition 16 05 34 3d 01
02 s:f_flush 16 00 34 3d 01 20 01 03 g:f_end 16 01 34 3d 01 3f"
    } else {
        "00 00 54 01 00 04 r:g_player_group o:o_player_root 32 01
01 00 20 01 32 03 01 00 20 01 32 02
20 01 03 g:f_begin 16 01 34 3d 01
01 00 03 g:v_duration 2c 20 05 01 00 20 01 02 s:f_transition 16 05 34 3d 01
02 s:f_flush 16 00 34 3d 01 20 01 03 g:f_end 16 01 34 3d 01
01 00 20 01 32 01 01 00 20 01 32 02 3f"
    };
    match_template(pattern, body)
}

/// The row/mark setter requests a complete sequencer row; seek state is separate.
/// Its caller's mark argument and resource-changing callbacks need their own gate.
pub(super) fn playback_forward(body: &[u8]) -> Result<BTreeMap<&'static str, u16>> {
    match_template("00 00 0a 20 01 02 s:f_dispatch 16 01 3f", body)
}

pub(super) fn playback_dispatch(body: &[u8]) -> Result<BTreeMap<&'static str, u16>> {
    match_template(
        "02 01 9d 20 03 02 s:f_is_menu 16 01 34 22 01 03 g:f_title
16 00 34 22 02 03 g:v_title_switch 2c 4c 45 00 0f 02 ff ff
20 03 02 s:f_set_row 16 02 14 01 75 20 03 05 66 75 6e
63 50 6c 61 79 20 2d 20 6e 53 74 61 74 65 3a 20
25 64 00 15 03 00 3e 01 3d 01 05 49 6e 20 6d 65
6e 75 20 73 74 61 74 65 3a 20 00 20 01 03 g:f_string
16 01 35 15 10 00 3e 02 35 15 02 00 3e 01 20 02
01 01 0c 45 00 3c 20 01 45 00 37 20 03 02 s:v_pending
2e 01 01 01 00 03 g:f_change_title 16 02 34 3d 01 05 66 75
6e 63 50 6c 61 79 20 2d 20 67 6f 69 6e 67 20 74
6f 20 6d 65 6e 75 73 00 15 02 00 3e 01 14 00 ef
20 02 01 02 0c 45 00 89 20 03 02 s:v_pending 2e 20 01
45 00 3f 01 01 01 00 03 g:f_change_title 16 02 34 3d 01 05
66 75 6e 63 50 6c 61 79 20 2d 20 66 72 6f 6d 20
77 61 72 6e 69 6e 67 73 20 67 6f 69 6e 67 20 74
6f 20 6d 65 6e 75 73 00 15 02 00 3e 01 14 00 3e
01 01 01 01 03 g:f_change_title 16 02 34 3d 01 05 66 75 6e
63 50 6c 61 79 20 2d 20 66 72 6f 6d 20 77 61 72
6e 69 6e 67 73 20 67 6f 69 6e 67 20 74 6f 20 54
69 74 6c 65 20 31 00 15 02 00 3e 01 14 00 60 20
02 01 00 0c 45 00 4e 20 01 4c 45 00 48 20 03 02
s:v_pending 2e 01 06 02 o:o_modes 32 02 15 08 00 01 b:o_player 17
02 00 01 01 01 01 03 g:f_change_title 16 02 34 3d 01 05 66
75 6e 63 50 6c 61 79 20 2d 20 67 6f 69 6e 67 20
74 6f 20 54 69 74 6c 65 20 31 00 15 02 00 3e 01
14 00 0c 02 ff ff 20 03 02 s:f_set_row 16 02 3f",
        body,
    )
}

pub(super) fn playback_row_setter(body: &[u8]) -> Result<BTreeMap<&'static str, u16>> {
    match_template(
        "00 00 80 20 01 01 00 0f 44 00 0f 20 01 02 o:o_sequencer
17 04 00 34 0e 45 00 05 14 00 67 20 01 02 s:f_before_row
16 01 20 01 02 o:o_sequencer 32 02 20 02 02 s:v_mark 2e 15
08 00 05 66 75 6e 63 50 6c 61 79 4d 61 72 6b 28
29 20 53 74 61 74 65 3a 20 00 02 o:o_sequencer 30 02 15
05 01 35 15 10 00 3e 02 35 05 20 6d 61 72 6b 49
6e 64 65 78 3a 20 00 15 10 00 3e 02 35 02 s:v_mark
2c 15 05 01 35 15 10 00 3e 02 35 15 02 00 3e 01 3f",
        body,
    )
}

fn match_template<'a>(pattern: &'a str, body: &[u8]) -> Result<BTreeMap<&'a str, u16>> {
    let mut pos = 0;
    let mut bindings: BTreeMap<&'a str, u16> = BTreeMap::new();
    for token in pattern.split_ascii_whitespace() {
        if let Some((domain, name)) = token.split_once(':') {
            let width = if domain == "b" { 1 } else { 2 };
            let bytes = body.get(pos..pos + width).ok_or(Reject::Truncated)?;
            let value = if width == 1 {
                u16::from(bytes[0])
            } else {
                u16::from_be_bytes([bytes[0], bytes[1]])
            };
            let mask = match domain {
                "s" => 0x4000,
                "g" => 0xc000,
                "o" | "b" | "r" => 0,
                _ => return Err(Reject::Invalid),
            };
            if value & 0xc000 != mask || value & 0x3fff == 0 {
                return Err(Reject::Invalid);
            }
            // Variable/function/object namespaces are separate, but distinct
            // roles within one namespace cannot collapse onto one reference.
            fn namespace(role: &str) -> Option<(&str, &str)> {
                let (domain, name) = role.split_once(':')?;
                let family = name.split_once('_')?.0;
                Some((if domain == "b" { "o" } else { domain }, family))
            }
            if bindings.iter().any(|(other, old)| {
                *other != token && namespace(other) == namespace(token) && *old == value
            }) {
                return Err(Reject::Invalid);
            }
            if bindings
                .insert(token, value)
                .is_some_and(|old| old != value)
            {
                return Err(Reject::Invalid);
            }
            let _ = name;
            pos += width;
        } else {
            let byte = u8::from_str_radix(token, 16).map_err(|_| Reject::Invalid)?;
            if body.get(pos) != Some(&byte) {
                return Err(Reject::Unsupported);
            }
            pos += 1;
        }
    }
    if pos != body.len() {
        return Err(Reject::Unsupported);
    }
    Ok(bindings)
}

// flags2 QCS programs replace column-metadata dispatch. Native method1/2 reads
// the supplied row/column, then each exact suffix writes its declared target.
const QCS_PROGRAM: &str = "00 00 bd
20 01 01 00 20 02 17 01 02 34 04 r:g_player_group o:o_player_root 32 0d
20 01 01 01 20 02 17 01 02 34 02 s:v_menu 2e
20 01 01 02 20 02 17 01 02 34 01 b:o_media 49 64
20 01 01 03 20 02 17 01 02 34 01 b:o_media 49 5a
20 01 01 04 20 02 17 01 02 34 01 b:o_media 49 68
20 01 01 05 20 02 17 01 02 34 02 o:o_mode 32 02
20 01 01 06 20 02 17 02 02 35 02 s:v_text_a 2f
20 01 01 07 20 02 17 02 02 35 02 s:v_text_b 2f
20 01 01 08 20 02 17 01 02 34 02 s:v_state_a 2e
20 01 01 09 20 02 17 01 02 34 02 s:v_state_b 2e
20 01 01 0a 20 02 17 01 02 34 02 s:v_state_c 2e
20 01 01 0b 20 02 17 01 02 34 02 s:v_state_d 2e
20 01 01 0c 20 02 17 01 02 34 02 s:v_state_e 2e 3f";

pub(super) fn table_program(body: &[u8]) -> Result<BTreeMap<&'static str, u16>> {
    match_template(QCS_PROGRAM, body)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct PlaybackThunk {
    pub row: u16,
    pub callee: u16,
}

/// Reviewed straight-line role: zero locals, push positive row, push screen
/// function reference, call with exactly one argument, return. Every byte is
/// consumed. Stack contract is `[] -> [row] -> [row,callee] -> [] -> return`.
pub(super) fn playback_thunk(body: &[u8]) -> Result<PlaybackThunk> {
    if body.len() != 11
        || body[0] != 0
        || body[1..3] != [0, 10]
        || body[3] != 1
        || body[5] != 2
        || body[8..] != [22, 1, 63]
    {
        return Err(Reject::Unsupported);
    }
    let row = u16::from(body[4]);
    let reference = u16::from_be_bytes([body[6], body[7]]);
    if row == 0 || reference & 0xc000 != 0x4000 || reference & 0x3fff == 0 {
        return Err(Reject::Invalid);
    }
    Ok(PlaybackThunk {
        row,
        callee: reference & 0x3fff,
    })
}

/// A local structural candidate only. This deliberately cannot be converted to
/// Authored evidence; count/navigation/writer/runtime proofs are still required.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ActionRows {
    pub rows: Vec<u16>,
    pub playback_function: u16,
}

pub(super) fn selected_action_rows(group: &Group<'_>, handler: usize) -> Result<ActionRows> {
    let code = group
        .functions
        .get(handler.checked_sub(1).ok_or(Reject::Invalid)?)
        .ok_or(Reject::Invalid)?;
    let bindings = selection_handler(code)?;
    let array = |role| -> Result<&[i32]> {
        let reference = bindings.get(role).ok_or(Reject::Invalid)? & 0x3fff;
        match group.variables.get(reference as usize - 1) {
            Some(Value::Integers {
                cols: 1, values, ..
            }) => Ok(values),
            _ => Err(Reject::Unsupported),
        }
    };
    action_rows(
        array("s:v_buttons")?,
        array("s:v_submenus")?,
        array("s:v_actions")?,
        &group.objects,
        &group.functions,
    )
}

fn action_rows(
    buttons: &[i32],
    flags: &[i32],
    actions: &[i32],
    objects: &[Object<'_>],
    functions: &[&[u8]],
) -> Result<ActionRows> {
    let count = buttons
        .iter()
        .position(|v| *v == 0)
        .unwrap_or(buttons.len());
    if count == 0
        || buttons[count..].iter().any(|v| *v != 0)
        || flags.len() != buttons.len()
        || actions.len() != buttons.len()
        || flags.iter().any(|v| *v != 0)
    {
        return Err(Reject::Unsupported);
    }
    let mut rows = Vec::with_capacity(count);
    let mut seen_buttons = std::collections::BTreeSet::new();
    let mut seen_actions = std::collections::BTreeSet::new();
    let mut playback_function = None;
    for (&button, &action) in buttons[..count].iter().zip(&actions[..count]) {
        let button = u16::try_from(button).map_err(|_| Reject::Invalid)?;
        let action = u16::try_from(action).map_err(|_| Reject::Invalid)?;
        if button == 0
            || action == 0
            || !seen_buttons.insert(button)
            || !seen_actions.insert(action)
            || !objects.iter().any(|o| o.id == button)
        {
            return Err(Reject::Invalid);
        }
        let object = objects
            .iter()
            .find(|o| o.id == action)
            .ok_or(Reject::Invalid)?;
        if object.kind != 3 || object.events.len() != 1 {
            return Err(Reject::Unsupported);
        }
        let event = &object.events[0];
        if event.event != 3 || event.target != 0 || event.function & 0xc000 != 0x4000 {
            return Err(Reject::Unsupported);
        }
        let index = usize::from(event.function & 0x3fff)
            .checked_sub(1)
            .ok_or(Reject::Invalid)?;
        let thunk = playback_thunk(functions.get(index).ok_or(Reject::Invalid)?)?;
        if playback_function.is_some_and(|previous| previous != thunk.callee)
            || rows.contains(&thunk.row)
        {
            return Err(Reject::Unsupported);
        }
        playback_function = Some(thunk.callee);
        rows.push(thunk.row);
    }
    Ok(ActionRows {
        rows,
        playback_function: playback_function.ok_or(Reject::Invalid)?,
    })
}

#[cfg(test)]
mod tests {
    use super::super::qco::Event;
    use super::*;

    #[test]
    fn object_operand_width_does_not_create_an_alias_namespace() {
        assert!(match_template("01 b:o_first 02 o:o_second", &[1, 7, 2, 0, 7]).is_err());
        assert!(match_template("02 o:o_first 01 b:o_second", &[2, 0, 7, 1, 7]).is_err());
        assert!(match_template("01 b:o_first 02 o:o_second", &[1, 7, 2, 0, 8]).is_ok());
        // Group IDs and object IDs really are separate runtime namespaces.
        assert!(match_template("r:g_group o:o_root", &[0, 1, 0, 1]).is_ok());
        assert!(match_template("r:g_group o:o_root", &[0x40, 1, 0, 1]).is_err());
    }

    // Independent transcription of the disassembled selection handler; unlike
    // selection_fixture this is not generated from the recognizer pattern.
    const AUTHORED_SELECT: [u8; 144] = [
        0x03, 0x00, 0x8f, 0x02, 0x41, 0x91, 0x2c, 0x02, 0x41, 0xa6, 0x24, 0x22, 0x01, 0x02, 0x41,
        0x91, 0x2c, 0x02, 0x41, 0x8d, 0x24, 0x22, 0x02, 0x02, 0x41, 0x91, 0x2c, 0x02, 0x41, 0x98,
        0x24, 0x22, 0x03, 0x03, 0xc0, 0x39, 0x2c, 0x03, 0xc0, 0x58, 0x16, 0x01, 0x20, 0x03, 0x02,
        0x41, 0xa3, 0x2c, 0x02, 0x41, 0xa1, 0x2c, 0x02, 0x01, 0x1e, 0x02, 0x01, 0x1d, 0x02, 0x41,
        0x9f, 0x47, 0x02, 0x41, 0x9d, 0x47, 0x02, 0x41, 0xa0, 0x47, 0x02, 0x41, 0x9c, 0x47, 0x02,
        0x41, 0x9a, 0x47, 0x02, 0x41, 0xa6, 0x47, 0x02, 0x41, 0xa4, 0x47, 0x02, 0x41, 0x8d, 0x47,
        0x02, 0x41, 0x91, 0x2c, 0x02, 0x40, 0x26, 0x16, 0x0e, 0x02, 0x41, 0x91, 0x2c, 0x02, 0x41,
        0x98, 0x24, 0x45, 0x00, 0x12, 0x02, 0x41, 0x91, 0x2c, 0x02, 0x41, 0x99, 0x24, 0x02, 0x40,
        0x5d, 0x16, 0x01, 0x14, 0x00, 0x13, 0x02, 0x41, 0x91, 0x2c, 0x02, 0x41, 0x99, 0x24, 0x02,
        0x41, 0x91, 0x2c, 0x02, 0x40, 0x28, 0x16, 0x02, 0x3f,
    ];

    #[test]
    fn authored_navigation_directions_share_roles_not_fixed_ids() {
        let bytes = "00004f0241a22c0241a12c02011e02011d0241a04702419e4702419d4702419b4702419a470241a5470241a4470241902c02418d470241912c0241912c010106024024160f340241912e02413416003f";
        let mut next: Vec<u8> = bytes
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let roles = navigation_handler(&next, true).unwrap();
        assert_eq!(roles["s:v_buttons"], 0x418d);
        let mut previous = next.clone();
        previous[63] = 0x0a;
        assert_eq!(navigation_handler(&previous, false).unwrap(), roles);
        assert!(navigation_handler(&previous, true).is_err());
        assert!(navigation_handler(&next, false).is_err());
        // Change all occurrences of one role without changing the template.
        for offset in [54, 58, 71] {
            next[offset..offset + 2].copy_from_slice(&0x4201_u16.to_be_bytes());
        }
        let renamed = navigation_handler(&next, true).unwrap();
        assert_eq!(renamed["s:v_selected"], 0x4201);
        next[71..73].copy_from_slice(&0x4190_u16.to_be_bytes());
        assert!(navigation_handler(&next, true).is_err());
    }

    #[test]
    fn count_templates_reject_changed_loop_and_operand_domains() {
        // Independent bytes from the authored count routine and its caller.
        let count = [
            2, 0, 42, 32, 1, 70, 3, 75, 1, 15, 69, 0, 28, 32, 1, 24, 3, 34, 2, 32, 2, 1, 0, 12, 69,
            0, 8, 32, 1, 54, 20, 0, 11, 65, 1, 1, 20, 255, 222, 32, 1, 54, 63,
        ];
        first_zero_count(&count).unwrap();
        for offset in 0..count.len() {
            let mut bad = count;
            bad[offset] ^= 1;
            assert!(first_zero_count(&bad).is_err(), "count byte {offset}");
        }
        let setup = [
            0, 0, 22, 2, 65, 141, 71, 2, 64, 39, 22, 1, 52, 2, 65, 144, 46, 2, 65, 53, 22, 0, 63,
        ];
        count_setup(&setup).unwrap();
        for base in [1_u16, 200, 500] {
            let mut changed = setup;
            for (offset, value) in [(4, base), (8, base + 1), (14, base + 2), (18, base + 3)] {
                changed[offset..offset + 2].copy_from_slice(&(0x4000 | value).to_be_bytes());
            }
            count_setup(&changed).unwrap();
            changed[14..16].copy_from_slice(&(0x4000 | base).to_be_bytes());
            assert!(count_setup(&changed).is_err()); // count cannot alias its array
            changed[14..16].copy_from_slice(&(0xc000 | base).to_be_bytes());
            assert!(count_setup(&changed).is_err());
        }
        for end in 0..setup.len() {
            assert!(count_setup(&setup[..end]).is_err());
        }
    }

    #[test]
    fn independent_authored_selection_fixture_and_alias_domain_negatives() {
        let bindings = selection_handler(&AUTHORED_SELECT).unwrap();
        assert_eq!(bindings["s:v_buttons"], 0x418d);
        assert_eq!(bindings["s:v_actions"], 0x4199);
        for replacement in [0, 0xc191, 0x0191] {
            let mut b = AUTHORED_SELECT;
            b[4..6].copy_from_slice(&u16::to_be_bytes(replacement));
            assert!(selection_handler(&b).is_err());
        }
        let mut b = AUTHORED_SELECT;
        b[14..16].copy_from_slice(&0x4192_u16.to_be_bytes());
        assert!(selection_handler(&b).is_err()); // repeated role differs
        let mut b = AUTHORED_SELECT;
        b[8..10].copy_from_slice(&0x4191_u16.to_be_bytes());
        assert!(selection_handler(&b).is_err()); // distinct variable roles alias
        let mut pos = 0;
        for token in SELECT.split_ascii_whitespace() {
            if token.contains(':') {
                pos += 2;
            } else {
                let mut b = AUTHORED_SELECT;
                b[pos] ^= 1;
                assert!(selection_handler(&b).is_err(), "literal {pos}");
                pos += 1;
            }
        }
    }

    #[test]
    fn action_order_comes_from_variable_roster_not_object_or_row_sort() {
        for count in [2, 3, 4] {
            let mut objects = Vec::new();
            let mut bodies = Vec::new();
            let mut buttons = vec![0; 6];
            let mut actions = vec![0; 6];
            let mut expected: Vec<u16> = Vec::new();
            for i in 0..count {
                let button = 100 + i as u16 * 3;
                let action = 700 - i as u16;
                let row = 20 - i as u8 * 2;
                buttons[i] = button.into();
                actions[i] = action.into();
                expected.push(row.into());
                objects.push(Object {
                    span: 0..0,
                    id: button,
                    parent: 1,
                    kind: 6,
                    payload: &[],
                    events: vec![],
                });
                objects.push(Object {
                    span: 0..0,
                    id: action,
                    parent: 1,
                    kind: 3,
                    payload: &[],
                    events: vec![Event {
                        event: 3,
                        target: 0,
                        function: 0x4001 + i as u16,
                    }],
                });
                bodies.push(vec![0, 0, 10, 1, row, 2, 0x40, 90, 22, 1, 63]);
            }
            objects.reverse();
            let functions: Vec<_> = bodies.iter().map(Vec::as_slice).collect();
            assert_eq!(
                action_rows(&buttons, &[0; 6], &actions, &objects, &functions)
                    .unwrap()
                    .rows,
                expected
            );
            let mut bad = buttons.clone();
            bad[0] = 0;
            assert!(action_rows(&bad, &[0; 6], &actions, &objects, &functions).is_err());
            let mut bad = actions.clone();
            bad[1] = bad[0];
            assert!(action_rows(&buttons, &[0; 6], &bad, &objects, &functions).is_err());
            assert!(action_rows(&buttons, &[1; 6], &actions, &objects, &functions).is_err());
        }
    }

    fn selection_fixture(seed: u16) -> Vec<u8> {
        let mut values = BTreeMap::new();
        let mut out = Vec::new();
        for token in SELECT.split_ascii_whitespace() {
            if let Some((domain, _)) = token.split_once(':') {
                let next = seed + values.len() as u16;
                let value = *values.entry(token).or_insert(next);
                let prefix = match domain {
                    "s" => 0x4000,
                    "g" => 0xc000,
                    _ => 0,
                };
                out.extend_from_slice(&(value | prefix).to_be_bytes());
            } else {
                out.push(u8::from_str_radix(token, 16).unwrap());
            }
        }
        out
    }

    #[test]
    fn selection_role_accepts_renumbered_ids_and_rejects_code_mutations() {
        for seed in [1, 200, 700] {
            let b = selection_fixture(seed);
            let bindings = selection_handler(&b).unwrap();
            assert_eq!(bindings["s:v_selected"], 0x4000 | seed);
            assert_eq!(b.len(), 144);
            let mut bad = b.clone();
            bad[0] = 0;
            assert!(selection_handler(&bad).is_err());
            let mut bad = b.clone();
            bad[3] = 63;
            assert!(selection_handler(&bad).is_err());
            let mut bad = b.clone();
            bad.push(63);
            assert!(selection_handler(&bad).is_err());
            for n in 0..b.len() {
                assert!(selection_handler(&b[..n]).is_err());
            }
        }
    }

    #[test]
    fn recognizes_symbolic_rows_and_callees_not_fixed_disc_ids() {
        for (row, callee) in [(2, 94), (7, 9), (255, 16383)] {
            let mut b = [0, 0, 10, 1, row, 2, 0, 0, 22, 1, 63];
            b[6..8].copy_from_slice(&(0x4000_u16 | callee).to_be_bytes());
            assert_eq!(
                playback_thunk(&b),
                Ok(PlaybackThunk {
                    row: row.into(),
                    callee
                })
            );
        }
    }

    #[test]
    fn rejects_every_nonoperand_mutation_and_truncation() {
        let b = [0, 0, 10, 1, 2, 2, 0x40, 94, 22, 1, 63];
        for n in 0..b.len() {
            assert!(playback_thunk(&b[..n]).is_err());
        }
        for offset in [0, 1, 2, 3, 5, 8, 9, 10] {
            let mut changed = b;
            changed[offset] ^= 1;
            assert!(playback_thunk(&changed).is_err());
        }
        let mut changed = b;
        changed[4] = 0;
        assert!(playback_thunk(&changed).is_err());
        let mut changed = b;
        changed[6] = 0xc0;
        assert!(playback_thunk(&changed).is_err());
        for reference in [0_u16, 0x4000, 0x0094, 0xc094] {
            let mut changed = b;
            changed[6..8].copy_from_slice(&reference.to_be_bytes());
            assert!(playback_thunk(&changed).is_err());
        }
        let mut appended = b.to_vec();
        appended.push(63);
        assert!(playback_thunk(&appended).is_err());
    }
}
