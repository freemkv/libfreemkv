//! Root-menu scope only. Successful routes never imply episode or identity proof.

use super::super::{read_bounded, u16_at, u32_at};
use super::{
    ifo::{self, Pgc, required},
    pci,
    trace::{self, Outcome},
    vm::{Flow, Registers},
};
use crate::{
    disc::{
        DiscTitle, DvdLaunchEvidence, DvdLaunchReviewReason as Reason, DvdLaunchRoute,
        DvdLaunchStep,
    },
    sector::SectorSource,
    udf::UdfFs,
};

#[derive(Debug)]
pub(super) enum Failure {
    Io(crate::Error),
    Review(Reason),
}
impl From<crate::Error> for Failure {
    fn from(e: crate::Error) -> Self {
        Self::Io(e)
    }
}
impl From<Reason> for Failure {
    fn from(e: Reason) -> Self {
        Self::Review(e)
    }
}
type Result<T> = std::result::Result<T, Failure>;

/// Follow command-only PGC dispatchers; stop at the first actual menu or title.
pub(super) fn follow(
    pgcs: &[Pgc<'_>],
    first: usize,
    input: Outcome,
) -> ifo::Result<Vec<(usize, Outcome)>> {
    let mut pending = vec![(first, input)];
    let mut out = Vec::new();
    let mut budget = 128usize;
    while let Some((index, input)) = pending.pop() {
        if pending.len() + out.len() >= 64 || input.steps.len() > 1024 {
            return Err(Reason::BudgetExceeded);
        }
        budget = budget.checked_sub(1).ok_or(Reason::BudgetExceeded)?;
        let pgc = required(pgcs.get(index))?;
        for mut o in trace::run(&pgc.pre, input.registers.clone())? {
            let mut prefix = input.steps.clone();
            prefix.extend(o.steps);
            o.steps = prefix;
            match o.flow {
                Flow::Pgc(n) => pending.push((usize::from(n) - 1, o)),
                Flow::Next | Flow::Program(1) if pgc.bytes[2] > 0 && pgc.bytes[3] > 0 => {
                    out.push((index, o))
                }
                Flow::VtsTitle { .. } | Flow::VtsMenu { .. } | Flow::VmgMenu(_) => {
                    out.push((index, o))
                }
                _ => return Err(Reason::UnsupportedNavigation),
            }
        }
    }
    Ok(out)
}

fn full_title(
    reader: &mut dyn SectorSource,
    pgc: &Pgc<'_>,
    title: &DiscTitle,
    base: u32,
    budget: &mut usize,
) -> Result<()> {
    let b = pgc.bytes;
    let programs = usize::from(b[2]);
    let count = usize::from(b[3]);
    if programs == 0
        || programs > count
        || b[0x9c..0xa2] != [0; 6]
        || b[0xa2] != 0
        || b[0xa3] != 0
        || !pgc.cell.is_empty()
    {
        return Err(Reason::UnprovenPresentation.into());
    }
    let map = required(u16_at(b, 0xe6))?;
    let cells = required(u16_at(b, 0xe8))?;
    let pos = required(u16_at(b, 0xea))?;
    if map < 236 || cells < map + programs || pos < cells + count * 24 {
        return Err(Reason::IncompleteNavigation.into());
    }
    let mapping = required(b.get(map..map + programs))?;
    if mapping[0] != 1
        || mapping.windows(2).any(|p| p[0] >= p[1])
        || mapping.iter().any(|&n| n == 0 || usize::from(n) > count)
    {
        return Err(Reason::UnprovenPresentation.into());
    }
    required(b.get(pos..pos + count * 4))?;
    let raw = required(b.get(cells..cells + count * 24))?;
    let mut expected = Vec::new();
    for (i, c) in raw.as_chunks::<24>().0.iter().enumerate() {
        if c[0] & 0xf0 != 0 || c[1] != 0 || c[2] != 0 || c[3] != 0 {
            return Err(Reason::UnprovenPresentation.into());
        }
        let first = required(u32_at(c, 8))? as u32;
        let last = required(u32_at(c, 20))? as u32;
        if c[0] & 4 != 0 {
            expected.extend(super::interleave::walk(
                reader,
                base,
                first,
                last,
                &b[pos + i * 4..pos + i * 4 + 4],
                budget,
            )?);
        } else {
            expected.push(crate::disc::Extent {
                start_lba: base
                    .checked_add(first)
                    .ok_or(Reason::UnprovenPresentation)?,
                sector_count: last
                    .checked_sub(first)
                    .and_then(|n| n.checked_add(1))
                    .ok_or(Reason::UnprovenPresentation)?,
            });
        }
    }
    if expected != title.extents {
        return Err(Reason::UnprovenPresentation.into());
    }
    Ok(())
}

pub(super) fn produce(
    reader: &mut dyn SectorSource,
    fs: &UdfFs,
    vmg: &[u8],
    addresses: &[(u8, u8)],
    titles: &[DiscTitle],
) -> Result<Vec<DvdLaunchEvidence>> {
    if !reader.random_access() || titles.is_empty() || titles.len() != addresses.len() {
        return Err(Reason::IncompleteNavigation.into());
    }
    let vmg_menus = ifo::programs(vmg, 0xc8, 0, true)?;
    let root = ifo::entry(&vmg_menus, 2)?;
    let start = Outcome {
        flow: Flow::Next,
        registers: Registers::default(),
        steps: Vec::new(),
    };
    let entries = follow(&vmg_menus, root, start)?;
    let mut vts_number = None;
    for (_, e) in &entries {
        let Flow::VtsMenu { vts, menu: 3 } = e.flow else {
            return Err(Reason::UnsupportedNavigation.into());
        };
        if vts == 0 || vts_number.is_some_and(|n| n != vts) {
            return Err(Reason::AmbiguousNavigation.into());
        }
        vts_number = Some(vts);
    }
    let vts_number = required(vts_number)?;
    let path = format!("VTS_{vts_number:02}_0.IFO");
    let bytes = read_bounded(reader, fs, &path)?;
    let menus = ifo::programs(&bytes, 0xd0, vts_number, true)?;
    let title_pgcs = ifo::programs(&bytes, 0xcc, vts_number, false)?;
    let root = ifo::entry(&menus, 3)?;
    let mut states = Vec::new();
    for (_, e) in entries {
        states.extend(follow(&menus, root, e)?);
        if states.len() > 64 {
            return Err(Reason::BudgetExceeded.into());
        }
    }
    if states.is_empty()
        || states
            .iter()
            .any(|(i, e)| *i != root || !matches!(e.flow, Flow::Next | Flow::Program(1)))
    {
        return Err(Reason::AmbiguousNavigation.into());
    }
    let pgc = &menus[root];
    let b = pgc.bytes;
    if b[2] != 1
        || b[3] != 1
        || b[0x9c..0xa2] != [0; 6]
        || b[0xa2] != 0
        || b[0xa3] != 0
        || !pgc.post.is_empty()
    {
        return Err(Reason::UnsupportedNavigation.into());
    }
    let cell = required(u16_at(b, 0xe8))?;
    let map = required(u16_at(b, 0xe6))?;
    let position = required(u16_at(b, 0xea))?;
    if map < 236
        || b.get(map) != Some(&1)
        || cell < map + 1
        || position < cell + 24
        || b.get(position..position + 4).is_none()
        || required(u32_at(b, 8))? & (1 << 17) != 0
    {
        return Err(Reason::IncompleteNavigation.into());
    }
    let c = required(b.get(cell..cell + 24))?;
    if c[0] & 0xf4 != 0 || c[1] != 0 || c[2] != 0 {
        return Err(Reason::UnsupportedNavigation.into());
    }
    if c[3] != 0 {
        let cmd = required(pgc.cell.get(usize::from(c[3]) - 1))?;
        for (_, state) in &states {
            let outcomes = trace::run(std::slice::from_ref(cmd), state.registers.clone())?;
            if outcomes
                .iter()
                .any(|o| o.flow != Flow::TopProgram || o.registers != state.registers)
            {
                return Err(Reason::UnsupportedNavigation.into());
            }
        }
    } else if !pgc.cell.is_empty() {
        return Err(Reason::UnsupportedNavigation.into());
    }
    let first = required(u32_at(c, 8))?;
    let last = required(u32_at(c, 20))?;
    if last < first || last - first > 4095 {
        return Err(Reason::BudgetExceeded.into());
    }
    let vob = read_bounded(reader, fs, &format!("VTS_{vts_number:02}_0.VOB"))?;
    let mut buttons = None;
    let mut button_sector = 0;
    for sector in first..=last {
        let pack = required(vob.get(sector * 2048..(sector + 1) * 2048))?;
        if let Some(found) = pci::parse(pack, sector, buttons.as_ref())? {
            if buttons.as_ref().is_some_and(|b| b != &found) {
                return Err(Reason::AmbiguousNavigation.into());
            }
            if buttons.is_none() {
                button_sector = sector;
                buttons = Some(found);
            }
        }
    }
    let buttons = required(buttons)?;
    let ifo_base = fs.file_start_lba(reader, &format!("/VIDEO_TS/{path}"))?;
    let title_base = ifo_base
        .checked_add(required(u32_at(&bytes, 0xc4))? as u32)
        .ok_or(Reason::IncompleteNavigation)?;
    let mut evidence = vec![
        DvdLaunchEvidence::VerifiedRoot {
            vts: vts_number,
            pgcn: (root + 1) as u16,
            title_count: titles.len(),
            routes: Vec::new()
        };
        titles.len()
    ];
    let mut interleave_budget = 4096;
    let mut trace_steps_budget = 65_536usize;
    for (button, command) in buttons.commands.iter().enumerate() {
        let step = DvdLaunchStep {
            vts: vts_number,
            menu_vob: true,
            byte_offset: (button_sector * 2048 + 45 + 96 + 46 + button * 18 + 10) as u32,
            command: *command,
        };
        let mut launches = Vec::new();
        let mut is_menu = false;
        for (_, state) in &states {
            let mut registers = state.registers.clone();
            registers.sprm[8] = Some(((button + 1) as u16) << 10);
            for mut o in trace::run(std::slice::from_ref(&step), registers)? {
                let mut prefix = state.steps.clone();
                prefix.extend(o.steps);
                o.steps = prefix;
                let outcomes = match o.flow {
                    Flow::Pgc(n) => follow(&menus, usize::from(n) - 1, o)?,
                    _ => vec![(root, o)],
                };
                for (_, o) in outcomes {
                    match o.flow {
                        Flow::VtsTitle { title, part: 1 } => {
                            if launches.len() >= 64 {
                                return Err(Reason::BudgetExceeded.into());
                            }
                            launches.push((title, o));
                        }
                        Flow::Next | Flow::Program(1) => is_menu = true,
                        _ => return Err(Reason::UnsupportedNavigation.into()),
                    }
                }
            }
        }
        if launches.is_empty() && is_menu {
            continue;
        }
        if is_menu || launches.is_empty() {
            return Err(Reason::AmbiguousNavigation.into());
        }
        let target = launches[0].0;
        if launches.iter().any(|(t, _)| *t != target) {
            return Err(Reason::AmbiguousNavigation.into());
        }
        let matches: Vec<_> = addresses
            .iter()
            .enumerate()
            .filter(|(_, a)| **a == (vts_number, target))
            .map(|(i, _)| i)
            .collect();
        if matches.len() != 1 {
            return Err(Reason::IncompleteNavigation.into());
        }
        let index = matches[0];
        let global = required(super::super::sector_table(vmg, 0xc4))?;
        if super::super::u16_at(global, 0) != Some(titles.len()) {
            return Err(Reason::IncompleteNavigation.into());
        }
        let matching_entries: Vec<_> = required(global.get(8..8 + titles.len() * 12))?
            .as_chunks::<12>()
            .0
            .iter()
            .filter(|e| e[6] == vts_number && e[7] == target)
            .collect();
        if matching_entries.len() != 1 || matching_entries[0][1] != 1 {
            return Err(Reason::UnprovenPresentation.into());
        }
        let pgcn = ifo::title_pgc(&bytes, target, &title_pgcs)?;
        let target_pgc = &title_pgcs[pgcn];
        let mut audio = None;
        let mut traces = Vec::new();
        for (_, launch) in launches {
            for mut o in trace::run(&target_pgc.pre, launch.registers)? {
                if o.flow != Flow::Next {
                    return Err(Reason::UnprovenPresentation.into());
                }
                let stream = required(o.registers.sprm[1])?;
                if stream > 7
                    || u16_at(target_pgc.bytes, 12 + usize::from(stream) * 2)
                        .is_none_or(|n| n & 0x8000 == 0)
                    || audio.is_some_and(|a| a != stream)
                {
                    return Err(Reason::AmbiguousNavigation.into());
                }
                audio = Some(stream);
                for post in trace::run(&target_pgc.post, o.registers)? {
                    if post.flow != Flow::ReturnMenu {
                        return Err(Reason::UnprovenPresentation.into());
                    }
                    let command = required(post.steps.last())?.command;
                    if command[4] > target_pgc.bytes[3] {
                        return Err(Reason::UnprovenPresentation.into());
                    }
                    match command[5] {
                        0x42 => {
                            ifo::entry(&vmg_menus, 2)?;
                        }
                        0x83 => {
                            ifo::entry(&menus, 3)?;
                        }
                        0xc0 if usize::from(command[3]) <= vmg_menus.len() => {}
                        _ => return Err(Reason::IncompleteNavigation.into()),
                    }
                }
                let mut prefix = launch.steps.clone();
                prefix.append(&mut o.steps);
                trace_steps_budget = trace_steps_budget
                    .checked_sub(prefix.len())
                    .ok_or(Reason::BudgetExceeded)?;
                traces.push(prefix);
            }
        }
        tracing::debug!(target:"freemkv::scan",button=button+1,vts=vts_number,title=target,?audio,"dvd: root command target resolved; full presentation proof still required");
        full_title(
            reader,
            target_pgc,
            &titles[index],
            title_base,
            &mut interleave_budget,
        )?;
        let DvdLaunchEvidence::VerifiedRoot { routes, .. } = &mut evidence[index] else {
            return Err(Reason::IncompleteNavigation.into());
        };
        let audio_stream = required(audio)? as u8;
        let (audio_pid, audio_language) =
            super::audio::selected(&bytes, target_pgc.bytes, audio_stream, &titles[index])?;
        routes.push(DvdLaunchRoute {
            button: (button + 1) as u8,
            display_masks: buttons.masks.clone(),
            target_vts: vts_number,
            target_title: target,
            target_part: 1,
            audio_stream,
            audio_pid,
            audio_language,
            traces,
        });
    }
    Ok(evidence)
}
