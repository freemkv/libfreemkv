//! DVD-Video navigation executor — the DVD arm of "mimic a real player" title
//! selection (issue #40).
//!
//! A player enters a disc at the **First-Play PGC** (FP_PGC): a short program of
//! VM commands (parsed by [`super::vmcmd`]) that runs before any menu draws and
//! typically dispatches straight to the feature title. This module reads that
//! program out of `VIDEO_TS.IFO` (VMGI), executes its pre-command list on a
//! minimal register machine, and — when the program deterministically reaches a
//! title dispatch (`JumpTT`) — maps the selected VMG title through the title
//! search pointer table (`TT_SRPT`) to its `(VTS number, title-in-set)`
//! coordinates. That is the title a player would land on, so DVD title
//! selection can prefer it over the structural leading-cell heuristic.
//!
//! Contract: this is a strict improvement over the fallback. Any parse error,
//! interactive/menu-only entry, or non-convergence yields `None`, and the caller
//! keeps today's behaviour. Every table count and offset is attacker-controlled,
//! so all arithmetic is checked and all reads are bounds-guarded — the entry
//! point [`resolve_from_vmg`] never panics on any input (exercised by the
//! never-panic harness).

use super::vmcmd::{self, Compare, Instr};
use crate::consts::SECTOR_BYTES;
use crate::sector::SectorSource;
use crate::udf::UdfFs;

// ── VMGI management-table byte offsets into VIDEO_TS.IFO (DVD-Video spec) ─────
//
// The VMGI_MAT header carries these fixed offsets. The values they hold are
// per-disc; the offsets are constant.
const VMGI_MAGIC: &[u8; 12] = b"DVDVIDEO-VMG";
/// u32 **byte** offset (from the start of VIDEO_TS.IFO) of the First-Play PGC.
const VMGI_FP_PGC_PTR: usize = 0x84;
/// u32 **sector** offset (from the start of VIDEO_TS.IFO) of TT_SRPT.
const VMGI_TT_SRPT_PTR: usize = 0xC4;

/// Within a PGC, the command-table pointer is a u16 at `PGC + 0xE4` giving the
/// command table's byte offset relative to the PGC start (DVD-Video PGC layout;
/// the same layout `ifo::parse_pgc` reads for cell/program tables).
const PGC_CMD_TBL_PTR: usize = 0xE4;

/// Upper bound on commands executed before declaring non-convergence. A
/// conformant First-Play routine reaches a title in a handful of steps; the cap
/// stops a crafted command list (self-`Goto`, mutual jumps) from spinning.
const STEP_BUDGET: usize = 1024;

/// Maximum commands honoured in one PGC command list. The DVD-Video VM caps a
/// list at 128 pre / 128 post / 128 cell commands; the on-disc count is an
/// untrusted u16, so it is clamped to this format maximum.
const MAX_CMDS: usize = 128;

/// Maximum TT_SRPT entries honoured — the DVD-Video 99-title format maximum
/// (mirrors `ifo::MAX_TT_SRPT_TITLES`; the on-disc count is an untrusted u16).
const MAX_TT_SRPT_TITLES: usize = 99;

/// The title the First-Play navigation selects, in VMG coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedTitle {
    /// 1-based VMG title number (the TT_SRPT index the `JumpTT` named).
    pub title: u8,
    /// 1-based VTS (title set) number the title lives in.
    pub vtsn: u8,
    /// 1-based title-within-set number (TT_SRPT `VTS_TTN`).
    pub vts_ttn: u8,
}

// ── Bounds-guarded big-endian reads (return None past the end) ────────────────

#[inline]
fn u16_at(b: &[u8], o: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*b.get(o)?, *b.get(o.checked_add(1)?)?]))
}

#[inline]
fn u32_at(b: &[u8], o: usize) -> Option<u32> {
    Some(u32::from_be_bytes([
        *b.get(o)?,
        *b.get(o.checked_add(1)?)?,
        *b.get(o.checked_add(2)?)?,
        *b.get(o.checked_add(3)?)?,
    ]))
}

/// A register operand index names a system parameter (SPRM) when it carries the
/// `0x80` bias — matching [`Vm::reg`]'s split of GPRM (`< 128`) vs SPRM
/// (`>= 128`). Only SPRM reads are session-specific and therefore taint a
/// compare; GPRM reads do not.
#[inline]
fn is_sprm(idx: u8) -> bool {
    idx >= 128
}

// ── Minimal navigation register machine ──────────────────────────────────────

/// The subset of the DVD-Video VM register file the First-Play resolver needs:
/// 16 general parameters (GPRMs) and the system parameters (SPRMs). Execution
/// starts from a cold machine (all zero) — the same starting point a player has
/// before any user interaction. The SPRMs a First-Play routine might branch on
/// (region, parental level, language) are player-/session-specific and unknown
/// at scan time; leaving them zero means the resolver only commits to a title
/// when the program reaches one *without* depending on them (an unconditional
/// dispatch, or a conditional whose zero-register path a player also takes).
/// When a branch genuinely depends on an unknown SPRM the routine is treated as
/// undecidable and the caller falls back — see the module contract.
struct Vm {
    gprm: [u16; 16],
    sprm: [u16; 24],
    /// Per-GPRM taint: set when the register holds a value derived from a system
    /// parameter (SPRM). An SPRM read taints a compare directly ([`is_sprm`]);
    /// this bit tracks an SPRM *laundered through* a GPRM store so a later
    /// GPRM-only compare that reads it is still recognised as undecidable.
    gprm_tainted: [bool; 16],
}

impl Vm {
    fn new() -> Self {
        Self {
            gprm: [0; 16],
            sprm: [0; 24],
            gprm_tainted: [false; 16],
        }
    }

    /// Whether a register *operand read* is session-specific: an SPRM read is
    /// always undecidable at scan time; a GPRM read is undecidable only when the
    /// register currently holds an SPRM-derived (tainted) value.
    fn reg_tainted(&self, idx: u8) -> bool {
        if is_sprm(idx) {
            true
        } else {
            self.gprm_tainted[(idx & 0x0F) as usize]
        }
    }

    /// Mark a GPRM's post-state as unknown. Used when a store to it was guarded
    /// by an undecidable (tainted) predicate: whether the write happened is
    /// itself unknown, so the destination is conservatively tainted.
    fn taint_gprm(&mut self, reg: u8) {
        self.gprm_tainted[(reg & 0x0F) as usize] = true;
    }

    /// Read a register operand. Per the VM command model, indices `< 16` are
    /// GPRMs and `>= 128` are SPRMs (0-23 after the `0x80` bias); anything out
    /// of range reads as 0 rather than panicking.
    fn reg(&self, idx: u8) -> u16 {
        if idx >= 128 {
            self.sprm.get((idx - 128) as usize).copied().unwrap_or(0)
        } else {
            self.gprm.get((idx & 0x0F) as usize).copied().unwrap_or(0)
        }
    }

    /// Evaluate a compare predicate (op codes per [`Compare`]). Returns the
    /// predicate result together with a *taint* flag that is set when either
    /// operand read a system parameter (SPRM). An SPRM's value is
    /// player-/session-specific (region, parental level, language) and unknown
    /// at scan time, so a branch decision that depends on one is undecidable —
    /// the caller abstains rather than commit to the cold-power-on arm. A GPRM
    /// read taints only when that register was itself loaded from an SPRM
    /// (tracked by [`Vm::gprm_tainted`]); a GPRM holding a program-established
    /// value does not taint.
    fn eval(&self, c: &Compare) -> (bool, bool) {
        let mut tainted = self.reg_tainted(c.lhs_reg);
        let l = self.reg(c.lhs_reg);
        let r = if c.immediate {
            c.imm
        } else {
            tainted |= self.reg_tainted(c.rhs_reg);
            self.reg(c.rhs_reg)
        };
        let result = match c.op {
            1 => (l & r) != 0,
            2 => l == r,
            3 => l != r,
            4 => l >= r,
            5 => l > r,
            6 => l <= r,
            7 => l < r,
            _ => false,
        };
        (result, tainted)
    }

    /// Apply a `SetGPRM`. Only the arithmetic/logic set-ops that a First-Play
    /// routine uses to prepare a dispatch are modelled; unmodelled ops
    /// (swap/rnd) leave the register unchanged (best-effort, never panic).
    fn set(&mut self, reg: u8, op: u8, immediate: bool, imm: u16, src: u8) {
        let idx = (reg & 0x0F) as usize;
        let v = if immediate { imm } else { self.reg(src) };
        // The value's taint follows its inputs: an immediate is concrete
        // (untainted); a register source carries its own taint ([`reg_tainted`]
        // covers both a direct SPRM read and a laundered GPRM).
        let src_tainted = !immediate && self.reg_tainted(src);
        let cur = self.gprm[idx];
        let cur_tainted = self.gprm_tainted[idx];
        // Each arm sets (new value, new taint). `mov` overwrites, so its taint is
        // the source's alone — this is what CLEARS a prior taint on a concrete
        // store. The accumulating ops fold the source into the current value, so
        // their taint is the union. Unmodelled ops leave value and taint intact.
        let (nv, nt) = match op {
            1 => (v, src_tainted),                                  // mov
            3 => (cur.wrapping_add(v), cur_tainted || src_tainted), // add
            4 => (cur.wrapping_sub(v), cur_tainted || src_tainted), // sub
            5 => (cur.wrapping_mul(v), cur_tainted || src_tainted), // mul
            6 => (
                cur.checked_div(v).unwrap_or(cur),
                cur_tainted || src_tainted,
            ), // div (÷0 → unchanged)
            7 => (
                cur.checked_rem(v).unwrap_or(cur),
                cur_tainted || src_tainted,
            ), // mod (÷0 → unchanged)
            9 => (cur & v, cur_tainted || src_tainted),             // and
            10 => (cur | v, cur_tainted || src_tainted),            // or
            11 => (cur ^ v, cur_tainted || src_tainted),            // xor
            _ => (cur, cur_tainted),                                // swap/rnd/unmodelled
        };
        self.gprm[idx] = nv;
        self.gprm_tainted[idx] = nt;
    }
}

// ── VMGI structure readers ───────────────────────────────────────────────────

/// Extract the First-Play PGC pre-command list from VMGI bytes. Returns `None`
/// when there is no First-Play PGC, no command table, or the table is out of
/// bounds — all of which mean "nothing to execute" → fall back.
fn fp_pre_commands(vmg: &[u8]) -> Option<Vec<[u8; 8]>> {
    let pgc = u32_at(vmg, VMGI_FP_PGC_PTR)? as usize;
    if pgc == 0 {
        return None; // no First-Play PGC on this disc
    }
    let cmd_ptr = u16_at(vmg, pgc.checked_add(PGC_CMD_TBL_PTR)?)? as usize;
    if cmd_ptr == 0 {
        return None; // First-Play PGC carries no command table
    }
    let tbl = pgc.checked_add(cmd_ptr)?;
    // Command table header: nr_of_pre (u16) at +0, then pre-commands at +8.
    let nr_pre = (u16_at(vmg, tbl)? as usize).min(MAX_CMDS);
    let base = tbl.checked_add(8)?;
    let mut cmds = Vec::with_capacity(nr_pre);
    for i in 0..nr_pre {
        let o = base.checked_add(i.checked_mul(8)?)?;
        if o.checked_add(8)? > vmg.len() {
            break; // truncated table — execute what parsed
        }
        let mut c = [0u8; 8];
        c.copy_from_slice(&vmg[o..o + 8]);
        cmds.push(c);
    }
    Some(cmds)
}

/// Parse TT_SRPT into a per-title `(vtsn, vts_ttn)` list indexed by 1-based VMG
/// title number. This is the mapping `JumpTT` needs: a title number → the VTS
/// title-set coordinates that identify the corresponding scanned title.
fn tt_srpt_titles(vmg: &[u8]) -> Option<Vec<(u8, u8)>> {
    let sector = u32_at(vmg, VMGI_TT_SRPT_PTR)? as usize;
    let base = sector.checked_mul(SECTOR_BYTES)?;
    let n = (u16_at(vmg, base)? as usize).min(MAX_TT_SRPT_TITLES);
    let entries = base.checked_add(8)?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        // Title entries are 12 bytes: VTSN at +6, VTS_TTN at +7.
        let e = entries.checked_add(i.checked_mul(12)?)?;
        if e.checked_add(12)? > vmg.len() {
            break; // truncated — map what parsed
        }
        out.push((vmg[e + 6], vmg[e + 7]));
    }
    Some(out)
}

// ── First-Play executor ──────────────────────────────────────────────────────

/// Execute the First-Play pre-command list and return the VMG title number it
/// dispatches to, if it reaches one deterministically. Returns `None` for a
/// program that ends in a menu/sub-domain, links within the menu domain, exits,
/// or does not converge inside the step budget — every case the caller falls
/// back on.
fn run_first_play(cmds: &[[u8; 8]]) -> Option<u8> {
    let mut vm = Vm::new();
    let mut pc = 0usize;
    let mut budget = STEP_BUDGET;

    while pc < cmds.len() {
        if budget == 0 {
            return None; // non-convergence guard
        }
        budget -= 1;

        let cmd = vmcmd::decode(&cmds[pc]);

        // A guarded command's predicate decides whether its instruction runs.
        // `taken` is the predicate result; `tainted` marks it undecidable at scan
        // time (it read a system parameter, directly or laundered through a
        // GPRM). A command with no predicate always runs and is never tainted.
        let (taken, tainted) = match cmd.compare {
            Some(cmp) => vm.eval(&cmp),
            None => (true, false),
        };

        // A decidable false predicate skips this line — no effect, taint
        // unchanged. When the predicate is tainted we cannot decide `taken`, so
        // we do not skip; the per-instruction handling below is conservative.
        if !tainted && !taken {
            pc += 1;
            continue;
        }

        match cmd.instr {
            // Title dispatch — a control transfer, the answer we are looking for.
            // Abstain only when an undecidable predicate gates it: committing
            // would pick the cold-power-on arm of an SPRM-dependent decision.
            Instr::JumpTt { ttn } => {
                if tainted {
                    return None;
                }
                return Some(ttn);
            }

            // Control flow within the pre list (Goto lines are 1-based) — also a
            // control transfer, so an undecidable guard abstains.
            Instr::Goto { line } => {
                if tainted {
                    return None;
                }
                let l = line as usize;
                if l == 0 || l > cmds.len() {
                    return None;
                }
                pc = l - 1;
                continue;
            }

            // Register preparation before a dispatch — a store, not a control
            // transfer, so a tainted guard does NOT abstain. If the guard is
            // undecidable we do not know whether the store ran, so the
            // destination's post-state is unknown → taint it. Otherwise apply the
            // store (which itself propagates or clears the destination's taint).
            Instr::SetGprm {
                reg,
                op,
                immediate,
                imm,
                src,
            } => {
                if tainted {
                    vm.taint_gprm(reg);
                } else {
                    vm.set(reg, op, immediate, imm, src);
                }
            }

            // No-effect (for resolution) instructions: keep executing. SetSystem
            // writes an SPRM (never a GPRM in this decoder), and SPRM reads are
            // caught at read time, so no GPRM taint is needed here.
            Instr::Nop | Instr::SetSystem => {}

            // Everything else leaves the deterministically-followable path:
            // Break/Exit end the pre list with no title selected; the JumpSS
            // sub-domain jumps and the intra-domain Link ops land in a menu or
            // depend on an interactive button selection that cannot be resolved
            // statically. Abstain → caller falls back.
            _ => return None,
        }

        pc += 1;
    }

    None
}

// ── Public entry points ──────────────────────────────────────────────────────

/// Resolve the main-feature title from raw VMGI (`VIDEO_TS.IFO`) bytes.
///
/// Pure and total: any malformed/hostile input returns `None`, never panics.
/// This is the harness entry point.
pub fn resolve_from_vmg(vmg: &[u8]) -> Option<ResolvedTitle> {
    if vmg.len() < 0xC8 || &vmg[0..12] != VMGI_MAGIC {
        return None;
    }
    let cmds = fp_pre_commands(vmg)?;
    let ttn = run_first_play(&cmds)?;
    if ttn == 0 {
        return None; // title 0 is not addressable
    }
    let titles = tt_srpt_titles(vmg)?;
    let (vtsn, vts_ttn) = titles.get((ttn as usize) - 1).copied()?;
    if vtsn == 0 {
        return None; // invalid TT_SRPT entry
    }
    Some(ResolvedTitle {
        title: ttn,
        vtsn,
        vts_ttn,
    })
}

/// Resolve the main-feature title by reading the disc's VMGI and following its
/// First-Play navigation. Returns `None` (→ caller keeps the heuristic) when the
/// IFO cannot be read or the navigation does not deterministically reach a
/// title.
pub fn resolve_main_title(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ResolvedTitle> {
    let vmg = udf.read_file(reader, "/VIDEO_TS/VIDEO_TS.IFO").ok()?;
    let resolved = resolve_from_vmg(&vmg)?;
    tracing::debug!(
        target: "freemkv::dvdnav",
        title = resolved.title,
        vtsn = resolved.vtsn,
        vts_ttn = resolved.vts_ttn,
        "nav resolved main-feature title from First-Play program"
    );
    Some(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Fixture builders ────────────────────────────────────────────────────

    /// Assemble a VMGI (`VIDEO_TS.IFO`) image with a First-Play PGC whose
    /// pre-command list is `pre`, and a TT_SRPT at sector `tt_srpt_sector`
    /// mapping each title to `(vtsn, vts_ttn)`.
    ///
    /// Layout: the fixed VMGI header (magic + the FP_PGC and TT_SRPT pointers),
    /// then the FP_PGC (with a command table holding `pre`), then the TT_SRPT at
    /// its sector. Kept minimal but spec-shaped so the same code path a real
    /// disc takes is exercised.
    fn build_vmgi(pre: &[[u8; 8]], tt_srpt_sector: u32, titles: &[(u8, u8)]) -> Vec<u8> {
        // Place the FP_PGC at a fixed byte offset past the header.
        let fp_pgc_off: u32 = 0x400;
        // Command table sits 0x100 bytes into the PGC.
        let cmd_tbl_rel: u16 = 0x100;

        let mut v = vec![0u8; 0x800];
        v[0..12].copy_from_slice(VMGI_MAGIC);
        v[VMGI_FP_PGC_PTR..VMGI_FP_PGC_PTR + 4].copy_from_slice(&fp_pgc_off.to_be_bytes());
        v[VMGI_TT_SRPT_PTR..VMGI_TT_SRPT_PTR + 4].copy_from_slice(&tt_srpt_sector.to_be_bytes());

        // FP_PGC: command-table pointer at +0xE4.
        let pgc = fp_pgc_off as usize;
        v[pgc + PGC_CMD_TBL_PTR..pgc + PGC_CMD_TBL_PTR + 2]
            .copy_from_slice(&cmd_tbl_rel.to_be_bytes());

        // Command table: nr_of_pre at +0, pre-commands from +8.
        let tbl = pgc + cmd_tbl_rel as usize;
        v[tbl..tbl + 2].copy_from_slice(&(pre.len() as u16).to_be_bytes());
        for (i, c) in pre.iter().enumerate() {
            let o = tbl + 8 + i * 8;
            v[o..o + 8].copy_from_slice(c);
        }

        // TT_SRPT at its sector: count at +0, 12-byte entries from +8.
        let base = tt_srpt_sector as usize * SECTOR_BYTES;
        let need = base + 8 + titles.len() * 12;
        if v.len() < need {
            v.resize(need, 0);
        }
        v[base..base + 2].copy_from_slice(&(titles.len() as u16).to_be_bytes());
        for (i, &(vtsn, vts_ttn)) in titles.iter().enumerate() {
            let e = base + 8 + i * 12;
            v[e + 6] = vtsn;
            v[e + 7] = vts_ttn;
        }
        v
    }

    fn h(s: &str) -> [u8; 8] {
        let v: Vec<u8> = (0..8)
            .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    // ── Executor: convergence to a title ────────────────────────────────────

    /// Unconditional-dispatch shape: First-Play is an unconditional `JumpTT 1`,
    /// and TT_SRPT maps title 1 to the feature title set (here VTS_02, title 1).
    /// The resolver must return that title.
    #[test]
    fn first_play_jumptt_1_resolves_to_its_title_set() {
        // 3002...0001 = JumpTT ttn=1.
        let vmgi = build_vmgi(&[h("3002000000010000")], 1, &[(2, 1), (3, 1)]);
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 2,
                vts_ttn: 1
            })
        );
    }

    /// A First-Play that sets up a register and then unconditionally dispatches
    /// still resolves (the SetGPRM is a no-op for an unconditional JumpTT).
    #[test]
    fn setgprm_then_jumptt_resolves() {
        let vmgi = build_vmgi(
            &[h("7100000603e80000"), h("3002000000020000")],
            1,
            &[(1, 1), (4, 1)],
        );
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 2,
                vtsn: 4,
                vts_ttn: 1
            })
        );
    }

    /// A conditional dispatch whose predicate is true on the cold (zero)
    /// machine is taken. `if g0 == 0 -> JumpTT 1` with g0 defaulting to 0.
    #[test]
    fn conditional_jumptt_true_on_cold_machine_is_taken() {
        // 30 22: jump with EQ compare (op=2), operands are registers b6/b7 both
        // 0 (g0 == g0) → true; JumpTT ttn=b5=1.
        let vmgi = build_vmgi(&[h("3022000000010000")], 1, &[(5, 1)]);
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 5,
                vts_ttn: 1
            })
        );
    }

    /// A conditional dispatch that is false falls through to the next line's
    /// unconditional dispatch.
    #[test]
    fn false_conditional_falls_through_to_next_dispatch() {
        // line 1: 30 32 with GT compare (op=3 = !=) g0(b6=1?) ... build a clearly
        // false compare: if g0 != g0 -> JumpTT 9 (false, skipped);
        // line 2: unconditional JumpTT 1.
        let vmgi = build_vmgi(
            &[h("3032000000090000"), h("3002000000010000")],
            1,
            &[(7, 1)],
        );
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 7,
                vts_ttn: 1
            })
        );
    }

    // ── Executor: abstention (→ caller falls back) ──────────────────────────

    /// Menu-entry shape: First-Play jumps into a VTS menu (`JumpSS VTSM root`).
    /// That is an interactive menu, not a static dispatch — the resolver must
    /// abstain so the caller keeps the leading-cell heuristic.
    #[test]
    fn first_play_into_menu_abstains() {
        // 3006...0183 = JumpSS VTSM root.
        let vmgi = build_vmgi(&[h("3006000101830000")], 1, &[(3, 1)]);
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    /// A dispatch gated on a system parameter (SPRM) is undecidable at scan
    /// time — the resolver must abstain rather than commit to the cold-power-on
    /// arm. Here `if SPRM0 == g0 -> JumpTT 1` is *true* on the cold machine
    /// (both read 0), so without SPRM taint-tracking the resolver would wrongly
    /// return title 1; with it, the SPRM read taints the branch and it abstains.
    #[test]
    fn sprm_gated_dispatch_abstains() {
        // 30 22: jump with EQ compare (op=2); if_v2 operands are registers
        // b6/b7. b6=0x80 = SPRM0 (session-specific), b7=0x00 = g0. ttn=b5=1.
        let vmgi = build_vmgi(&[h("3022000000018000")], 1, &[(2, 1)]);
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    /// The mirror of the SPRM case: a compare that reads only GPRMs is decidable
    /// and must NOT over-abstain. `if g0 == g1 -> JumpTT 1` is true on the cold
    /// machine and resolves to title 1 (no SPRM operand → no taint).
    #[test]
    fn gprm_only_compare_does_not_taint() {
        // 30 22: jump EQ; b6=0x00 = g0, b7=0x01 = g1 (both 0 on the cold
        // machine → equal). ttn=b5=1.
        let vmgi = build_vmgi(&[h("3022000000010001")], 1, &[(6, 1)]);
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 6,
                vts_ttn: 1
            })
        );
    }

    /// Defect-1 regression (over-abstention): a store guarded by an SPRM-tainted
    /// compare does NOT gate a control transfer, so it must not abstain. Here
    /// `if SPRM0 == g0 -> g0 = 5` (a guarded store) is followed by an
    /// UNCONDITIONAL `JumpTT 1`. The tainted predicate only decides whether the
    /// store runs; the dispatch that follows is SPRM-independent, so the resolver
    /// must resolve to title 1.
    #[test]
    fn sprm_guarded_store_then_unconditional_dispatch_resolves() {
        // line 0: 71 20 | lhs=b2=0x80 (SPRM0), cmp EQ(op=2, register), rhs=b7=0
        //         (g0)  → tainted guard; SetGPRM g0 = imm(bytes4-5)=5 (mov).
        // line 1: unconditional JumpTT 1.
        let vmgi = build_vmgi(
            &[h("7120800000050000"), h("3002000000010000")],
            1,
            &[(2, 1)],
        );
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 2,
                vts_ttn: 1
            })
        );
    }

    /// Defect-2 regression (under-abstention): an SPRM laundered through a GPRM
    /// must still taint a later branch. `SetGPRM g0 = SPRM20` (register source,
    /// index >= 128) followed by `if g0 == g1 -> JumpTT 1` — the compare reads
    /// only GPRMs, but g0 now holds an SPRM-derived value, so the decision is
    /// undecidable and the resolver must abstain.
    #[test]
    fn sprm_laundered_through_gprm_abstains() {
        // line 0: 61 00 | SetGPRM g0 = SPRM20 (mov, register src=b5=0x94).
        // line 1: 30 22 | if g0 == g1 -> JumpTT 1 (both 0 on cold machine).
        let vmgi = build_vmgi(
            &[h("6100000000940000"), h("3022000000010001")],
            1,
            &[(2, 1)],
        );
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    /// Guard: an unconditional immediate store CLEARS a prior taint (no sticky
    /// latch). Taint g0 from an SPRM, overwrite it with an immediate, then branch
    /// on g0 — the branch is now decidable and must resolve, not abstain.
    #[test]
    fn unconditional_immediate_store_clears_taint() {
        // line 0: SetGPRM g0 = SPRM20 (taints g0).
        // line 1: SetGPRM g0 = imm 0 (mov, immediate) → clears taint, g0 = 0.
        // line 2: if g0 == g1 -> JumpTT 1 (both 0) → resolves.
        let vmgi = build_vmgi(
            &[
                h("6100000000940000"),
                h("7100000000000000"),
                h("3022000000010001"),
            ],
            1,
            &[(2, 1)],
        );
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 2,
                vts_ttn: 1
            })
        );
    }

    /// Guard: copying an UNtainted GPRM does not taint the destination. `g0 = g1`
    /// (register source, g1 untainted) then `if g0 == g2 -> JumpTT 1` must
    /// resolve (no SPRM ever entered g0).
    #[test]
    fn gprm_copy_of_untainted_gprm_does_not_taint() {
        // line 0: 61 00 | SetGPRM g0 = g1 (mov, register src=b5=1).
        // line 1: 30 22 | if g0 == g2 -> JumpTT 1 (both 0) → resolves.
        let vmgi = build_vmgi(
            &[h("6100000000010000"), h("3022000000010002")],
            1,
            &[(2, 1)],
        );
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 2,
                vts_ttn: 1
            })
        );
    }

    /// An empty First-Play pre list selects no title.
    #[test]
    fn empty_first_play_abstains() {
        let vmgi = build_vmgi(&[], 1, &[(2, 1)]);
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    /// A JumpTT whose title number is past the TT_SRPT table resolves to no
    /// coordinates (→ abstain) rather than indexing out of bounds.
    #[test]
    fn jumptt_past_tt_srpt_abstains() {
        // JumpTT ttn=9 but TT_SRPT declares only 1 title.
        let vmgi = build_vmgi(&[h("3002000000090000")], 1, &[(2, 1)]);
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    /// A self-referential `Goto` cannot spin forever — the step budget stops it
    /// and the resolver abstains.
    #[test]
    fn self_goto_hits_budget_and_abstains() {
        // Goto line 1 (0-special sub 1), byte7 = 01.
        let vmgi = build_vmgi(&[h("0001000000000001")], 1, &[(2, 1)]);
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    // ── Robustness ──────────────────────────────────────────────────────────

    #[test]
    fn bad_magic_abstains() {
        let mut vmgi = build_vmgi(&[h("3002000000010000")], 1, &[(2, 1)]);
        vmgi[0] = b'X';
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    #[test]
    fn truncated_and_short_inputs_never_panic() {
        // Below the header minimum, and a valid header truncated at every length.
        assert_eq!(resolve_from_vmg(&[]), None);
        assert_eq!(resolve_from_vmg(VMGI_MAGIC), None);
        let full = build_vmgi(&[h("3002000000010000")], 1, &[(2, 1)]);
        for len in 0..full.len() {
            let _ = resolve_from_vmg(&full[..len]);
        }
    }

    /// A TT_SRPT pointer that would overflow when scaled to a byte offset must
    /// be rejected, not wrap. Build a normal fixture, then overwrite just the
    /// TT_SRPT sector pointer with the extreme value (building a real table at
    /// that sector would allocate terabytes in the fixture, not the resolver).
    #[test]
    fn overflowing_tt_srpt_sector_abstains() {
        let mut vmgi = build_vmgi(&[h("3002000000010000")], 1, &[(2, 1)]);
        vmgi[VMGI_TT_SRPT_PTR..VMGI_TT_SRPT_PTR + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }
}
