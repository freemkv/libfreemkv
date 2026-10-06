//! DVD-Video navigation executor — the DVD arm of "mimic a real player" title selection (issue
//! #40).
//!
//! Reads the First-Play PGC pre-command list from `VIDEO_TS.IFO`, executes it
//! on a minimal register machine, and when it deterministically reaches a
//! title dispatch (`JumpTT`), maps the VMG title through `TT_SRPT` to
//! `(VTS number, title-in-set)` coordinates.
//!
//! Contract: any parse error, menu-only entry, or non-convergence yields
//! `None`; [`resolve_from_vmg`] never panics on any input.

use super::vmcmd::{self, Compare, Instr};
use crate::consts::SECTOR_BYTES;
use crate::sector::SectorSource;
use crate::udf::UdfFs;

// ── VMGI management-table byte offsets into VIDEO_TS.IFO (DVD-Video spec) ─────
// The VMGI_MAT header carries these fixed offsets: per-disc values live at
// disc-constant offsets.
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

// Compare ops (libdvdnav eval_compare).
const CMP_AND: u8 = 1;
const CMP_EQ: u8 = 2;
const CMP_NE: u8 = 3;
const CMP_GE: u8 = 4;
const CMP_GT: u8 = 5;
const CMP_LE: u8 = 6;
const CMP_LT: u8 = 7;

// Set-op codes (libdvdnav eval_set_op).
const SET_MOV: u8 = 1;
const SET_SWAP: u8 = 2;
const SET_ADD: u8 = 3;
const SET_SUB: u8 = 4;
const SET_MUL: u8 = 5;
const SET_DIV: u8 = 6;
const SET_MOD: u8 = 7;
const SET_RND: u8 = 8;
const SET_AND: u8 = 9;
const SET_OR: u8 = 10;
const SET_XOR: u8 = 11;

/// Maximum TT_SRPT entries honoured — the DVD-Video 99-title format maximum
/// (the on-disc count is an untrusted u16). Shared with the IFO parser so the
/// two can never diverge.
use crate::ifo::MAX_TT_SRPT_TITLES;

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

// Register operand index >= 0x80 (128) names a system parameter (SPRM),
// matching [`Vm::reg`]'s GPRM/SPRM split. Only SPRM reads are session-specific
// and therefore taint a compare; GPRM reads do not.
#[inline]
fn is_sprm(idx: u8) -> bool {
    idx >= 128
}

// ── Minimal navigation register machine ──────────────────────────────────────

// Minimal VM register file (GPRMs + SPRMs) for the First-Play resolver, cold (all-zero) at
// start.
struct Vm {
    gprm: [u16; 16],
    sprm: [u16; 24],
    // Per-GPRM taint: set when SPRM-derived (or random).
    gprm_tainted: [bool; 16],
    // Counter-mode GPRMs count elapsed time, so their value is never decidable.
    gprm_counter: [bool; 16],
}

impl Vm {
    fn new() -> Self {
        Self {
            gprm: [0; 16],
            sprm: [0; 24],
            gprm_tainted: [false; 16],
            gprm_counter: [false; 16],
        }
    }

    /// Whether a register *operand read* is session-specific: an SPRM read is
    /// always undecidable at scan time; a GPRM read is undecidable only when the
    /// register currently holds an SPRM-derived (tainted) value.
    fn reg_tainted(&self, idx: u8) -> bool {
        if is_sprm(idx) {
            true
        } else {
            let i = (idx & 0x0F) as usize;
            self.gprm_tainted[i] || self.gprm_counter[i]
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

    // Evaluate a compare predicate (op codes per [`Compare`]); returns (result, taint). Taint
    // marks an SPRM-dependent (undecidable) branch.
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
            CMP_AND => (l & r) != 0,
            CMP_EQ => l == r,
            CMP_NE => l != r,
            CMP_GE => l >= r,
            CMP_GT => l > r,
            CMP_LE => l <= r,
            CMP_LT => l < r,
            _ => false,
        };
        (result, tainted)
    }

    /// Apply a `SetGPRM` per libdvdnav eval_set_op (reverse-engineered player
    /// behaviour): add/mul saturate at 0xFFFF, sub clamps at 0, ÷0 and mod 0
    /// yield 0xFFFF, swap also writes the source register, rnd taints.
    fn set(&mut self, reg: u8, op: u8, immediate: bool, imm: u16, src: u8) {
        let idx = (reg & 0x0F) as usize;
        let v = if immediate { imm } else { self.reg(src) };
        // The value's taint follows its inputs: an immediate is concrete
        // (untainted); a register source carries its own taint ([`reg_tainted`]
        // covers both a direct SPRM read and a laundered GPRM).
        let src_tainted = !immediate && self.reg_tainted(src);
        let cur = self.gprm[idx];
        let cur_tainted = self.reg_tainted(reg & 0x0F);
        let t = cur_tainted || src_tainted;
        // Each arm sets (new value, new taint). `mov` overwrites with the source's
        // taint alone (clearing prior taint); accumulating ops union both.
        let (nv, nt) = match op {
            SET_MOV => (v, src_tainted), // mov
            SET_SWAP => {
                // swap: reg2 (byte5 low nibble) takes the old value first.
                let reg2 = (src & 0x0F) as usize;
                self.gprm[reg2] = cur;
                self.gprm_tainted[reg2] = cur_tainted;
                (v, src_tainted)
            }
            SET_ADD => (cur.saturating_add(v), t),
            SET_SUB => (cur.saturating_sub(v), t),
            SET_MUL => (cur.saturating_mul(v), t), // libdvdnav's i32 product overflows (C UB) past 0x7FFF_FFFF
            SET_DIV => (cur.checked_div(v).unwrap_or(0xFFFF), t),
            SET_MOD => (cur.checked_rem(v).unwrap_or(0xFFFF), t),
            SET_RND => (cur, true), // rnd: non-deterministic
            SET_AND => (cur & v, t),
            SET_OR => (cur | v, t),
            SET_XOR => (cur ^ v, t),
            _ => (cur, self.gprm_tainted[idx]), // unknown op: no-op
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

// Execute the First-Play pre-command list; returns the VMG title number
// reached deterministically, or None (menu/exit/non-convergence) to fall back.
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

        // A guarded command's predicate decides whether its instruction runs. `taken`
        // is the predicate result; `tainted` marks it undecidable (read a system
        // parameter, directly or via GPRM). No predicate = always runs, never tainted.
        let (taken, tainted) = match cmd.compare {
            Some(cmp) => vm.eval(&cmp),
            None => (true, false),
        };

        // SetGPRMMD sets the register mode even when its predicate is false
        // (libdvdnav eval_system_set; reverse-engineered).
        if let Instr::SetGprmMd { reg, counter, .. } = cmd.instr {
            vm.gprm_counter[(reg & 0x0F) as usize] = counter;
        }

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

            // Register prep before dispatch is a store, not a control transfer, so a
            // tainted guard does NOT abstain. Undecidable guard -> unknown post-state
            // -> taint the destination; otherwise apply the store normally.
            Instr::SetGprm {
                reg,
                op,
                immediate,
                imm,
                src,
            } => {
                if tainted {
                    vm.taint_gprm(reg);
                    if op == SET_SWAP {
                        vm.taint_gprm(src); // swap also writes its source
                    }
                } else {
                    vm.set(reg, op, immediate, imm, src);
                }
            }

            // SetGPRMMD stores like a `mov` (mode was applied above).
            Instr::SetGprmMd {
                reg,
                immediate,
                imm,
                src,
                ..
            } => {
                if tainted {
                    vm.taint_gprm(reg);
                } else {
                    vm.set(reg, 1, immediate, imm, src);
                }
            }

            // No-effect (for resolution) instructions: keep executing. Other
            // SetSystem ops write SPRMs only, and SPRM reads are caught at read time.
            Instr::Nop | Instr::SetSystem => {}

            // Everything else leaves the deterministically-followable path: Break/Exit
            // end the pre list with no title; JumpSS/Link land in a menu or depend on
            // an interactive button selection that can't be resolved statically.
            _ => return None,
        }

        // A set's trailing link leaves the pre list (or might): not followable.
        if cmd.link {
            return None;
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
pub fn resolve_main_title(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    vmg_bytes: Option<&[u8]>,
) -> Option<ResolvedTitle> {
    // Reuse caller-supplied VIDEO_TS.IFO bytes when present, else read them.
    let read;
    let vmg: &[u8] = match vmg_bytes {
        Some(bytes) => bytes,
        None => {
            read = udf.read_file(reader, "/VIDEO_TS/VIDEO_TS.IFO").ok()?;
            &read
        }
    };
    let resolved = resolve_from_vmg(vmg)?;
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
#[path = "nav_tests.rs"]
mod tests;
