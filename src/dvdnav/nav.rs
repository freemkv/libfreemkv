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
mod tests {
    use super::*;

    // ── Fixture builders ────────────────────────────────────────────────────

    // Assemble a spec-shaped VMGI image (FP_PGC + TT_SRPT).
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

    // Vm::set per libdvdnav eval_set_op (reverse-engineered): add/mul saturate,
    // sub clamps at 0, div/mod by 0 yields 0xFFFF, an unknown op is a no-op.
    #[test]
    fn set_ops_saturate_and_divide_by_zero_yields_ffff() {
        let mut vm = Vm::new();
        vm.set(0, 1, true, 42, 0); // mov
        assert_eq!(vm.gprm[0], 42);
        vm.set(0, 3, true, 10, 0); // add → 52
        assert_eq!(vm.gprm[0], 52);
        vm.set(0, 4, true, 2, 0); // sub → 50
        assert_eq!(vm.gprm[0], 50);
        vm.set(0, 5, true, 3, 0); // mul → 150
        assert_eq!(vm.gprm[0], 150);

        vm.set(1, 1, true, 0xFFFF, 0);
        vm.set(1, 5, true, 2, 0);
        assert_eq!(vm.gprm[1], 0xFFFF, "mul saturates");
        vm.set(1, 3, true, 1, 0);
        assert_eq!(vm.gprm[1], 0xFFFF, "add saturates");
        vm.set(3, 4, true, 1, 0);
        assert_eq!(vm.gprm[3], 0, "sub clamps at 0");

        vm.set(0, 6, true, 7, 0); // div → 150/7 = 21
        assert_eq!(vm.gprm[0], 21);
        vm.set(0, 6, true, 0, 0);
        assert_eq!(vm.gprm[0], 0xFFFF, "divide by zero yields 0xFFFF");
        vm.set(0, 1, true, 21, 0);
        vm.set(0, 7, true, 5, 0); // mod → 21 % 5 = 1
        assert_eq!(vm.gprm[0], 1);
        vm.set(0, 7, true, 0, 0);
        assert_eq!(vm.gprm[0], 0xFFFF, "modulo by zero yields 0xFFFF");

        vm.set(2, 1, true, 0b1100, 0);
        vm.set(2, 9, true, 0b1010, 0); // and → 0b1000
        assert_eq!(vm.gprm[2], 0b1000);
        vm.set(2, 10, true, 0b0011, 0); // or → 0b1011
        assert_eq!(vm.gprm[2], 0b1011);
        vm.set(2, 11, true, 0b1111, 0); // xor → 0b0100
        assert_eq!(vm.gprm[2], 0b0100);

        vm.set(2, 13, true, 99, 0);
        assert_eq!(vm.gprm[2], 0b0100, "an unknown set op is a no-op");
        assert!(!vm.gprm_tainted[2]);
    }

    // swp g0,g1 moves g1 into g0 and the old g0 into g1 (libdvdnav eval_set_op case 2).
    #[test]
    fn swap_exchanges_both_registers() {
        let mut vm = Vm::new();
        vm.gprm[0] = 5;
        vm.gprm[1] = 9;
        vm.gprm_tainted[0] = true;
        vm.set(0, SET_SWAP, false, 0, 1);
        assert_eq!((vm.gprm[0], vm.gprm[1]), (9, 5), "both registers written");
        assert!(vm.gprm_tainted[1], "the source takes the old taint");
        assert!(!vm.gprm_tainted[0]);
    }

    // Every compare op decides on l<r, l==r and l>r (libdvdnav eval_compare).
    #[test]
    fn compare_ops_decide_on_ordering() {
        let mut vm = Vm::new();
        vm.gprm[0] = 5;
        let ev = |vm: &Vm, op, imm| {
            let c = Compare {
                op,
                lhs_reg: 0,
                immediate: true,
                imm,
                rhs_reg: 0,
            };
            vm.eval(&c).0
        };
        // (op, result for imm 4 [l>r], 5 [l==r], 6 [l<r])
        let table = [
            (CMP_AND, [true, true, false]), // 5&4, 5&5 nonzero; 5&6 = 4 nonzero
            (CMP_EQ, [false, true, false]),
            (CMP_NE, [true, false, true]),
            (CMP_GE, [true, true, false]),
            (CMP_GT, [true, false, false]),
            (CMP_LE, [false, true, true]),
            (CMP_LT, [false, false, true]),
        ];
        for (op, want) in table {
            for (imm, w) in [4u16, 5, 6].into_iter().zip(want) {
                let w = if op == CMP_AND { (5 & imm) != 0 } else { w };
                assert_eq!(ev(&vm, op, imm), w, "op {op} imm {imm}");
            }
        }
        assert!(!ev(&vm, CMP_AND, 2), "5 & 2 is zero");
        assert!(!ev(&vm, 0, 5), "an unknown op is false");
    }

    // A set followed by a link sub-instruction (here LinkTailPGC) leaves the pre list:
    // the next line never runs, so the resolver must abstain.
    #[test]
    fn set_with_link_subinstruction_abstains() {
        for set in ["7101000000010002", "4001000000000002", "7104000000000005"] {
            let vmgi = build_vmgi(&[h(set), h("3002000000020000")], 1, &[(2, 1), (3, 1)]);
            assert_eq!(resolve_from_vmg(&vmgi), None, "{set}");
        }
        // LinkNoLink (sub-op 0) still ends the command list (libdvdnav
        // eval_link_subins returns cond), so the next line never runs.
        let vmgi = build_vmgi(
            &[h("7101000000010000"), h("3002000000020000")],
            1,
            &[(2, 1), (3, 1)],
        );
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    // SetGPRMMD counter mode makes a GPRM time-dependent: a compare on it must abstain,
    // including when the mode was set by a line whose own predicate was false.
    #[test]
    fn counter_mode_gprm_makes_a_later_compare_abstain() {
        // 53 .. imm 5, b5 = 0x83: counter bit + g3. Then `if g3 == g3 -> JumpTT 1`.
        let cmp = h("3022000000010303");
        for set in ["5300000500830000", "5330000500830000"] {
            let vmgi = build_vmgi(&[h(set), cmp], 1, &[(2, 1)]);
            assert_eq!(resolve_from_vmg(&vmgi), None, "{set}");
        }
        // Counter bit clear: the compare is decidable.
        let vmgi = build_vmgi(&[h("5300000500030000"), cmp], 1, &[(2, 1)]);
        assert!(resolve_from_vmg(&vmgi).is_some());
    }

    // SetGPRMMD stores like a mov: the immediate (bytes 2-3) and source register (byte 3)
    // land in the register named by byte 5.
    #[test]
    fn setgprmmd_stores_its_immediate_and_source_register() {
        // g1 = 9; SetGPRMMD g2 = imm 9 / g2 = g1; then `if g2 == g1 -> JumpTT 1`.
        let jump = h("3022000000010201");
        for md in ["5300000900020000", "4300000100020000"] {
            let vmgi = build_vmgi(&[h("7100000100090000"), h(md), jump], 1, &[(2, 1)]);
            assert!(resolve_from_vmg(&vmgi).is_some(), "{md}");
        }
    }

    // A random value is never decidable, and a swap under an undecidable guard leaves
    // its source register unknown too.
    #[test]
    fn rnd_and_guarded_swap_taint_their_registers() {
        let mut vm = Vm::new();
        vm.set(0, SET_RND, true, 1, 0);
        assert!(vm.reg_tainted(0));
        // line 0: swap g0,g1 guarded by an SPRM compare; line 1: `if g1 == g1 -> JumpTT 1`.
        let vmgi = build_vmgi(
            &[h("6220800000010000"), h("3022000000010101")],
            1,
            &[(2, 1)],
        );
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }

    // A SetSystem (SPRM write, type 2) in the First-Play list is a no-op FOR
    // RESOLUTION: it must keep executing, not abort, so a following JumpTT still
    // resolves. `40..` = type 2, zero compare nibble → unconditional.
    #[test]
    fn first_play_setsystem_is_a_noop_and_resolution_continues() {
        let vmgi = build_vmgi(
            &[h("4000000000000000"), h("3002000000010000")],
            1,
            &[(2, 1), (3, 1)],
        );
        assert_eq!(
            resolve_from_vmg(&vmgi),
            Some(ResolvedTitle {
                title: 1,
                vtsn: 2,
                vts_ttn: 1
            }),
            "a SetSystem must not abort First-Play resolution"
        );
    }

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

    // SPRM-gated dispatch is undecidable at scan time and must abstain.
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

    // Defect-1 regression: a store guarded by taint must not abstain the unconditional dispatch
    // that follows.
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

    // Defect-2 regression: an SPRM laundered through a GPRM must still taint a later GPRM-only
    // branch.
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

    /// A `Goto` lands on its 1-based target line, skipping the lines between.
    #[test]
    fn goto_skips_to_the_target_line() {
        // line 1: Goto 3; line 2: JumpTT 9 (skipped); line 3: JumpTT 1.
        let vmgi = build_vmgi(
            &[
                h("0001000000000003"),
                h("3002000000090000"),
                h("3002000000010000"),
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

    /// JumpTT 0 and a TT_SRPT entry with VTS 0 are unaddressable, so abstain.
    #[test]
    fn jumptt_zero_and_vtsn_zero_abstain() {
        let vmgi = build_vmgi(&[h("3002000000000000")], 1, &[(2, 1)]);
        assert_eq!(resolve_from_vmg(&vmgi), None, "title 0");
        let vmgi = build_vmgi(&[h("3002000000010000")], 1, &[(0, 1)]);
        assert_eq!(resolve_from_vmg(&vmgi), None, "vtsn 0");
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

    // A TT_SRPT pointer far past the image abstains. On 64-bit this trips the
    // u16_at bounds check; the checked_mul guard only matters on 32-bit.
    #[test]
    fn overflowing_tt_srpt_sector_abstains() {
        let mut vmgi = build_vmgi(&[h("3002000000010000")], 1, &[(2, 1)]);
        vmgi[VMGI_TT_SRPT_PTR..VMGI_TT_SRPT_PTR + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(resolve_from_vmg(&vmgi), None);
    }
}
