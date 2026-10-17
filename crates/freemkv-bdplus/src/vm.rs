//! Original Rust BD+ integer CPU and cooperative execution boundaries.
//!
//! CPU details follow the researched BD+ instruction descriptions, rather than
//! assuming the educational DLX ISA. In particular D/S1/S2 occupy bits 25/20/15,
//! instruction words are filtered, and memory wraps/alignment masks are applied.

use super::memory::MEMORY_SIZE;
use super::{BdPlusError, BdPlusMemory, BdPlusResult, Svm};
use crate::Halt;

const PC_MASK: u32 = (MEMORY_SIZE as u32 - 1) & !3;
const IDLE_WATCHDOG: i64 = 4000;

/// Why execution returned control to the Rust host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VmBoundary {
    /// Service a trap, then resume explicitly; `pc` identifies the trap word.
    Trap { trap_id: u32, pc: u32 },
    /// Architectural watchdog expired; reset to the idle quantum.
    Watchdog,
    /// Caller instruction budget exhausted. CPU state is preserved.
    Yield,
}

/// Architectural BD+ events and their mailbox arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BdPlusEvent {
    Start,
    Shutdown,
    TitleInit { title: u32 },
    ApplicationLayer,
    ComputeSp { clip_id: u32, segment: u32 },
}

/// Integer CPU state, without HTTP, filesystem, player secrets or trap policy.
pub struct BdPlusVm {
    memory: BdPlusMemory,
    registers: [u32; 32],
    pc: u32,
    filter: u32,
    watchdog: i64,
    instructions: u64,
    pending_trap: Option<u32>,
    event: Option<BdPlusEvent>,
}

impl std::fmt::Debug for BdPlusVm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusVm")
            .field("pc", &self.pc)
            .field("instructions", &self.instructions)
            .field("pending_trap", &self.pending_trap)
            .finish_non_exhaustive()
    }
}

impl BdPlusVm {
    /// Load section-zero's body and initialize the first 4 KiB control area.
    /// The parsed source object is never modified.
    pub fn load(svm: &Svm) -> BdPlusResult<Self> {
        let body = svm.primary().body();
        let body = body.as_bytes();
        if !(0x1004..=MEMORY_SIZE).contains(&body.len()) {
            return Err(BdPlusError::BootstrapLength { length: body.len() });
        }
        let mut memory = BdPlusMemory::default();
        memory.write(0, body)?;
        memory.read_mut(0, 0x1000)?.fill(0);
        Ok(Self {
            memory,
            registers: [0; 32],
            pc: 0x1000,
            filter: 0,
            watchdog: 0x7fff_ffff,
            instructions: 0,
            pending_trap: None,
            event: None,
        })
    }

    pub fn memory(&self) -> &BdPlusMemory {
        &self.memory
    }
    pub fn memory_mut(&mut self) -> &mut BdPlusMemory {
        &mut self.memory
    }
    pub fn registers(&self) -> &[u32; 32] {
        &self.registers
    }
    pub fn pc(&self) -> u32 {
        self.pc
    }
    pub fn instruction_filter(&self) -> u32 {
        self.filter
    }
    pub fn instructions(&self) -> u64 {
        self.instructions
    }

    /// Set a host-visible register. R0 remains hardwired to zero.
    pub fn set_register(&mut self, index: usize, value: u32) -> BdPlusResult<()> {
        let register = self
            .registers
            .get_mut(index)
            .ok_or(BdPlusError::RegisterIndex { index })?;
        if index != 0 {
            *register = value;
        }
        Ok(())
    }

    pub fn event(&self) -> Option<BdPlusEvent> {
        self.event
    }

    /// Enter at the content-code entry point, preserving prior machine state.
    pub fn begin_event(&mut self, event: BdPlusEvent) -> BdPlusResult<()> {
        if self.event.is_some() || self.pending_trap.is_some() {
            return Err(BdPlusError::EventInProgress);
        }
        self.set_event_fields(event);
        Ok(())
    }

    fn set_event_fields(&mut self, event: BdPlusEvent) {
        match event {
            BdPlusEvent::Start => {
                self.memory.store_u32(0, 0);
                self.memory.store_u32(4, 0);
            }
            BdPlusEvent::Shutdown => {
                self.memory.store_u32(0, 0x10);
                self.memory.store_u32(4, 0);
            }
            BdPlusEvent::TitleInit { title } => {
                self.memory.store_u32(0, 0x110);
                self.memory.store_u32(4, 0);
                self.memory.store_u32(8, title);
            }
            BdPlusEvent::ApplicationLayer => {
                self.memory.store_u32(0, 0x210);
                self.memory.store_u32(4, 0);
                self.memory.store_u32(8, 1);
            }
            BdPlusEvent::ComputeSp { clip_id, segment } => {
                self.memory.store_u32(0, 0x220);
                self.memory.store_u32(4, 0);
                self.memory.store_u32(8, clip_id);
                self.memory.store_u32(12, segment);
            }
        }
        if matches!(event, BdPlusEvent::ComputeSp { .. }) {
            self.memory.store_u32(0x20, 0);
            self.memory.store_u32(0x24, 0);
        }
        if event != BdPlusEvent::Start {
            self.registers[28] = self.pc;
        }
        self.pc = 0x1000;
        self.watchdog = 0x7fff_ffff;
        self.event = Some(event);
    }

    /// End the current event at a pending Finished trap; retain mailbox output.
    ///
    /// A `Finished` fired while no event is active drops the pending trap and
    /// leaves PC alone, since the content code is already in its idle loop.
    /// Returns `None` so the session can observe the idle completion.
    pub fn finish_event(&mut self) -> BdPlusResult<Option<BdPlusEvent>> {
        if self.pending_trap != Some(0x10) {
            return Err(BdPlusError::NoPendingTrap);
        }
        self.pending_trap = None;
        match self.event.take() {
            Some(event) => {
                // Start code itself establishes the idle continuation in R28;
                // later events save their interrupted continuation there on
                // entry.
                self.pc = self.registers[28] & PC_MASK;
                self.watchdog = IDLE_WATCHDOG;
                Ok(Some(event))
            }
            None => Ok(None),
        }
    }

    /// Return arguments from the stack with architectural word addressing.
    /// Host trap implementations still validate the pointed-to data ranges.
    pub fn argument(&self, index: u8) -> u32 {
        self.memory
            .load_u32(self.registers[29].wrapping_add(u32::from(index) * 4))
    }

    /// Satisfy a pending trap, optionally replacing its R1 result.
    /// Finished uses `None` because it does not overwrite R1.
    pub fn resume_trap(&mut self, status: Option<u32>) -> BdPlusResult<()> {
        if self.pending_trap.take().is_none() {
            return Err(BdPlusError::NoPendingTrap);
        }
        if let Some(status) = status {
            self.registers[1] = status;
        }
        Ok(())
    }

    /// Run a bounded instruction slice. The session owns event/trap semantics.
    pub fn run(&mut self, budget: u64, halt: &Halt) -> BdPlusResult<VmBoundary> {
        if let Some(trap_id) = self.pending_trap {
            return Err(BdPlusError::PendingTrap { trap_id });
        }
        for _ in 0..budget {
            if halt.is_cancelled() {
                return Err(BdPlusError::Cancelled);
            }
            let boundary = self.step();
            if let Some(boundary) = boundary {
                return Ok(boundary);
            }
        }
        if halt.is_cancelled() {
            return Err(BdPlusError::Cancelled);
        }
        Ok(VmBoundary::Yield)
    }

    fn step(&mut self) -> Option<VmBoundary> {
        let at = self.pc;
        let instruction = self.memory.load_u32(at) ^ self.filter;
        let opcode = instruction >> 26;
        let destination = ((instruction >> 21) & 31) as usize;
        let s1 = ((instruction >> 16) & 31) as usize;
        let s2 = ((instruction >> 11) & 31) as usize;
        let a = self.registers[s1];
        let b = self.registers[s2];
        let immediate = instruction & 0xffff;
        let signed = immediate as i16 as i32 as u32;
        let next = at.wrapping_add(4);
        let mut target = next;
        let mut output = None;
        let mut cost = 1;
        match opcode {
            0x01 => output = Some(a.wrapping_add(b)),
            0x02 => output = Some(a.wrapping_sub(b)),
            0x03 => {
                output = Some(a.wrapping_mul(b));
                cost = 4;
            }
            0x04 => {
                output = Some(if b == 0 {
                    0
                } else {
                    (a as i32).wrapping_div(b as i32) as u32
                });
                cost = 16;
            }
            0x05 => {
                output = Some(a.checked_div(b).unwrap_or(0));
                cost = 16;
            }
            0x06 => output = Some(a.wrapping_shl(b)),
            0x07 => output = Some(a.wrapping_shr(b)),
            0x08 => output = Some((a as i32).wrapping_shr(b) as u32),
            0x09 => output = Some(a & b),
            0x0a => output = Some(a | b),
            0x0b => output = Some(a ^ b),
            0x0c..=0x15 => output = Some(compare(opcode - 0x0c, a, b)),
            0x16 | 0x17 => {
                target = a;
                if opcode == 0x17 {
                    self.registers[31] = next;
                }
                cost = 2;
            }
            0x18 => output = Some(a.wrapping_add(signed)),
            0x19 => output = Some(a.wrapping_add(immediate)),
            0x1a => output = Some(a.wrapping_sub(signed)),
            0x1b => output = Some(a.wrapping_sub(immediate)),
            0x1c => output = Some(a.wrapping_shl(immediate)),
            0x1d => output = Some(a.wrapping_shr(immediate)),
            0x1e => output = Some((a as i32).wrapping_shr(immediate) as u32),
            0x1f => output = Some(a & immediate),
            0x20 => output = Some(a | immediate),
            0x21 => output = Some(a ^ immediate),
            0x22..=0x2b => {
                let operation = opcode - 0x22;
                let rhs = if matches!(operation, 3 | 5 | 7 | 9) {
                    immediate
                } else {
                    signed
                };
                output = Some(compare(operation, a, rhs));
            }
            0x2c | 0x2d => {
                let displacement = ((instruction << 6) as i32 >> 6) as u32;
                target = next.wrapping_add(displacement);
                if opcode == 0x2d {
                    self.registers[31] = next;
                }
                cost = 2;
            }
            0x2e | 0x2f => {
                if (a == 0) == (opcode == 0x2e) {
                    target = next.wrapping_add(signed);
                }
                cost = 2;
            }
            0x30 => output = Some(immediate << 16),
            0x31..=0x35 => {
                let address = a.wrapping_add(signed);
                output = Some(match opcode {
                    0x31 => self.memory.load_u8(address) as i8 as i32 as u32,
                    0x32 => u32::from(self.memory.load_u8(address)),
                    0x33 => self.memory.load_u16(address) as i16 as i32 as u32,
                    0x34 => u32::from(self.memory.load_u16(address)),
                    _ => self.memory.load_u32(address),
                });
                cost = if opcode == 0x35 { 2 } else { 4 };
            }
            0x36..=0x38 => {
                let address = a.wrapping_add(signed);
                let data = self.registers[destination];
                match opcode {
                    0x36 => self.memory.store_u8(address, data as u8),
                    0x37 => self.memory.store_u16(address, data as u16),
                    _ => self.memory.store_u32(address, data),
                }
                cost = if opcode == 0x38 { 2 } else { 4 };
            }
            0x39 => {
                let trap_id = instruction & 0x03ff_ffff;
                self.pending_trap = Some(trap_id);
                self.pc = next & PC_MASK;
                self.instructions += 1;
                return Some(VmBoundary::Trap { trap_id, pc: at });
            }
            0x3a => {
                self.filter = a;
                cost = 6;
            }
            // Unassigned base-mode opcodes leave state unchanged.
            _ => {}
        }
        if let Some(value) = output {
            self.registers[destination] = value;
        }
        self.registers[0] = 0;
        self.pc = target & PC_MASK;
        self.instructions += 1;
        self.watchdog -= cost;
        if self.watchdog <= 0 {
            self.watchdog = IDLE_WATCHDOG;
            Some(VmBoundary::Watchdog)
        } else {
            None
        }
    }
}

fn compare(operation: u32, a: u32, b: u32) -> u32 {
    u32::from(match operation {
        0 => a == b,
        1 => a != b,
        2 => (a as i32) < (b as i32),
        3 => a < b,
        4 => (a as i32) > (b as i32),
        5 => a > b,
        6 => (a as i32) <= (b as i32),
        7 => a <= b,
        8 => (a as i32) >= (b as i32),
        _ => a >= b,
    })
}

#[cfg(test)]
#[path = "vm_tests.rs"]
mod tests;
