//! Independent BD+ content execution and plaintext M2TS repair.
//!
//! Player inputs are explicit; the engine contains no supplier or network adapter.
//! This development crate does not yet provide complete disc preparation.

mod content;
mod crypto;
mod disc;
mod fault;
mod fixup;
mod halt;
mod memory;
mod player;
mod session;
mod svm;
mod vm;

pub use content::{BdPlusContent, BdPlusContentLibrary};
pub use disc::{BdPlusDiscContext, BdPlusTimestamp};
pub use fault::{BdPlusError, BdPlusResult};
pub use fixup::{BdPlusFixupTable, BdPlusPatch, BdPlusPatchSet};
pub use halt::Halt;
pub use memory::BdPlusMemory;
pub use player::{BdPlusPlayer, BdPlusPlayerProfile, PlayerReply};
pub use session::{BdPlusSession, PlayerMaterial, PlayerOperation, PlayerRequest, SessionBoundary};
pub use svm::{Svm, SvmBody, SvmDate, SvmHeader, SvmSection};
pub use vm::{BdPlusEvent, BdPlusVm, VmBoundary};
