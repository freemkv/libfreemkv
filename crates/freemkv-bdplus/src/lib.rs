//! Independent BD+ content execution and plaintext M2TS repair.
//!
//! Player inputs are explicit; the engine contains no supplier or network adapter.
//! This development crate does not yet provide complete disc preparation.
//!
//! Architecture and repair behavior are cross-checked against hard documentary
//! sources. Dell's [BD-ROM content-protection overview] describes the deployed
//! system, while a [BDSVM content-code patent] and [player-security-check patent]
//! explicitly describe on-disc SVM files, player/content-code categories,
//! per-segment parameters, security checks and plaintext repair. These establish
//! the system around the independently implemented observable ABI; none is
//! treated as a complete VM specification. The [BD+ licensing announcement]
//! establishes the administered specification and conformance program.
//!
//! [BD-ROM content-protection overview]: https://i.dell.com/sites/content/business/solutions/whitepapers/en/documents/wp-2006-brcp.pdf
//! [BDSVM content-code patent]: https://patentimages.storage.googleapis.com/aa/30/8e/7e223065e263a8/US8625967.pdf
//! [player-security-check patent]: https://patentimages.storage.googleapis.com/09/a3/1a/b8884b8530f95b/US8424101.pdf
//! [BD+ licensing announcement]: https://www.bdplusllc.com/news

mod content;
mod crypto;
mod disc;
mod fault;
mod fixup;
mod halt;
mod implementation;
mod memory;
mod player;
mod preparation;
mod session;
mod svm;
mod vm;

pub use content::{BdPlusContent, BdPlusContentLibrary};
pub use disc::{BdPlusDiscContext, BdPlusTimestamp};
pub use fault::{BdPlusError, BdPlusResult};
pub use fixup::{BdPlusFixupTable, BdPlusPatch, BdPlusPatchSet};
pub use halt::Halt;
pub use implementation::{HOST_SERVICES, HostService, ImplementationState, host_service_counts};
pub use memory::BdPlusMemory;
pub use player::{BdPlusPlayer, BdPlusPlayerProfile, PlayerReply};
pub use preparation::{BdPlusPreparation, PreparationBoundary};
pub use session::{BdPlusSession, PlayerMaterial, PlayerOperation, PlayerRequest, SessionBoundary};
pub use svm::{Svm, SvmBody, SvmDate, SvmHeader, SvmSection};
pub use vm::{BdPlusEvent, BdPlusVm, VmBoundary};
