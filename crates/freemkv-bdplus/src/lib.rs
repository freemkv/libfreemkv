//! Independent BD+ content execution and plaintext M2TS repair.
//!
//! Player inputs are explicit; the engine contains no supplier or network adapter.
//! This development crate does not yet provide complete disc preparation.
//!
//! Architecture and repair behavior are cross-checked against independent public
//! descriptions, including Dell's [BD-ROM content-protection overview], a
//! [contemporaneous BD+ VM description], and the detailed [fix-up-table patent].
//! These describe the system around the independently implemented observable ABI;
//! none is treated as a complete VM specification.
//! Period context is also cross-checked against the [BD+ licensing announcement],
//! [Thomson security newsletter], [independent security analysis], and a later
//! [BD+/AACS protection patent].
//!
//! [BD-ROM content-protection overview]: https://i.dell.com/sites/content/business/solutions/whitepapers/en/documents/wp-2006-brcp.pdf
//! [contemporaneous BD+ VM description]: https://www.cdrinfo.com/d7/node/14545
//! [fix-up-table patent]: https://patentimages.storage.googleapis.com/b1/23/c9/89589f92009492/US7917964.pdf
//! [BD+ licensing announcement]: https://www.bdplusllc.com/news
//! [Thomson security newsletter]: https://cetinkayakoc.net/news/TheSecurityNewsletter-Thomson2007No5.pdf
//! [independent security analysis]: https://central.bac-lac.gc.ca/.item?app=Library&id=TC-OWTU-3718&oclc_number=1033163995&op=pdf
//! [BD+/AACS protection patent]: https://patentimages.storage.googleapis.com/2c/b0/96/afa3daaefc8fcc/WO2014154291A1.pdf

mod content;
mod crypto;
mod disc;
mod fault;
mod fixup;
mod halt;
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
pub use memory::BdPlusMemory;
pub use player::{BdPlusPlayer, BdPlusPlayerProfile, PlayerReply};
pub use preparation::{BdPlusPreparation, PreparationBoundary};
pub use session::{BdPlusSession, PlayerMaterial, PlayerOperation, PlayerRequest, SessionBoundary};
pub use svm::{Svm, SvmBody, SvmDate, SvmHeader, SvmSection};
pub use vm::{BdPlusEvent, BdPlusVm, VmBoundary};
