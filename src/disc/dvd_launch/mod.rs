//! Bounded root-menu launch traces. Selection policy does not consume these yet.

mod audio;
mod ifo;
mod interleave;
mod pci;
mod produce;
#[cfg(test)]
mod tests;
mod trace;
mod vm;
use crate::disc::DiscTitle;
use crate::{error::Result, sector::SectorSource, udf::UdfFs};

pub(super) fn annotate(
    reader: &mut dyn SectorSource,
    fs: &UdfFs,
    vmg: &[u8],
    addresses: &[(u8, u8)],
    titles: &mut [DiscTitle],
) -> Result<()> {
    use crate::disc::DvdLaunchEvidence;
    let evidence = match produce::produce(reader, fs, vmg, addresses, titles) {
        Ok(evidence) => evidence,
        Err(produce::Failure::Io(crate::Error::Halted)) => return Err(crate::Error::Halted),
        Err(error) => {
            let reason = match error {
                produce::Failure::Io(_) => crate::disc::DvdLaunchReviewReason::IncompleteNavigation,
                produce::Failure::Review(reason) => reason,
            };
            tracing::debug!(target:"freemkv::scan", ?reason, "dvd: bounded root launch evidence needs review");
            vec![DvdLaunchEvidence::Review(reason); titles.len()]
        }
    };
    for (title, evidence) in titles.iter_mut().zip(evidence) {
        title.selection_evidence.dvd_launch = evidence;
    }
    Ok(())
}
