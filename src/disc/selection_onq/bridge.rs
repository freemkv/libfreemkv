//! UDF adapter for the bounded canonical-intent certificate.
use super::{Reject, annotation};
use crate::{disc::DiscTitle, error::Error, halt::Halt, sector::SectorSource, udf::UdfFs};

pub(in crate::disc) fn annotate(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    halt: Option<&Halt>,
    titles: &mut [DiscTitle],
) -> crate::error::Result<()> {
    if halt.is_some_and(Halt::is_cancelled) {
        return Err(Error::Halted);
    }
    if titles.is_empty()
        || titles
            .iter()
            .any(|title| title.selection_evidence.episodes != crate::disc::EpisodeEvidence::Unknown)
    {
        return Ok(());
    }
    let result = annotation::annotate_inputs(
        |path, limit| {
            if halt.is_some_and(Halt::is_cancelled) {
                return Err(Reject::Cancelled);
            }
            // The cap is logical file bytes; UDF also reads bounded metadata
            // and rounds physical data reads to sectors.
            udf.read_file_prefix(reader, path, limit)
                .map_err(|error| match error {
                    Error::Halted => Reject::Cancelled,
                    _ => Reject::MissingAsset,
                })
        },
        || halt.is_some_and(Halt::is_cancelled),
        titles,
    );
    match result {
        Err(Reject::Cancelled) => Err(Error::Halted),
        Err(reason) => {
            tracing::debug!(target: "freemkv::disc", ?reason, "onQ selection requires review");
            Ok(())
        }
        Ok(()) => Ok(()),
    }
}

#[cfg(test)]
#[path = "bridge_tests.rs"]
mod tests;
