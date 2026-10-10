//! Bounded onQ canonical selection under normal platform operation.
//! Parsing alone grants no authority; unsupported certificates leave Unknown.

pub(super) use bridge::annotate;

mod annotation;
mod archive;
mod assets;
mod binary;
mod bootstrap;
mod bridge;
mod bytecode;
mod canonical_frames;
mod certificate;
mod frames;
mod invariants;
mod program_version;
mod qcd;
mod qco;
mod qcs;
mod registers;
mod runtime;
mod startup_data;
mod table_effects;
mod template;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reject {
    Cancelled,
    MissingAsset,
    Truncated,
    Budget,
    Unsupported,
    Invalid,
    Unproven,
}

type Result<T> = std::result::Result<T, Reject>;

/// Deliberately has no success variant: parser success cannot confer authority.
#[derive(Debug, PartialEq, Eq)]
#[cfg(test)]
enum Assessment {
    ReviewRequired(Reject),
}

#[cfg(test)]
fn assess(qco: &[u8], qcs: &[u8], qcd: &[u8]) -> Assessment {
    let parsed = qco::parse(qco)
        .and_then(|_| qcs::parse(qcs))
        .and_then(|_| qcd::parse(qcd));
    Assessment::ReviewRequired(parsed.err().unwrap_or(Reject::Unproven))
}

#[cfg(test)]
mod tests;
