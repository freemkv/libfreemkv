//! Addressable BD+ content storage for `TRAP_LoadContentCode`.
//!
//! The engine never opens disc files directly. The application supplies a
//! reader addressed by numeric `BDSVM/<name>.svm` identifier and parsed
//! section index. Section numbers are the on-disc 2 MiB stride indexes used by
//! the existing `Svm` parser, not FUT segment numbers.

use super::{BdPlusError, BdPlusResult, Svm};
use std::collections::BTreeMap;

/// Supply the declared body of a section from a numbered SVM file.
///
/// Implementations do not seek within or alter the original file; the engine
/// writes a bounded prefix into VM memory according to the content code's
/// requested length.
pub trait BdPlusContent {
    fn section_body(&self, name: u32, section: u32) -> BdPlusResult<&[u8]>;
}

/// Collection of parsed `BDSVM/<name>.svm` files keyed by numeric name.
///
/// `00000.svm` is the bootstrap and is normally supplied through
/// [`crate::BdPlusSession::new`] separately; a library can still carry
/// it for diagnostics and symmetry when a session wants to re-read bootstrap
/// bytes through the same addressing path.
#[derive(Clone, Default, Debug)]
pub struct BdPlusContentLibrary {
    files: BTreeMap<u32, Svm>,
}

impl BdPlusContentLibrary {
    pub fn insert(&mut self, name: u32, svm: Svm) {
        self.files.insert(name, svm);
    }

    pub fn contains(&self, name: u32) -> bool {
        self.files.contains_key(&name)
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl BdPlusContent for BdPlusContentLibrary {
    fn section_body(&self, name: u32, section: u32) -> BdPlusResult<&[u8]> {
        let svm = self
            .files
            .get(&name)
            .ok_or(BdPlusError::MissingContent { name, section })?;
        let section_bytes = svm
            .section(section as usize)
            .ok_or(BdPlusError::MissingContent { name, section })?;
        Ok(section_bytes.body().as_bytes())
    }
}

#[cfg(test)]
#[path = "content_tests.rs"]
mod tests;
