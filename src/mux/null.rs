//! NullStream — discards all data. Write-only PES sink. For benchmarking.

use crate::disc::DiscTitle;
use std::io;

/// Null stream — accepts PES writes, discards data. For benchmarking rip speed.
pub struct NullStream {
    disc_title: DiscTitle,
}

impl NullStream {
    pub fn new(title: &DiscTitle) -> Self {
        Self {
            disc_title: title.clone(),
        }
    }
}

impl crate::pes::PesSink for NullStream {
    fn write(&mut self, _: &crate::pes::PesFrame) -> io::Result<()> {
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }
}

#[cfg(test)]
#[path = "null_tests.rs"]
mod tests;
