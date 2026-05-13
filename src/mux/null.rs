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

impl crate::pes::Stream for NullStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        Ok(None)
    }
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
mod tests {
    use super::*;
    use crate::pes::Stream;

    /// Verify NullStream routes through the `Stream` trait object cleanly.
    #[test]
    fn stream_via_dyn_object_writes_and_finishes() {
        let title = DiscTitle::empty();
        let mut sink: Box<dyn Stream> = Box::new(NullStream::new(&title));

        let frame = crate::pes::PesFrame {
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x01, 0x02, 0x03],
        };
        sink.write(&frame).unwrap();
        let _ = sink.info();
        sink.finish().unwrap();
    }
}
