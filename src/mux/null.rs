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

#[allow(deprecated)] // 0.18 trait split: migrate to FrameSink in follow-up commit.
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

/// FrameSink sibling to the deprecated Stream impl; both coexist during the
/// 0.18 deprecation window. Caller may pick either at the trait-object
/// boundary — `Box<dyn Stream>` (deprecated) or `Box<dyn FrameSink>` (new).
/// The deprecation-window callers eventually migrate; this impl exists so
/// new callers can target `FrameSink` without waiting for the rest of the
/// migration to complete.
#[allow(deprecated)] // delegating to deprecated Stream during the 0.18 deprecation window so callers don't see the deprecation twice.
impl crate::pes::FrameSink for NullStream {
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        <Self as crate::pes::Stream>::write(self, frame)
    }

    fn finish(self: Box<Self>) -> io::Result<()> {
        // Why: Stream::finish takes &mut self, FrameSink::finish takes Box<Self>.
        // Re-borrow inside the box, call Stream::finish, drop the box.
        let mut s: Self = *self;
        <Self as crate::pes::Stream>::finish(&mut s)
    }

    fn info(&self) -> &DiscTitle {
        <Self as crate::pes::Stream>::info(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pes::FrameSink;

    /// Smallest credible witness that the new FrameSink impl on a concrete
    /// `mux/*` sink works through the trait object: build a boxed
    /// `dyn FrameSink`, write a frame, finish it. The trait-bridge correctness
    /// is what's being verified — not NullStream-specific behaviour.
    #[test]
    fn frame_sink_via_dyn_object_writes_and_finishes() {
        let title = DiscTitle::empty();
        let mut sink: Box<dyn FrameSink> = Box::new(NullStream::new(&title));

        let frame = crate::pes::PesFrame {
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x01, 0x02, 0x03],
        };
        sink.write(&frame).unwrap();
        // info() routes through the trait object.
        let _ = sink.info();
        // finish() consumes the Box<Self> — must compile and run.
        sink.finish().unwrap();
    }
}
