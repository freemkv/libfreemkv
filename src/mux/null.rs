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
        // Write-only sink: per the Stream trait contract, read() on a
        // write-opened stream returns StreamWriteOnly. Returning Ok(None)
        // would be misread as a legitimate empty stream.
        Err(crate::error::Error::StreamWriteOnly.into())
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
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x01, 0x02, 0x03],
            duration_ns: None,
        };
        sink.write(&frame).unwrap();
        let _ = sink.info();
        sink.finish().unwrap();
    }

    /// read() on the write-only NullStream must return StreamWriteOnly,
    /// not Ok(None) (which a caller would misread as an empty stream).
    #[test]
    fn read_returns_write_only_error() {
        let title = DiscTitle::empty();
        let mut sink = NullStream::new(&title);
        let err = Stream::read(&mut sink).expect_err("read on a sink must error");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    /// finish() must be idempotent and safe to call repeatedly — a benchmark
    /// driver may finish more than once. Each must be Ok(()), and writes
    /// after finish must still succeed (NullStream has no terminal state).
    #[test]
    fn finish_is_idempotent_and_write_after_finish_ok() {
        let title = DiscTitle::empty();
        let mut sink = NullStream::new(&title);
        sink.finish().unwrap();
        sink.finish().unwrap();
        let frame = crate::pes::PesFrame {
            coding: None,
            source: None,
            track: 3,
            pts: 42,
            keyframe: false,
            data: vec![0xFF; 4096],
            duration_ns: Some(1000),
        };
        // Discard-sink contract: write always returns Ok regardless of frame
        // size, track index, or post-finish state.
        sink.write(&frame).unwrap();
    }
}
