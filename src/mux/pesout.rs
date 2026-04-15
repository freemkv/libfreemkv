//! PES output adapters — every output format muxes from PES frames.
//!
//! Each output knows its own format:
//! - M2TS: PES → BD-TS packets → file (via TsMuxer)
//! - Null: discard
//! - Stdio: raw frame data to stdout
//! - Network: PES → BD-TS → TCP (via TsMuxer)

use super::tsmux::TsMuxer;
use crate::disc::DiscTitle;
use crate::pes::{OutputStream, PesFrame};
use std::io::{self, Write};

/// M2TS output — PES frames → BD-TS packets → file.
pub struct M2tsOutputStream {
    muxer: TsMuxer<io::BufWriter<std::fs::File>>,
}

impl M2tsOutputStream {
    pub fn create(path: &str, title: &DiscTitle) -> io::Result<Self> {
        let file = std::fs::File::create(path)
            .map_err(|e| io::Error::new(e.kind(), format!("m2ts://{}: {}", path, e)))?;
        let writer = io::BufWriter::with_capacity(4 * 1024 * 1024, file);
        let pids = Self::extract_pids(title);
        Ok(Self {
            muxer: TsMuxer::new(writer, &pids),
        })
    }

    fn extract_pids(title: &DiscTitle) -> Vec<u16> {
        title.streams.iter().map(|s| match s {
            crate::disc::Stream::Video(v) => v.pid,
            crate::disc::Stream::Audio(a) => a.pid,
            crate::disc::Stream::Subtitle(s) => s.pid,
        }).collect()
    }
}

impl OutputStream for M2tsOutputStream {
    fn write_frame(&mut self, frame: &PesFrame) -> io::Result<()> {
        self.muxer.write_frame(frame.track, frame.pts, &frame.data)
    }
    fn finish(&mut self) -> io::Result<()> {
        self.muxer.finish_ref()
    }
}

/// Null output — discards all frames.
pub struct NullOutputStream;

impl OutputStream for NullOutputStream {
    fn write_frame(&mut self, _frame: &PesFrame) -> io::Result<()> { Ok(()) }
    fn finish(&mut self) -> io::Result<()> { Ok(()) }
}

/// Stdio output — writes raw frame data to stdout.
pub struct StdioOutputStream {
    writer: io::BufWriter<io::Stdout>,
}

impl StdioOutputStream {
    pub fn new() -> Self {
        Self { writer: io::BufWriter::new(io::stdout()) }
    }
}

impl OutputStream for StdioOutputStream {
    fn write_frame(&mut self, frame: &PesFrame) -> io::Result<()> {
        self.writer.write_all(&frame.data)
    }
    fn finish(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Network output — PES frames → BD-TS → TCP.
pub struct NetworkOutputStream {
    muxer: TsMuxer<io::BufWriter<std::net::TcpStream>>,
}

impl NetworkOutputStream {
    pub fn connect(addr: &str, title: &DiscTitle) -> io::Result<Self> {
        let stream = std::net::TcpStream::connect(addr)?;
        let writer = io::BufWriter::with_capacity(256 * 1024, stream);
        let pids = M2tsOutputStream::extract_pids(title);
        Ok(Self {
            muxer: TsMuxer::new(writer, &pids),
        })
    }
}

impl OutputStream for NetworkOutputStream {
    fn write_frame(&mut self, frame: &PesFrame) -> io::Result<()> {
        self.muxer.write_frame(frame.track, frame.pts, &frame.data)
    }
    fn finish(&mut self) -> io::Result<()> {
        self.muxer.finish_ref()
    }
}
