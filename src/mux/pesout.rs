//! PES output streams — each writes its own format from PES frames.

use super::tsmux::TsMuxer;
use crate::disc::DiscTitle;
use crate::pes::PesFrame;
use std::io::{self, Write};

// ── M2TS ────────────────────────────────────────────────────────────────────

pub struct M2tsOutputStream {
    muxer: TsMuxer<io::BufWriter<std::fs::File>>,
    title: DiscTitle,
}

impl M2tsOutputStream {
    pub fn create(path: &str, title: &DiscTitle) -> io::Result<Self> {
        let file = std::fs::File::create(path)
            .map_err(|e| io::Error::new(e.kind(), format!("m2ts://{}: {}", path, e)))?;
        let mut writer = io::BufWriter::with_capacity(4 * 1024 * 1024, file);
        if !title.streams.is_empty() {
            let m = super::meta::M2tsMeta::from_title(title);
            super::meta::write_header(&mut writer, &m)?;
        }
        let pids = extract_pids(title);
        let mut muxer = TsMuxer::new(writer, &pids);
        for (i, cp) in title.codec_privates.iter().enumerate() {
            if let Some(data) = cp {
                muxer.set_codec_private(i, data.clone());
            }
        }
        Ok(Self { muxer, title: title.clone() })
    }
}

impl crate::pes::Stream for M2tsOutputStream {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "M2TS output is write-only"))
    }
    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        self.muxer.write_frame(frame.track, frame.pts, &frame.data)
    }
    fn finish(&mut self) -> io::Result<()> { self.muxer.finish() }
    fn info(&self) -> &DiscTitle { &self.title }
}

// ── Null ────────────────────────────────────────────────────────────────────

pub struct NullOutputStream { title: DiscTitle }

impl NullOutputStream {
    pub fn new(title: &DiscTitle) -> Self { Self { title: title.clone() } }
}

impl crate::pes::Stream for NullOutputStream {
    fn read(&mut self) -> io::Result<Option<PesFrame>> { Ok(None) }
    fn write(&mut self, _: &PesFrame) -> io::Result<()> { Ok(()) }
    fn finish(&mut self) -> io::Result<()> { Ok(()) }
    fn info(&self) -> &DiscTitle { &self.title }
}

// ── Stdio — serializes PES frames directly ──────────────────────────────────

pub struct StdioOutputStream {
    writer: io::BufWriter<io::Stdout>,
    title: DiscTitle,
}

impl StdioOutputStream {
    pub fn new(title: &DiscTitle) -> Self {
        Self { writer: io::BufWriter::new(io::stdout()), title: title.clone() }
    }
}

impl crate::pes::Stream for StdioOutputStream {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "stdio output is write-only"))
    }
    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        frame.serialize(&mut self.writer)
    }
    fn finish(&mut self) -> io::Result<()> { self.writer.flush() }
    fn info(&self) -> &DiscTitle { &self.title }
}

// ── Network — serializes PES frames over TCP ────────────────────────────────

pub struct NetworkOutputStream {
    writer: io::BufWriter<std::net::TcpStream>,
    title: DiscTitle,
}

impl NetworkOutputStream {
    pub fn connect(addr: &str, title: &DiscTitle) -> io::Result<Self> {
        let stream = std::net::TcpStream::connect(addr)?;
        let mut writer = io::BufWriter::with_capacity(256 * 1024, stream);
        if !title.streams.is_empty() {
            let m = super::meta::M2tsMeta::from_title(title);
            super::meta::write_header(&mut writer, &m)?;
            writer.flush()?;
        }
        Ok(Self { writer, title: title.clone() })
    }
}

impl crate::pes::Stream for NetworkOutputStream {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "network output is write-only"))
    }
    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        frame.serialize(&mut self.writer)
    }
    fn finish(&mut self) -> io::Result<()> { self.writer.flush() }
    fn info(&self) -> &DiscTitle { &self.title }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn extract_pids(title: &DiscTitle) -> Vec<u16> {
    title.streams.iter().map(|s| match s {
        crate::disc::Stream::Video(v) => v.pid,
        crate::disc::Stream::Audio(a) => a.pid,
        crate::disc::Stream::Subtitle(s) => s.pid,
    }).collect()
}
