//! NetworkStream — BD-TS over TCP with embedded metadata.
//!
//! Write side (sender): connects to a listener, sends FMKV header + BD-TS data.
//! Read side (receiver): listens for a connection, reads FMKV header + BD-TS data.
//!
//! The FMKV metadata header is the same format as M2tsStream uses — so a
//! NetworkStream reader can hand off to any output stream (MKV, M2TS, etc.)
//! with full metadata (labels, languages, duration).

use std::io::{self, Read, Write, BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use super::{IOStream, meta};
use crate::disc::DiscTitle;

/// I/O buffer size for network reads/writes.
const NET_BUF_SIZE: usize = 256 * 1024;

enum Mode {
    Write {
        writer: BufWriter<TcpStream>,
        header_written: bool,
    },
    Read {
        reader: BufReader<TcpStream>,
    },
}

/// TCP network stream for distributed rip/remux.
pub struct NetworkStream {
    disc_title: DiscTitle,
    mode: Mode,
    finished: bool,
}

impl NetworkStream {
    /// Connect to a remote listener for writing.
    /// Sends FMKV metadata header on first write.
    pub fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Self {
            disc_title: DiscTitle::empty(),
            mode: Mode::Write {
                writer: BufWriter::with_capacity(NET_BUF_SIZE, stream),
                header_written: false,
            },
            finished: false,
        })
    }

    /// Set stream metadata (for write side). Returns self for chaining.
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    /// Listen for an incoming connection and read from it.
    /// Extracts FMKV metadata header from the sender.
    pub fn listen(addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let (stream, _peer) = listener.accept()?;
        stream.set_nodelay(true)?;
        let mut reader = BufReader::with_capacity(NET_BUF_SIZE, stream);

        // Read FMKV metadata header (inline, since TcpStream doesn't impl Seek)
        let disc_title = meta::read_header_from_stream(&mut reader)?
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::InvalidData,
                "no FMKV metadata header from sender",
            ))?
            .to_title();

        Ok(Self {
            disc_title,
            mode: Mode::Read { reader },
            finished: false,
        })
    }
}

impl IOStream for NetworkStream {
    fn info(&self) -> &DiscTitle { &self.disc_title }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished { return Ok(()); }
        self.finished = true;
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()?;
            writer.get_ref().shutdown(std::net::Shutdown::Write)?;
        }
        Ok(())
    }
}

impl Write for NetworkStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Write { ref mut writer, ref mut header_written } => {
                if !*header_written {
                    if !self.disc_title.streams.is_empty() {
                        let m = meta::M2tsMeta::from_title(&self.disc_title);
                        meta::write_header(&mut *writer, &m)?;
                    }
                    *header_written = true;
                }
                writer.write(buf)
            }
            Mode::Read { .. } => Err(io::Error::new(io::ErrorKind::Unsupported, "stream opened for reading")),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

impl Read for NetworkStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Read { ref mut reader } => reader.read(buf),
            Mode::Write { .. } => Err(io::Error::new(io::ErrorKind::Unsupported, "stream opened for writing")),
        }
    }
}
