//! NetworkStream — BD-TS over TCP with embedded metadata.
//!
//! Write side (sender): connects to a listener, sends FMKV header + BD-TS data.
//! Read side (receiver): listens for a connection, reads FMKV header + BD-TS data.
//!
//! The FMKV metadata header is the same format as M2tsStream uses — so a
//! NetworkStream reader can hand off to any output stream (MKV, M2TS, etc.)
//! with full metadata (labels, languages, duration).

use super::{meta, IOStream};
use crate::disc::DiscTitle;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};

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
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no FMKV metadata header from sender",
                )
            })?
            .to_title();

        Ok(Self {
            disc_title,
            mode: Mode::Read { reader },
            finished: false,
        })
    }
}

impl IOStream for NetworkStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
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
            Mode::Write {
                ref mut writer,
                ref mut header_written,
            } => {
                if !*header_written {
                    if !self.disc_title.streams.is_empty() {
                        let m = meta::M2tsMeta::from_title(&self.disc_title);
                        meta::write_header(&mut *writer, &m)?;
                    }
                    *header_written = true;
                }
                writer.write(buf)
            }
            Mode::Read { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for reading",
            )),
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
            Mode::Write { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for writing",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        AudioStream, Codec, ColorSpace, ContentFormat, HdrFormat, Stream, VideoStream,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// Build a DiscTitle with streams for metadata tests.
    fn sample_title() -> DiscTitle {
        DiscTitle {
            playlist: "NetworkTest".into(),
            playlist_id: 1,
            duration_secs: 3600.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: vec![
                Stream::Video(VideoStream {
                    pid: 0x1011,
                    codec: Codec::Hevc,
                    resolution: "2160p".into(),
                    frame_rate: "23.976".into(),
                    hdr: HdrFormat::Hdr10,
                    color_space: ColorSpace::Bt2020,
                    secondary: false,
                    label: "Main".into(),
                }),
                Stream::Audio(AudioStream {
                    pid: 0x1100,
                    codec: Codec::TrueHd,
                    channels: "7.1".into(),
                    language: "eng".into(),
                    sample_rate: "48kHz".into(),
                    secondary: false,
                    label: "English".into(),
                }),
            ],
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: ContentFormat::BdTs,
        }
    }

    #[test]
    #[ignore] // Requires TCP; may be flaky in CI environments
    fn network_listen_connect_roundtrip() {
        // Bind to OS-assigned port
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // Release so NetworkStream::listen can bind

        let addr = format!("127.0.0.1:{}", port);
        let addr_clone = addr.clone();

        // Spawn listener in a thread
        let handle = std::thread::spawn(move || {
            let mut ns = NetworkStream::listen(&addr_clone).unwrap();
            let mut buf = vec![0u8; 4096];
            let mut received = Vec::new();
            loop {
                match ns.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => received.extend_from_slice(&buf[..n]),
                    Err(_) => break,
                }
            }
            received
        });

        // Small delay to let the listener thread bind
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Connect and write data
        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr).unwrap().meta(&dt);
        let payload = b"Hello from the write side of the network stream!";
        writer.write_all(payload).unwrap();
        writer.finish().unwrap();

        let received = handle.join().unwrap();
        // The received data should end with our payload (after the FMKV header)
        assert!(
            received.windows(payload.len()).any(|w| w == payload),
            "payload not found in received data (got {} bytes)",
            received.len()
        );
    }

    #[test]
    #[ignore] // Requires TCP; may be flaky in CI environments
    fn network_metadata_flows() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let addr = format!("127.0.0.1:{}", port);
        let addr_clone = addr.clone();

        let handle = std::thread::spawn(move || {
            let ns = NetworkStream::listen(&addr_clone).unwrap();
            let info = ns.info().clone();
            info
        });

        std::thread::sleep(std::time::Duration::from_millis(50));

        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr).unwrap().meta(&dt);
        // Must write at least one byte to trigger header send
        writer.write_all(&[0u8; 192]).unwrap();
        writer.finish().unwrap();

        let info = handle.join().unwrap();
        assert_eq!(info.playlist, "NetworkTest");
        assert_eq!(info.duration_secs, 3600.0);
        assert_eq!(info.streams.len(), 2);
    }

    #[test]
    fn network_empty_addr_errors() {
        let result = NetworkStream::connect("");
        assert!(result.is_err(), "empty address should fail");
    }

    #[test]
    fn network_no_port_errors() {
        // Connecting to an address without a port should fail
        let result = NetworkStream::connect("127.0.0.1");
        assert!(result.is_err(), "address without port should fail");
    }
}
