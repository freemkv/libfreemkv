//! NetworkStream — PES frames over TCP with embedded metadata.
//!
//! **Security:** Data is transmitted over plain TCP with no encryption.
//! Use only on trusted networks (LAN).
//!
//! Write side (sender): connects to a listener, sends FMKV header + PES frames.
//! Read side (receiver): listens for a connection, reads FMKV header + PES frames.

use super::meta;
use crate::disc::DiscTitle;
use std::io::{self, BufReader, BufWriter, Write};
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

        // Read FMKV metadata header
        let disc_title = meta::read_header(&mut reader)?
            .ok_or_else(|| -> io::Error { crate::error::Error::NoMetadata.into() })?
            .to_title();

        Ok(Self {
            disc_title,
            mode: Mode::Read { reader },
        })
    }
}

impl crate::pes::Stream for NetworkStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        match &mut self.mode {
            Mode::Read { reader } => crate::pes::PesFrame::deserialize(reader),
            _ => Err(crate::error::Error::StreamWriteOnly.into()),
        }
    }
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match &mut self.mode {
            Mode::Write {
                writer,
                header_written,
                ..
            } => {
                if !*header_written {
                    if !self.disc_title.streams.is_empty() {
                        let m = meta::M2tsMeta::from_title(&self.disc_title);
                        meta::write_header(&mut *writer, &m)?;
                    }
                    *header_written = true;
                }
                frame.serialize(writer)
            }
            _ => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }
    fn finish(&mut self) -> io::Result<()> {
        if let Mode::Write { writer, .. } = &mut self.mode {
            writer.flush()?;
            writer.get_ref().shutdown(std::net::Shutdown::Write)?;
        }
        Ok(())
    }
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }
}

// NetworkStream is PES-only — no IOStream/Read/Write byte interface.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, ContentFormat, FrameRate, HdrFormat,
        Resolution, SampleRate, Stream, VideoStream,
    };
    use std::net::TcpListener;

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
                    resolution: Resolution::R2160p,
                    frame_rate: FrameRate::F23_976,
                    hdr: HdrFormat::Hdr10,
                    color_space: ColorSpace::Bt2020,
                    secondary: false,
                    label: "Main".into(),
                }),
                Stream::Audio(AudioStream {
                    pid: 0x1100,
                    codec: Codec::TrueHd,
                    channels: AudioChannels::Surround71,
                    language: "eng".into(),
                    sample_rate: SampleRate::S48,
                    secondary: false,
                    purpose: crate::disc::LabelPurpose::Normal,
                    label: "English".into(),
                }),
            ],
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    #[test]
    #[ignore] // Requires TCP; may be flaky in CI environments
    fn network_pes_roundtrip() {
        use crate::pes;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let addr = format!("127.0.0.1:{}", port);
        let addr_clone = addr.clone();

        let handle = std::thread::spawn(move || {
            let mut ns = NetworkStream::listen(&addr_clone).unwrap();
            let info = pes::Stream::info(&ns).clone();
            let mut frames = Vec::new();
            while let Ok(Some(f)) = pes::Stream::read(&mut ns) {
                frames.push(f);
            }
            (info, frames)
        });

        std::thread::sleep(std::time::Duration::from_millis(50));

        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr).unwrap().meta(&dt);
        let frame = pes::PesFrame {
            track: 0,
            pts: 90000,
            keyframe: true,
            data: vec![0x47; 192],
        };
        pes::Stream::write(&mut writer, &frame).unwrap();
        pes::Stream::finish(&mut writer).unwrap();

        let (info, frames) = handle.join().unwrap();
        assert_eq!(info.playlist, "NetworkTest");
        assert_eq!(info.streams.len(), 2);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].track, 0);
        assert_eq!(frames[0].pts, 90000);
    }

    #[test]
    fn network_empty_addr_errors() {
        let result = NetworkStream::connect("");
        assert!(result.is_err());
    }

    #[test]
    fn network_no_port_errors() {
        let result = NetworkStream::connect("127.0.0.1");
        assert!(result.is_err());
    }
}
