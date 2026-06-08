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
        // The sender is the latency-sensitive side; set nodelay here too
        // (the listen side already does) so the final sub-MSS flush after
        // finish() isn't held by Nagle. The 256 KB BufWriter coalesces
        // bulk writes, so this only affects the tail.
        stream.set_nodelay(true)?;
        Ok(Self {
            disc_title: DiscTitle::empty(),
            mode: Mode::Write {
                writer: BufWriter::with_capacity(NET_BUF_SIZE, stream),
                header_written: false,
            },
        })
    }

    /// Set stream metadata (write side only). Returns self for chaining.
    ///
    /// Only meaningful on a [`connect`](Self::connect)-constructed
    /// (write) stream — the title is sent in the FMKV header on first
    /// write. On a [`listen`](Self::listen)-constructed (read) stream
    /// the stored title is immediately overwritten by the header read in
    /// `listen()`, so calling `meta()` there is a silent no-op.
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    /// Listen for an incoming connection and read from it.
    /// Extracts FMKV metadata header from the sender.
    ///
    /// Accepts exactly one connection; the listening socket is dropped after
    /// `accept`, so the bound port is freed and any subsequent connection
    /// attempt to the same address is refused.
    pub fn listen(addr: &str) -> io::Result<Self> {
        Self::accept_from(TcpListener::bind(addr)?)
    }

    /// Accept one connection from an already-bound listener and read from it.
    /// Lets a caller bind first (learning the actual port for an ephemeral
    /// `:0` bind) and hand the listener in, closing the bind/drop/re-bind race
    /// that `listen(addr)` would otherwise have.
    pub fn accept_from(listener: TcpListener) -> io::Result<Self> {
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

/// Write the FMKV metadata header exactly once, before any frames. Always
/// writes (even when the title has no streams) so the receiver's
/// `read_header()` always finds the magic and never falls into the
/// NoMetadata path on a zero-frame stream.
fn ensure_header_written(
    writer: &mut BufWriter<TcpStream>,
    header_written: &mut bool,
    disc_title: &DiscTitle,
) -> io::Result<()> {
    if !*header_written {
        let m = meta::M2tsMeta::from_title(disc_title);
        meta::write_header(writer, &m)?;
        *header_written = true;
    }
    Ok(())
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
            } => {
                ensure_header_written(writer, header_written, &self.disc_title)?;
                frame.serialize(writer)
            }
            _ => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }
    fn finish(&mut self) -> io::Result<()> {
        if let Mode::Write {
            writer,
            header_written,
        } = &mut self.mode
        {
            // Always emit the FMKV header before shutdown, even for a
            // zero-frame stream (e.g. a title that produced no PES frames).
            // Without it the receiver's read_header() sees a clean EOF and
            // rejects the stream with NoMetadata.
            ensure_header_written(writer, header_written, &self.disc_title)?;
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
    fn network_pes_roundtrip() {
        use crate::pes;
        use std::sync::mpsc;

        // The listener thread owns the bound socket and reports its actual
        // local address back over a channel before accept(). The main thread
        // connects only after receiving the address — no bind/drop/re-bind
        // window, no sleep-as-synchronisation.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (addr_tx, addr_rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            addr_tx.send(addr).unwrap();
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let info = pes::Stream::info(&ns).clone();
            let mut frames = Vec::new();
            while let Ok(Some(f)) = pes::Stream::read(&mut ns) {
                frames.push(f);
            }
            (info, frames)
        });

        let addr = addr_rx.recv().unwrap();
        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr.to_string()).unwrap().meta(&dt);
        let frame = pes::PesFrame {
            track: 0,
            pts: 90000,
            keyframe: true,
            data: vec![0x47; 192],
            duration_ns: None,
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
    fn network_zero_frame_finish_still_sends_header() {
        use crate::pes;
        use std::sync::mpsc;

        // A title that produces no PES frames must still send the FMKV header
        // on finish(), so the receiver gets the metadata instead of rejecting
        // the stream with NoMetadata on a clean EOF.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (addr_tx, addr_rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            addr_tx.send(addr).unwrap();
            // listen()'s read_header must succeed (header present), not error.
            let ns = NetworkStream::accept_from(listener).unwrap();
            pes::Stream::info(&ns).playlist.clone()
        });

        let addr = addr_rx.recv().unwrap();
        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr.to_string()).unwrap().meta(&dt);
        // No write() at all — straight to finish().
        pes::Stream::finish(&mut writer).unwrap();

        let playlist = handle.join().unwrap();
        assert_eq!(
            playlist, "NetworkTest",
            "zero-frame finish() must still deliver the metadata header"
        );
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

    /// Spawn an accepting reader and return (its address, join handle that
    /// yields all frames read after the FMKV header).
    fn spawn_reader() -> (
        std::net::SocketAddr,
        std::thread::JoinHandle<(DiscTitle, Vec<crate::pes::PesFrame>)>,
    ) {
        use crate::pes;
        // Bind BEFORE spawning so the port is live when connect() runs — no
        // channel handshake needed (the listener already owns the socket).
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let info = pes::Stream::info(&ns).clone();
            let mut frames = Vec::new();
            while let Ok(Some(f)) = pes::Stream::read(&mut ns) {
                frames.push(f);
            }
            (info, frames)
        });
        (addr, handle)
    }

    /// write() on a listen()/accept-constructed (READ) stream must return
    /// StreamReadOnly — the read side has no writer. (Returning Ok would let
    /// a caller silently lose frames written into a receive-only socket.)
    #[test]
    fn write_on_read_side_is_read_only_error() {
        use crate::pes;
        let (addr, handle) = spawn_reader();

        // Sender connects, sends header (zero frames), finishes — so the
        // reader's accept_from() returns. We test the reader's write guard.
        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr.to_string()).unwrap().meta(&dt);
        pes::Stream::finish(&mut writer).unwrap();
        let (_info, _frames) = handle.join().unwrap();

        // Now build a fresh read-side stream and confirm its write() errors.
        // (Re-bind, accept once, then immediately try to write to it.)
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr2 = listener.local_addr().unwrap();
        let h = std::thread::spawn(move || {
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let frame = pes::PesFrame {
                track: 0,
                pts: 0,
                keyframe: true,
                data: vec![0u8; 8],
                duration_ns: None,
            };
            // Read side: writing must be a typed StreamReadOnly error.
            let err = pes::Stream::write(&mut ns, &frame).expect_err("read side write must error");
            err.kind()
        });
        // Drive the accept: connect + send header so accept_from completes.
        let mut w2 = NetworkStream::connect(&addr2.to_string())
            .unwrap()
            .meta(&dt);
        pes::Stream::finish(&mut w2).unwrap();
        let kind = h.join().unwrap();
        // E_STREAM_READ_ONLY (9000) maps to Unsupported.
        assert_eq!(kind, io::ErrorKind::Unsupported);
    }

    /// read() on a connect()-constructed (WRITE) stream must return
    /// StreamWriteOnly — never Ok(None), which a caller would read as a
    /// legitimately empty stream.
    #[test]
    fn read_on_write_side_is_write_only_error() {
        use crate::pes;
        let (addr, handle) = spawn_reader();
        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr.to_string()).unwrap().meta(&dt);
        let err = pes::Stream::read(&mut writer).expect_err("write side read must error");
        // E_STREAM_WRITE_ONLY (9001) maps to Unsupported.
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        pes::Stream::finish(&mut writer).unwrap();
        let _ = handle.join().unwrap();
    }

    /// The FMKV header must be written exactly once, before the first frame,
    /// even across many frames. The receiver must therefore reconstruct the
    /// title exactly once and read every frame after it — a header re-emitted
    /// between frames would desync PesFrame::deserialize and corrupt frame N.
    #[test]
    fn header_written_once_then_all_frames_roundtrip() {
        use crate::pes;
        let (addr, handle) = spawn_reader();
        let dt = sample_title();
        let mut writer = NetworkStream::connect(&addr.to_string()).unwrap().meta(&dt);
        for i in 0..5u8 {
            let frame = pes::PesFrame {
                track: (i % 2) as usize,
                pts: i as i64 * 90_000,
                keyframe: i == 0,
                data: vec![i; 100 + i as usize],
                duration_ns: None,
            };
            pes::Stream::write(&mut writer, &frame).unwrap();
        }
        pes::Stream::finish(&mut writer).unwrap();
        let (info, frames) = handle.join().unwrap();
        // Title parsed once and intact.
        assert_eq!(info.streams.len(), 2);
        // Every frame survived in order with exact payloads — no desync from
        // a duplicated header.
        assert_eq!(frames.len(), 5);
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.pts, i as i64 * 90_000, "frame {i} pts");
            assert_eq!(f.data.len(), 100 + i, "frame {i} payload length");
            assert!(
                f.data.iter().all(|&b| b == i as u8),
                "frame {i} payload bytes"
            );
        }
    }

    /// The receiver's title comes strictly from the SENDER's FMKV header:
    /// the sender's meta() title is what accept_from() reconstructs, proving
    /// the metadata flows sender→receiver over the header (not from the
    /// receiver's empty default). Distinct sender title confirms the source.
    #[test]
    fn receiver_title_comes_from_sender_header() {
        use crate::pes;
        let (addr, handle) = spawn_reader();
        let mut dt = sample_title();
        dt.playlist = "SenderControlled".into();
        dt.playlist_id = 42;
        let mut writer = NetworkStream::connect(&addr.to_string()).unwrap().meta(&dt);
        pes::Stream::finish(&mut writer).unwrap();
        let (info, _frames) = handle.join().unwrap();
        // The receiver default title is empty (playlist ""); it must have
        // been replaced by the sender's header-carried title.
        assert_eq!(info.playlist, "SenderControlled");
        assert_eq!(
            info.streams.len(),
            2,
            "stream descriptors round-trip via header"
        );
    }

    /// accept_from() must reject a connection whose first bytes are NOT the
    /// FMKV magic — there is no metadata to drive muxing, so it surfaces
    /// NoMetadata rather than proceeding with an empty/garbage title.
    #[test]
    fn accept_from_rejects_stream_without_fmkv_header() {
        use std::io::Write as _;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            // Raw non-FMKV bytes (not starting with 'F') then close.
            let mut s = TcpStream::connect(addr).unwrap();
            s.write_all(&[0x47u8; 64]).unwrap(); // TS sync bytes, no FMKV magic
            s.shutdown(std::net::Shutdown::Both).unwrap();
        });
        let err = match NetworkStream::accept_from(listener) {
            Ok(_) => panic!("missing FMKV header must error, not silently accept"),
            Err(e) => e,
        };
        // E_NO_METADATA (9008) maps to InvalidInput.
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        handle.join().unwrap();
    }
}
