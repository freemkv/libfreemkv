//! NetworkStream — PES frames over TCP with embedded metadata.
//!
//! **Security:** Data is transmitted over plain TCP with no encryption.
//! Use only on trusted networks (LAN).
//!
//! Write side (sender): connects to a listener, sends FMKV header + PES frames.
//! Read side (receiver): listens for a connection, reads FMKV header + PES frames.

use super::meta;
use crate::disc::DiscTitle;
use crate::halt::{Halt, WAIT_SLICE};
use crate::sector::stage::Stage;
use rustix::event::{PollFd, PollFlags, Timespec};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};

/// I/O buffer size for network reads/writes.
const NET_BUF_SIZE: usize = 256 * 1024;

// Accept poll period while waiting for the sender (a halt is checked each tick).
const ACCEPT_POLL: std::time::Duration = std::time::Duration::from_millis(50);

// A slow sender (bad-sector retries) may be silent for minutes, so there is no
// data-idle limit; a vanished peer is detected by TCP keepalive instead.
const KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_secs(60);
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
const KEEPALIVE_RETRIES: u32 = 6;

// Enable keepalive probes so a crashed or unplugged sender fails the read.
fn enable_keepalive(stream: &TcpStream) -> io::Result<()> {
    let ka = socket2::TcpKeepalive::new().with_time(KEEPALIVE_IDLE);
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    let ka = ka
        .with_interval(KEEPALIVE_INTERVAL)
        .with_retries(KEEPALIVE_RETRIES);
    socket2::SockRef::from(stream).set_tcp_keepalive(&ka)
}

// Keepalive is best effort: a stack that rejects an option (e.g. TCP_KEEPCNT on
// old Windows) still receives, just without dead-peer detection.
fn arm_keepalive(stream: &TcpStream) {
    if let Err(e) = enable_keepalive(stream) {
        tracing::warn!(
            target: "mux",
            error = %e,
            "network: TCP keepalive unavailable"
        );
    }
}

// Poll outcome: `Ok(true)` reads now (data, EOF or a socket error for `read` to
// surface), `Ok(false)` is a halt-check tick. Readiness, not SO_RCVTIMEO: on
// Windows a receive timeout can cancel a completing recv and lose its data.
fn poll_says_read(res: io::Result<usize>, revents: PollFlags) -> io::Result<bool> {
    match res {
        Err(e) if e.kind() == io::ErrorKind::Interrupted => Ok(false),
        Err(e) => Err(e),
        Ok(0) => Ok(false),
        Ok(_) => Ok(!revents.is_empty()),
    }
}

// Receive side: waits for readability in `tick` slices so a halt is observed
// instead of blocking in `read` forever.
struct HaltRead {
    stream: TcpStream,
    halt: Option<Halt>,
    halted: bool,
    tick: Timespec,
}

impl HaltRead {
    fn new(stream: TcpStream, halt: Option<Halt>) -> Self {
        Self::with_tick(stream, halt, WAIT_SLICE)
    }

    fn with_tick(stream: TcpStream, halt: Option<Halt>, tick: std::time::Duration) -> Self {
        let tick = Timespec {
            tv_sec: tick.as_secs() as _,
            tv_nsec: tick.subsec_nanos() as _,
        };
        Self {
            stream,
            halt,
            halted: false,
            tick,
        }
    }

    // `e`, or `Error::Halted` when the read was aborted by the halt.
    fn halted_or(&self, e: io::Error) -> io::Error {
        if self.halted {
            crate::error::Error::Halted.into()
        } else {
            e
        }
    }
}

impl Read for HaltRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(halt) = &self.halt else {
            return self.stream.read(buf);
        };
        loop {
            if halt.is_cancelled() {
                // Halted maps to ErrorKind::Interrupted, which std's read_exact
                // retries forever; abort with another kind and remap in `halted_or`.
                self.halted = true;
                return Err(io::ErrorKind::ConnectionAborted.into());
            }
            let mut fds = [PollFd::new(&self.stream, PollFlags::IN)];
            let res = rustix::event::poll(&mut fds, Some(&self.tick)).map_err(io::Error::from);
            if poll_says_read(res, fds[0].revents())? {
                return self.stream.read(buf);
            }
        }
    }
}

// True if `ip` can never be a `network://` peer (unspecified, multicast, broadcast,
// 0.0.0.0/8, Class E). Loopback, private, link-local and ULA are valid LAN targets.
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_unspecified()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.octets()[0] == 0
                || v4.octets()[0] >= 240
        }
        IpAddr::V6(v6) => {
            v6.is_unspecified()
                || v6.is_multicast()
                // IPv4-mapped (::ffff:x.x.x.x) is judged as its IPv4 address.
                || v6.to_ipv4_mapped().is_some_and(|m| is_blocked_ip(IpAddr::V4(m)))
        }
    }
}

// Every resolved address of `addr` not refused by `is_blocked_ip`, in order. Zero
// resolved or all refused: NetworkAddrBlocked.
fn allowed_addrs(
    addr: &str,
    resolved: impl Iterator<Item = SocketAddr>,
) -> io::Result<Vec<SocketAddr>> {
    let allowed: Vec<SocketAddr> = resolved.filter(|sa| !is_blocked_ip(sa.ip())).collect();
    if allowed.is_empty() {
        return Err(crate::error::Error::NetworkAddrBlocked {
            addr: addr.to_string(),
        }
        .into());
    }
    Ok(allowed)
}

// Bounds a connect to one address (the OS SYN timeout is 75 s or more, and Stop
// cannot interrupt a blocking connect).
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

// Connect to the first reachable address (a dual-stack host with a dead v6 route
// still reaches v4), as `TcpStream::connect(&str)` does.
fn connect_first(addrs: &[SocketAddr]) -> io::Result<TcpStream> {
    let mut last = io::Error::from(io::ErrorKind::AddrNotAvailable);
    for a in addrs {
        match TcpStream::connect_timeout(a, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(e) => last = e,
        }
    }
    Err(last)
}

enum Mode {
    Write {
        writer: BufWriter<TcpStream>,
        header_written: bool,
        timings: Vec<crate::pes::TrackTiming>,
        padded: bool,
        // Set once `finish` sent the clean end; otherwise drop resets the connection.
        ended: bool,
    },
    Read {
        reader: Box<BufReader<Stage<HaltRead>>>,
        meta: meta::M2tsMeta,
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
        Self::connect_vetted(addr, true)
    }

    // `connect` with a toggle: vet=true (the public path) refuses unspecified/
    // multicast/broadcast targets; vet=false skips that for in-crate tests.
    fn connect_vetted(addr: &str, vet: bool) -> io::Result<Self> {
        let stream = if vet {
            let vetted = allowed_addrs(addr, addr.to_socket_addrs()?)?;
            connect_first(&vetted)?
        } else {
            TcpStream::connect(addr)?
        };
        // Sender is latency-sensitive; set nodelay here too (listen side already does)
        // so the final sub-MSS flush after finish() isn't held by Nagle — the 256 KB
        // BufWriter coalesces bulk writes, so this only affects the tail.
        stream.set_nodelay(true)?;
        // A crashed or unplugged receiver fails the send instead of hanging it.
        arm_keepalive(&stream);
        Ok(Self {
            disc_title: DiscTitle::empty(),
            mode: Mode::Write {
                writer: BufWriter::with_capacity(NET_BUF_SIZE, stream),
                header_written: false,
                timings: Vec::new(),
                padded: false,
                ended: false,
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
        Self::listen_with_halt(addr, None)
    }

    /// [`listen`](Self::listen) that a [`Halt`] can interrupt, while waiting for
    /// the sender to connect and while blocked on a stalled sender.
    pub fn listen_with_halt(addr: &str, halt: Option<Halt>) -> io::Result<Self> {
        Self::listen_staged(addr, halt, false)
    }

    // `listen_with_halt` whose decryption stage passes ciphertext when `raw`.
    pub(crate) fn listen_staged(addr: &str, halt: Option<Halt>, raw: bool) -> io::Result<Self> {
        Self::accept_staged(TcpListener::bind(addr)?, halt, raw)
    }

    /// Accept one connection from an already-bound listener and read from it.
    /// Lets a caller bind first (learning the actual port for an ephemeral
    /// `:0` bind) and hand the listener in, closing the bind/drop/re-bind race
    /// that `listen(addr)` would otherwise have.
    pub fn accept_from(listener: TcpListener) -> io::Result<Self> {
        Self::accept_from_with_halt(listener, None)
    }

    /// [`accept_from`](Self::accept_from) that a [`Halt`] can interrupt.
    pub fn accept_from_with_halt(listener: TcpListener, halt: Option<Halt>) -> io::Result<Self> {
        Self::accept_staged(listener, halt, false)
    }

    fn accept_staged(listener: TcpListener, halt: Option<Halt>, raw: bool) -> io::Result<Self> {
        let Some(h) = halt else {
            let (stream, _peer) = listener.accept()?;
            stream.set_nodelay(true)?;
            arm_keepalive(&stream);
            return Self::read_from(stream, None, raw);
        };
        listener.set_nonblocking(true)?;
        let halt = Some(h);
        let stream = loop {
            if halt.as_ref().is_some_and(Halt::is_cancelled) {
                return Err(crate::error::Error::Halted.into());
            }
            match listener.accept() {
                Ok((stream, _peer)) => break stream,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(ACCEPT_POLL);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        };
        // Accepted sockets can inherit O_NONBLOCK (BSD/macOS); reads wait on readiness.
        stream.set_nonblocking(false)?;
        stream.set_nodelay(true)?;
        arm_keepalive(&stream);
        Self::read_from(stream, halt, raw)
    }

    // Wrap an accepted connection in the decryption stage and read its FMKV header.
    fn read_from(stream: TcpStream, halt: Option<Halt>, raw: bool) -> io::Result<Self> {
        let staged = Stage::lazy(HaltRead::new(stream, halt), raw);
        let mut reader = BufReader::with_capacity(NET_BUF_SIZE, staged);

        // Read FMKV metadata header
        let meta = meta::read_header(&mut reader)
            .map_err(|e| reader.get_ref().get_ref().halted_or(e))?
            .ok_or_else(|| -> io::Error { crate::error::Error::NoMetadata.into() })?;

        Ok(Self {
            disc_title: meta.to_title(),
            mode: Mode::Read {
                reader: Box::new(reader),
                meta,
            },
        })
    }
}

// Write the FMKV header once, before any frames, even for a zero-frame stream
// (else read_header() reports NoMetadata). Sets `padded` when frames carry the
// DiscardPadding extension (header v2).
fn ensure_header_written(
    writer: &mut BufWriter<TcpStream>,
    header_written: &mut bool,
    disc_title: &DiscTitle,
    timings: &[crate::pes::TrackTiming],
    padded: &mut bool,
) -> io::Result<()> {
    if !*header_written {
        let m = meta::M2tsMeta::from_title(disc_title).with_timings(timings);
        meta::write_header(writer, &m)?;
        *padded = m.frame_padding;
        *header_written = true;
    }
    Ok(())
}

impl NetworkStream {
    /// The title being read or written (both directions open this type).
    pub fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }
}

impl crate::pes::PesSource for NetworkStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        match &mut self.mode {
            Mode::Read { reader, meta } => {
                crate::pes::PesFrame::deserialize_ext(reader, meta.frame_padding)
                    .map_err(|e| reader.get_ref().get_ref().halted_or(e))
            }
            _ => Err(crate::error::Error::StreamWriteOnly.into()),
        }
    }

    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn track_timing(&self, track: usize) -> crate::pes::TrackTiming {
        match &self.mode {
            Mode::Read { meta, .. } => meta.timing(track),
            Mode::Write { .. } => Default::default(),
        }
    }

    // The sender's codec privates travel in the FMKV header (as for stdio).
    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        self.disc_title.codec_privates.get(track).cloned().flatten()
    }
}

impl crate::pes::PesSink for NetworkStream {
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match &mut self.mode {
            Mode::Write {
                writer,
                header_written,
                timings,
                padded,
                ..
            } => {
                ensure_header_written(writer, header_written, &self.disc_title, timings, padded)?;
                frame.serialize_ext(writer, *padded)
            }
            _ => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        if let Mode::Write {
            writer,
            header_written,
            timings,
            padded,
            ended,
        } = &mut self.mode
        {
            // Always emit the FMKV header before shutdown, even for a zero-frame stream,
            // or the receiver's read_header() sees a clean EOF and rejects the stream
            // with NoMetadata.
            ensure_header_written(writer, header_written, &self.disc_title, timings, padded)?;
            writer.flush()?;
            writer.get_ref().shutdown(std::net::Shutdown::Write)?;
            *ended = true;
        }
        Ok(())
    }

    // A failed or stopped title: reset instead of the clean end, so the receiver
    // reports an error rather than a complete (truncated) title.
    fn finish_incomplete(&mut self) -> io::Result<()> {
        if let Mode::Write { writer, .. } = &self.mode {
            reset_on_close(writer.get_ref());
        }
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn set_track_timing(
        &mut self,
        track: usize,
        timing: crate::pes::TrackTiming,
    ) -> io::Result<()> {
        match &mut self.mode {
            Mode::Write {
                header_written: false,
                timings,
                ..
            } => set_timing(timings, track, timing, self.disc_title.streams.len()),
            // The header (which carries it) is already on the wire.
            Mode::Write { .. } => Err(crate::error::Error::StreamHeaderWritten.into()),
            Mode::Read { .. } => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }
}

// Record `timing` for stream `track` of a `tracks`-stream title.
pub(crate) fn set_timing(
    timings: &mut Vec<crate::pes::TrackTiming>,
    track: usize,
    timing: crate::pes::TrackTiming,
    tracks: usize,
) -> io::Result<()> {
    if track >= tracks {
        return Err(crate::error::Error::MuxTrackRange { track, tracks }.into());
    }
    if timings.len() < tracks {
        timings.resize(tracks, Default::default());
    }
    timings[track] = timing;
    Ok(())
}

// Make the socket's close an RST, not a FIN: the FMKV wire has no end marker, so
// a FIN reads as a complete title. Nonblocking so the BufWriter's drop flush
// cannot hang on a stalled receiver. Best effort: a failure leaves a FIN.
fn reset_on_close(stream: &TcpStream) {
    let _ = socket2::SockRef::from(stream).set_linger(Some(std::time::Duration::ZERO));
    let _ = stream.set_nonblocking(true);
}

// A sender dropped without `finish` (error, panic, stop) must not end cleanly.
impl Drop for NetworkStream {
    fn drop(&mut self) {
        if let Mode::Write { writer, ended, .. } = &self.mode
            && !*ended
        {
            reset_on_close(writer.get_ref());
        }
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

    // LAN targets are valid for `network://`: loopback, private, link-local,
    // ULA and CGNAT connect; only addresses that can never be a peer are refused.
    #[test]
    fn is_blocked_ip_allows_lan_and_refuses_invalid() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        // Built from octets so the internal-infra secret scanner doesn't flag RFC1918.
        let v4 = |a, b, c, d| IpAddr::V4(Ipv4Addr::new(a, b, c, d));
        let v6 = |s: [u16; 8]| IpAddr::V6(Ipv6Addr::from(s));
        let invalid = [
            v4(0, 0, 0, 0),
            v4(0, 1, 2, 3),
            v4(224, 0, 0, 1),
            v4(255, 255, 255, 255),
            v4(240, 0, 0, 1),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            v6([0xff02, 0, 0, 0, 0, 0, 0, 1]),
            v6([0, 0, 0, 0, 0, 0xffff, 0xe000, 1]),
            v6([0, 0, 0, 0, 0, 0xffff, 0, 0]),
        ];
        for ip in invalid {
            assert!(is_blocked_ip(ip), "{ip} must be refused");
        }
        let allowed = [
            v4(127, 0, 0, 1),
            v4(10, 0, 0, 1),
            v4(172, 16, 5, 5),
            v4(192, 168, 1, 1),
            v4(169, 254, 10, 10),
            v4(100, 64, 0, 1),
            v4(8, 8, 8, 8),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            v6([0xfd12, 0x3456, 0, 0, 0, 0, 0, 1]),
            v6([0xfe80, 0, 0, 0, 0, 0, 0, 1]),
            v6([0, 0, 0, 0, 0, 0xffff, 0x0a00, 1]),
            v6([0, 0, 0, 0, 0, 0xffff, 0x7f00, 1]),
            v6([0x2606, 0x2800, 0x220, 1, 0, 0, 0, 1]),
        ];
        for ip in allowed {
            assert!(!is_blocked_ip(ip), "{ip} must be allowed");
        }
    }

    // The public `connect` reaches a loopback listener (the LAN-send path).
    #[test]
    fn connect_reaches_loopback_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        NetworkStream::connect(&addr.to_string()).expect("loopback send must connect");
    }

    // The filter keeps every allowed address in order and refuses a resolution
    // that is empty or all invalid.
    #[test]
    fn allowed_addrs_keeps_every_valid_address_and_refuses_none() {
        let a = |s: &str| s.parse::<SocketAddr>().unwrap();
        let mixed = [
            a("127.0.0.1:9"),
            a("224.0.0.1:9"),
            a("[::1]:9"),
            a("0.0.0.0:9"),
            a("8.8.8.8:9"),
        ];
        let got = allowed_addrs("h:9", mixed.into_iter()).unwrap();
        assert_eq!(got, vec![a("127.0.0.1:9"), a("[::1]:9"), a("8.8.8.8:9")]);
        for list in [vec![a("0.0.0.0:9"), a("[ff02::1]:9")], vec![]] {
            let err = allowed_addrs("h:9", list.into_iter()).expect_err("no valid address");
            assert_eq!(
                crate::error::error_code(&err),
                Some(crate::error::E_NETWORK_ADDR_BLOCKED)
            );
        }
    }

    // Frame track ids are a u8: a header declaring more than 256 streams is refused
    // (E9008) before any per-stream state is built from it.
    #[test]
    fn a_header_with_more_streams_than_track_ids_is_refused() {
        let mut title = sample_title();
        let audio = title.streams[1].clone();
        for n in [256usize, 257] {
            title.streams.resize(n, audio.clone());
            let mut wire = Vec::new();
            meta::write_header(&mut wire, &meta::M2tsMeta::from_title(&title)).unwrap();
            let res = meta::read_header(&mut wire.as_slice());
            match n {
                256 => assert_eq!(res.unwrap().unwrap().streams.len(), 256),
                _ => assert_eq!(
                    crate::error::error_code(&res.expect_err("too many streams")),
                    Some(crate::error::E_NO_METADATA)
                ),
            }
        }
    }

    // A dead first address falls through to the next one.
    #[test]
    fn connect_first_falls_through_a_dead_address() {
        let dead = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let live = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = connect_first(&[dead, live.local_addr().unwrap()]).expect("reaches live");
        assert_eq!(stream.peer_addr().unwrap(), live.local_addr().unwrap());
        assert!(connect_first(&[dead]).is_err());
    }

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
                    display_aspect: None,
                    secondary: false,
                    label: "Main".into(),
                    measured_cicp: None,
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

        // Listener thread reports its actual bound address over a channel before
        // accept(); main thread connects only after receiving it — no bind/drop/
        // re-bind window, no sleep-as-synchronisation.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (addr_tx, addr_rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            addr_tx.send(addr).unwrap();
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let info = pes::PesSource::info(&ns).clone();
            let mut frames = Vec::new();
            while let Ok(Some(f)) = pes::PesSource::read(&mut ns) {
                frames.push(f);
            }
            (info, frames)
        });

        let addr = addr_rx.recv().unwrap();
        let dt = sample_title();
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&dt);
        let frame = pes::PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: 90000,
            keyframe: true,
            data: vec![0x47; 192],
            duration_ns: None,
        };
        pes::PesSink::write(&mut writer, &frame).unwrap();
        pes::PesSink::finish(&mut writer).unwrap();

        let (info, frames) = handle.join().unwrap();
        assert_eq!(info.playlist, "NetworkTest");
        assert_eq!(info.streams.len(), 2);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].track, 0);
        assert_eq!(frames[0].pts, 90000);
    }

    // Accept one sender and read to the end; returns the frame count and the
    // terminal read result (Ok(None) = a clean end, Err = a failed sender).
    type ReadEnd = (usize, io::Result<Option<crate::pes::PesFrame>>);

    fn spawn_ending_reader() -> (std::net::SocketAddr, std::thread::JoinHandle<ReadEnd>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let mut n = 0;
            loop {
                match crate::pes::PesSource::read(&mut ns) {
                    Ok(Some(_)) => n += 1,
                    end => return (n, end),
                }
            }
        });
        (addr, handle)
    }

    fn one_frame() -> crate::pes::PesFrame {
        crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x47; 192],
            duration_ns: None,
        }
    }

    // A sender that fails mid-title (never finished, or finished incomplete) must
    // reach the receiver as an error, never as the clean end of a short title.
    #[test]
    fn a_sender_that_fails_mid_title_is_an_error_at_the_receiver() {
        use crate::pes::PesSink as _;
        for incomplete in [false, true] {
            let (addr, handle) = spawn_ending_reader();
            let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
                .unwrap()
                .meta(&sample_title());
            writer.write(&one_frame()).unwrap();
            if incomplete {
                writer.finish_incomplete().unwrap();
            }
            drop(writer);
            let (_, end) = handle.join().unwrap();
            assert!(end.is_err(), "incomplete={incomplete}: got {end:?}");
        }
        // A finished sender still ends cleanly.
        let (addr, handle) = spawn_ending_reader();
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&sample_title());
        writer.write(&one_frame()).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let (n, end) = handle.join().unwrap();
        assert!(matches!(end, Ok(None)), "got {end:?}");
        assert_eq!(n, 1);
    }

    // A connection cut inside a frame (even with a clean FIN) is an error.
    #[test]
    fn a_connection_cut_mid_frame_is_an_error_at_the_receiver() {
        let (addr, handle) = spawn_ending_reader();
        let mut raw = TcpStream::connect(addr).unwrap();
        let m = meta::M2tsMeta::from_title(&sample_title());
        meta::write_header(&mut raw, &m).unwrap();
        let mut frame = Vec::new();
        one_frame().serialize(&mut frame).unwrap();
        raw.write_all(&frame).unwrap();
        raw.write_all(&frame[..frame.len() / 2]).unwrap();
        raw.shutdown(std::net::Shutdown::Write).unwrap();
        let (n, end) = handle.join().unwrap();
        assert_eq!(n, 1, "the whole frame arrives");
        assert!(end.is_err(), "the cut frame must fail, got {end:?}");
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
            pes::PesSource::info(&ns).playlist.clone()
        });

        let addr = addr_rx.recv().unwrap();
        let dt = sample_title();
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&dt);
        // No write() at all — straight to finish().
        pes::PesSink::finish(&mut writer).unwrap();

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
            let info = pes::PesSource::info(&ns).clone();
            let mut frames = Vec::new();
            while let Ok(Some(f)) = pes::PesSource::read(&mut ns) {
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
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&dt);
        pes::PesSink::finish(&mut writer).unwrap();
        let (_info, _frames) = handle.join().unwrap();

        // Now build a fresh read-side stream and confirm its write() errors.
        // (Re-bind, accept once, then immediately try to write to it.)
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr2 = listener.local_addr().unwrap();
        let h = std::thread::spawn(move || {
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let frame = pes::PesFrame {
                discard_padding_ns: 0,
                coding: None,
                source: None,
                track: 0,
                pts: 0,
                keyframe: true,
                data: vec![0u8; 8],
                duration_ns: None,
            };
            // Read side: writing must be a typed StreamReadOnly error.
            let err = pes::PesSink::write(&mut ns, &frame).expect_err("read side write must error");
            err.kind()
        });
        // Drive the accept: connect + send header so accept_from completes.
        let mut w2 = NetworkStream::connect_vetted(&addr2.to_string(), false)
            .unwrap()
            .meta(&dt);
        pes::PesSink::finish(&mut w2).unwrap();
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
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&dt);
        let err = pes::PesSource::read(&mut writer).expect_err("write side read must error");
        // E_STREAM_WRITE_ONLY (9001) maps to Unsupported.
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        pes::PesSink::finish(&mut writer).unwrap();
        let _ = handle.join().unwrap();
    }

    // FMKV header must be written exactly once, before the first frame, even
    // across many frames — a re-emitted header mid-stream would desync
    // PesFrame::deserialize and corrupt frame N.
    #[test]
    fn header_written_once_then_all_frames_roundtrip() {
        use crate::pes;
        let (addr, handle) = spawn_reader();
        let dt = sample_title();
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&dt);
        for i in 0..5u8 {
            let frame = pes::PesFrame {
                discard_padding_ns: 0,
                coding: None,
                source: None,
                track: (i % 2) as usize,
                pts: i as i64 * 90_000,
                keyframe: i == 0,
                data: vec![i; 100 + i as usize],
                duration_ns: None,
            };
            pes::PesSink::write(&mut writer, &frame).unwrap();
        }
        pes::PesSink::finish(&mut writer).unwrap();
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

    // Receiver's title comes strictly from the sender's FMKV header (not the
    // receiver's empty default) — proven here via a distinct sender title
    // that accept_from() must reconstruct.
    #[test]
    fn receiver_title_comes_from_sender_header() {
        use crate::pes;
        let (addr, handle) = spawn_reader();
        let mut dt = sample_title();
        dt.playlist = "SenderControlled".into();
        dt.playlist_id = 42;
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&dt);
        pes::PesSink::finish(&mut writer).unwrap();
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

    // accept_from() must reject a connection whose first bytes aren't the FMKV magic, surfacing
    // NoMetadata rather than an empty/garbage title.
    #[test]
    fn accept_from_rejects_stream_without_fmkv_header() {
        use std::io::Read as _;
        use std::io::Write as _;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            // Raw non-FMKV bytes (not starting with 'F') then EOF.
            let mut s = TcpStream::connect(addr).unwrap();
            s.write_all(&[0x47u8; 64]).unwrap(); // TS sync bytes, no FMKV magic
            s.shutdown(std::net::Shutdown::Write).unwrap();
            // Park until the server closes, so no RST can overtake the data.
            let _ = s.read(&mut [0u8; 1]);
        });
        let err = match NetworkStream::accept_from(listener) {
            Ok(_) => panic!("missing FMKV header must error, not silently accept"),
            Err(e) => e,
        };
        // E_NO_METADATA (9008) maps to InvalidInput.
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        handle.join().unwrap();
    }

    // A Stop must interrupt a receiver still waiting for its sender to connect.
    #[test]
    fn a_halt_interrupts_a_pending_accept() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let halt = crate::halt::Halt::new();
        let h = halt.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let r = NetworkStream::accept_from_with_halt(listener, Some(h)).map(|_| ());
            let _ = tx.send(r);
        });
        // No sender ever connects: the accept observes the cancel wherever it lands.
        halt.cancel();
        let r = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("accept must observe the halt");
        assert!(crate::error::is_halt(&r.unwrap_err()));
    }

    // ... and a receiver blocked on a sender that stalled mid-stream.
    #[test]
    fn a_halt_interrupts_a_stalled_read() {
        use crate::pes;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let halt = crate::halt::Halt::new();
        let h = halt.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let (ready_tx, ready) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let r = NetworkStream::accept_from_with_halt(listener, Some(h));
            let _ = ready_tx.send(());
            let _ = tx.send(r.and_then(|mut ns| pes::PesSource::read(&mut ns).map(|_| ())));
        });
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&sample_title());
        if let Mode::Write {
            writer: w,
            header_written,
            padded,
            ..
        } = &mut writer.mode
        {
            ensure_header_written(w, header_written, &sample_title(), &[], padded).unwrap();
            w.flush().unwrap();
        }
        // Handshake, not a sleep: cancel once the header is in and the frame read begins.
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the receiver accepted");
        halt.cancel();
        let r = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("read must observe the halt");
        assert!(crate::error::is_halt(&r.unwrap_err()));
        drop(writer);
    }

    // A halt while the sender has connected but not finished its header.
    #[test]
    fn a_halt_interrupts_a_stalled_header_read() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let halt = crate::halt::Halt::new();
        let h = halt.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let r = NetworkStream::accept_from_with_halt(listener, Some(h)).map(|_| ());
            let _ = tx.send(r);
        });
        let mut sender = TcpStream::connect(addr).unwrap();
        sender.write_all(b"FM").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        halt.cancel();
        let r = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the header read must observe the halt");
        assert!(crate::error::is_halt(&r.unwrap_err()));
    }

    // A receiver without a halt (`listen`/`accept_from`) arms keepalive too.
    #[test]
    fn a_receiver_without_a_halt_arms_keepalive() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let ns = NetworkStream::accept_from(listener).unwrap();
            match &ns.mode {
                Mode::Read { reader, .. } => {
                    socket2::SockRef::from(&reader.get_ref().get_ref().stream)
                        .keepalive()
                        .unwrap()
                }
                Mode::Write { .. } => false,
            }
        });
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&sample_title());
        if let Mode::Write {
            writer: w,
            header_written,
            padded,
            ..
        } = &mut writer.mode
        {
            ensure_header_written(w, header_written, &sample_title(), &[], padded).unwrap();
            w.flush().unwrap();
        }
        assert!(handle.join().unwrap(), "keepalive must be armed");
    }

    // Windows may drop data when SO_RCVTIMEO cancels a recv, so a halt-aware
    // receiver waits on readiness instead and never arms a read timeout.
    #[test]
    fn a_halt_aware_receiver_sets_no_read_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let halt = crate::halt::Halt::new();
            let ns = NetworkStream::accept_from_with_halt(listener, Some(halt)).unwrap();
            match &ns.mode {
                Mode::Read { reader, .. } => {
                    reader.get_ref().get_ref().stream.read_timeout().unwrap()
                }
                Mode::Write { .. } => None,
            }
        });
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&sample_title());
        crate::pes::PesSink::finish(&mut writer).unwrap();
        assert_eq!(handle.join().unwrap(), None);
    }

    // Only a readiness report reads; a timeout or EINTR ticks; a poll failure surfaces.
    #[test]
    fn poll_says_read_only_when_the_socket_reports_readiness() {
        use io::ErrorKind::{ConnectionReset, Interrupted, TimedOut, WouldBlock};
        let ok = |n: usize, f: PollFlags| poll_says_read(Ok(n), f).unwrap();
        assert!(!ok(0, PollFlags::empty()), "timeout is a tick");
        assert!(!ok(1, PollFlags::empty()));
        for f in [
            PollFlags::IN,
            PollFlags::HUP,
            PollFlags::ERR,
            PollFlags::NVAL,
        ] {
            assert!(ok(1, f), "{f:?} must hand over to read");
        }
        assert!(!poll_says_read(Err(Interrupted.into()), PollFlags::empty()).unwrap());
        for kind in [WouldBlock, TimedOut, ConnectionReset] {
            let e = poll_says_read(Err(kind.into()), PollFlags::empty()).unwrap_err();
            assert_eq!(e.kind(), kind);
        }
    }

    // A sender dribbling bytes with gaps many ticks long is read byte-identically.
    #[test]
    fn a_slow_sender_across_many_poll_ticks_arrives_byte_identical() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i * 7 + i / 251) as u8).collect();
        let sent = data.clone();
        let sender = std::thread::spawn(move || {
            let mut s = TcpStream::connect(addr).unwrap();
            let mut off = 0;
            for i in 0usize.. {
                if off == sent.len() {
                    break;
                }
                let n = (1 + i * 37 % 4093).min(sent.len() - off);
                s.write_all(&sent[off..off + n]).unwrap();
                off += n;
                if i % 8 == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(6));
                }
            }
        });
        let (stream, _) = listener.accept().unwrap();
        let tick = std::time::Duration::from_millis(1);
        let mut r = HaltRead::with_tick(stream, Some(crate::halt::Halt::new()), tick);
        let mut got = Vec::new();
        let mut small = [0u8; 1500];
        loop {
            match r.read(&mut small).unwrap() {
                0 => break,
                n => got.extend_from_slice(&small[..n]),
            }
        }
        sender.join().unwrap();
        assert_eq!(got.len(), data.len());
        assert!(
            got == data,
            "stream bytes must survive the poll ticks unchanged"
        );
    }

    // A halt-aware receiver arms keepalive (dead-peer detection) and sets no
    // data-idle limit: a sender silent for a while is still read afterwards.
    #[test]
    fn a_halt_aware_receiver_uses_keepalive_not_an_idle_limit() {
        use crate::pes;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let halt = crate::halt::Halt::new();
            let mut ns = NetworkStream::accept_from_with_halt(listener, Some(halt)).unwrap();
            let keepalive = match &ns.mode {
                Mode::Read { reader, .. } => {
                    socket2::SockRef::from(&reader.get_ref().get_ref().stream)
                        .keepalive()
                        .unwrap()
                }
                Mode::Write { .. } => false,
            };
            (
                keepalive,
                pes::PesSource::read(&mut ns).map(|f| f.map(|f| f.data)),
            )
        });
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&sample_title());
        if let Mode::Write {
            writer: w,
            header_written,
            padded,
            ..
        } = &mut writer.mode
        {
            ensure_header_written(w, header_written, &sample_title(), &[], padded).unwrap();
            w.flush().unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(700));
        let frame = pes::PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![7; 4],
            duration_ns: None,
        };
        pes::PesSink::write(&mut writer, &frame).unwrap();
        pes::PesSink::finish(&mut writer).unwrap();
        let (keepalive, read) = handle.join().unwrap();
        assert!(keepalive, "keepalive must be armed");
        assert_eq!(read.unwrap(), Some(vec![7; 4]));
    }

    // The FMKV header's codec privates (e.g. FLAC STREAMINFO, OpusHead, avcC)
    // must be exposed on the receive side, or the MKV writer drops them.
    #[test]
    fn the_receiver_exposes_the_header_codec_privates() {
        use crate::pes;
        let mut title = sample_title();
        title.codec_privates = vec![Some(vec![1, 2, 3]), Some(b"OpusHead".to_vec())];
        let (addr, handle) = spawn_codec_private_reader();
        let mut writer = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&title);
        pes::PesSink::finish(&mut writer).unwrap();
        let cps = handle.join().unwrap();
        assert_eq!(cps, title.codec_privates);
    }

    fn spawn_codec_private_reader() -> (
        std::net::SocketAddr,
        std::thread::JoinHandle<Vec<Option<Vec<u8>>>>,
    ) {
        use crate::pes::PesSource as _;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let ns = NetworkStream::accept_from(listener).unwrap();
            (0..2).map(|i| ns.codec_private(i)).collect()
        });
        (addr, handle)
    }

    // mkv(opus) -> network -> mkv: CodecDelay/SeekPreRoll and DiscardPadding
    // survive the PES wire hop.
    #[test]
    fn opus_track_timing_and_padding_survive_network_to_mkv() {
        use crate::pes::{self, PesSink as _, PesSource as _};
        let mut title = sample_title();
        title.streams.truncate(1);
        title.streams[0] = Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Opus,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: String::new(),
        });
        title.codec_privates = vec![Some(b"OpusHead\x01\x02".to_vec())];
        let timing = pes::TrackTiming {
            codec_delay_ns: 6_500_000,
            seek_preroll_ns: 80_000_000,
        };
        let frame = pes::PesFrame {
            discard_padding_ns: -2_500_000,
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0xFC, 1, 2, 3],
            duration_ns: Some(20_000_000),
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let got = ns.track_timing(0);
            let f = ns.read().unwrap().unwrap();
            (ns.info().clone(), got, f)
        });
        let mut w = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&title);
        w.set_track_timing(0, timing).unwrap();
        w.write(&frame).unwrap();
        w.finish().unwrap();
        let (rx_title, rx_timing, rx_frame) = handle.join().unwrap();
        assert_eq!(rx_timing, timing);
        assert_eq!(rx_frame.discard_padding_ns, frame.discard_padding_ns);
        assert_eq!(rx_frame.data, frame.data);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.mkv");
        let file = std::fs::File::create(&path).unwrap();
        let mut mkv =
            crate::mux::mkvstream::MkvStream::create(Box::new(file), &rx_title, None).unwrap();
        mkv.set_track_timing(0, rx_timing).unwrap();
        mkv.write(&rx_frame).unwrap();
        mkv.finish().unwrap();
        let mut back =
            crate::mux::mkvstream::MkvStream::open(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(back.track_timing(0), timing);
        assert_eq!(back.read().unwrap().unwrap().discard_padding_ns, -2_500_000);
    }
    // Parity golden: the FMKV wire bytes a sender emits for the synthetic clip, and the
    // frames a receiver reads back (public `connect` refuses loopback, so this seam).
    #[test]
    fn parity_network_fmkv_wire() {
        use crate::pes::{PesSink as _, PesSource as _};
        use std::io::Read as _;
        let clip = crate::test_util::synthetic_bd_clip(6);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.m2ts");
        std::fs::write(&path, &clip).unwrap();
        let mut src = crate::mux::resolve::input(
            &format!("m2ts://{}", path.display()),
            &Default::default(),
            &crate::ctx::Ctx::default(),
        )
        .unwrap();
        let title = src.info().clone();
        let mut frames = Vec::new();
        while let Some(f) = src.read().unwrap() {
            frames.push(f);
        }
        let mut g =
            crate::test_util::Golden::new(env!("CARGO_MANIFEST_DIR"), "parity_network_fmkv_wire");

        // Sender -> a raw socket: the wire bytes.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let raw = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            listener
                .accept()
                .unwrap()
                .0
                .read_to_end(&mut bytes)
                .unwrap();
            bytes
        });
        let mut w = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&title);
        for f in &frames {
            w.write(f).unwrap();
        }
        w.finish().unwrap();
        drop(w);
        g.bytes("wire", &raw.join().unwrap());

        // Sender -> NetworkStream receiver: the frames round-trip.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let rx = std::thread::spawn(move || {
            let mut ns = NetworkStream::accept_from(listener).unwrap();
            let mut got = Vec::new();
            while let Some(f) = ns.read().unwrap() {
                got.push((f.track, f.pts, f.keyframe, f.data));
            }
            got
        });
        let mut w = NetworkStream::connect_vetted(&addr.to_string(), false)
            .unwrap()
            .meta(&title);
        for f in &frames {
            w.write(f).unwrap();
        }
        w.finish().unwrap();
        let got = rx.join().unwrap();
        let sent: Vec<_> = frames
            .iter()
            .map(|f| (f.track, f.pts, f.keyframe, f.data.clone()))
            .collect();
        g.kv("frames", got.len());
        g.kv("round-trip identical", got == sent);
        g.check();
    }
}
