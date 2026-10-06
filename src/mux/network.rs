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
#[path = "network_tests.rs"]
mod tests;
