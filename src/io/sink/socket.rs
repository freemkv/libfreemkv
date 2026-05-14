//! TCP / UDP socket sinks (sequential-only).
//!
//! [`SocketSink`] wraps a `TcpStream` in a 1 MiB `BufWriter`. Constructor
//! tunes `SO_SNDBUF` to a caller hint when provided. `finish()` flushes
//! the buffer then `shutdown(Write)`s the socket so the peer sees clean
//! end-of-stream.
//!
//! [`UdpSocketSink`] wraps a connected `UdpSocket`. Each `write` call
//! emits exactly one datagram — the caller is responsible for packetizing
//! to a reasonable MTU (188 × 7 = 1316 bytes for MPEG-TS-over-UDP is the
//! conventional choice). `finish()` is a no-op; UDP has no end-of-stream
//! marker.
//!
//! Both types satisfy [`SequentialSink`] via the blanket impl in
//! `super::mod`. Neither implements `Seek`, so neither satisfies
//! [`RandomAccessSink`] — using one with `MkvMux` is a compile error,
//! which is the design intent.
//!
//! [`SequentialSink`]: super::SequentialSink
//! [`RandomAccessSink`]: super::RandomAccessSink

use std::io::{self, BufWriter, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs, UdpSocket};

/// `BufWriter` capacity for [`SocketSink`]. 1 MiB matches the typical
/// kernel send-buffer ceiling and keeps small-write amplification from
/// containers (TS = 188-byte packets, fMP4 fragment headers = ~100 bytes)
/// from translating into syscall storms.
const TCP_BUF_CAPACITY: usize = 1024 * 1024;

/// Sequential-only sink over a TCP connection.
///
/// Wraps a `BufWriter<TcpStream>`; the inner `TcpStream` is kept as a
/// clone so [`finish`](Self::finish) can call `shutdown(Write)` after
/// flushing the buffer (the buffered writer doesn't expose the socket
/// directly).
pub struct SocketSink {
    /// Buffered write half. All payload bytes go through this.
    buf: BufWriter<TcpStream>,
    /// Shutdown handle — clone of the socket inside `buf`. Used only by
    /// `finish()` for `shutdown(Write)`; never read or written through.
    shutdown_handle: TcpStream,
}

impl SocketSink {
    /// Open a TCP connection to `addr` and wrap it for sequential
    /// writing. `sndbuf_bytes`, when present, is forwarded to
    /// `setsockopt(SO_SNDBUF)` as a kernel hint — the OS may clamp it.
    ///
    /// `addr` accepts anything `ToSocketAddrs` does: `"10.0.0.1:1234"`,
    /// `("host", 1234)`, a `SocketAddr`, etc.
    pub fn connect<A: ToSocketAddrs>(addr: A, sndbuf_bytes: Option<usize>) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        // `set_nodelay(true)` keeps small writes (TS packet trains, fMP4
        // moof headers) from sitting in Nagle's algorithm until the buffer
        // fills. The BufWriter already absorbs syscall overhead; Nagle
        // would just add latency without coalescing more.
        stream.set_nodelay(true)?;
        if let Some(n) = sndbuf_bytes {
            set_send_buffer(&stream, n)?;
        }
        let shutdown_handle = stream.try_clone()?;
        Ok(Self {
            buf: BufWriter::with_capacity(TCP_BUF_CAPACITY, stream),
            shutdown_handle,
        })
    }
}

impl Write for SocketSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.buf.flush()
    }
}

impl SocketSink {
    /// Drain the BufWriter and `shutdown(Write)` the underlying socket
    /// so the peer sees a clean EOF.
    ///
    /// Note: [`SequentialSink::finish`](super::SequentialSink::finish)'s
    /// blanket-impl default is a no-op. Trait-object call sites that
    /// need socket shutdown should call this inherent method directly
    /// before dropping the sink, or hold the concrete `SocketSink` type
    /// (typical pattern: each muxer's `finish()` calls the appropriate
    /// inherent close method on its captured concrete sink).
    pub fn finish(&mut self) -> io::Result<()> {
        self.buf.flush()?;
        // `shutdown(Write)` signals clean EOF to the peer. Errors here
        // are non-fatal — the connection may have already been torn down
        // by the peer — but we surface them so callers can log.
        self.shutdown_handle.shutdown(Shutdown::Write)
    }
}

/// Sequential-only sink over a connected UDP socket.
///
/// Each [`write`](Write::write) call sends exactly one datagram. The
/// caller is responsible for splitting payload at packet boundaries —
/// for MPEG-TS this means 7 × 188 = 1316 bytes per datagram, the
/// industry standard for MPEG-TS-over-UDP. No buffering happens here;
/// adding it would silently merge datagrams.
///
/// `finish()` is a no-op: UDP has no end-of-stream marker. Closing the
/// socket happens on drop.
pub struct UdpSocketSink {
    socket: UdpSocket,
}

impl UdpSocketSink {
    /// Bind a local UDP socket to an ephemeral port and `connect` it to
    /// `peer`. `connect` doesn't open a connection — it just fixes the
    /// peer address so subsequent `send` calls don't need to repeat it,
    /// and so receive-side filtering rejects packets from other sources.
    ///
    /// `sndbuf_bytes`, when present, is a hint to `SO_SNDBUF`.
    pub fn connect<A: ToSocketAddrs>(peer: A, sndbuf_bytes: Option<usize>) -> io::Result<Self> {
        // Bind to all-zeros / any port. The kernel picks an ephemeral
        // source port and the source IP at first send.
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect(peer)?;
        if let Some(n) = sndbuf_bytes {
            set_udp_send_buffer(&socket, n)?;
        }
        Ok(Self { socket })
    }
}

impl Write for UdpSocketSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // `send` writes the entire datagram or fails — no partial sends
        // for UDP. Match `Write::write`'s contract by reporting bytes
        // accepted.
        self.socket.send(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl UdpSocketSink {
    /// No-op — UDP has no end-of-stream marker. Provided for parity
    /// with [`SocketSink::finish`] so call sites can treat them uniformly.
    pub fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ── Platform `SO_SNDBUF` tuning ────────────────────────────────────────────
//
// std's `TcpStream` / `UdpSocket` don't expose `SO_SNDBUF`. We drop to
// libc on Linux + macOS (the libc-dep targets in Cargo.toml). On other
// targets the hint is silently ignored — the socket still works, the
// kernel just picks its own send-buffer size.

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn set_send_buffer(stream: &TcpStream, bytes: usize) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    setsockopt_sndbuf(stream.as_raw_fd(), bytes)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn set_udp_send_buffer(socket: &UdpSocket, bytes: usize) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    setsockopt_sndbuf(socket.as_raw_fd(), bytes)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn set_send_buffer(_stream: &TcpStream, _bytes: usize) -> io::Result<()> {
    // Non-Linux-non-macOS targets aren't in Cargo.toml's libc dep list;
    // silently ignore the hint rather than failing the connect. Callers
    // can detect via the lack of an explicit "sndbuf applied" signal
    // (not provided, intentionally — this is a hint, not a guarantee).
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn set_udp_send_buffer(_socket: &UdpSocket, _bytes: usize) -> io::Result<()> {
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn setsockopt_sndbuf(fd: std::os::unix::io::RawFd, bytes: usize) -> io::Result<()> {
    // Clamp into c_int range; SO_SNDBUF takes an `int` argument.
    let want: libc::c_int = bytes.try_into().unwrap_or(libc::c_int::MAX);
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &want as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::{TcpListener, UdpSocket};
    use std::thread;

    /// Bind a listener, accept on a thread, return (listener_addr,
    /// accepted-bytes future via JoinHandle).
    #[test]
    fn socket_sink_round_trips_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            sock.read_to_end(&mut buf).unwrap();
            buf
        });

        let mut sink = SocketSink::connect(addr, Some(256 * 1024)).unwrap();
        // Write enough to overflow the BufWriter at least once, then a
        // tail that lives in the buffer until `finish` flushes.
        let big: Vec<u8> = (0..(2 * TCP_BUF_CAPACITY))
            .map(|i| (i & 0xff) as u8)
            .collect();
        sink.write_all(&big).unwrap();
        sink.write_all(b"tail\n").unwrap();
        sink.finish().unwrap();
        drop(sink);

        let received = accept.join().unwrap();
        assert_eq!(received.len(), big.len() + 5);
        assert_eq!(&received[..big.len()], &big[..]);
        assert_eq!(&received[big.len()..], b"tail\n");
    }

    #[test]
    fn socket_sink_is_sequential_only() {
        // Compile-time assertion via dyn — if this ever started
        // satisfying `RandomAccessSink`, the trait split would be broken.
        fn _assert_seq(_: &mut dyn super::super::SequentialSink) {}
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _accept = thread::spawn(move || {
            let _ = listener.accept();
        });
        let mut sink = SocketSink::connect(addr, None).unwrap();
        _assert_seq(&mut sink);
        // The negative is harder to assert directly (no `is_not<T>`),
        // but `SocketSink` does not impl `Seek`, so it can't unify with
        // `RandomAccessSink`'s super-bound. The Phase 2 blanket impl
        // `impl<T: SequentialSink + Seek> RandomAccessSink for T {}` thus
        // excludes it by construction.
    }

    #[test]
    fn udp_socket_sink_delivers_datagrams() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let addr = receiver.local_addr().unwrap();
        let mut sink = UdpSocketSink::connect(addr, Some(128 * 1024)).unwrap();

        sink.write_all(&[1, 2, 3, 4, 5]).unwrap();
        sink.write_all(&[9, 9, 9]).unwrap();
        sink.finish().unwrap();

        let mut buf = [0u8; 64];
        let n1 = receiver.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n1], &[1, 2, 3, 4, 5]);
        let n2 = receiver.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n2], &[9, 9, 9]);
    }
}
