# `src/io/sink/socket.rs` — socket sink design notes

## `SocketSink` (TCP)

Wraps a `TcpStream` in a 1 MiB `BufWriter`. Constructor tunes `SO_SNDBUF`
to a caller hint when provided. `finish()` flushes the buffer then
`shutdown(Write)`s the socket so the peer sees clean end-of-stream.

`TCP_BUF_CAPACITY` (1 MiB) matches the typical kernel send-buffer
ceiling and keeps small-write amplification from containers (TS =
188-byte packets, fMP4 fragment headers = ~100 bytes) from translating
into syscall storms.

The inner `TcpStream` is kept as a second clone (`shutdown_handle`) so
`finish()` can call `shutdown(Write)` after flushing the buffer — the
buffered writer doesn't expose the socket directly.

`set_nodelay(true)` keeps small writes (TS packet trains, fMP4 moof
headers) from sitting in Nagle's algorithm; `BufWriter` already absorbs
syscall overhead. This is a latency hint, not a correctness requirement
— a rejecting platform must not fail connect.

`shutdown(Write)` signals clean EOF to the peer. Errors from it are
non-fatal — the connection may have already been torn down by the peer
— but they are surfaced so callers can log.

## `UdpSocketSink` (UDP)

Wraps a connected `UdpSocket`. Each `write` call emits exactly one
datagram — the caller is responsible for packetizing to a reasonable
MTU (188 × 7 = 1316 bytes for MPEG-TS-over-UDP is the conventional
choice). `finish()` is a no-op; UDP has no end-of-stream marker, and
closing the socket happens on drop.

`connect()` resolves the peer first so the local bind matches its
address family: binding `0.0.0.0:0` (IPv4) then connecting to an IPv6
peer fails with `EAFNOSUPPORT`. Binding to the matching wildcard / any
port lets the kernel pick an ephemeral source port and the source IP
at first send. `connect` on a UDP socket doesn't open a connection —
it just fixes the peer address so subsequent `send` calls don't need
to repeat it, and so receive-side filtering rejects packets from other
sources.

## Trait wiring

Both types implement [`SequentialSink`] explicitly so their `finish()`
dispatches correctly through a `dyn SequentialSink` trait object (the
`SocketSink` override drains the buffer and `shutdown(Write)`s; the
`UdpSocketSink` override flushes only). Neither implements `Seek`, so
neither satisfies `RandomAccessSink` — using one with `MkvMux` is a
compile error, which is the design intent.

## Platform `SO_SNDBUF` tuning

std doesn't expose `SO_SNDBUF`; the code drops to libc on Linux + macOS.
Other targets silently ignore the hint — the socket still works, the
kernel just picks its own size. Non-Linux-non-macOS targets aren't in
Cargo.toml's libc dep list, so failing the connect there would be wrong
— it's a hint, not a guarantee, so there is intentionally no
"sndbuf applied" signal on those platforms.

## Test notes

- `finish_signals_eof_to_peer`: `SocketSink::finish` must call
  `shutdown(Write)` so the peer's `read_to_end` observes EOF and
  returns instead of blocking forever on a half-open socket. Mutation:
  replacing the `shutdown(Write)` line with `Ok(())` makes the accept
  thread hang and the join times out.
- `udp_write_is_one_datagram_per_call`: UDP `write` must emit one
  datagram per call carrying exactly the bytes passed — no buffering,
  no coalescing. Mutation: adding a `BufWriter` to `UdpSocketSink`
  (explicitly forbidden) would merge writes into one datagram and the
  second `recv` would time out.
- `udp_finish_is_noop_ok`: UDP `finish` is a documented no-op since
  there is no EOF marker for UDP; it must not error and must not
  affect prior datagrams. Mutation: if `finish` tried to `shutdown`
  the UDP socket it could error or close it prematurely.
