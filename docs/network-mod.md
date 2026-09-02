# src/mux/network.rs — notes

## `is_blocked_ip` / SSRF vetting rationale

`validate_network_target` (in autorip) vets the host once at
settings-save time, but the raw hostname is re-resolved here at rip
time — a DNS-rebinding attacker can flip a previously-public name to
`127.0.0.1` / `10.x` / `169.254.x` in that window. Re-checking the
actually-resolved address at connect time closes that TOCTOU.

`resolve_allowed_addr` resolves `addr` (host:port) and returns the
first socket address whose IP is NOT `is_blocked_ip`. It errors with
`crate::error::Error::NetworkAddrBlocked` if every resolved address is
blocked, or propagates the resolver's own error if resolution fails.
The returned `SocketAddr` carries a vetted IP literal, so the
subsequent `TcpStream::connect` cannot be re-pointed by a second DNS
lookup (it connects to the IP we vetted, not the name).

## `accept_from_rejects_stream_without_fmkv_header` — flake fix

FLAKE FIXED, not the behaviour under test: this test used to
`shutdown(Shutdown::Both)` the instant the bytes were written. Closing
the READ half while the server had not yet read makes the kernel answer
the server's in-flight data with an RST, so `accept_from` came back
`ConnectionReset` instead of the `InvalidInput` this asserts — rarely
when run alone, reproducibly under the loaded concurrent suite, where
the server thread is descheduled long enough for the race to open. A
logging/protocol assertion that fails at random teaches the next person
to re-run until green, which is how a real regression gets waved
through.

Half-closing (`Shutdown::Write`) delivers the same EOF the test needs
while leaving the read half open, and blocking on a read until the
server drops its end keeps the socket alive for as long as the server
is looking at it. The port was already ephemeral (`:0`), so it was
never a port collision.
