# rust_net

Async TCP networking built on top of `rust_io`. Requires Linux (epoll).

Mirrors the TCP slice of Chromium's `net/` socket stack, layered the same way.
Application code normally uses the top two; the lower layers are there when you
need finer control.

```
SocketPosix        raw non-blocking fd + epoll-driven connect/read/write/accept
   ↑
TcpSocket          adds TCP socket options (SO_REUSEADDR, TCP_NODELAY)
   ↑
TcpClientSocket    open + client defaults + connect — the connected-stream handle
TcpServerSocket    open + server defaults + bind + listen — accept → TcpClientSocket
```

| Type | Chromium | Use it when |
|------|----------|-------------|
| `TcpClientSocket` | `net::TCPClientSocket` | Connect out and read/write a stream |
| `TcpServerSocket` | `net::TCPServerSocket` | Listen + accept connections |
| `TcpSocket` | `net::TCPSocket` | Set TCP options yourself / build a custom flow |
| `SocketPosix` | `net::SocketPosix` | The raw fd primitive (rarely needed) |

All methods that touch epoll **must be called from the IO thread**, and the
socket must be kept alive until its callbacks fire — `IoTaskRunner` holds only
`Weak` references to watchers.

> Naming follows Rust convention (`Tcp`, not `TCP`): acronyms are treated as one
> word, which clippy's `upper_case_acronyms` lint enforces.

## Client

`connect` opens the fd, applies client defaults (`TCP_NODELAY`), and connects.

```rust
use rust_net::TcpClientSocket;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;
use std::sync::Arc;

let io     = IoTaskRunner::new();
let client = Arc::new(TcpClientSocket::new());   // keep alive outside the closure
let c      = Arc::clone(&client);

io.post_task(Box::new(move || {
    let c2 = Arc::clone(&c);
    c.connect(addr, move |result| {
        result.unwrap();
        c2.write(b"hello".to_vec(), |_| {});
        c2.read(4096, |r| println!("received: {:?}", r.unwrap()));
    });
}));
```

## Server

`listen` opens the fd, sets `SO_REUSEADDR`, binds, and listens. Bind to `addr:0`
and read `local_addr()` for the kernel-assigned port. `accept` is one-shot; each
peer arrives as a connected `TcpClientSocket`.

```rust
use rust_net::TcpServerSocket;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;
use std::sync::Arc;

let io     = IoTaskRunner::new();
let server = Arc::new(TcpServerSocket::new());   // keep alive outside the closure
let s      = Arc::clone(&server);

io.post_task(Box::new(move || {
    s.listen("127.0.0.1:0".parse().unwrap(), 128).unwrap();
    let addr = s.local_addr().unwrap();

    s.accept(move |result| {
        let peer = result.unwrap();              // TcpClientSocket
        peer.write(b"hello".to_vec(), |_| {});
        // call accept() again here to keep accepting
    });
}));
```

## Operations

`TcpClientSocket`: `connect(addr, cb)`, `read(len, cb)`, `read_if_ready(cb)`,
`write(buf, cb)`, `local_addr()`, `disconnect()`,
`from_connected(TcpSocket)`.

`TcpServerSocket`: `listen(addr, backlog)`, `accept(cb)`, `local_addr()`.

`TcpSocket`: the above plus the option setters `set_default_options_for_server()`,
`set_default_options_for_client()`, `set_reuse_addr(bool)`, `set_no_delay(bool)`.

`SocketPosix` (low-level): `open` / `connect` / `read` / `read_if_ready` /
`write` / `bind` / `listen` / `accept` / `local_addr` / `close`. `bind` here is
the bare `bind(2)` — TCP options live in `TcpSocket`.

## StreamSocket

`StreamSocket` (Chromium's `net::StreamSocket`) is the trait abstracting a
connected, reliable byte stream — `read` / `write` / `disconnect` with boxed
callbacks. `TcpClientSocket` implements it, and so does `TlsClientSocket` (the
`tls` feature), so a higher layer (e.g. HTTP) written against `dyn StreamSocket`
runs unchanged over plaintext and TLS.

## TLS (`tls` feature)

The optional `tls` feature adds `TlsClientSocket` — async TLS over any
`StreamSocket`, using [`rustls`](https://github.com/rustls/rustls)'s **sans-IO**
core. Port of the client side of Chromium's `net::SSLClientSocket`. rustls does
no I/O itself; `TlsClientSocket` pumps its bytes through the transport's
callbacks on the IO thread:

```
plaintext write → conn.writer() encrypts → write_tls → transport.write
plaintext read  ← conn.reader() decrypts ← read_tls  ← transport.read
handshake       → ping-pong of the two until is_handshaking() == false
```

`TlsClientSocket` also implements `StreamSocket`, so after the handshake you
read/write plaintext through the same trait an HTTP layer uses for plaintext TCP.

```rust
use rust_net::{StreamSocket, TcpClientSocket, TlsClientSocket};
use rustls::pki_types::ServerName;
use std::sync::Arc;

// Built with the ring backend and no default provider — install one once.
let _ = rustls::crypto::ring::default_provider().install_default();
let mut roots = rustls::RootCertStore::empty();
roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
let config = Arc::new(
    rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth(),
);

// On the IO thread, transport already TCP-connected:
let name = ServerName::try_from("example.com").unwrap().to_owned();
let tls = TlsClientSocket::new(transport, config, name).unwrap(); // Arc<TlsClientSocket>
tls.handshake(Box::new(move |r| {
    r.unwrap();
    tls_handle.write(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n".to_vec(), Box::new(|_| {}));
}));
```

`server_name` must be the host name (not an IP) — it drives SNI and certificate
verification. Keep the `TlsClientSocket` and its transport alive until callbacks
fire.

`TlsClientSocket`: `new(transport, config, server_name) -> io::Result<Arc<Self>>`,
`handshake(cb)`, and the `StreamSocket` methods (`read` / `write` / `disconnect`).

### read vs. read_if_ready

- `read(len, cb)` — the socket owns the buffer and delivers the bytes.
- `read_if_ready(cb)` — the socket signals readability; the caller does the read.
  The per-operation (non-persistent fd watch) pattern from `rust_io`.

## Examples

```bash
cargo run --example tcp_echo                      # TcpServerSocket + TcpClientSocket echo, one IO thread
cargo run --example socket_posix                  # low-level SocketPosix: connect+write+read, ReadIfReady, streaming
cargo run --features tls --example https_get -- example.com   # HTTPS GET over TcpClientSocket + TlsClientSocket
```

```bash
cargo test --features tls           # includes the offline TLS round-trip test (rcgen self-signed cert)
```
