# rust_net `tls` feature — async TLS / HTTPS

Linux-only. `TlsClientSocket` lives in `rust_net` behind the optional **`tls`**
feature (`cargo ... --features tls`); off by default so a plain TCP user doesn't
compile rustls. It ports the client side of Chromium's `net::SSLClientSocket`:
wraps a connected transport and speaks TLS over it. Read the `rust_task` page in
`SKILL.md` and [`rust_net.md`](rust_net.md) first — `TlsClientSocket` wraps a
`StreamSocket`, so the same two rules still dominate:

1. **Every operation runs on the IO thread.**
2. **Keep the socket alive until callbacks fire** — including both the
   `TlsClientSocket` and its underlying transport.

## How it works

TLS is provided by [`rustls`](https://github.com/rustls/rustls), used through
its **sans-IO** core: rustls does *no* I/O itself, it's just a byte-in/byte-out
state machine. `TlsClientSocket` pumps those bytes through the transport's
existing callback `read`/`write`:

```
plaintext write → conn.writer() encrypts → write_tls → transport.write
plaintext read  ← conn.reader() decrypts ← read_tls  ← transport.read
handshake       → ping-pong of the two until is_handshaking() == false
```

`TlsClientSocket` **also implements `StreamSocket`**, so once the handshake
finishes you read/write plaintext through the same trait an HTTP layer uses for
plaintext TCP. That's the whole point of the `StreamSocket` seam: one HTTP layer,
both `http://` and `https://`.

## Usage

Two phases: build a shared `rustls::ClientConfig` once, then per-connection
**TCP-connect → wrap → handshake → read/write**.

```rust
use rust_net::{StreamSocket, TcpClientSocket, TlsClientSocket};
use rustls::pki_types::ServerName;
use std::sync::Arc;

// Built without a default crypto provider — install ring once per process.
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
    // From here, `tls` is just a StreamSocket carrying plaintext.
    tls_handle.write(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n".to_vec(), Box::new(|_| {}));
}));
```

## API

`TlsClientSocket` (always `Arc<TlsClientSocket>`):
- `new(transport: Arc<dyn StreamSocket>, config: Arc<rustls::ClientConfig>, server_name: ServerName<'static>) -> io::Result<Arc<Self>>`
- `handshake(cb: Box<dyn FnOnce(io::Result<()>) + Send>)` — must complete before app data
- implements `StreamSocket`: `read(len, cb)`, `write(buf, cb)`, `disconnect()`
  (`disconnect` sends `close_notify` then drops the transport)

## Gotchas

- **`server_name` must be the host name, not an IP** — it drives SNI and cert
  verification.
- **Crypto provider**: this crate builds rustls with the `ring` backend and *no*
  default provider, so `ClientConfig::builder()` will panic unless you
  `install_default()` (or use `builder_with_provider`). The examples/tests do this.
- **Lifetime**: an `Arc<TlsClientSocket>` must outlive its callbacks. A common
  pattern is to stash it in an `Arc<Mutex<Option<Arc<TlsClientSocket>>>>` slot and
  pull a clone inside each callback (see the example/test).
- **EOF**: a `read` callback receiving an empty `Vec` means the peer sent
  `close_notify` (clean end of stream), same convention as `TcpClientSocket`.

## Try it

```bash
cd rust_net
cargo run --features tls --example https_get -- example.com   # real HTTPS GET, prints response head
cargo test --features tls                                      # offline: self-signed (rcgen) round trip
```

## Chromium correspondence

| rust_net (`tls`) | Chromium |
|------------------|----------|
| `TlsClientSocket` | `net::SSLClientSocket` |
| rustls `ClientConnection` (sans-IO) | BoringSSL `SSL` driven via a BIO pair |

> Server-side TLS (`net::SSLServerSocket`) isn't implemented yet — it would be a
> symmetric `TlsServerSocket` wrapping an accepted `TcpClientSocket` with a
> `rustls::ServerConnection` (cert + key).
