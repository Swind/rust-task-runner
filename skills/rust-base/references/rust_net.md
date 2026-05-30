# rust_net — async TCP socket

Linux-only. Ports the TCP slice of Chromium's `net/` socket stack. Built on
`rust_io` — read both the `rust_task` page in `SKILL.md` and
[`rust_io.md`](rust_io.md) first, because the same two rules apply at every layer
and dominate correct usage:

1. **Every operation runs on the IO thread.** Drive sockets from inside an
   `IoTaskRunner::post_task` closure (or from another socket callback, which
   already runs there).
2. **Keep the socket object alive until all callbacks fire.** `IoTaskRunner`
   holds only `Weak` references to watchers, so if the last owner drops, pending
   callbacks are silently skipped. Store it in a struct, or move it into the
   closure chain.

## The layer stack

`rust_net` mirrors Chromium's `net/` layering — pick the layer that matches your
job; the higher ones are what application code normally wants:

```
SocketPosix        raw non-blocking fd + epoll-driven connect/read/write/accept
   ↑
TcpSocket          adds TCP socket options (SO_REUSEADDR, TCP_NODELAY)
   ↑
TcpClientSocket    open + client defaults + connect; the connected-stream handle
TcpServerSocket    open + server defaults + bind + listen; accept → TcpClientSocket
```

| Type | Chromium | Use it when |
|------|----------|-------------|
| `TcpClientSocket` | `net::TCPClientSocket` | You want to **connect out** and read/write a stream |
| `TcpServerSocket` | `net::TCPServerSocket` | You want to **listen + accept** connections |
| `TcpSocket` | `net::TCPSocket` | You need to set TCP options yourself, or build a custom flow |
| `SocketPosix` | `net::SocketPosix` | You need the raw fd primitive (rarely) |

`TcpClientSocket` applies `TCP_NODELAY` on connect; `TcpServerSocket` applies
`SO_REUSEADDR` on listen and `TCP_NODELAY` on each accepted peer. None of these
types are `Arc` — you own the value directly and keep *it* alive (it owns the
`Arc<SocketPosix>` inside).

## The `StreamSocket` trait

`StreamSocket` (Chromium's `net::StreamSocket`) abstracts "a connected, reliable,
bidirectional byte stream you can read/write":

```rust
pub trait StreamSocket: Send + Sync {
    fn read(&self, len: usize, cb: ReadCallback);     // Box<dyn FnOnce(io::Result<Vec<u8>>) + Send>
    fn write(&self, buf: Vec<u8>, cb: WriteCallback); // Box<dyn FnOnce(io::Result<usize>) + Send>
    fn disconnect(&self);
}
```

`TcpClientSocket` implements it (the plaintext base case). The point of the
abstraction is that a TLS socket implements it too, so a layer written against
`dyn StreamSocket` (e.g. HTTP) works over both `http://` and `https://` — see
the `tls` feature in [`rust_tls.md`](rust_tls.md). Note the trait uses **boxed** callbacks (it must be
object-safe), whereas the inherent `TcpClientSocket` methods take `impl FnOnce`;
a boxed `FnOnce` satisfies that bound, so they interoperate freely.

## Client — TcpClientSocket

`connect` does open + `set_default_options_for_client()` + connect in one call.

```rust
use rust_net::TcpClientSocket;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;
use std::sync::Arc;

let io     = IoTaskRunner::new();
let client = Arc::new(TcpClientSocket::new());   // keep alive past the closure
let c      = Arc::clone(&client);

io.post_task(Box::new(move || {
    let c2 = Arc::clone(&c);
    c.connect(addr, move |result| {
        result.unwrap();
        c2.write(b"hello".to_vec(), |_| {});
        c2.read(4096, |r| println!("received {:?}", r.unwrap()));
    });
}));
```

Methods: `connect(addr, cb)`, `read(len, cb)`, `read_if_ready(cb)`,
`write(buf, cb)`, `local_addr()`, `disconnect()`.
`TcpClientSocket::from_connected(TcpSocket)` adopts an already-connected socket
(this is what `accept` hands you).

## Server — TcpServerSocket

`listen(addr, backlog)` does open + `SO_REUSEADDR` + bind + listen. Bind to
`addr:0` and read back `local_addr()` to discover the kernel-assigned port.
`accept` is **one-shot** — call it again inside its own callback to keep
accepting; each peer arrives as a connected `TcpClientSocket`.

```rust
use rust_net::TcpServerSocket;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;
use std::sync::Arc;

let io     = IoTaskRunner::new();
let server = Arc::new(TcpServerSocket::new());   // keep alive
let s      = Arc::clone(&server);

io.post_task(Box::new(move || {
    s.listen("127.0.0.1:0".parse().unwrap(), 128).unwrap();
    let addr = s.local_addr().unwrap();          // ephemeral port now known

    fn accept_loop(s: Arc<TcpServerSocket>) {
        let again = Arc::clone(&s);
        s.accept(move |result| {
            let peer = result.unwrap();           // TcpClientSocket
            peer.write(b"hello".to_vec(), |_| {});
            // peer must outlive its callbacks — store it somewhere if you keep using it
            accept_loop(again);                    // re-arm for the next connection
        });
    }
    accept_loop(s);
}));
```

## Lower layers

**`TcpSocket`** — use when you want to drive the options/flow yourself:
`new()` / `from_connected_fd(fd)`, `open(addr)`, `set_default_options_for_server()`,
`set_default_options_for_client()`, `set_reuse_addr(bool)`, `set_no_delay(bool)`,
`bind`/`listen`/`connect`/`read`/`read_if_ready`/`write`/`accept` (→ `TcpSocket`),
`local_addr()`, `close()`.

**`SocketPosix`** — the raw primitive. `new()` / `from_fd(fd)` return
`Arc<Self>`; `bind` here is the bare `bind(2)` with **no** `SO_REUSEADDR` (that
moved up to `TcpSocket`). Most methods take `&self`; `connect`/`read`/`write`/
`accept` take `self: &Arc<Self>` so they can register the watcher. Reach for this
only when the typed layers don't fit.

### read vs. read_if_ready (all layers)

- `read(len, cb)` — the socket owns the buffer and hands you the bytes.
- `read_if_ready(cb)` — the socket only signals readability; **you** then issue
  the actual `read`. This is the per-operation (non-persistent fd watch) pattern
  from `rust_io`; use it to control buffer allocation or readiness signaling.

## Try it

```bash
cd rust_net
cargo run --example tcp_echo       # TcpServerSocket + TcpClientSocket echo, one IO thread
cargo run --example socket_posix   # low-level SocketPosix: connect+write+read, read_if_ready
cargo test                         # includes a TcpClientSocket↔TcpServerSocket round trip
```

## Chromium correspondence

| rust_net | Chromium |
|----------|----------|
| `SocketPosix` | `net::SocketPosix` |
| `TcpSocket` | `net::TCPSocket` / `TCPSocketPosix` |
| `TcpClientSocket` | `net::TCPClientSocket` |
| `TcpServerSocket` | `net::TCPServerSocket` |
| `read_if_ready` | `SocketPosix::ReadIfReady` (non-persistent fd watch) |

> Naming: Rust API guidelines treat acronyms as one word (`TcpSocket`, not
> `TCPSocket`) — clippy's `upper_case_acronyms` enforces this, so the Chromium
> `TCP*` spelling becomes `Tcp*` here.
