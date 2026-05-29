use std::io;

/// Callback delivering the bytes read (or an error).
pub type ReadCallback = Box<dyn FnOnce(io::Result<Vec<u8>>) + Send>;
/// Callback delivering the number of bytes written (or an error).
pub type WriteCallback = Box<dyn FnOnce(io::Result<usize>) + Send>;

/// A connected, reliable, bidirectional byte stream.
///
/// This is the abstraction that lets higher layers stay agnostic about what
/// sits underneath. A plaintext TCP connection ([`crate::TcpClientSocket`]) and
/// a future TLS connection both present the same read/write surface, so an HTTP
/// layer written against `dyn StreamSocket` works over `http://` and `https://`
/// without change. Mirrors Chromium's `net::StreamSocket`.
///
/// Reads and writes are byte-oriented: there are no message boundaries, so a
/// single `read` may return fewer bytes than requested and a single `write` may
/// accept only part of the buffer (the returned count says how much).
///
/// Like the concrete sockets it abstracts, every method **must be called from
/// the IO thread**, and the implementor must be kept alive until its callbacks
/// fire.
pub trait StreamSocket: Send + Sync {
    /// Read up to `len` bytes; the callback receives whatever arrived.
    fn read(&self, len: usize, cb: ReadCallback);

    /// Write `buf`; the callback receives the number of bytes accepted.
    fn write(&self, buf: Vec<u8>, cb: WriteCallback);

    /// Close the stream and cancel pending operations.
    fn disconnect(&self);
}
