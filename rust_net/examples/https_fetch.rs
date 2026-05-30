//! Fetch an HTTPS page and print its body — a small real application built on
//! `TcpClientSocket` + `TlsClientSocket` (Linux only).
//!
//! It parses an `https://host[:port][/path]` URL, opens a TCP connection, wraps
//! it in TLS, sends an HTTP/1.0 `GET` (with `Connection: close` so the server
//! ends the body by closing the stream), reads the whole response, then splits
//! the head from the body and prints the status line + the body.
//!
//! Usage:
//!   cargo run -p rust_net --features tls --example https_fetch -- https://example.com/
//!   cargo run -p rust_net --features tls --example https_fetch -- https://www.rust-lang.org/

fn main() {
    #[cfg(target_os = "linux")]
    linux::run();
    #[cfg(not(target_os = "linux"))]
    eprintln!("This example requires Linux (epoll + eventfd).");
}

#[cfg(target_os = "linux")]
mod linux {
    use rust_io::IoTaskRunner;
    use rust_net::{StreamSocket, TcpClientSocket, TlsClientSocket};
    use rust_task::TaskRunner;
    use rustls::pki_types::ServerName;
    use std::net::ToSocketAddrs;
    use std::sync::{Arc, Barrier, Mutex};

    /// Split `https://host[:port][/path]` into (host, port, path).
    fn parse_url(url: &str) -> (String, u16, String) {
        let rest = url.strip_prefix("https://").unwrap_or(url);
        let (authority, path) = match rest.split_once('/') {
            Some((a, p)) => (a.to_string(), format!("/{p}")),
            None => (rest.to_string(), "/".to_string()),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().unwrap_or(443)),
            None => (authority, 443),
        };
        (host, port, path)
    }

    pub fn run() {
        let url = std::env::args().nth(1).unwrap_or_else(|| "https://example.com/".to_string());
        let (host, port, path) = parse_url(&url);

        // rustls is built without a default provider — install ring once.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let addr = (host.as_str(), port)
            .to_socket_addrs()
            .expect("DNS resolution failed")
            .next()
            .expect("no address for host");

        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = Arc::new(
            rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth(),
        );

        // HTTP/1.0 + Connection: close → the server closes the stream after the
        // body, so "read until EOF" gives us the complete response.
        let request = format!(
            "GET {path} HTTP/1.0\r\nHost: {host}\r\nUser-Agent: rust_net-https_fetch\r\nAccept: */*\r\nConnection: close\r\n\r\n"
        );

        let io = IoTaskRunner::new();
        let tcp = Arc::new(TcpClientSocket::new());
        // Slot keeps the TLS socket alive across the whole callback chain.
        let tls_slot: Arc<Mutex<Option<Arc<TlsClientSocket>>>> = Arc::new(Mutex::new(None));
        let response = Arc::new(Mutex::new(Vec::<u8>::new()));
        let done = Arc::new(Barrier::new(2));

        eprintln!("GET {url}  →  {addr}");

        let t = Arc::clone(&tcp);
        let slot = Arc::clone(&tls_slot);
        let resp = Arc::clone(&response);
        let d = Arc::clone(&done);
        io.post_task(Box::new(move || {
            let t_inner = Arc::clone(&t);
            t.connect(addr, move |result| {
                result.expect("tcp connect failed");

                let name = ServerName::try_from(host).unwrap();
                let tls = TlsClientSocket::new(t_inner, config, name).expect("tls construct");
                *slot.lock().unwrap() = Some(Arc::clone(&tls));

                tls.handshake(Box::new(move |result| {
                    result.expect("tls handshake failed");

                    let writer = slot.lock().unwrap().clone().unwrap();
                    let reader = slot.lock().unwrap().clone().unwrap();
                    writer.write(
                        request.into_bytes(),
                        Box::new(move |w| {
                            w.expect("write failed");
                            read_to_end(reader, resp, d);
                        }),
                    );
                }));
            });
        }));

        done.wait();
        io.shutdown();

        // Split the head (status line + headers) from the body.
        let raw = response.lock().unwrap();
        match find(&raw, b"\r\n\r\n") {
            Some(i) => {
                let head = String::from_utf8_lossy(&raw[..i]);
                let body = &raw[i + 4..];
                let status = head.lines().next().unwrap_or("");
                eprintln!("\n{status}");
                eprintln!("(headers: {} bytes, body: {} bytes)\n", i, body.len());
                // The body is the page content — print it to stdout.
                print!("{}", String::from_utf8_lossy(body));
            }
            None => eprintln!("malformed response ({} bytes, no header terminator)", raw.len()),
        }
    }

    /// Read until the peer closes (empty read = clean EOF), accumulating bytes.
    fn read_to_end(tls: Arc<TlsClientSocket>, out: Arc<Mutex<Vec<u8>>>, done: Arc<Barrier>) {
        let next = Arc::clone(&tls);
        tls.read(
            16 * 1024,
            Box::new(move |result| match result {
                Ok(data) if data.is_empty() => {
                    done.wait();
                }
                Ok(data) => {
                    out.lock().unwrap().extend_from_slice(&data);
                    read_to_end(next, out, done);
                }
                Err(e) => {
                    eprintln!("read error: {e}");
                    done.wait();
                }
            }),
        );
    }

    /// Index of the first occurrence of `needle` in `hay`.
    fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len()).position(|w| w == needle)
    }
}
