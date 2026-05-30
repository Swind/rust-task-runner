//! Minimal HTTPS GET over the async TCP + TLS stack (Linux only).
//!
//! Resolves a host, opens a `TcpClientSocket`, wraps it in a `TlsClientSocket`,
//! does the TLS handshake, sends a tiny HTTP/1.0 request, and prints the
//! response head. Demonstrates that the HTTP bytes don't care whether the
//! `StreamSocket` underneath is plaintext or TLS.
//!
//! Usage:
//!   cargo run -p rust_net --features tls --example https_get -- example.com

fn main() {
    #[cfg(target_os = "linux")]
    linux::run();
    #[cfg(not(target_os = "linux"))]
    eprintln!("This example requires Linux (epoll + eventfd).");
}

#[cfg(target_os = "linux")]
mod linux {
    use rust_io::IoTaskRunner;
    use rust_net::TlsClientSocket;
    use rust_net::{StreamSocket, TcpClientSocket};
    use rust_task::TaskRunner;
    use rustls::pki_types::ServerName;
    use std::net::ToSocketAddrs;
    use std::sync::{Arc, Barrier, Mutex};

    pub fn run() {
        let host = std::env::args().nth(1).unwrap_or_else(|| "example.com".to_string());

        // Install the ring crypto provider (we built rustls without a default).
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Resolve host:443 up front (blocking is fine before we start the loop).
        let addr = (host.as_str(), 443u16)
            .to_socket_addrs()
            .expect("DNS resolution failed")
            .next()
            .expect("no address for host");

        // Trust the OS-independent Mozilla root set.
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = Arc::new(
            rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth(),
        );

        let request = format!(
            "GET / HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\nUser-Agent: rust_tls\r\n\r\n"
        );

        let io = IoTaskRunner::new();
        let tcp = Arc::new(TcpClientSocket::new());
        // Slot keeps the TLS socket alive across the callback chain.
        let tls_slot: Arc<Mutex<Option<Arc<TlsClientSocket>>>> = Arc::new(Mutex::new(None));
        let response = Arc::new(Mutex::new(Vec::<u8>::new()));
        let done = Arc::new(Barrier::new(2));

        let t = Arc::clone(&tcp);
        let slot = Arc::clone(&tls_slot);
        let resp = Arc::clone(&response);
        let d = Arc::clone(&done);
        io.post_task(Box::new(move || {
            println!("connecting to {host} ({addr})...");
            let t_inner = Arc::clone(&t);
            t.connect(addr, move |result| {
                result.expect("tcp connect failed");
                println!("TCP connected; starting TLS handshake...");

                let name = ServerName::try_from(host.as_str()).unwrap().to_owned();
                let tls = TlsClientSocket::new(t_inner, config, name).expect("tls construct");
                *slot.lock().unwrap() = Some(Arc::clone(&tls));

                tls.handshake(Box::new(move |result| {
                    result.expect("handshake failed");
                    println!("TLS handshake done; sending request\n");

                    let writer = slot.lock().unwrap().clone().unwrap();
                    let reader = slot.lock().unwrap().clone().unwrap();
                    writer.write(
                        request.into_bytes(),
                        Box::new(move |w| {
                            w.expect("write failed");
                            read_until_eof(reader, resp, d);
                        }),
                    );
                }));
            });
        }));

        done.wait();
        io.shutdown();

        // Print the response head (up to the end of headers).
        let bytes = response.lock().unwrap();
        let text = String::from_utf8_lossy(&bytes);
        let head = text.split("\r\n\r\n").next().unwrap_or("");
        println!("── response head ──\n{head}");
        println!("\n({} bytes received total)", bytes.len());
    }

    /// Read repeatedly until the peer closes (empty read), accumulating bytes.
    fn read_until_eof(tls: Arc<TlsClientSocket>, out: Arc<Mutex<Vec<u8>>>, done: Arc<Barrier>) {
        let next = Arc::clone(&tls);
        tls.read(
            16 * 1024,
            Box::new(move |result| match result {
                Ok(data) if data.is_empty() => {
                    done.wait(); // clean EOF (close_notify)
                }
                Ok(data) => {
                    out.lock().unwrap().extend_from_slice(&data);
                    read_until_eof(next, out, done);
                }
                Err(e) => {
                    eprintln!("read error: {e}");
                    done.wait();
                }
            }),
        );
    }
}
