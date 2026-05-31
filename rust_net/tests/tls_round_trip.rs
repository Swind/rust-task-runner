//! End-to-end TLS test, fully offline.
//!
//! A blocking std::net + rustls echo server runs on a helper thread with a
//! freshly generated self-signed cert. The client side uses our async
//! `TcpClientSocket` + `TlsClientSocket` on an `IoTaskRunner`, trusting that
//! cert. This exercises the real handshake and the read/write pumps.

#![cfg(all(target_os = "linux", feature = "tls"))]

use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::sync::{Arc, Barrier, Mutex};

use rust_io::IoTaskRunner;
use rust_net::{StreamSocket, TcpClientSocket};
use rust_task::TaskRunner;
use rustls::pki_types::ServerName;

#[test]
fn tls_echo_round_trip() {
    // One shared crypto provider for both client and server config builders.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Self-signed cert for "localhost".
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = issued.cert.der().clone();
    let key_der =
        rustls::pki_types::PrivateKeyDer::try_from(issued.key_pair.serialize_der()).unwrap();

    // ── blocking TLS echo server ────────────────────────────────────────────
    let server_config = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().unwrap();
        let mut conn = rustls::ServerConnection::new(server_config).unwrap();
        let mut tls = rustls::Stream::new(&mut conn, &mut tcp); // handshakes lazily
        let mut buf = [0u8; 64];
        let n = tls.read(&mut buf).unwrap();
        tls.write_all(&buf[..n]).unwrap();
        tls.flush().unwrap();
    });

    // ── async client: TCP connect → TLS handshake → write → read ────────────
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).unwrap();
    let client_config = Arc::new(
        rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth(),
    );

    let io = IoTaskRunner::new();
    let tcp = Arc::new(TcpClientSocket::new());
    // Keep the TLS socket alive across callbacks via a shared slot.
    let tls_slot: Arc<Mutex<Option<Arc<rust_net::TlsClientSocket>>>> = Arc::new(Mutex::new(None));
    let received = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(2));

    let t = Arc::clone(&tcp);
    let slot = Arc::clone(&tls_slot);
    let recv = Arc::clone(&received);
    let b = Arc::clone(&barrier);
    io.post_task(Box::new(move || {
        let t_inner = Arc::clone(&t);
        t.connect(addr, move |result| {
            result.expect("tcp connect failed");

            let name = ServerName::try_from("localhost").unwrap().to_owned();
            let tls = rust_net::TlsClientSocket::new(t_inner, client_config, name)
                .expect("tls construct failed");
            *slot.lock().unwrap() = Some(Arc::clone(&tls));

            // Drive everything through the slot's handle so we never move `tls`
            // (which `handshake` is borrowing for the call itself).
            tls.handshake(Box::new(move |result| {
                result.expect("handshake failed");

                let tls_w = slot.lock().unwrap().clone().unwrap();
                let tls_r = slot.lock().unwrap().clone().unwrap();
                tls_w.write(
                    b"hello tls".to_vec(),
                    Box::new(move |w| {
                        w.expect("write failed");
                        tls_r.read(
                            64,
                            Box::new(move |r| {
                                *recv.lock().unwrap() = r.expect("read failed");
                                b.wait();
                            }),
                        );
                    }),
                );
            }));
        });
    }));

    barrier.wait();
    io.shutdown();
    assert_eq!(*received.lock().unwrap(), b"hello tls");
}

/// Transfer a payload several TLS records long, proving `feed_one` reassembles
/// the ciphertext stream correctly.
///
/// Regression test: `feed_one` used to call `read_tls` once per transport chunk
/// and ignore how many bytes it consumed. `read_tls` only takes what its
/// deframer buffer can hold per call, so once a chunk straddled a record
/// boundary the leftover bytes were dropped, desyncing the stream into a
/// `DecryptError`. A single small record (like the echo test above) never hit
/// it; a multi-record transfer does.
#[test]
fn tls_large_multi_record_transfer() {
    // ~128 KiB ≫ the 16 KiB TLS record limit, so the payload spans many records
    // and transport reads land mid-record.
    const N: usize = 128 * 1024;
    let expected: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();

    let _ = rustls::crypto::ring::default_provider().install_default();

    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = issued.cert.der().clone();
    let key_der =
        rustls::pki_types::PrivateKeyDer::try_from(issued.key_pair.serialize_der()).unwrap();

    // ── blocking TLS server: read a "go", then stream the big payload ────────
    let server_config = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_payload = expected.clone();
    std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().unwrap();
        let mut conn = rustls::ServerConnection::new(server_config).unwrap();
        let mut tls = rustls::Stream::new(&mut conn, &mut tcp);
        let mut buf = [0u8; 8];
        let _ = tls.read(&mut buf).unwrap();
        tls.write_all(&server_payload).unwrap();
        tls.flush().unwrap();
    });

    // ── async client: connect → handshake → write "go" → read N bytes ───────
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).unwrap();
    let client_config = Arc::new(
        rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth(),
    );

    let io = IoTaskRunner::new();
    let tcp = Arc::new(TcpClientSocket::new());
    let tls_slot: Arc<Mutex<Option<Arc<rust_net::TlsClientSocket>>>> = Arc::new(Mutex::new(None));
    let received = Arc::new(Mutex::new(Vec::with_capacity(N)));
    let barrier = Arc::new(Barrier::new(2));

    /// Accumulate exactly `need` bytes, following short reads, then signal.
    fn read_until(
        stream: Arc<dyn StreamSocket>,
        need: usize,
        got: Arc<Mutex<Vec<u8>>>,
        done: Arc<Barrier>,
    ) {
        let remaining = need - got.lock().unwrap().len();
        let next = Arc::clone(&stream);
        stream.read(
            remaining,
            Box::new(move |r| {
                let data = r.expect("read failed");
                assert!(!data.is_empty(), "unexpected EOF before full payload");
                let len = {
                    let mut g = got.lock().unwrap();
                    g.extend_from_slice(&data);
                    g.len()
                };
                if len >= need {
                    done.wait();
                } else {
                    read_until(next, need, got, done);
                }
            }),
        );
    }

    let t = Arc::clone(&tcp);
    let slot = Arc::clone(&tls_slot);
    let recv = Arc::clone(&received);
    let b = Arc::clone(&barrier);
    io.post_task(Box::new(move || {
        let t_inner = Arc::clone(&t);
        t.connect(addr, move |result| {
            result.expect("tcp connect failed");
            let name = ServerName::try_from("localhost").unwrap().to_owned();
            let tls = rust_net::TlsClientSocket::new(t_inner, client_config, name)
                .expect("tls construct failed");
            *slot.lock().unwrap() = Some(Arc::clone(&tls));

            tls.handshake(Box::new(move |result| {
                result.expect("handshake failed");
                let tls_w = slot.lock().unwrap().clone().unwrap();
                let tls_r = slot.lock().unwrap().clone().unwrap();
                tls_w.write(
                    b"go".to_vec(),
                    Box::new(move |w| {
                        w.expect("write failed");
                        read_until(tls_r, N, recv, b);
                    }),
                );
            }));
        });
    }));

    barrier.wait();
    io.shutdown();
    let got = received.lock().unwrap();
    assert_eq!(got.len(), N, "received length mismatch");
    assert!(*got == expected, "received payload did not match");
}
