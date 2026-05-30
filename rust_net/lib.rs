#[cfg(target_os = "linux")]
pub mod socket_posix;
#[cfg(target_os = "linux")]
pub mod stream_socket;
#[cfg(target_os = "linux")]
pub mod tcp_client_socket;
#[cfg(target_os = "linux")]
pub mod tcp_server_socket;
#[cfg(target_os = "linux")]
pub mod tcp_socket;
#[cfg(all(target_os = "linux", feature = "tls"))]
pub mod tls_client_socket;

#[cfg(target_os = "linux")]
pub use self::socket_posix::SocketPosix;
#[cfg(target_os = "linux")]
pub use self::stream_socket::{ReadCallback, StreamSocket, WriteCallback};
#[cfg(target_os = "linux")]
pub use self::tcp_client_socket::TcpClientSocket;
#[cfg(target_os = "linux")]
pub use self::tcp_server_socket::TcpServerSocket;
#[cfg(target_os = "linux")]
pub use self::tcp_socket::TcpSocket;
#[cfg(all(target_os = "linux", feature = "tls"))]
pub use self::tls_client_socket::TlsClientSocket;
