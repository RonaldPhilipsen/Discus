//! One client connection.

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// Serve one connection. For now this reads and discards input.
pub async fn serve(mut stream: TcpStream) {
    let mut buf = [0u8; 4096];
    while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
}
