//! Length-prefixed frames on QUIC streams: a 4-byte big-endian length, then the `Envelope`.

use std::io;

use bytes::Bytes;
use futures::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

fn codec(max: usize) -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_length(4)
        .big_endian()
        .max_frame_length(max)
        .new_codec()
}

/// Write one frame. The caller finishes the stream.
pub async fn write_frame(send: &mut quinn::SendStream, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame size too big"))?;
    send.write_all(&len.to_be_bytes()).await?;
    send.write_all(bytes).await?;
    Ok(())
}

/// Read one frame of at most `max` bytes. `None` means the stream ended before a frame began. A
/// length above `max` is an `InvalidData` error.
pub async fn read_frame(recv: &mut quinn::RecvStream, max: usize) -> io::Result<Option<Bytes>> {
    let mut framed = FramedRead::new(recv, codec(max));
    match framed.next().await {
        None => Ok(None),
        Some(Ok(frame)) => Ok(Some(frame.freeze())),
        Some(Err(e)) => Err(e),
    }
}
