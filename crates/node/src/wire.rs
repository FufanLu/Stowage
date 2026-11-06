//! Framing on a TCP stream.
//!
//! ```text
//! frame := len u32 (LE, of kind+payload) ‖ kind u8 ‖ payload
//! kind 1  Hello     payload = node id u64        (first frame of a peer connection)
//! kind 2  Peer      payload = PeerMsg encoding
//! kind 3  Request   payload = token u64 ‖ Request encoding
//! kind 4  Response  payload = token u64 ‖ Response encoding
//! ```

use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const HELLO: u8 = 1;
pub const PEER: u8 = 2;
pub const REQUEST: u8 = 3;
pub const RESPONSE: u8 = 4;
/// Upper bound on one frame (a range snapshot can be large).
pub const MAX_FRAME: usize = 256 << 20;

pub fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&((payload.len() + 1) as u32).to_le_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    out
}

pub fn token_frame(kind: u8, token: u64, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + body.len());
    payload.extend_from_slice(&token.to_le_bytes());
    payload.extend_from_slice(body);
    frame(kind, &payload)
}

pub fn split_token(payload: &[u8]) -> Option<(u64, &[u8])> {
    let t = payload.get(..8)?;
    let mut a = [0u8; 8];
    a.copy_from_slice(t);
    Some((u64::from_le_bytes(a), &payload[8..]))
}

/// Read one frame: `(kind, payload)`. `Ok(None)` on a clean EOF.
pub async fn read_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> std::io::Result<Option<(u8, Vec<u8>)>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame of {len} bytes"),
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    let kind = buf[0];
    buf.remove(0);
    Ok(Some((kind, buf)))
}

pub async fn write_all<W: AsyncWriteExt + Unpin>(w: &mut W, bytes: &[u8]) -> std::io::Result<()> {
    w.write_all(bytes).await
}
