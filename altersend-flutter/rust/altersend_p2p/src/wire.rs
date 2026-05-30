//! Multiplexed wire framing over the encrypted peer stream.
//!
//! - `0x00` — length-prefixed JSON control message (AlterSend control protocol)
//! - `0x01` — file chunk (`file_id`, `offset`, `data`)

use crate::control::{decode_control_payload, encode_control_payload, PeerControlMessage};

pub const KIND_CONTROL: u8 = 0;
pub const KIND_FILE_CHUNK: u8 = 1;

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
const MAX_FILE_ID_LEN: usize = 128;

#[derive(Debug, Clone)]
pub enum WireFrame {
    Control(PeerControlMessage),
    FileChunk {
        file_id: String,
        offset: u64,
        data: Vec<u8>,
    },
}

pub fn encode_control_frame(msg: &PeerControlMessage) -> Vec<u8> {
    let payload = encode_control_payload(msg);
    let mut out = Vec::with_capacity(1 + 4 + payload.len());
    out.push(KIND_CONTROL);
    out.extend((payload.len() as u32).to_be_bytes());
    out.extend(payload);
    out
}

pub fn encode_file_chunk_frame(file_id: &str, offset: u64, data: &[u8]) -> Vec<u8> {
    let id_bytes = file_id.as_bytes();
    let mut out = Vec::with_capacity(1 + 2 + id_bytes.len() + 8 + 4 + data.len());
    out.push(KIND_FILE_CHUNK);
    out.extend((id_bytes.len() as u16).to_be_bytes());
    out.extend(id_bytes);
    out.extend(offset.to_be_bytes());
    out.extend((data.len() as u32).to_be_bytes());
    out.extend(data);
    out
}

/// Parse and remove complete frames from the front of `buffer`.
pub fn drain_frames(buffer: &mut Vec<u8>) -> Vec<WireFrame> {
    let mut frames = Vec::new();
    while let Some(frame) = pop_frame(buffer) {
        frames.push(frame);
    }
    frames
}

fn pop_frame(buffer: &mut Vec<u8>) -> Option<WireFrame> {
    if buffer.is_empty() {
        return None;
    }
    match buffer[0] {
        KIND_CONTROL => {
            if buffer.len() < 5 {
                return None;
            }
            let len = u32::from_be_bytes(buffer[1..5].try_into().ok()?) as usize;
            if len > MAX_FRAME_SIZE {
                buffer.drain(..1);
                return None;
            }
            let total = 5 + len;
            if buffer.len() < total {
                return None;
            }
            let msg = decode_control_payload(&buffer[5..total])?;
            buffer.drain(..total);
            Some(WireFrame::Control(msg))
        }
        KIND_FILE_CHUNK => {
            if buffer.len() < 3 {
                return None;
            }
            let id_len = u16::from_be_bytes(buffer[1..3].try_into().ok()?) as usize;
            if id_len > MAX_FILE_ID_LEN {
                buffer.drain(..1);
                return None;
            }
            let header = 3 + id_len + 8 + 4;
            if buffer.len() < header {
                return None;
            }
            let file_id = std::str::from_utf8(&buffer[3..3 + id_len]).ok()?.to_string();
            let offset_start = 3 + id_len;
            let offset = u64::from_be_bytes(buffer[offset_start..offset_start + 8].try_into().ok()?);
            let data_len_start = offset_start + 8;
            let data_len =
                u32::from_be_bytes(buffer[data_len_start..data_len_start + 4].try_into().ok()?) as usize;
            if data_len > MAX_FRAME_SIZE {
                buffer.drain(..1);
                return None;
            }
            let total = header + data_len;
            if buffer.len() < total {
                return None;
            }
            let data = buffer[header..total].to_vec();
            buffer.drain(..total);
            Some(WireFrame::FileChunk {
                file_id,
                offset,
                data,
            })
        }
        _ => {
            buffer.drain(..1);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::PeerControlMessage;

    #[test]
    fn roundtrip_control() {
        let msg = PeerControlMessage::TransferStart {
            transfer_id: "abc".into(),
            total_files: 1,
            total_bytes: 100,
        };
        let mut buf = encode_control_frame(&msg);
        let frames = drain_frames(&mut buf);
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            frames[0],
            WireFrame::Control(PeerControlMessage::TransferStart { .. })
        ));
    }

    #[test]
    fn roundtrip_file_chunk() {
        let mut buf = encode_file_chunk_frame("file-1", 0, b"hello");
        let frames = drain_frames(&mut buf);
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            WireFrame::FileChunk { file_id, offset, data } => {
                assert_eq!(file_id, "file-1");
                assert_eq!(*offset, 0);
                assert_eq!(data, b"hello");
            }
            _ => panic!("expected chunk"),
        }
    }
}
