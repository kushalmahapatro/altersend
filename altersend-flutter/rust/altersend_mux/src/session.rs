use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::channel::Channel;
use crate::codec::{
    decode_buffer, decode_optional_buffer, decode_string, decode_uint, encode_channel_message,
    encode_control_frame, encode_optional_buffer, encode_string, encode_uint,
};

#[derive(Debug, Error)]
pub enum MuxError {
    #[error("channel not open")]
    ChannelNotOpen,
    #[error("invalid frame")]
    InvalidFrame,
    #[error("protocol {0} already open")]
    DuplicateProtocol(String),
}

struct RemoteSlot {
    state: Option<Vec<u8>>,
    pending: Vec<(u64, Vec<u8>)>,
    session: Option<usize>,
}

pub struct PeerMuxBuilder {
    is_initiator: bool,
}

impl PeerMuxBuilder {
    pub fn new(is_initiator: bool) -> Self {
        Self { is_initiator }
    }

    pub fn build(self) -> PeerMux {
        PeerMux::new(self.is_initiator)
    }
}

pub struct PeerMux {
    is_initiator: bool,
    local: Vec<Option<Channel>>,
    remote: Vec<Option<RemoteSlot>>,
    free_local_ids: Vec<u64>,
    protocol_index: HashMap<String, usize>,
    outbound_tx: Option<mpsc::UnboundedSender<Bytes>>,
    next_local_id: u64,
}

impl PeerMux {
    pub fn new(is_initiator: bool) -> Self {
        Self {
            is_initiator,
            local: Vec::new(),
            remote: Vec::new(),
            free_local_ids: Vec::new(),
            protocol_index: HashMap::new(),
            outbound_tx: None,
            next_local_id: 1,
        }
    }

    pub fn attach_outbound(&mut self, tx: mpsc::UnboundedSender<Bytes>) {
        self.outbound_tx = Some(tx);
    }

    pub fn create_channel(&mut self, protocol: &str) -> Result<usize, MuxError> {
        if self.protocol_index.contains_key(protocol) {
            return Err(MuxError::DuplicateProtocol(protocol.to_string()));
        }
        let local_id = if let Some(id) = self.free_local_ids.pop() {
            id
        } else {
            let id = self.next_local_id;
            self.next_local_id += 1;
            while self.local.len() < id as usize {
                self.local.push(None);
            }
            id
        };

        let idx = local_id as usize - 1;
        while self.local.len() <= idx {
            self.local.push(None);
        }

        let mut channel = Channel::new(protocol);
        channel.local_id = local_id;
        self.local[idx] = Some(channel);
        self.protocol_index.insert(protocol.to_string(), idx);

        self.send_open(local_id, protocol, None)?;
        Ok(idx)
    }

    pub fn channel_mut(&mut self, idx: usize) -> Option<&mut Channel> {
        self.local.get_mut(idx).and_then(|c| c.as_mut())
    }

    pub fn send_json(&mut self, idx: usize, value: serde_json::Value) -> Result<(), MuxError> {
        let local_id = self
            .local
            .get(idx)
            .and_then(|c| c.as_ref())
            .map(|c| c.local_id)
            .ok_or(MuxError::ChannelNotOpen)?;

        let mut payload = BytesMut::new();
        crate::codec::encode_json(&value, &mut payload).map_err(|_| MuxError::InvalidFrame)?;
        let frame = encode_channel_message(local_id, 0, &mut payload);
        self.write_frame(frame)
    }

    pub fn ingest(&mut self, data: &[u8]) -> Result<(), MuxError> {
        let mut input = data;
        while !input.is_empty() {
            let remote_id = decode_uint(&mut input).ok_or(MuxError::InvalidFrame)?;
            if remote_id == 0 {
                self.handle_control(&mut input)?;
            } else {
                let msg_type = decode_uint(&mut input).ok_or(MuxError::InvalidFrame)?;
                let payload = decode_buffer(&mut input).ok_or(MuxError::InvalidFrame)?;
                self.handle_channel_message(remote_id, msg_type, &payload)?;
            }
        }
        Ok(())
    }

    fn handle_control(&mut self, input: &mut &[u8]) -> Result<(), MuxError> {
        let control_type = decode_uint(input).ok_or(MuxError::InvalidFrame)?;
        match control_type {
            0 => self.handle_batch(input),
            1 => self.handle_open(input),
            2 => self.handle_reject(input),
            3 => self.handle_close(input),
            _ => Err(MuxError::InvalidFrame),
        }
    }

    fn handle_batch(&mut self, input: &mut &[u8]) -> Result<(), MuxError> {
        let end = input.len();
        let mut remote_id = decode_uint(input).ok_or(MuxError::InvalidFrame)?;
        while !input.is_empty() {
            let len = decode_uint(input).ok_or(MuxError::InvalidFrame)? as usize;
            if len == 0 {
                remote_id = decode_uint(input).ok_or(MuxError::InvalidFrame)?;
                continue;
            }
            if input.len() < len {
                return Err(MuxError::InvalidFrame);
            }
            let chunk = &input[..len];
            *input = &input[len..];
            if remote_id == 0 {
                self.handle_control(&mut &chunk.to_vec()[..])?;
            } else {
                let mut slice = chunk;
                let msg_type = decode_uint(&mut slice).ok_or(MuxError::InvalidFrame)?;
                let payload = decode_buffer(&mut slice).ok_or(MuxError::InvalidFrame)?;
                self.handle_channel_message(remote_id, msg_type, &payload)?;
            }
        }
        let _ = end;
        Ok(())
    }

    fn handle_open(&mut self, input: &mut &[u8]) -> Result<(), MuxError> {
        let remote_id = decode_uint(input).ok_or(MuxError::InvalidFrame)?;
        if remote_id == 0 {
            return Ok(());
        }
        let protocol = decode_string(input).ok_or(MuxError::InvalidFrame)?;
        let _id = decode_optional_buffer(input).ok_or(MuxError::InvalidFrame)?;

        let rid = remote_id as usize - 1;
        while self.remote.len() <= rid {
            self.remote.push(None);
        }

        let handshake_end = input.len();
        let handshake = input[..handshake_end].to_vec();
        *input = &[];

        if let Some(&idx) = self.protocol_index.get(&protocol) {
            if let Some(channel) = self.local.get_mut(idx).and_then(|c| c.as_mut()) {
                channel.remote_id = remote_id;
                channel.opened = true;
            }
            self.remote[rid] = Some(RemoteSlot {
                state: None,
                pending: Vec::new(),
                session: Some(idx),
            });
            return Ok(());
        }

        self.remote[rid] = Some(RemoteSlot {
            state: Some(handshake),
            pending: Vec::new(),
            session: None,
        });
        Ok(())
    }

    fn handle_reject(&mut self, _input: &mut &[u8]) -> Result<(), MuxError> {
        Ok(())
    }

    fn handle_close(&mut self, input: &mut &[u8]) -> Result<(), MuxError> {
        let remote_id = decode_uint(input).ok_or(MuxError::InvalidFrame)?;
        if remote_id == 0 {
            return Ok(());
        }
        let rid = remote_id as usize - 1;
        if rid < self.remote.len() {
            if let Some(slot) = self.remote[rid].take() {
                if let Some(idx) = slot.session {
                    if let Some(channel) = self.local.get_mut(idx).and_then(|c| c.as_mut()) {
                        channel.opened = false;
                        channel.remote_id = 0;
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_channel_message(
        &mut self,
        remote_id: u64,
        _msg_type: u64,
        payload: &[u8],
    ) -> Result<(), MuxError> {
        let rid = remote_id as usize - 1;
        let slot = self
            .remote
            .get(rid)
            .and_then(|s| s.as_ref())
            .ok_or(MuxError::InvalidFrame)?;

        let idx = slot.session.ok_or(MuxError::InvalidFrame)?;
        if let Some(channel) = self.local.get_mut(idx).and_then(|c| c.as_mut()) {
            channel.recv_json(payload);
        }
        Ok(())
    }

    fn send_open(&mut self, local_id: u64, protocol: &str, id: Option<&[u8]>) -> Result<(), MuxError> {
        let mut payload = BytesMut::new();
        encode_uint(local_id, &mut payload);
        encode_string(protocol, &mut payload);
        encode_optional_buffer(id, &mut payload);
        let frame = encode_control_frame(1, &mut payload);
        self.write_frame(frame)
    }

    fn write_frame(&self, frame: BytesMut) -> Result<(), MuxError> {
        if let Some(tx) = &self.outbound_tx {
            let _ = tx.send(frame.freeze());
            Ok(())
        } else {
            Err(MuxError::ChannelNotOpen)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CONTROL_PROTOCOL;

    #[test]
    fn open_control_channel() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut mux_a = PeerMux::new(true);
        mux_a.attach_outbound(tx);

        let idx = mux_a.create_channel(CONTROL_PROTOCOL).unwrap();
        assert_eq!(idx, 0);

        let frame = rx.try_recv().expect("open frame");
        assert_eq!(frame[0], 0);
        assert_eq!(frame[1], 1);
    }
}
