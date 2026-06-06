use altersend_mux::{MuxError, PeerMux, PeerMuxBuilder, CHUNK_PROTOCOL, CONTROL_PROTOCOL};
use bytes::Bytes;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::control::{encode_control_payload, PeerControlMessage};
use crate::wire::encode_file_chunk_frame;

pub struct PeerSession {
    pub mux: PeerMux,
    pub control_idx: usize,
    pub chunk_idx: usize,
}

impl PeerSession {
    pub fn new(
        is_initiator: bool,
        outbound: mpsc::UnboundedSender<Bytes>,
    ) -> Result<Self, MuxError> {
        let mut mux = PeerMuxBuilder::new(is_initiator).build();
        mux.attach_outbound(outbound);
        let control_idx = mux.create_channel(CONTROL_PROTOCOL)?;
        let chunk_idx = mux.create_channel(CHUNK_PROTOCOL)?;
        Ok(Self {
            mux,
            control_idx,
            chunk_idx,
        })
    }

    pub fn send_control(&mut self, msg: &PeerControlMessage) -> Result<(), MuxError> {
        let bytes = encode_control_payload(msg);
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| MuxError::InvalidFrame)?;
        self.mux.send_json(self.control_idx, value)
    }

    pub fn send_file_chunk(
        &mut self,
        file_id: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<(), MuxError> {
        let payload = encode_file_chunk_frame(file_id, offset, data);
        let value = serde_json::json!({
            "type": "file-chunk",
            "payload": hex::encode(payload),
        });
        self.mux.send_json(self.chunk_idx, value)
    }

    pub fn ingest(&mut self, data: &[u8]) -> Result<(), MuxError> {
        self.mux.ingest(data)
    }
}
