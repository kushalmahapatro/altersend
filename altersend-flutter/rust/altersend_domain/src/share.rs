use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::PeerDownloadStatusEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedPeer {
    pub peer_key: String,
    pub connected_at: u64,
    pub disconnected_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDownloadEvent {
    pub state: String,
    pub file_id: String,
    pub file_name: String,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
    pub saved_to: Option<String>,
    pub message: Option<String>,
    pub peer: String,
}

pub fn apply_peer_download_event(
    current: &HashMap<String, PeerDownloadEvent>,
    event: &PeerDownloadStatusEvent,
) -> HashMap<String, PeerDownloadEvent> {
    let file_id = match &event.file_id {
        Some(id) => id.clone(),
        None => return current.clone(),
    };
    let peer = event.peer.clone().unwrap_or_default();
    let key = format!("{peer}:{file_id}");
    let mut next = current.clone();
    next.insert(
        key,
        PeerDownloadEvent {
            state: event.state.clone(),
            file_id,
            file_name: event.file_name.clone().unwrap_or_default(),
            bytes_transferred: event.bytes_transferred.unwrap_or(0),
            total_bytes: event.total_bytes.unwrap_or(0),
            saved_to: event.saved_to.clone(),
            message: event.message.clone(),
            peer,
        },
    );
    next
}
