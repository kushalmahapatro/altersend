use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::download::DownloadItemState;
use crate::draft::{SelectedFile, SenderUploadItem};
use crate::page_ui::SendDraftPhase;
use crate::share::{ConnectedPeer, PeerDownloadEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransferRole {
    Sender,
    Receiver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectionState {
    Disconnected,
    Joining,
    Joined,
    #[serde(rename = "peer-connected")]
    PeerConnected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingFileOffer {
    pub id: String,
    pub transfer_id: String,
    pub name: String,
    pub path: String,
    pub size: u64,
    pub drive_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferSessionState {
    pub topic: String,
    pub connection_state: ConnectionState,
    pub role: Option<TransferRole>,
    pub peer_count: u32,
    pub is_reconnecting: bool,
    pub incoming_file_offers: Vec<IncomingFileOffer>,
    pub receive_download_states: HashMap<String, DownloadItemState>,
    pub selected_files: Vec<SelectedFile>,
    pub draft_phase: SendDraftPhase,
    pub upload_items: Vec<SenderUploadItem>,
    pub peer_downloads: HashMap<String, PeerDownloadEvent>,
    pub connected_peers: HashMap<String, ConnectedPeer>,
    pub error_message: Option<String>,
}

impl Default for TransferSessionState {
    fn default() -> Self {
        Self {
            topic: String::new(),
            connection_state: ConnectionState::Disconnected,
            role: None,
            peer_count: 0,
            is_reconnecting: false,
            incoming_file_offers: Vec::new(),
            receive_download_states: HashMap::new(),
            selected_files: Vec::new(),
            draft_phase: SendDraftPhase::Empty,
            upload_items: Vec::new(),
            peer_downloads: HashMap::new(),
            connected_peers: HashMap::new(),
            error_message: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransferAction {
    Booted,
    BootFailed { message: String },
    SessionHosted { topic: String },
    JoinRequested,
    ShareRequested,
    JoinFailed { message: String },
    ClearSession,
    SetError { message: String },
    StatusChanged {
        state: ConnectionState,
        peers: Option<u32>,
    },
    RoleChanged { role: Option<TransferRole> },
    ApplySharingProgress { event: SharingStatusEvent },
    InitUploadItems { items: Vec<SenderUploadItem> },
    CompleteAllUploads,
    ResetUploadingItems,
    PeerDownloadEvent { event: PeerDownloadStatusEvent },
    PeerJoined { peer_key: String },
    PeerLeft { peer_key: String },
    AddSelectedFiles { files: Vec<SelectedFile> },
    RemoveSelectedFile { path: String },
    SetDraftPhase { phase: SendDraftPhase },
    ClearSendDraft,
    ReceiveDownloadEvent { event: ReceiveDownloadStatusEvent },
    DownloadRouted {
        offer_key: String,
        destination: SaveDestination,
        intended_destination: SaveDestination,
        saved_to: Option<String>,
    },
    TransferReady { files: Vec<IncomingFileOffer> },
    Reconnecting,
    PeerUnreachable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharingStatusEvent {
    pub file_name: String,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDownloadStatusEvent {
    pub state: String,
    pub file_id: Option<String>,
    pub file_name: Option<String>,
    pub bytes_transferred: Option<u64>,
    pub total_bytes: Option<u64>,
    pub saved_to: Option<String>,
    pub message: Option<String>,
    pub peer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiveDownloadStatusEvent {
    pub state: String,
    pub file_id: Option<String>,
    pub file_name: Option<String>,
    pub bytes_transferred: Option<u64>,
    pub total_bytes: Option<u64>,
    pub saved_to: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SaveDestination {
    Filesystem,
    Photos,
}
