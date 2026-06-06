use serde::{Deserialize, Serialize};

use crate::download::all_downloads_completed;
use crate::format::format_file_size;
use crate::types::{TransferRole, TransferSessionState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendDraftPhase {
    Empty,
    Selected,
    Preparing,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendStep {
    Selecting,
    Preparing,
    WaitingForReceiver,
    ReceiverConnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiveStep {
    Join,
    Connecting,
    IncomingTransfer,
    Reconnecting,
    Interrupted,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendPageCopy {
    pub title: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivePageCopy {
    pub title: String,
    pub description: String,
}

pub fn get_send_step(draft_phase: SendDraftPhase, is_peer_connected: bool) -> SendStep {
    match draft_phase {
        SendDraftPhase::Empty | SendDraftPhase::Selected => SendStep::Selecting,
        SendDraftPhase::Preparing => SendStep::Preparing,
        SendDraftPhase::Ready if !is_peer_connected => SendStep::WaitingForReceiver,
        SendDraftPhase::Ready => SendStep::ReceiverConnected,
    }
}

pub fn get_send_page_copy(step: SendStep) -> SendPageCopy {
    match step {
        SendStep::Selecting => SendPageCopy {
            title: "Send files".to_string(),
            description: "Choose one or more files and generate a one-time code for a direct encrypted transfer.".to_string(),
        },
        SendStep::Preparing => SendPageCopy {
            title: "Preparing transfer".to_string(),
            description: "Preparing the selected files before the share code is revealed.".to_string(),
        },
        SendStep::WaitingForReceiver | SendStep::ReceiverConnected => SendPageCopy {
            title: "Share the code".to_string(),
            description: "Send the code or QR to your recipient to start the transfer.".to_string(),
        },
    }
}

pub fn get_receive_step(state: &TransferSessionState) -> ReceiveStep {
    let has_incoming = !state.incoming_file_offers.is_empty();
    let all_done = all_downloads_completed(&state.receive_download_states);

    if has_incoming && all_done {
        return ReceiveStep::Completed;
    }
    if state.role != Some(TransferRole::Receiver) {
        return ReceiveStep::Join;
    }
    if has_incoming && state.is_reconnecting {
        return ReceiveStep::Reconnecting;
    }
    if has_incoming && state.peer_count == 0 {
        return ReceiveStep::Interrupted;
    }
    if has_incoming {
        return ReceiveStep::IncomingTransfer;
    }
    ReceiveStep::Connecting
}

pub fn get_receive_page_copy(step: ReceiveStep, incoming_count: usize, total_bytes: u64) -> ReceivePageCopy {
    match step {
        ReceiveStep::Join => ReceivePageCopy {
            title: "Receive files".to_string(),
            description: "Enter a 64-character connection code from the sender to stream their files.".to_string(),
        },
        ReceiveStep::Connecting => ReceivePageCopy {
            title: "Connecting".to_string(),
            description: "Establishing a secure session with the sender.".to_string(),
        },
        ReceiveStep::IncomingTransfer => ReceivePageCopy {
            title: "Files available".to_string(),
            description: format!(
                "{incoming_count} {} · {}",
                if incoming_count == 1 { "file" } else { "files" },
                format_file_size(total_bytes)
            ),
        },
        ReceiveStep::Completed => ReceivePageCopy {
            title: if incoming_count == 1 {
                "File received".to_string()
            } else {
                "Files received".to_string()
            },
            description: String::new(),
        },
        ReceiveStep::Reconnecting => ReceivePageCopy {
            title: "Reconnecting".to_string(),
            description: "Reconnecting to the session. Files will be available again as soon as the link is restored.".to_string(),
        },
        ReceiveStep::Interrupted => ReceivePageCopy {
            title: "Transfer incomplete".to_string(),
            description: "The sender left before all files arrived.".to_string(),
        },
    }
}

/// View-model snapshot consumed by the Flutter UI layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferUiSnapshot {
    pub send_step: SendStep,
    pub send_copy: SendPageCopy,
    pub receive_step: ReceiveStep,
    pub receive_copy: ReceivePageCopy,
    pub topic: String,
    pub join_url: String,
    pub error_message: Option<String>,
    pub is_peer_connected: bool,
    pub selected_file_count: usize,
    pub incoming_file_count: usize,
}

pub fn build_ui_snapshot(state: &TransferSessionState) -> TransferUiSnapshot {
    let is_peer_connected = state.connection_state == crate::types::ConnectionState::PeerConnected;
    let send_step = get_send_step(state.draft_phase, is_peer_connected);
    let receive_step = get_receive_step(state);
    let total_bytes: u64 = state.incoming_file_offers.iter().map(|f| f.size).sum();

    TransferUiSnapshot {
        send_copy: get_send_page_copy(send_step),
        receive_copy: get_receive_page_copy(
            receive_step,
            state.incoming_file_offers.len(),
            total_bytes,
        ),
        join_url: if state.topic.is_empty() {
            String::new()
        } else {
            crate::join_code::build_join_url(&state.topic)
        },
        topic: state.topic.clone(),
        error_message: state.error_message.clone(),
        is_peer_connected,
        selected_file_count: state.selected_files.len(),
        incoming_file_count: state.incoming_file_offers.len(),
        send_step,
        receive_step,
    }
}
