use std::collections::HashMap;

use crate::download::{
    apply_download_message, apply_download_routed, create_download_state_map, resolve_offer_key,
};
use crate::draft::{
    apply_sharing_progress, get_phase_from_selection, merge_selected_files,
};
use crate::share::apply_peer_download_event;
use crate::types::{
    ConnectionState, TransferAction, TransferRole, TransferSessionState,
};

pub fn initial_transfer_session_state() -> TransferSessionState {
    TransferSessionState::default()
}

fn merge_incoming_file_offers(
    current: &[crate::types::IncomingFileOffer],
    next_files: &[crate::types::IncomingFileOffer],
) -> Vec<crate::types::IncomingFileOffer> {
    let existing: std::collections::HashSet<String> = current
        .iter()
        .map(|o| format!("{}:{}", o.drive_key, o.path))
        .collect();
    let unique: Vec<_> = next_files
        .iter()
        .filter(|o| !existing.contains(&format!("{}:{}", o.drive_key, o.path)))
        .cloned()
        .collect();
    if unique.is_empty() {
        current.to_vec()
    } else {
        let mut merged = current.to_vec();
        merged.extend(unique);
        merged
    }
}

fn end_session(_state: TransferSessionState) -> TransferSessionState {
    TransferSessionState {
        topic: String::new(),
        connection_state: ConnectionState::Disconnected,
        role: None,
        peer_count: 0,
        is_reconnecting: false,
        incoming_file_offers: Vec::new(),
        receive_download_states: HashMap::new(),
        selected_files: Vec::new(),
        draft_phase: crate::page_ui::SendDraftPhase::Empty,
        upload_items: Vec::new(),
        peer_downloads: HashMap::new(),
        connected_peers: HashMap::new(),
        error_message: None,
    }
}

pub fn transfer_session_reducer(
    state: TransferSessionState,
    action: TransferAction,
) -> TransferSessionState {
    match action {
        TransferAction::Booted => {
            if state.error_message.is_none() {
                state
            } else {
                TransferSessionState {
                    error_message: None,
                    ..state
                }
            }
        }
        TransferAction::BootFailed { message } => TransferSessionState {
            error_message: Some(message),
            ..state
        },

        TransferAction::StatusChanged { state: conn, peers } => match conn {
            ConnectionState::Disconnected => end_session(state),
            ConnectionState::Joining => TransferSessionState {
                connection_state: ConnectionState::Joining,
                error_message: None,
                ..state
            },
            ConnectionState::Joined => {
                let peer_count = peers.unwrap_or(state.peer_count);
                let connection_state = if peer_count > 0 {
                    ConnectionState::PeerConnected
                } else {
                    ConnectionState::Joined
                };
                TransferSessionState {
                    connection_state,
                    peer_count,
                    error_message: None,
                    ..state
                }
            }
            ConnectionState::PeerConnected => TransferSessionState {
                connection_state: ConnectionState::PeerConnected,
                peer_count: peers.unwrap_or(1),
                is_reconnecting: false,
                error_message: None,
                ..state
            },
        },

        TransferAction::Reconnecting => TransferSessionState {
            is_reconnecting: true,
            ..state
        },
        TransferAction::ClearSession => end_session(state),
        TransferAction::JoinFailed { message } => TransferSessionState {
            role: None,
            is_reconnecting: false,
            connection_state: ConnectionState::Disconnected,
            peer_count: 0,
            receive_download_states: HashMap::new(),
            upload_items: Vec::new(),
            peer_downloads: HashMap::new(),
            connected_peers: HashMap::new(),
            error_message: Some(message),
            ..state
        },

        TransferAction::ShareRequested => TransferSessionState {
            role: Some(TransferRole::Sender),
            peer_downloads: HashMap::new(),
            connected_peers: HashMap::new(),
            error_message: None,
            ..state
        },
        TransferAction::SessionHosted { topic } => TransferSessionState { topic, ..state },

        TransferAction::AddSelectedFiles { files } => {
            if files.is_empty() {
                return state;
            }
            let selected_files = merge_selected_files(&state.selected_files, &files);
            let draft_phase = get_phase_from_selection(selected_files.len());
            TransferSessionState {
                selected_files,
                draft_phase,
                upload_items: Vec::new(),
                ..state
            }
        }
        TransferAction::RemoveSelectedFile { path } => {
            let selected_files: Vec<_> = state
                .selected_files
                .iter()
                .filter(|f| f.path != path)
                .cloned()
                .collect();
            if selected_files.len() == state.selected_files.len() {
                return state;
            }
            TransferSessionState {
                draft_phase: get_phase_from_selection(selected_files.len()),
                selected_files,
                ..state
            }
        }
        TransferAction::SetDraftPhase { phase } => {
            if state.draft_phase == phase {
                state
            } else {
                TransferSessionState {
                    draft_phase: phase,
                    ..state
                }
            }
        }
        TransferAction::ClearSendDraft => TransferSessionState {
            selected_files: Vec::new(),
            draft_phase: crate::page_ui::SendDraftPhase::Empty,
            upload_items: Vec::new(),
            ..state
        },
        TransferAction::InitUploadItems { items } => TransferSessionState {
            upload_items: items,
            ..state
        },
        TransferAction::CompleteAllUploads => TransferSessionState {
            upload_items: state
                .upload_items
                .iter()
                .map(|i| crate::draft::SenderUploadItem {
                    status: crate::draft::UploadItemStatus::Completed,
                    ..i.clone()
                })
                .collect(),
            ..state
        },
        TransferAction::ResetUploadingItems => TransferSessionState {
            upload_items: state
                .upload_items
                .iter()
                .map(|i| {
                    let status = if i.status == crate::draft::UploadItemStatus::Uploading {
                        crate::draft::UploadItemStatus::Waiting
                    } else {
                        i.status
                    };
                    crate::draft::SenderUploadItem { status, ..i.clone() }
                })
                .collect(),
            ..state
        },
        TransferAction::ApplySharingProgress { event } => {
            if state.role != Some(TransferRole::Sender) {
                return state;
            }
            TransferSessionState {
                upload_items: apply_sharing_progress(&state.upload_items, &event),
                error_message: None,
                ..state
            }
        }
        TransferAction::PeerDownloadEvent { event } => {
            if state.role != Some(TransferRole::Sender) {
                return state;
            }
            TransferSessionState {
                peer_downloads: apply_peer_download_event(&state.peer_downloads, &event),
                ..state
            }
        }
        TransferAction::PeerJoined { peer_key } => {
            if state.role != Some(TransferRole::Sender) {
                return state;
            }
            let mut connected_peers = state.connected_peers.clone();
            connected_peers.insert(
                peer_key.clone(),
                crate::share::ConnectedPeer {
                    peer_key,
                    connected_at: now_ms(),
                    disconnected_at: None,
                },
            );
            TransferSessionState {
                connected_peers,
                ..state
            }
        }
        TransferAction::PeerLeft { peer_key } => {
            if state.role != Some(TransferRole::Sender) {
                return state;
            }
            let Some(existing) = state.connected_peers.get(&peer_key) else {
                return state;
            };
            if existing.disconnected_at.is_some() {
                return state;
            }
            let mut connected_peers = state.connected_peers.clone();
            connected_peers.insert(
                peer_key.clone(),
                crate::share::ConnectedPeer {
                    disconnected_at: Some(now_ms()),
                    ..existing.clone()
                },
            );
            TransferSessionState {
                connected_peers,
                ..state
            }
        }

        TransferAction::JoinRequested => TransferSessionState {
            role: Some(TransferRole::Receiver),
            incoming_file_offers: Vec::new(),
            receive_download_states: HashMap::new(),
            selected_files: Vec::new(),
            draft_phase: crate::page_ui::SendDraftPhase::Empty,
            upload_items: Vec::new(),
            peer_downloads: HashMap::new(),
            connected_peers: HashMap::new(),
            connection_state: ConnectionState::Joining,
            peer_count: 0,
            error_message: None,
            ..state
        },
        TransferAction::TransferReady { files } => {
            if state.role != Some(TransferRole::Receiver) {
                return state;
            }
            let incoming_file_offers =
                merge_incoming_file_offers(&state.incoming_file_offers, &files);
            let receive_download_states = create_download_state_map(
                &state.receive_download_states,
                &incoming_file_offers,
            );
            TransferSessionState {
                incoming_file_offers,
                receive_download_states,
                error_message: None,
                ..state
            }
        }
        TransferAction::ReceiveDownloadEvent { event } => {
            if state.role != Some(TransferRole::Receiver) {
                return state;
            }
            let Some(offer_key) = resolve_offer_key(&state.incoming_file_offers, &event) else {
                return state;
            };
            let receive_download_states =
                apply_download_message(&state.receive_download_states, &offer_key, &event);
            if event.state == "download-failed" {
                TransferSessionState {
                    receive_download_states,
                    error_message: event
                        .message
                        .or(state.error_message),
                    ..state
                }
            } else {
                TransferSessionState {
                    receive_download_states,
                    error_message: None,
                    ..state
                }
            }
        }
        TransferAction::DownloadRouted {
            offer_key,
            destination,
            intended_destination,
            saved_to,
        } => {
            if state.role != Some(TransferRole::Receiver) {
                return state;
            }
            TransferSessionState {
                receive_download_states: apply_download_routed(
                    &state.receive_download_states,
                    &offer_key,
                    destination,
                    intended_destination,
                    saved_to,
                ),
                ..state
            }
        }
        TransferAction::PeerUnreachable => {
            if !state.incoming_file_offers.is_empty() {
                TransferSessionState {
                    is_reconnecting: false,
                    ..state
                }
            } else {
                TransferSessionState {
                    role: None,
                    connection_state: ConnectionState::Disconnected,
                    peer_count: 0,
                    is_reconnecting: false,
                    topic: String::new(),
                    connected_peers: HashMap::new(),
                    error_message: Some(
                        "Couldn't reach the sender. Check the code and try again.".to_string(),
                    ),
                    ..state
                }
            }
        }

        TransferAction::SetError { message } => TransferSessionState {
            error_message: Some(message),
            ..state
        },
        TransferAction::RoleChanged { role } => {
            if role.is_none() {
                TransferSessionState {
                    role: None,
                    incoming_file_offers: Vec::new(),
                    receive_download_states: HashMap::new(),
                    selected_files: Vec::new(),
                    draft_phase: crate::page_ui::SendDraftPhase::Empty,
                    upload_items: Vec::new(),
                    peer_downloads: HashMap::new(),
                    connected_peers: HashMap::new(),
                    ..state
                }
            } else {
                TransferSessionState { role, ..state }
            }
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IncomingFileOffer;

    fn offer(id: &str, name: &str) -> IncomingFileOffer {
        IncomingFileOffer {
            id: id.to_string(),
            transfer_id: "tx-1".to_string(),
            name: name.to_string(),
            path: format!("/files/{name}"),
            size: 1024,
            drive_key: "drive-1".to_string(),
        }
    }

    #[test]
    fn disconnected_ends_session() {
        let state = transfer_session_reducer(
            TransferSessionState {
                role: Some(TransferRole::Receiver),
                connection_state: ConnectionState::PeerConnected,
                peer_count: 1,
                incoming_file_offers: vec![offer("a", "a.txt")],
                ..Default::default()
            },
            TransferAction::StatusChanged {
                state: ConnectionState::Disconnected,
                peers: None,
            },
        );
        assert_eq!(state.role, None);
        assert!(state.incoming_file_offers.is_empty());
    }

    #[test]
    fn joined_with_peers_becomes_peer_connected() {
        let state = transfer_session_reducer(
            initial_transfer_session_state(),
            TransferAction::StatusChanged {
                state: ConnectionState::Joined,
                peers: Some(2),
            },
        );
        assert_eq!(state.connection_state, ConnectionState::PeerConnected);
        assert_eq!(state.peer_count, 2);
    }
}
