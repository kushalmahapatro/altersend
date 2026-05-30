use std::path::PathBuf;
use std::sync::Arc;

use altersend_domain::{
    build_ui_snapshot, create_initial_upload_items, transfer_session_reducer,
    ConnectionState, PeerDownloadStatusEvent, ReceiveDownloadStatusEvent, SelectedFile,
    SendDraftPhase, SharingStatusEvent, TransferAction, TransferRole, TransferSessionState,
    TransferUiSnapshot,
};
use altersend_p2p::{EngineEvent, TransferOrchestrator};
use tokio::sync::{mpsc, Mutex, RwLock};

pub struct AlterSendEngine {
    state: Arc<RwLock<TransferSessionState>>,
    orchestrator: Arc<Mutex<Option<TransferOrchestrator>>>,
    storage_dir: PathBuf,
}

impl AlterSendEngine {
    pub fn new(storage_dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&storage_dir).map_err(|e| e.to_string())?;
        Ok(Self {
            state: Arc::new(RwLock::new(TransferSessionState::default())),
            orchestrator: Arc::new(Mutex::new(None)),
            storage_dir,
        })
    }

    pub async fn snapshot(&self) -> TransferUiSnapshot {
        build_ui_snapshot(&*self.state.read().await)
    }

    async fn dispatch(&self, action: TransferAction) {
        let current = self.state.read().await.clone();
        *self.state.write().await = transfer_session_reducer(current, action);
    }

    pub async fn boot(&self) -> Result<(), String> {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let download_dir = self.storage_dir.join("downloads");
        std::fs::create_dir_all(&download_dir).ok();

        let mut orchestrator = TransferOrchestrator::new(event_tx, download_dir)
            .await
            .map_err(|e| e.to_string())?;

        let state = self.state.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                apply_engine_event(&state, event).await;
            }
        });

        let orch_slot = self.orchestrator.clone();
        tokio::spawn(async move {
            loop {
                let mut guard = orch_slot.lock().await;
                if let Some(ref mut o) = *guard {
                    o.run_swarm_loop().await;
                } else {
                    drop(guard);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        });

        *self.orchestrator.lock().await = Some(orchestrator);
        self.dispatch(TransferAction::Booted).await;
        Ok(())
    }

    pub async fn host(&self) -> Result<String, String> {
        let mut guard = self.orchestrator.lock().await;
        let orch = guard.as_mut().ok_or("Engine not booted")?;
        let topic = orch.host().await?;
        self.dispatch(TransferAction::SessionHosted { topic: topic.clone() })
            .await;
        Ok(topic)
    }

    pub async fn join(&self, topic: &str) -> Result<(), String> {
        self.dispatch(TransferAction::JoinRequested).await;
        let mut guard = self.orchestrator.lock().await;
        let orch = guard.as_mut().ok_or("Engine not booted")?;
        orch.join(topic).await
    }

    pub async fn share_files(&self, paths: Vec<String>) -> Result<u32, String> {
        self.dispatch(TransferAction::ShareRequested).await;
        let mut guard = self.orchestrator.lock().await;
        let orch = guard.as_mut().ok_or("Engine not booted")?;
        orch.share_files(&paths).await
    }

    pub async fn download_all(&self) -> Result<(), String> {
        let state = self.state.read().await.clone();
        let requests: Vec<(String, String, u64)> = state
            .incoming_file_offers
            .iter()
            .map(|f| (f.id.clone(), f.name.clone(), f.size))
            .collect();
        let mut guard = self.orchestrator.lock().await;
        let orch = guard.as_mut().ok_or("Engine not booted")?;
        orch.download_files(requests).await
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        self.dispatch(TransferAction::ClearSession).await;
        let mut guard = self.orchestrator.lock().await;
        if let Some(orch) = guard.as_mut() {
            orch.disconnect().await?;
        }
        Ok(())
    }

    pub async fn add_selected_files(&self, files: Vec<SelectedFile>) {
        self.dispatch(TransferAction::AddSelectedFiles { files }).await;
    }

    pub async fn remove_selected_file(&self, path: String) {
        self.dispatch(TransferAction::RemoveSelectedFile { path })
            .await;
    }

    pub async fn continue_share(&self) -> Result<(), String> {
        let files = self.state.read().await.selected_files.clone();
        if files.is_empty() {
            return Ok(());
        }
        self.dispatch(TransferAction::InitUploadItems {
            items: create_initial_upload_items(&files),
        })
        .await;
        self.dispatch(TransferAction::SetDraftPhase {
            phase: SendDraftPhase::Preparing,
        })
        .await;
        let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
        self.host().await?;
        let count = self.share_files(paths).await?;
        if count > 0 {
            self.dispatch(TransferAction::CompleteAllUploads).await;
            self.dispatch(TransferAction::SetDraftPhase {
                phase: SendDraftPhase::Ready,
            })
            .await;
        }
        Ok(())
    }
}

async fn apply_engine_event(state: &RwLock<TransferSessionState>, event: EngineEvent) {
    let action = match event {
        EngineEvent::Ready => TransferAction::Booted,
        EngineEvent::BootFailed { message } => TransferAction::BootFailed { message },
        EngineEvent::Status { state: s, peers, peer } => match s.as_str() {
            "joining" => TransferAction::StatusChanged {
                state: ConnectionState::Joining,
                peers,
            },
            "joined" => TransferAction::StatusChanged {
                state: ConnectionState::Joined,
                peers,
            },
            "peer-connected" => TransferAction::StatusChanged {
                state: ConnectionState::PeerConnected,
                peers,
            },
            "disconnected" => TransferAction::StatusChanged {
                state: ConnectionState::Disconnected,
                peers,
            },
            "peer-disconnected" => {
                if let Some(peer_key) = peer {
                    let current = state.read().await.clone();
                    *state.write().await = transfer_session_reducer(
                        current,
                        TransferAction::PeerLeft { peer_key },
                    );
                }
                return;
            }
            _ => return,
        },
        EngineEvent::Role { role } => TransferAction::RoleChanged {
            role: role.as_deref().map(|r| {
                if r == "sender" {
                    TransferRole::Sender
                } else {
                    TransferRole::Receiver
                }
            }),
        },
        EngineEvent::Error { message } => TransferAction::SetError { message },
        EngineEvent::TransferReady { files } => TransferAction::TransferReady { files },
        EngineEvent::TransferStart { .. } => return,
        EngineEvent::DownloadStatus {
            state: s,
            file_name,
            bytes_transferred,
            total_bytes,
            file_id,
            saved_to,
            message,
        } => {
            if s == "sharing" {
                if let (Some(name), Some(bytes), Some(total)) =
                    (file_name, bytes_transferred, total_bytes)
                {
                    TransferAction::ApplySharingProgress {
                        event: SharingStatusEvent {
                            file_name: name,
                            bytes_transferred: bytes,
                            total_bytes: total,
                        },
                    }
                } else {
                    return;
                }
            } else {
                TransferAction::ReceiveDownloadEvent {
                    event: ReceiveDownloadStatusEvent {
                        state: s,
                        file_id,
                        file_name,
                        bytes_transferred,
                        total_bytes,
                        saved_to,
                        message,
                    },
                }
            }
        }
        EngineEvent::PeerDownload {
            state,
            file_id,
            file_name,
            bytes_transferred,
            total_bytes,
            saved_to,
            message,
            peer,
        } => TransferAction::PeerDownloadEvent {
            event: PeerDownloadStatusEvent {
                state,
                file_id: Some(file_id),
                file_name: Some(file_name),
                bytes_transferred: Some(bytes_transferred),
                total_bytes: Some(total_bytes),
                saved_to,
                message,
                peer: Some(peer),
            },
        },
    };
    let current = state.read().await.clone();
    *state.write().await = transfer_session_reducer(current, action);
}
