use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use altersend_domain::{
    build_ui_snapshot, create_initial_upload_items, transfer_session_reducer,
    ConnectionState, PeerDownloadStatusEvent, ReceiveDownloadStatusEvent, SaveDestination,
    SelectedFile, SendDraftPhase, SharingStatusEvent, TransferAction, TransferRole,
    TransferSessionState, TransferUiSnapshot,
};
use altersend_p2p::{DownloadRequest, EngineEvent, TransferOrchestrator};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

pub struct AlterSendEngine {
    state: Arc<RwLock<TransferSessionState>>,
    orchestrator: Arc<Mutex<Option<TransferOrchestrator>>>,
    storage_dir: PathBuf,
    _watchdog: Mutex<Option<JoinHandle<()>>>,
}

impl AlterSendEngine {
    pub fn new(storage_dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&storage_dir).map_err(|e| e.to_string())?;
        Ok(Self {
            state: Arc::new(RwLock::new(TransferSessionState::default())),
            orchestrator: Arc::new(Mutex::new(None)),
            storage_dir,
            _watchdog: Mutex::new(None),
        })
    }

    pub async fn snapshot(&self) -> TransferUiSnapshot {
        build_ui_snapshot(&*self.state.read().await)
    }

    pub async fn session_state_json(&self) -> Result<String, String> {
        let state = self.state.read().await.clone();
        serde_json::to_string(&state).map_err(|e| e.to_string())
    }

    pub async fn can_join_from_deep_link(&self, code: &str) -> bool {
        let state = self.state.read().await;
        altersend_domain::can_join_from_deep_link(&state, code)
    }

    async fn dispatch(&self, action: TransferAction) {
        let current = self.state.read().await.clone();
        *self.state.write().await = transfer_session_reducer(current, action);
    }

    pub async fn boot(&self) -> Result<(), String> {
        let download_dir = self.storage_dir.join("downloads");
        std::fs::create_dir_all(&download_dir).ok();

        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let orchestrator = TransferOrchestrator::new(event_tx, download_dir)
            .await
            .map_err(|e| e.to_string())?;

        let state = self.state.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                apply_engine_event(&state, event).await;
            }
        });

        *self.orchestrator.lock().await = Some(orchestrator);
        self.dispatch(TransferAction::Booted).await;

        let watchdog_state = self.state.clone();
        let orch = self.orchestrator.clone();
        let handle = tokio::spawn(async move {
            peer_watchdog_loop(watchdog_state, orch).await;
        });
        *self._watchdog.lock().await = Some(handle);

        Ok(())
    }

    async fn orchestrator(&self) -> Result<tokio::sync::MutexGuard<'_, Option<TransferOrchestrator>>, String> {
        // Can't return guard easily - use helper methods instead
        Err("internal".into())
    }

    pub async fn host(&self) -> Result<String, String> {
        let orch = self.orchestrator.lock().await;
        let orch = orch.as_ref().ok_or("Engine not booted")?;
        let topic = orch.host().await?;
        drop(orch);
        self.dispatch(TransferAction::SessionHosted { topic: topic.clone() })
            .await;
        Ok(topic)
    }

    pub async fn join(&self, topic: &str) -> Result<(), String> {
        self.dispatch(TransferAction::JoinRequested).await;
        let orch = self.orchestrator.lock().await;
        let orch = orch.as_ref().ok_or("Engine not booted")?;
        orch.join(topic).await
    }

    pub async fn share_files(&self, paths: Vec<String>) -> Result<u32, String> {
        self.dispatch(TransferAction::ShareRequested).await;
        let orch = self.orchestrator.lock().await;
        let orch = orch.as_ref().ok_or("Engine not booted")?;
        orch.share_files(&paths).await
    }

    pub async fn download_all(&self) -> Result<(), String> {
        let state = self.state.read().await.clone();
        let requests: Vec<DownloadRequest> = state
            .incoming_file_offers
            .iter()
            .map(|f| DownloadRequest {
                file_id: f.id.clone(),
                file_name: f.name.clone(),
                total_bytes: f.size,
            })
            .collect();
        let orch = self.orchestrator.lock().await;
        let orch = orch.as_ref().ok_or("Engine not booted")?;
        orch.download_files(requests).await
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        self.dispatch(TransferAction::ClearSession).await;
        let orch = self.orchestrator.lock().await;
        if let Some(orch) = orch.as_ref() {
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

    pub async fn route_download(
        &self,
        offer_key: String,
        destination: SaveDestination,
        intended_destination: SaveDestination,
        saved_to: String,
    ) {
        self.dispatch(TransferAction::DownloadRouted {
            offer_key,
            destination,
            intended_destination,
            saved_to: Some(saved_to),
        })
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

async fn peer_watchdog_loop(
    state: Arc<RwLock<TransferSessionState>>,
    orchestrator: Arc<Mutex<Option<TransferOrchestrator>>>,
) {
    let mut deadline: Option<tokio::time::Instant> = None;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let s = state.read().await.clone();
        let should_watch =
            s.role == Some(TransferRole::Receiver) && s.peer_count == 0 && !s.topic.is_empty();

        if !should_watch {
            deadline = None;
            continue;
        }

        let timeout = if s.is_reconnecting || !s.incoming_file_offers.is_empty() {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(60)
        };

        if deadline.is_none() {
            deadline = Some(tokio::time::Instant::now() + timeout);
        }

        if let Some(d) = deadline {
            if tokio::time::Instant::now() >= d {
                let current = state.read().await.clone();
                *state.write().await =
                    transfer_session_reducer(current, TransferAction::PeerUnreachable);
                if let Some(orch) = orchestrator.lock().await.as_ref() {
                    let _ = orch.disconnect().await;
                }
                deadline = None;
            }
        }
    }
}

async fn apply_engine_event(state: &RwLock<TransferSessionState>, event: EngineEvent) {
    match event {
        EngineEvent::Ready => {
            let current = state.read().await.clone();
            *state.write().await = transfer_session_reducer(current, TransferAction::Booted);
        }
        EngineEvent::BootFailed { message } => {
            let current = state.read().await.clone();
            *state.write().await =
                transfer_session_reducer(current, TransferAction::BootFailed { message });
        }
        EngineEvent::Status { state: s, peers, peer } => {
            if s == "peer-disconnected" {
                if let Some(peer_key) = peer {
                    let current = state.read().await.clone();
                    *state.write().await = transfer_session_reducer(
                        current,
                        TransferAction::PeerLeft { peer_key },
                    );
                }
                return;
            }
            if s == "peer-connected" {
                let current = state.read().await.clone();
                let next = transfer_session_reducer(
                    current,
                    TransferAction::StatusChanged {
                        state: ConnectionState::PeerConnected,
                        peers,
                    },
                );
                *state.write().await = next;
                if let Some(peer_key) = peer {
                    let current = state.read().await.clone();
                    *state.write().await = transfer_session_reducer(
                        current,
                        TransferAction::PeerJoined { peer_key },
                    );
                }
                return;
            }
            let conn = match s.as_str() {
                "joining" => ConnectionState::Joining,
                "joined" => ConnectionState::Joined,
                "disconnected" => ConnectionState::Disconnected,
                _ => return,
            };
            let current = state.read().await.clone();
            *state.write().await = transfer_session_reducer(
                current,
                TransferAction::StatusChanged { state: conn, peers },
            );
        }
        EngineEvent::Role { role } => {
            let current = state.read().await.clone();
            *state.write().await = transfer_session_reducer(
                current,
                TransferAction::RoleChanged {
                    role: role.as_deref().map(|r| {
                        if r == "sender" {
                            TransferRole::Sender
                        } else {
                            TransferRole::Receiver
                        }
                    }),
                },
            );
        }
        EngineEvent::Error { message } => {
            let current = state.read().await.clone();
            *state.write().await =
                transfer_session_reducer(current, TransferAction::SetError { message });
        }
        EngineEvent::TransferReady { files } => {
            let current = state.read().await.clone();
            *state.write().await =
                transfer_session_reducer(current, TransferAction::TransferReady { files });
        }
        EngineEvent::TransferStart { .. } => {}
        EngineEvent::DownloadStatus {
            state: s,
            file_name,
            bytes_transferred,
            total_bytes,
            file_id,
            saved_to,
            message,
        } => {
            let action = if s == "sharing" {
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
            };
            let current = state.read().await.clone();
            *state.write().await = transfer_session_reducer(current, action);
        }
        EngineEvent::PeerDownload {
            state: download_state,
            file_id,
            file_name,
            bytes_transferred,
            total_bytes,
            saved_to,
            message,
            peer,
        } => {
            let current = state.read().await.clone();
            *state.write().await = transfer_session_reducer(
                current,
                TransferAction::PeerDownloadEvent {
                    event: PeerDownloadStatusEvent {
                        state: download_state,
                        file_id: Some(file_id),
                        file_name: Some(file_name),
                        bytes_transferred: Some(bytes_transferred),
                        total_bytes: Some(total_bytes),
                        saved_to,
                        message,
                        peer: Some(peer),
                    },
                },
            );
        }
    }
}
