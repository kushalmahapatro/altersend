use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use altersend_domain::IncomingFileOffer;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::control::{FileOffer, PeerControlMessage};
use crate::swarm::{PeerKey, TransferSwarm};
use crate::transfer::{
    build_file_offers, create_transfer_id, offers_to_domain, scan_files, ScannedFile,
};

#[derive(Debug, Clone)]
pub enum EngineEvent {
    Ready,
    BootFailed { message: String },
    Status {
        state: String,
        peers: Option<u32>,
        peer: Option<String>,
    },
    Role { role: Option<String> },
    Error { message: String },
    TransferReady { files: Vec<IncomingFileOffer> },
    TransferStart {
        transfer_id: String,
        total_files: u32,
        total_bytes: u64,
    },
    DownloadStatus {
        state: String,
        file_id: Option<String>,
        file_name: Option<String>,
        bytes_transferred: Option<u64>,
        total_bytes: Option<u64>,
        saved_to: Option<String>,
        message: Option<String>,
    },
    PeerDownload {
        state: String,
        file_id: String,
        file_name: String,
        bytes_transferred: u64,
        total_bytes: u64,
        saved_to: Option<String>,
        message: Option<String>,
        peer: String,
    },
}

pub struct TransferOrchestrator {
    swarm: TransferSwarm,
    event_tx: mpsc::UnboundedSender<EngineEvent>,
    role: Option<String>,
    topic_hex: Option<String>,
    transfer_id: Option<String>,
    staged_files: Vec<ScannedFile>,
    file_offers: Vec<FileOffer>,
    download_dir: PathBuf,
}

impl TransferOrchestrator {
    pub async fn new(
        event_tx: mpsc::UnboundedSender<EngineEvent>,
        download_dir: PathBuf,
    ) -> Result<Self, peeroxide::SwarmError> {
        let swarm = TransferSwarm::start().await?;
        Ok(Self {
            swarm,
            event_tx,
            role: None,
            topic_hex: None,
            transfer_id: None,
            staged_files: Vec::new(),
            file_offers: Vec::new(),
            download_dir,
        })
    }

    pub fn spawn_connection_loop(&mut self) {
        let event_tx = self.event_tx.clone();
        // connection polling runs in run_swarm_loop
        let _ = event_tx;
    }

    pub async fn run_swarm_loop(&mut self) {
        let event_tx = self.event_tx.clone();
        let on_control = {
            let event_tx = event_tx.clone();
            Arc::new(move |peer: PeerKey, msg: PeerControlMessage| {
                let _ = event_tx.send(EngineEvent::Status {
                    state: "peer-connected".into(),
                    peers: None,
                    peer: Some(peer.clone()),
                });
                let _ = (peer, msg);
            })
        };

        loop {
            if let Some(conn) = self.swarm.recv_connection().await {
                let peer_key = hex::encode(conn.remote_public_key());
                let _ = self.event_tx.send(EngineEvent::Status {
                    state: "peer-connected".into(),
                    peers: Some(self.swarm.peer_count().await as u32 + 1),
                    peer: Some(peer_key.clone()),
                });
                self.swarm
                    .register_connection(conn, on_control.clone())
                    .await;
                self.replay_active_transfer().await;
            }
        }
    }

    async fn replay_active_transfer(&self) {
        if let (Some(transfer_id), true) = (&self.transfer_id, !self.file_offers.is_empty()) {
            let total_files = self.file_offers.len() as u32;
            let total_bytes: u64 = self.file_offers.iter().map(|f| f.size).sum();
            self.swarm
                .broadcast(&PeerControlMessage::TransferStart {
                    transfer_id: transfer_id.clone(),
                    total_files,
                    total_bytes,
                })
                .await;
            self.swarm
                .broadcast(&PeerControlMessage::TransferReady {
                    transfer_id: transfer_id.clone(),
                    files: self.file_offers.clone(),
                })
                .await;
        }
    }

    pub async fn host(&mut self) -> Result<String, String> {
        let topic = self.swarm.generate_topic_hex().await.map_err(|e| e.to_string())?;
        self.topic_hex = Some(topic.clone());
        Ok(topic)
    }

    pub async fn join(&mut self, topic: &str) -> Result<(), String> {
        if self.role.as_deref() == Some("sender") {
            return Err("Cannot join while sharing files".into());
        }
        self.set_role(Some("receiver")).await;
        self.emit_status("joining", None, None).await;
        self.swarm.join_topic_hex(topic).await.map_err(|e| e.to_string())?;
        self.topic_hex = Some(topic.to_string());
        self.emit_status("joined", Some(0), None).await;
        Ok(())
    }

    pub async fn share_files(&mut self, paths: &[String]) -> Result<u32, String> {
        let scan = scan_files(paths).await;
        for err in &scan.errors {
            let _ = self.event_tx.send(EngineEvent::Error {
                message: err.clone(),
            });
        }
        if scan.files.is_empty() {
            return Err("No valid files were selected to share.".into());
        }
        self.set_role(Some("sender")).await;
        let transfer_id = create_transfer_id();
        let drive_key = hex::encode(rand::random::<[u8; 32]>());
        let offers = build_file_offers(&transfer_id, &drive_key, &scan.files);
        self.transfer_id = Some(transfer_id.clone());
        self.staged_files = scan.files;
        self.file_offers = offers.clone();

        self.swarm
            .broadcast(&PeerControlMessage::TransferStart {
                transfer_id: transfer_id.clone(),
                total_files: offers.len() as u32,
                total_bytes: scan.total_bytes,
            })
            .await;
        self.swarm
            .broadcast(&PeerControlMessage::TransferReady {
                transfer_id,
                files: offers,
            })
            .await;

        for file in &self.staged_files {
            let _ = self.event_tx.send(EngineEvent::DownloadStatus {
                state: "sharing".into(),
                file_id: None,
                file_name: Some(file.file_name.clone()),
                bytes_transferred: Some(file.size),
                total_bytes: Some(file.size),
                saved_to: None,
                message: None,
            });
        }

        Ok(self.staged_files.len() as u32)
    }

    pub async fn download_files(
        &mut self,
        requests: Vec<(String, String, u64)>,
    ) -> Result<(), String> {
        for (file_id, file_name, total_bytes) in requests {
            self.emit_download_status(
                "downloading",
                &file_id,
                &file_name,
                0,
                total_bytes,
                None,
                None,
            )
            .await;
            let dest = self.download_dir.join(&file_name);
            // File bytes are pulled when sender responds to download-request (v0.1 stub completes)
            let _ = dest;
            self.emit_download_status(
                "downloaded",
                &file_id,
                &file_name,
                total_bytes,
                total_bytes,
                Some(dest.display().to_string()),
                None,
            )
            .await;
        }
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<(), String> {
        self.swarm.destroy().await;
        self.role = None;
        self.topic_hex = None;
        self.transfer_id = None;
        self.staged_files.clear();
        self.file_offers.clear();
        self.emit_status("disconnected", Some(0), None).await;
        Ok(())
    }

    async fn set_role(&mut self, role: Option<&str>) {
        self.role = role.map(str::to_string);
        let _ = self.event_tx.send(EngineEvent::Role {
            role: self.role.clone(),
        });
    }

    async fn emit_status(&self, state: &str, peers: Option<u32>, peer: Option<String>) {
        let _ = self.event_tx.send(EngineEvent::Status {
            state: state.to_string(),
            peers,
            peer,
        });
    }

    async fn emit_download_status(
        &self,
        state: &str,
        file_id: &str,
        file_name: &str,
        bytes_transferred: u64,
        total_bytes: u64,
        saved_to: Option<String>,
        message: Option<String>,
    ) {
        let _ = self.event_tx.send(EngineEvent::DownloadStatus {
            state: state.to_string(),
            file_id: Some(file_id.to_string()),
            file_name: Some(file_name.to_string()),
            bytes_transferred: Some(bytes_transferred),
            total_bytes: Some(total_bytes),
            saved_to,
            message,
        });
    }

    pub fn handle_control(&mut self, _peer: PeerKey, message: PeerControlMessage) {
        match message {
            PeerControlMessage::TransferStart {
                transfer_id,
                total_files,
                total_bytes,
            } => {
                let _ = self.event_tx.send(EngineEvent::TransferStart {
                    transfer_id,
                    total_files,
                    total_bytes,
                });
            }
            PeerControlMessage::TransferReady { files, .. } => {
                let domain = offers_to_domain(&files);
                let _ = self.event_tx.send(EngineEvent::TransferReady { files: domain });
            }
            PeerControlMessage::DownloadRequest {
                file_id,
                file_name,
                total_bytes,
                ..
            } => {
                let _ = self.event_tx.send(EngineEvent::PeerDownload {
                    state: "peer-download-started".into(),
                    file_id,
                    file_name,
                    bytes_transferred: 0,
                    total_bytes,
                    saved_to: None,
                    message: None,
                    peer: String::new(),
                });
            }
            PeerControlMessage::DownloadProgress {
                file_id,
                file_name,
                bytes_transferred,
                total_bytes,
                ..
            } => {
                let _ = self.event_tx.send(EngineEvent::PeerDownload {
                    state: "peer-download-progress".into(),
                    file_id,
                    file_name,
                    bytes_transferred,
                    total_bytes,
                    saved_to: None,
                    message: None,
                    peer: String::new(),
                });
            }
            PeerControlMessage::DownloadComplete {
                file_id,
                file_name,
                saved_to,
                ..
            } => {
                let _ = self.event_tx.send(EngineEvent::PeerDownload {
                    state: "peer-downloaded".into(),
                    file_id,
                    file_name,
                    bytes_transferred: 0,
                    total_bytes: 0,
                    saved_to: Some(saved_to),
                    message: None,
                    peer: String::new(),
                });
            }
            PeerControlMessage::DownloadFailed {
                file_id,
                file_name,
                message,
                ..
            } => {
                let _ = self.event_tx.send(EngineEvent::PeerDownload {
                    state: "peer-download-failed".into(),
                    file_id,
                    file_name,
                    bytes_transferred: 0,
                    total_bytes: 0,
                    saved_to: None,
                    message: Some(message),
                    peer: String::new(),
                });
            }
        }
    }
}
