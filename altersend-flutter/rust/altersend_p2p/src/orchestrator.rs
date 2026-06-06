use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use altersend_domain::IncomingFileOffer;
use altersend_storage::{OutgoingDrive, ReplicationRegistry};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::control::{FileOffer, PeerControlMessage};
use crate::identity::PeerIdentityStore;
use crate::peer_mode::PeerInteropMode;
use crate::swarm::{SwarmHandleClient, SwarmInbound, SwarmRuntime};
use crate::transfer::{
    build_file_offers, create_transfer_id, is_safe_file_name, offers_to_domain, scan_files,
    IncomingDownload, ScannedFile,
};
use crate::wire::{encode_control_frame, WireFrame};

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

pub struct DownloadRequest {
    pub file_id: String,
    pub file_name: String,
    pub total_bytes: u64,
}

enum OrchestratorCommand {
    Host(oneshot::Sender<Result<String, String>>),
    Join(String, oneshot::Sender<Result<(), String>>),
    ShareFiles(Vec<String>, oneshot::Sender<Result<u32, String>>),
    DownloadFiles(Vec<DownloadRequest>, oneshot::Sender<Result<(), String>>),
    Disconnect(oneshot::Sender<Result<(), String>>),
}

struct OrchestratorState {
    role: Option<String>,
    topic_hex: Option<String>,
    transfer_id: Option<String>,
    staged_files: Vec<ScannedFile>,
    file_offers: Vec<FileOffer>,
    downloads: HashMap<String, IncomingDownload>,
    connected_peers: HashSet<String>,
    peer_modes: HashMap<String, PeerInteropMode>,
    drive: Option<Arc<Mutex<OutgoingDrive>>>,
    drive_key: Option<String>,
}

pub struct TransferOrchestrator {
    cmd_tx: mpsc::UnboundedSender<OrchestratorCommand>,
    _task: JoinHandle<()>,
}

impl TransferOrchestrator {
    pub async fn new(
        event_tx: mpsc::UnboundedSender<EngineEvent>,
        storage_root: PathBuf,
        identity_root: PathBuf,
        download_dir: PathBuf,
    ) -> Result<Self, peeroxide::SwarmError> {
        let (orch_tx, mut orch_rx) = mpsc::unbounded_channel();
        let (swarm_inbound_tx, mut swarm_inbound_rx) = mpsc::unbounded_channel();

        let replication_registry = ReplicationRegistry::new();
        let identity_store = Arc::new(PeerIdentityStore::new(identity_root));
        let swarm = SwarmRuntime::start(swarm_inbound_tx, replication_registry.clone()).await?;
        let swarm_client = swarm.client.clone();

        let state = Arc::new(Mutex::new(OrchestratorState {
            role: None,
            topic_hex: None,
            transfer_id: None,
            staged_files: Vec::new(),
            file_offers: Vec::new(),
            downloads: HashMap::new(),
            connected_peers: HashSet::new(),
            peer_modes: HashMap::new(),
            drive: None,
            drive_key: None,
        }));

        let event_tx_loop = event_tx.clone();
        let swarm_for_loop = swarm_client.clone();
        let drive_dir = storage_root.join("outgoing-drive");
        let identity_for_loop = identity_store.clone();

        let task = tokio::spawn(async move {
            let mut swarm_client = swarm_for_loop;
            loop {
                tokio::select! {
                    inbound = swarm_inbound_rx.recv() => {
                        let Some(inbound) = inbound else { break };
                        handle_swarm_inbound(
                            &state,
                            &swarm_client,
                            &event_tx_loop,
                            &download_dir,
                            &replication_registry,
                            inbound,
                        ).await;
                    }
                    cmd = orch_rx.recv() => {
                        let Some(cmd) = cmd else { break };
                        handle_command(
                            &state,
                            &mut swarm_client,
                            &event_tx_loop,
                            &download_dir,
                            &drive_dir,
                            &replication_registry,
                            &identity_for_loop,
                            cmd,
                        ).await;
                    }
                }
            }
        });

        let _ = event_tx.send(EngineEvent::Ready);

        Ok(Self {
            cmd_tx: orch_tx,
            _task: task,
        })
    }

    async fn send_cmd<T>(&self, build: impl FnOnce(oneshot::Sender<T>) -> OrchestratorCommand) -> Result<T, String> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(build(tx))
            .map_err(|_| "orchestrator stopped".to_string())?;
        rx.await.map_err(|_| "orchestrator stopped".to_string())
    }

    pub async fn host(&self) -> Result<String, String> {
        self.send_cmd(|reply| OrchestratorCommand::Host(reply)).await?
    }

    pub async fn join(&self, topic: &str) -> Result<(), String> {
        self.send_cmd(|reply| OrchestratorCommand::Join(topic.to_string(), reply))
            .await?
    }

    pub async fn share_files(&self, paths: &[String]) -> Result<u32, String> {
        self.send_cmd(|reply| OrchestratorCommand::ShareFiles(paths.to_vec(), reply))
            .await?
    }

    pub async fn download_files(&self, requests: Vec<DownloadRequest>) -> Result<(), String> {
        self.send_cmd(|reply| OrchestratorCommand::DownloadFiles(requests, reply))
            .await?
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        self.send_cmd(|reply| OrchestratorCommand::Disconnect(reply))
            .await?
    }
}

async fn handle_command(
    state: &Arc<Mutex<OrchestratorState>>,
    swarm: &mut SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    download_dir: &PathBuf,
    drive_dir: &Path,
    replication_registry: &Arc<ReplicationRegistry>,
    identity_store: &Arc<PeerIdentityStore>,
    cmd: OrchestratorCommand,
) {
    match cmd {
        OrchestratorCommand::Host(reply) => {
            let result = async {
                let topic = swarm.generate_topic_hex().await?;
                state.lock().await.topic_hex = Some(topic.clone());
                Ok(topic)
            }
            .await;
            let _ = reply.send(result);
        }
        OrchestratorCommand::Join(topic, reply) => {
            let result = async {
                let guard = state.lock().await;
                if guard.role.as_deref() == Some("sender") {
                    return Err("Cannot join while sharing files".into());
                }
                drop(guard);
                set_role(state, event_tx, Some("receiver")).await;
                emit_status(event_tx, "joining", None, None).await;
                let key_pair = identity_store
                    .get_or_create(&topic)
                    .await
                    .map_err(|e| e.to_string())?;
                swarm.recreate_with_keypair(key_pair).await?;
                swarm.join_topic_hex(&topic).await?;
                state.lock().await.topic_hex = Some(topic);
                emit_status(event_tx, "joined", Some(0), None).await;
                Ok(())
            }
            .await;
            if result.is_err() {
                set_role(state, event_tx, None).await;
            }
            let _ = reply.send(result);
        }
        OrchestratorCommand::ShareFiles(paths, reply) => {
            let result = share_files(
                state,
                swarm,
                event_tx,
                drive_dir,
                replication_registry,
                paths,
            )
            .await;
            let _ = reply.send(result);
        }
        OrchestratorCommand::DownloadFiles(requests, reply) => {
            let result =
                download_files(state, swarm, event_tx, download_dir, requests).await;
            let _ = reply.send(result);
        }
        OrchestratorCommand::Disconnect(reply) => {
            let result = disconnect(state, swarm, event_tx, drive_dir, replication_registry).await;
            let _ = reply.send(result);
        }
    }
}

async fn handle_swarm_inbound(
    state: &Arc<Mutex<OrchestratorState>>,
    swarm: &SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    download_dir: &PathBuf,
    replication_registry: &Arc<ReplicationRegistry>,
    inbound: SwarmInbound,
) {
    match inbound {
        SwarmInbound::PeerConnected(peer) => {
            state.lock().await.connected_peers.insert(peer.clone());
            register_drive_for_peer(state, replication_registry, &peer).await;
            let count = swarm.peer_count().await as u32;
            emit_status(event_tx, "peer-connected", Some(count), Some(peer.clone())).await;
            replay_active_transfer(state, swarm).await;
        }
        SwarmInbound::PeerInteropMode(peer, mode) => {
            if mode != PeerInteropMode::Unknown {
                state.lock().await.peer_modes.insert(peer, mode);
            }
        }
        SwarmInbound::PeerDisconnected(peer) => {
            {
                let mut guard = state.lock().await;
                guard.connected_peers.remove(&peer);
                guard.peer_modes.remove(&peer);
            }
            replication_registry.clear_peer(&peer).await;
            let count = swarm.peer_count().await as u32;
            let _ = event_tx.send(EngineEvent::Status {
                state: "peer-disconnected".into(),
                peers: Some(count),
                peer: Some(peer),
            });
            if count == 0 {
                emit_status(event_tx, "joined", Some(0), None).await;
            } else {
                emit_status(event_tx, "peer-connected", Some(count), None).await;
            }
        }
        SwarmInbound::Frame(peer, frame) => {
            handle_frame(state, swarm, event_tx, download_dir, peer, frame).await;
        }
    }
}

async fn handle_frame(
    state: &Arc<Mutex<OrchestratorState>>,
    swarm: &SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    download_dir: &PathBuf,
    peer: String,
    frame: WireFrame,
) {
    match frame {
        WireFrame::Control(msg) => {
            handle_control(state, swarm, event_tx, download_dir, peer, msg).await;
        }
        WireFrame::FileChunk {
            file_id,
            offset,
            data,
        } => {
            handle_file_chunk(state, event_tx, &file_id, offset, data).await;
        }
    }
}

async fn handle_control(
    state: &Arc<Mutex<OrchestratorState>>,
    swarm: &SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    download_dir: &PathBuf,
    peer: String,
    message: PeerControlMessage,
) {
    match message {
        PeerControlMessage::TransferStart {
            transfer_id,
            total_files,
            total_bytes,
        } => {
            let guard = state.lock().await;
            if guard.role.as_deref() != Some("receiver") {
                return;
            }
            drop(guard);
            let _ = event_tx.send(EngineEvent::TransferStart {
                transfer_id,
                total_files,
                total_bytes,
            });
        }
        PeerControlMessage::TransferReady { files, .. } => {
            let mut guard = state.lock().await;
            if guard.role.as_deref() != Some("receiver") {
                return;
            }
            guard.file_offers = files.clone();
            drop(guard);
            let domain = offers_to_domain(&files);
            let _ = event_tx.send(EngineEvent::TransferReady { files: domain });
        }
        PeerControlMessage::DownloadRequest {
            transfer_id,
            file_id,
            file_name,
            total_bytes,
            ..
        } => {
            let guard = state.lock().await;
            if guard.role.as_deref() != Some("sender") {
                return;
            }
            let staged = guard.staged_files.clone();
            let offers = guard.file_offers.clone();
            let drive = guard.drive.clone();
            let peer_mode = guard
                .peer_modes
                .get(&peer)
                .copied()
                .unwrap_or(PeerInteropMode::Unknown);
            drop(guard);

            if peer_mode.uses_hypercore_replication() {
                // Legacy receivers pull file bytes via Hyperdrive / hypercore replication.
                return;
            }

            let _ = event_tx.send(EngineEvent::PeerDownload {
                state: "peer-download-started".into(),
                file_id: file_id.clone(),
                file_name: file_name.clone(),
                bytes_transferred: 0,
                total_bytes,
                saved_to: None,
                message: None,
                peer: peer.clone(),
            });

            let Some(file) = staged.iter().find(|f| f.file_id == file_id) else {
                warn!("download-request for unknown file {file_id}");
                return;
            };

            let Some(drive) = drive else {
                warn!("download-request before drive staged");
                return;
            };

            let display_path = file.input_path.display().to_string();
            let size = file.size;
            let transfer_id_clone = transfer_id.clone();
            let file_id_clone = file_id.clone();
            let file_name_clone = file_name.clone();
            let peer_clone = peer.clone();
            let swarm = swarm.clone();
            let event_tx = event_tx.clone();

            tokio::spawn(async move {
                if let Err(err) = send_file_to_peer(
                    &swarm,
                    &event_tx,
                    &peer_clone,
                    drive,
                    &file_id_clone,
                    &file_name_clone,
                    size,
                    &transfer_id_clone,
                    &display_path,
                )
                .await
                {
                    let fail = PeerControlMessage::DownloadFailed {
                        transfer_id: transfer_id_clone,
                        file_id: file_id_clone,
                        file_name: file_name_clone,
                        message: err,
                    };
                    swarm
                        .broadcast_bytes(encode_control_frame(&fail))
                        .await;
                }
                let _ = offers;
            });
        }
        PeerControlMessage::DownloadProgress {
            transfer_id,
            file_id,
            file_name,
            bytes_transferred,
            total_bytes,
            ..
        } => {
            let guard = state.lock().await;
            if guard.role.as_deref() != Some("receiver") {
                return;
            }
            drop(guard);
            let _ = event_tx.send(EngineEvent::DownloadStatus {
                state: "download-progress".into(),
                file_id: Some(file_id),
                file_name: Some(file_name),
                bytes_transferred: Some(bytes_transferred),
                total_bytes: Some(total_bytes),
                saved_to: None,
                message: None,
            });
            let _ = transfer_id;
        }
        PeerControlMessage::DownloadComplete {
            transfer_id,
            file_id,
            file_name,
            saved_to,
            ..
        } => {
            let mut guard = state.lock().await;
            if guard.role.as_deref() != Some("receiver") {
                return;
            }
            if let Some(dl) = guard.downloads.remove(&file_id) {
                let _ = dl.finish().await;
            }
            drop(guard);
            let _ = event_tx.send(EngineEvent::DownloadStatus {
                state: "downloaded".into(),
                file_id: Some(file_id),
                file_name: Some(file_name),
                bytes_transferred: None,
                total_bytes: None,
                saved_to: Some(saved_to),
                message: None,
            });
            let _ = transfer_id;
        }
        PeerControlMessage::DownloadFailed {
            file_id,
            file_name,
            message,
            ..
        } => {
            let mut guard = state.lock().await;
            guard.downloads.remove(&file_id);
            drop(guard);
            let _ = event_tx.send(EngineEvent::DownloadStatus {
                state: "download-failed".into(),
                file_id: Some(file_id),
                file_name: Some(file_name),
                bytes_transferred: None,
                total_bytes: None,
                saved_to: None,
                message: Some(message),
            });
        }
    }
}

async fn handle_file_chunk(
    state: &Arc<Mutex<OrchestratorState>>,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    file_id: &str,
    offset: u64,
    data: Vec<u8>,
) {
    let mut guard = state.lock().await;
    if guard.role.as_deref() != Some("receiver") {
        return;
    }
    let Some(dl) = guard.downloads.get_mut(file_id) else {
        return;
    };
    let total = dl.total_bytes;
    let name = dl.path.file_name().and_then(|n| n.to_str()).unwrap_or("file").to_string();
    if dl.write_at(offset, &data).await.is_err() {
        return;
    }
    let received = offset + data.len() as u64;
    let path_display = dl.path.display().to_string();
    let done = received >= total && total > 0;
    if done {
        let finished = guard.downloads.remove(file_id);
        drop(guard);
        if let Some(dl) = finished {
            let _ = dl.finish().await;
        }
        let _ = event_tx.send(EngineEvent::DownloadStatus {
            state: "downloaded".into(),
            file_id: Some(file_id.to_string()),
            file_name: Some(name),
            bytes_transferred: Some(total),
            total_bytes: Some(total),
            saved_to: Some(path_display),
            message: None,
        });
        return;
    }
    drop(guard);

    let _ = event_tx.send(EngineEvent::DownloadStatus {
        state: "download-progress".into(),
        file_id: Some(file_id.to_string()),
        file_name: Some(name),
        bytes_transferred: Some(received.min(total)),
        total_bytes: Some(total),
        saved_to: None,
        message: None,
    });
}

async fn send_file_to_peer(
    swarm: &SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    peer: &str,
    drive: Arc<Mutex<OutgoingDrive>>,
    file_id: &str,
    file_name: &str,
    total_size: u64,
    transfer_id: &str,
    display_path: &str,
) -> Result<(), String> {
    use crate::transfer::CHUNK_SIZE;

    let mut offset = 0u64;
    let mut buf = vec![0u8; CHUNK_SIZE];
    while offset < total_size {
        let n = {
            let mut guard = drive.lock().await;
            guard
                .read_file_range(file_id, offset, &mut buf)
                .await
                .map_err(|e| e.to_string())?
        };
        if n == 0 {
            break;
        }
        swarm
            .send_chunk_to_peer(peer, file_id, offset, buf[..n].to_vec())
            .await;
        offset += n as u64;
    }

    let progress = PeerControlMessage::DownloadProgress {
        transfer_id: transfer_id.to_string(),
        file_id: file_id.to_string(),
        file_name: file_name.to_string(),
        bytes_transferred: total_size,
        total_bytes: total_size,
    };
    swarm
        .broadcast_bytes(encode_control_frame(&progress))
        .await;

    let _ = event_tx.send(EngineEvent::PeerDownload {
        state: "peer-downloaded".into(),
        file_id: file_id.to_string(),
        file_name: file_name.to_string(),
        bytes_transferred: total_size,
        total_bytes: total_size,
        saved_to: None,
        message: None,
        peer: peer.to_string(),
    });

    let complete = PeerControlMessage::DownloadComplete {
        transfer_id: transfer_id.to_string(),
        file_id: file_id.to_string(),
        file_name: file_name.to_string(),
        saved_to: display_path.to_string(),
    };
    swarm
        .broadcast_bytes(encode_control_frame(&complete))
        .await;

    Ok(())
}

async fn share_files(
    state: &Arc<Mutex<OrchestratorState>>,
    swarm: &SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    drive_dir: &Path,
    replication_registry: &Arc<ReplicationRegistry>,
    paths: Vec<String>,
) -> Result<u32, String> {
    let scan = scan_files(&paths).await;
    for err in &scan.errors {
        let _ = event_tx.send(EngineEvent::Error {
            message: err.clone(),
        });
    }
    if scan.files.is_empty() {
        return Err("No valid files were selected to share.".into());
    }

    set_role(state, event_tx, Some("sender")).await;

    let transfer_id = create_transfer_id();
    reset_outgoing_drive_dir(drive_dir).await?;

    let session_drive_dir = drive_dir.join(&transfer_id);
    let mut drive = OutgoingDrive::open(&session_drive_dir)
        .await
        .map_err(|e| e.to_string())?;

    for file in &scan.files {
        drive
            .stage_file(
                &file.file_id,
                &file.file_name,
                &format!("/{}", file.file_name),
                &file.input_path,
            )
            .await
            .map_err(|e| e.to_string())?;
    }

    let drive_key = drive.key_hex();
    let drive = Arc::new(Mutex::new(drive));
    replication_registry.set_outgoing_drive(drive.clone()).await;
    let offers = build_file_offers(&transfer_id, &drive_key, &scan.files);

    let peers_to_register = {
        let mut guard = state.lock().await;
        guard.transfer_id = Some(transfer_id.clone());
        guard.staged_files = scan.files.clone();
        guard.file_offers = offers.clone();
        guard.drive = Some(drive.clone());
        guard.drive_key = Some(drive_key.clone());
        guard.connected_peers.iter().cloned().collect::<Vec<_>>()
    };

    for peer in peers_to_register {
        replication_registry
            .set_active_drive(&peer, &drive_key)
            .await;
    }

    let start = PeerControlMessage::TransferStart {
        transfer_id: transfer_id.clone(),
        total_files: offers.len() as u32,
        total_bytes: scan.total_bytes,
    };
    swarm
        .broadcast_bytes(encode_control_frame(&start))
        .await;

    let ready = PeerControlMessage::TransferReady {
        transfer_id: transfer_id.clone(),
        files: offers,
    };
    swarm
        .broadcast_bytes(encode_control_frame(&ready))
        .await;

    for file in &scan.files {
        let _ = event_tx.send(EngineEvent::DownloadStatus {
            state: "sharing".into(),
            file_id: Some(file.file_id.clone()),
            file_name: Some(file.file_name.clone()),
            bytes_transferred: Some(file.size),
            total_bytes: Some(file.size),
            saved_to: None,
            message: None,
        });
    }

    Ok(scan.files.len() as u32)
}

async fn download_files(
    state: &Arc<Mutex<OrchestratorState>>,
    swarm: &SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    download_dir: &PathBuf,
    requests: Vec<DownloadRequest>,
) -> Result<(), String> {
    let (transfer_id, has_legacy_peer) = {
        let guard = state.lock().await;
        let transfer_id = guard
            .transfer_id
            .clone()
            .unwrap_or_else(create_transfer_id);
        let has_legacy_peer = guard
            .peer_modes
            .values()
            .any(|mode| mode.uses_hypercore_replication());
        (transfer_id, has_legacy_peer)
    };

    if has_legacy_peer {
        return Err(
            "Downloading from legacy AlterSend peers requires Hyperdrive replication (in progress)."
                .into(),
        );
    }

    for req in requests {
        if !is_safe_file_name(&req.file_name) {
            return Err(format!("Unsafe file name: {}", req.file_name));
        }

        let dest = download_dir.join(&req.file_name);
        let dl = IncomingDownload::open(dest.clone(), req.total_bytes).await?;

        {
            let mut guard = state.lock().await;
            guard.downloads.insert(req.file_id.clone(), dl);
        }

        emit_download(
            event_tx,
            "downloading",
            &req.file_id,
            &req.file_name,
            0,
            req.total_bytes,
            None,
            None,
        )
        .await;

        let ctrl = PeerControlMessage::DownloadRequest {
            transfer_id: transfer_id.clone(),
            file_id: req.file_id.clone(),
            file_name: req.file_name.clone(),
            path: format!("/{}", req.file_name),
            total_bytes: req.total_bytes,
        };
        swarm
            .broadcast_bytes(encode_control_frame(&ctrl))
            .await;
    }

    Ok(())
}

async fn disconnect(
    state: &Arc<Mutex<OrchestratorState>>,
    swarm: &SwarmHandleClient,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    drive_dir: &Path,
    replication_registry: &Arc<ReplicationRegistry>,
) -> Result<(), String> {
    swarm.end_session().await;
    replication_registry.clear_all().await;
    let mut guard = state.lock().await;
    guard.role = None;
    guard.topic_hex = None;
    guard.transfer_id = None;
    guard.staged_files.clear();
    guard.file_offers.clear();
    guard.downloads.clear();
    guard.connected_peers.clear();
    guard.peer_modes.clear();
    guard.drive = None;
    guard.drive_key = None;
    drop(guard);
    reset_outgoing_drive_dir(drive_dir).await?;
    set_role(state, event_tx, None).await;
    emit_status(event_tx, "disconnected", Some(0), None).await;
    Ok(())
}

async fn register_drive_for_peer(
    state: &Arc<Mutex<OrchestratorState>>,
    replication_registry: &Arc<ReplicationRegistry>,
    peer: &str,
) {
    let guard = state.lock().await;
    if guard.role.as_deref() != Some("sender") {
        return;
    }
    let Some(drive_key) = guard.drive_key.clone() else {
        return;
    };
    drop(guard);
    replication_registry
        .set_active_drive(peer, &drive_key)
        .await;
}

async fn reset_outgoing_drive_dir(drive_dir: &Path) -> Result<(), String> {
    if drive_dir.exists() {
        tokio::fs::remove_dir_all(drive_dir)
            .await
            .map_err(|e| e.to_string())?;
    }
    tokio::fs::create_dir_all(drive_dir)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn replay_active_transfer(state: &Arc<Mutex<OrchestratorState>>, swarm: &SwarmHandleClient) {
    let guard = state.lock().await;
    let Some(transfer_id) = guard.transfer_id.clone() else {
        return;
    };
    if guard.file_offers.is_empty() {
        return;
    }
    let total_files = guard.file_offers.len() as u32;
    let total_bytes: u64 = guard.file_offers.iter().map(|f| f.size).sum();
    let offers = guard.file_offers.clone();
    drop(guard);

    let start = PeerControlMessage::TransferStart {
        transfer_id: transfer_id.clone(),
        total_files,
        total_bytes,
    };
    swarm
        .broadcast_bytes(encode_control_frame(&start))
        .await;
    let ready = PeerControlMessage::TransferReady {
        transfer_id,
        files: offers,
    };
    swarm
        .broadcast_bytes(encode_control_frame(&ready))
        .await;
}

async fn set_role(
    state: &Arc<Mutex<OrchestratorState>>,
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    role: Option<&str>,
) {
    state.lock().await.role = role.map(str::to_string);
    let _ = event_tx.send(EngineEvent::Role {
        role: role.map(str::to_string),
    });
}

async fn emit_status(
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    state: &str,
    peers: Option<u32>,
    peer: Option<String>,
) {
    let _ = event_tx.send(EngineEvent::Status {
        state: state.to_string(),
        peers,
        peer,
    });
}

async fn emit_download(
    event_tx: &mpsc::UnboundedSender<EngineEvent>,
    state: &str,
    file_id: &str,
    file_name: &str,
    bytes: u64,
    total: u64,
    saved_to: Option<String>,
    message: Option<String>,
) {
    let _ = event_tx.send(EngineEvent::DownloadStatus {
        state: state.to_string(),
        file_id: Some(file_id.to_string()),
        file_name: Some(file_name.to_string()),
        bytes_transferred: Some(bytes),
        total_bytes: Some(total),
        saved_to,
        message,
    });
}
