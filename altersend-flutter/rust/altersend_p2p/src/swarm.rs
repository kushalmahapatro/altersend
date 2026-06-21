use std::collections::HashMap;
use std::sync::Arc;

use altersend_mux::HYPERCORE_ALPHA_PROTOCOL;
use altersend_storage::{
    encode_handshake, local_capability, send_replication_outbound, HypercoreReplicationClient,
    ReplicationHandle, ReplicationRegistry, ReplicationOutbound,
};
use bytes::Bytes;
use peeroxide::{discovery_key, spawn, JoinOpts, KeyPair, SwarmConfig, SwarmConnection, SwarmHandle};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::control::{decode_control_payload, PeerControlMessage};
use crate::peer_mode::{is_hypercore_replication_protocol, PeerInteropMode};
use crate::peer_session::PeerSession;
use crate::wire::WireFrame;

pub type PeerKey = String;

struct HypercoreChannelOpen {
    remote_id: u64,
    discovery_key_hex: String,
    handshake: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum SwarmInbound {
    PeerConnected(PeerKey),
    PeerDisconnected(PeerKey),
    PeerInteropMode(PeerKey, PeerInteropMode),
    Frame(PeerKey, WireFrame),
}

struct PeerHandle {
    cmd_tx: mpsc::UnboundedSender<PeerCommand>,
}

enum PeerCommand {
    SendControl(PeerControlMessage),
    SendChunk {
        file_id: String,
        offset: u64,
        data: Vec<u8>,
    },
    OpenIncomingReplication,
}

/// Handle to the background Hyperswarm actor (non-blocking).
#[derive(Clone)]
pub struct SwarmHandleClient {
    cmd_tx: mpsc::UnboundedSender<SwarmCommand>,
}

enum SwarmCommand {
    GenerateTopic(oneshot::Sender<Result<String, String>>),
    RecreateWithKeyPair(KeyPair, oneshot::Sender<Result<(), String>>),
    JoinTopic(String, oneshot::Sender<Result<(), String>>),
    BroadcastControl(PeerControlMessage),
    SendControlToPeer { peer: PeerKey, msg: PeerControlMessage },
    SendChunkToPeer {
        peer: PeerKey,
        file_id: String,
        offset: u64,
        data: Vec<u8>,
    },
    PeerCount(oneshot::Sender<usize>),
    OpenIncomingReplication { peer: PeerKey },
    EndSession(oneshot::Sender<()>),
    Destroy(oneshot::Sender<()>),
}

pub struct SwarmRuntime {
    pub client: SwarmHandleClient,
    _task: JoinHandle<()>,
}

impl SwarmRuntime {
    pub async fn start(
        inbound_tx: mpsc::UnboundedSender<SwarmInbound>,
        replication_registry: Arc<ReplicationRegistry>,
    ) -> Result<Self, peeroxide::SwarmError> {
        let config = SwarmConfig::with_public_bootstrap();
        let (_join_handle, swarm, conn_rx) = spawn(config).await?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let sessions: Arc<RwLock<HashMap<PeerKey, PeerHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let peer_count = Arc::new(Mutex::new(0usize));
        let raw_topic: Arc<Mutex<Option<[u8; 32]>>> = Arc::new(Mutex::new(None));

        let task = tokio::spawn(async move {
            swarm_actor(
                swarm,
                conn_rx,
                cmd_rx,
                inbound_tx,
                sessions,
                peer_count,
                raw_topic,
                replication_registry,
            )
            .await;
        });

        Ok(Self {
            client: SwarmHandleClient { cmd_tx },
            _task: task,
        })
    }
}

impl SwarmHandleClient {
    pub async fn generate_topic_hex(&self) -> Result<String, String> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::GenerateTopic(tx))
            .map_err(|_| "swarm stopped".to_string())?;
        rx.await.map_err(|_| "swarm stopped".to_string())?
    }

    pub async fn recreate_with_keypair(&self, key_pair: KeyPair) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::RecreateWithKeyPair(key_pair, tx))
            .map_err(|_| "swarm stopped".to_string())?;
        rx.await.map_err(|_| "swarm stopped".to_string())?
    }

    pub async fn join_topic_hex(&self, topic_hex: &str) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::JoinTopic(topic_hex.to_string(), tx))
            .map_err(|_| "swarm stopped".to_string())?;
        rx.await.map_err(|_| "swarm stopped".to_string())?
    }

    pub async fn broadcast_control(&self, msg: &PeerControlMessage) {
        let _ = self
            .cmd_tx
            .send(SwarmCommand::BroadcastControl(msg.clone()));
    }

    pub async fn send_control_to_peer(&self, peer: &str, msg: &PeerControlMessage) {
        let _ = self.cmd_tx.send(SwarmCommand::SendControlToPeer {
            peer: peer.to_string(),
            msg: msg.clone(),
        });
    }

    pub async fn send_chunk_to_peer(&self, peer: &str, file_id: &str, offset: u64, data: Vec<u8>) {
        let _ = self.cmd_tx.send(SwarmCommand::SendChunkToPeer {
            peer: peer.to_string(),
            file_id: file_id.to_string(),
            offset,
            data,
        });
    }

    /// Legacy helper — encodes a control message and broadcasts via Protomux.
    pub async fn broadcast_bytes(&self, data: Vec<u8>) {
        if let Some(msg) = decode_control_payload(&data) {
            self.broadcast_control(&msg).await;
        }
    }

    /// Legacy helper — sends encoded control to one peer via Protomux.
    pub async fn send_to_peer(&self, peer: &str, data: Vec<u8>) {
        if let Some(msg) = decode_control_payload(&data) {
            self.send_control_to_peer(peer, &msg).await;
        }
    }

    pub async fn open_incoming_replication(&self, peer: &str) {
        let _ = self.cmd_tx.send(SwarmCommand::OpenIncomingReplication {
            peer: peer.to_string(),
        });
    }

    pub async fn peer_count(&self) -> usize {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.send(SwarmCommand::PeerCount(tx)).is_err() {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    pub async fn end_session(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(SwarmCommand::EndSession(tx));
        let _ = rx.await;
    }

    pub async fn destroy(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(SwarmCommand::Destroy(tx));
        let _ = rx.await;
    }
}

async fn swarm_actor(
    mut handle: SwarmHandle,
    mut conn_rx: mpsc::Receiver<SwarmConnection>,
    mut cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
    inbound_tx: mpsc::UnboundedSender<SwarmInbound>,
    sessions: Arc<RwLock<HashMap<PeerKey, PeerHandle>>>,
    peer_count: Arc<Mutex<usize>>,
    raw_topic: Arc<Mutex<Option<[u8; 32]>>>,
    replication_registry: Arc<ReplicationRegistry>,
) {
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                match cmd {
                    SwarmCommand::RecreateWithKeyPair(key_pair, reply) => {
                        if let Some(topic) = raw_topic.lock().await.take() {
                            let discovery = discovery_key(&topic);
                            let _ = handle.leave(discovery).await;
                        }
                        sessions.write().await.clear();
                        *peer_count.lock().await = 0;
                        let _ = handle.destroy().await;

                        let mut config = SwarmConfig::with_public_bootstrap();
                        config.key_pair = Some(key_pair);
                        match spawn(config).await {
                            Ok((_task, new_handle, new_conn_rx)) => {
                                handle = new_handle;
                                conn_rx = new_conn_rx;
                                let _ = reply.send(Ok(()));
                            }
                            Err(err) => {
                                let _ = reply.send(Err(err.to_string()));
                            }
                        }
                    }
                    SwarmCommand::GenerateTopic(reply) => {
                        let result = async {
                            let mut topic = [0u8; 32];
                            rand::Rng::fill(&mut rand::rng(), &mut topic);
                            join_raw_topic(&handle, &raw_topic, topic).await?;
                            Ok(hex::encode(topic))
                        }.await;
                        let _ = reply.send(result);
                    }
                    SwarmCommand::JoinTopic(hex, reply) => {
                        let result = async {
                            let topic = parse_topic_hex(&hex)?;
                            join_raw_topic(&handle, &raw_topic, topic).await
                        }.await;
                        let _ = reply.send(result);
                    }
                    SwarmCommand::BroadcastControl(msg) => {
                        let guard = sessions.read().await;
                        for peer in guard.values() {
                            let _ = peer.cmd_tx.send(PeerCommand::SendControl(msg.clone()));
                        }
                    }
                    SwarmCommand::SendControlToPeer { peer, msg } => {
                        let guard = sessions.read().await;
                        if let Some(p) = guard.get(&peer) {
                            let _ = p.cmd_tx.send(PeerCommand::SendControl(msg));
                        }
                    }
                    SwarmCommand::SendChunkToPeer { peer, file_id, offset, data } => {
                        let guard = sessions.read().await;
                        if let Some(p) = guard.get(&peer) {
                            let _ = p.cmd_tx.send(PeerCommand::SendChunk { file_id, offset, data });
                        }
                    }
                    SwarmCommand::OpenIncomingReplication { peer } => {
                        let guard = sessions.read().await;
                        if let Some(p) = guard.get(&peer) {
                            let _ = p.cmd_tx.send(PeerCommand::OpenIncomingReplication);
                        }
                    }
                    SwarmCommand::PeerCount(reply) => {
                        let count = *peer_count.lock().await;
                        let _ = reply.send(count);
                    }
                    SwarmCommand::EndSession(reply) => {
                        if let Some(topic) = raw_topic.lock().await.take() {
                            let discovery = discovery_key(&topic);
                            let _ = handle.leave(discovery).await;
                        }
                        sessions.write().await.clear();
                        *peer_count.lock().await = 0;
                        let _ = reply.send(());
                    }
                    SwarmCommand::Destroy(reply) => {
                        if let Some(topic) = raw_topic.lock().await.take() {
                            let discovery = discovery_key(&topic);
                            let _ = handle.leave(discovery).await;
                        }
                        let _ = handle.destroy().await;
                        let _ = reply.send(());
                        return;
                    }
                }
            }
            conn = conn_rx.recv() => {
                let Some(conn) = conn else { continue };
                let peer_key = hex::encode(conn.remote_public_key());
                let is_initiator = conn.is_initiator;
                {
                    let mut count = peer_count.lock().await;
                    *count += 1;
                }
                let _ = inbound_tx.send(SwarmInbound::PeerConnected(peer_key.clone()));

                let (peer_cmd_tx, mut peer_cmd_rx) = mpsc::unbounded_channel();
                sessions.write().await.insert(
                    peer_key.clone(),
                    PeerHandle { cmd_tx: peer_cmd_tx },
                );

                let inbound = inbound_tx.clone();
                let sessions_read = sessions.clone();
                let peer_count_read = peer_count.clone();
                let peer_for_task = peer_key.clone();
                let replication_registry_peer = replication_registry.clone();

                let mut stream = conn.peer.stream;
                let handshake_hash = *stream.handshake_hash();
                tokio::spawn(async move {
                    let (mux_out_tx, mut mux_out_rx) = mpsc::unbounded_channel::<Bytes>();
                    let (hc_open_tx, mut hc_open_rx) =
                        mpsc::unbounded_channel::<HypercoreChannelOpen>();
                    let (repl_out_tx, mut repl_out_rx) =
                        mpsc::unbounded_channel::<ReplicationOutbound>();
                    let inbound_mode = inbound.clone();
                    let peer_for_mode = peer_for_task.clone();
                    let replication_registry_for_open = replication_registry_peer.clone();
                    let on_remote_open = Box::new(move |open: altersend_mux::RemoteChannelOpen| {
                        let mode =
                            PeerInteropMode::Unknown.observe_remote_protocol(&open.protocol);
                        let _ = inbound_mode.send(SwarmInbound::PeerInteropMode(
                            peer_for_mode.clone(),
                            mode,
                        ));
                        if is_hypercore_replication_protocol(&open.protocol) {
                            if let Some(id) = open.channel_id {
                                let _ = hc_open_tx.send(HypercoreChannelOpen {
                                    remote_id: open.remote_id,
                                    discovery_key_hex: hex::encode(id),
                                    handshake: open.handshake,
                                });
                            }
                        }
                    });
                    let Ok(mut session) =
                        PeerSession::new(is_initiator, mux_out_tx, Some(on_remote_open))
                    else {
                        return;
                    };

                    if let Some(channel) = session.mux.channel_mut(session.control_idx) {
                        let inbound_ctrl = inbound.clone();
                        let peer = peer_for_task.clone();
                        channel.on_json_message(move |value| {
                            if let Ok(bytes) = serde_json::to_vec(&value) {
                                if let Some(msg) = decode_control_payload(&bytes) {
                                    let _ = inbound_ctrl.send(SwarmInbound::Frame(
                                        peer.clone(),
                                        WireFrame::Control(msg),
                                    ));
                                }
                            }
                        });
                    }

                    if let Some(channel) = session.mux.channel_mut(session.chunk_idx) {
                        let inbound_chunk = inbound.clone();
                        let peer = peer_for_task.clone();
                        channel.on_json_message(move |value| {
                            let _ = inbound_chunk.send(SwarmInbound::PeerInteropMode(
                                peer.clone(),
                                PeerInteropMode::Rust,
                            ));
                            if let Some(frame) = decode_chunk_json(&value) {
                                let _ = inbound_chunk.send(SwarmInbound::Frame(peer.clone(), frame));
                            }
                        });
                    }

                    let mut read_buffer = Vec::new();
                    let replication_handle =
                        ReplicationHandle::attach_peer(&peer_for_task, replication_registry_for_open.clone());
                    loop {
                        tokio::select! {
                            hc_open = hc_open_rx.recv() => {
                                if let Some(open) = hc_open {
                                    if let Err(err) = replication_handle.attach_hypercore_channel(
                                        &mut session.mux,
                                        open.remote_id,
                                        &open.discovery_key_hex,
                                        &open.handshake,
                                        is_initiator,
                                        &handshake_hash,
                                        repl_out_tx.clone(),
                                    ).await {
                                        warn!(
                                            "hypercore/alpha attach for {}: {err}",
                                            open.discovery_key_hex
                                        );
                                    }
                                }
                            }
                            repl_out = repl_out_rx.recv() => {
                                if let Some(outbound) = repl_out {
                                    if let Err(err) =
                                        send_replication_outbound(&mut session.mux, outbound)
                                    {
                                        warn!("hypercore replication send: {err}");
                                    }
                                }
                            }
                            cmd = peer_cmd_rx.recv() => {
                                match cmd {
                                    Some(PeerCommand::SendControl(msg)) => {
                                        let _ = session.send_control(&msg);
                                    }
                                    Some(PeerCommand::SendChunk { file_id, offset, data }) => {
                                        if session.send_file_chunk(&file_id, offset, &data).is_ok() {
                                            let _ = inbound.send(SwarmInbound::PeerInteropMode(
                                                peer_for_task.clone(),
                                                PeerInteropMode::Rust,
                                            ));
                                        }
                                    }
                                    Some(PeerCommand::OpenIncomingReplication) => {
                                        open_incoming_replication_channels(
                                            &mut session.mux,
                                            &replication_registry_for_open,
                                            is_initiator,
                                            &handshake_hash,
                                            repl_out_tx.clone(),
                                        )
                                        .await;
                                    }
                                    None => break,
                                }
                            }
                            out = mux_out_rx.recv() => {
                                match out {
                                    Some(bytes) => {
                                        if stream.write(&bytes).await.is_err() {
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                            read = stream.read() => {
                                match read {
                                    Ok(Some(chunk)) => {
                                        read_buffer.extend_from_slice(&chunk);
                                        if let Err(err) = session.ingest(&read_buffer) {
                                            warn!("protomux ingest: {err}");
                                        } else {
                                            read_buffer.clear();
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(err) => {
                                        warn!("peer read ended: {err}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    sessions_read.write().await.remove(&peer_for_task);
                    {
                        let mut count = peer_count_read.lock().await;
                        *count = count.saturating_sub(1);
                    }
                    replication_handle.detach().await;
                    let _ = inbound.send(SwarmInbound::PeerDisconnected(peer_for_task));
                });
            }
        }
    }
}

fn decode_chunk_json(value: &serde_json::Value) -> Option<WireFrame> {
    let hex_payload = value.get("payload")?.as_str()?;
    let bytes = hex::decode(hex_payload).ok()?;
    let mut buf = bytes;
    crate::wire::drain_frames(&mut buf).into_iter().next()
}

async fn open_incoming_replication_channels(
    mux: &mut altersend_mux::PeerMux,
    registry: &ReplicationRegistry,
    is_initiator: bool,
    handshake_hash: &[u8; 64],
    repl_out_tx: mpsc::UnboundedSender<ReplicationOutbound>,
) {
    let targets = registry.download_targets().await;
    for (meta, core) in targets {
        let Ok(pk) = hex::decode(&meta.public_key_hex) else {
            continue;
        };
        if pk.len() != 32 {
            continue;
        }
        let mut public_key = [0u8; 32];
        public_key.copy_from_slice(&pk);
        let Ok(dk) = hex::decode(&meta.discovery_key_hex) else {
            continue;
        };
        if dk.len() != 32 {
            continue;
        }
        let mut discovery = [0u8; 32];
        discovery.copy_from_slice(&dk);
        let capability = local_capability(is_initiator, &public_key, handshake_hash);
        let handshake = encode_handshake(true, &capability);
        let local_id = match mux.open_hypercore_channel(HYPERCORE_ALPHA_PROTOCOL, &discovery, &handshake)
        {
            Ok(id) => id,
            Err(err) => {
                warn!("open hypercore channel for {}: {err}", meta.discovery_key_hex);
                continue;
            }
        };
        if let Err(err) = HypercoreReplicationClient::attach_outbound(
            mux,
            local_id,
            is_initiator,
            handshake_hash,
            public_key,
            core,
            None,
            repl_out_tx.clone(),
        ) {
            warn!("attach outbound replication: {err}");
        }
    }
}

async fn join_raw_topic(
    handle: &SwarmHandle,
    raw_topic: &Mutex<Option<[u8; 32]>>,
    topic: [u8; 32],
) -> Result<(), String> {
    let discovery = discovery_key(&topic);
    handle
        .join(discovery, JoinOpts::default())
        .await
        .map_err(|e| e.to_string())?;
    handle.flush().await.map_err(|e| e.to_string())?;
    *raw_topic.lock().await = Some(topic);
    Ok(())
}

fn parse_topic_hex(topic_hex: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(topic_hex).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err("topic must be 32 bytes".into());
    }
    let mut topic = [0u8; 32];
    topic.copy_from_slice(&bytes);
    Ok(topic)
}
