use std::collections::HashMap;
use std::sync::Arc;

use peeroxide::{discovery_key, spawn, JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::wire::{drain_frames, WireFrame};

pub type PeerKey = String;

#[derive(Debug, Clone)]
pub enum SwarmInbound {
    PeerConnected(PeerKey),
    PeerDisconnected(PeerKey),
    Frame(PeerKey, WireFrame),
}

struct PeerOutbound {
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

/// Handle to the background Hyperswarm actor (non-blocking).
#[derive(Clone)]
pub struct SwarmHandleClient {
    cmd_tx: mpsc::UnboundedSender<SwarmCommand>,
}

enum SwarmCommand {
    GenerateTopic(oneshot::Sender<Result<String, String>>),
    JoinTopic(String, oneshot::Sender<Result<(), String>>),
    Broadcast(Vec<u8>),
    SendToPeer { peer: PeerKey, data: Vec<u8> },
    PeerCount(oneshot::Sender<usize>),
    EndSession(oneshot::Sender<()>),
    Destroy(oneshot::Sender<()>),
}

pub struct SwarmRuntime {
    pub client: SwarmHandleClient,
    _task: JoinHandle<()>,
}

impl SwarmRuntime {
    pub async fn start(inbound_tx: mpsc::UnboundedSender<SwarmInbound>) -> Result<Self, peeroxide::SwarmError> {
        let config = SwarmConfig::with_public_bootstrap();
        let (_join_handle, swarm, conn_rx) = spawn(config).await?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let sessions: Arc<RwLock<HashMap<PeerKey, PeerOutbound>>> =
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

    pub async fn join_topic_hex(&self, topic_hex: &str) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::JoinTopic(topic_hex.to_string(), tx))
            .map_err(|_| "swarm stopped".to_string())?;
        rx.await.map_err(|_| "swarm stopped".to_string())?
    }

    pub async fn broadcast_bytes(&self, data: Vec<u8>) {
        let _ = self.cmd_tx.send(SwarmCommand::Broadcast(data));
    }

    pub async fn send_to_peer(&self, peer: &str, data: Vec<u8>) {
        let _ = self.cmd_tx.send(SwarmCommand::SendToPeer {
            peer: peer.to_string(),
            data,
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
    handle: SwarmHandle,
    mut conn_rx: mpsc::Receiver<SwarmConnection>,
    mut cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
    inbound_tx: mpsc::UnboundedSender<SwarmInbound>,
    sessions: Arc<RwLock<HashMap<PeerKey, PeerOutbound>>>,
    peer_count: Arc<Mutex<usize>>,
    raw_topic: Arc<Mutex<Option<[u8; 32]>>>,
) {
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                match cmd {
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
                    SwarmCommand::Broadcast(data) => {
                        let guard = sessions.read().await;
                        for peer in guard.values() {
                            let _ = peer.tx.send(data.clone());
                        }
                    }
                    SwarmCommand::SendToPeer { peer, data } => {
                        let guard = sessions.read().await;
                        if let Some(p) = guard.get(&peer) {
                            let _ = p.tx.send(data);
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
                {
                    let mut count = peer_count.lock().await;
                    *count += 1;
                }
                let _ = inbound_tx.send(SwarmInbound::PeerConnected(peer_key.clone()));

                let (out_tx, mut out_rx) = mpsc::unbounded_channel();
                sessions.write().await.insert(
                    peer_key.clone(),
                    PeerOutbound { tx: out_tx },
                );

                let inbound = inbound_tx.clone();
                let sessions_read = sessions.clone();
                let peer_count_read = peer_count.clone();
                let peer_for_task = peer_key.clone();

                let mut stream = conn.peer.stream;
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    loop {
                        tokio::select! {
                            out = out_rx.recv() => {
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
                                        buffer.extend_from_slice(&chunk);
                                        for frame in drain_frames(&mut buffer) {
                                            let _ = inbound.send(SwarmInbound::Frame(
                                                peer_for_task.clone(),
                                                frame,
                                            ));
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
                    let _ = inbound.send(SwarmInbound::PeerDisconnected(peer_for_task));
                });
            }
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
