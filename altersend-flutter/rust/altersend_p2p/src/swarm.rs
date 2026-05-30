use std::collections::HashMap;
use std::sync::Arc;

use peeroxide::{discovery_key, spawn, JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::control::{decode_control, encode_control, PeerControlMessage};

pub type PeerKey = String;

pub struct PeerSession {
    pub peer_key: PeerKey,
    pub outbound: mpsc::UnboundedSender<PeerControlMessage>,
}

pub struct TransferSwarm {
    handle: SwarmHandle,
    _join_handle: JoinHandle<()>,
    conn_rx: mpsc::Receiver<SwarmConnection>,
    sessions: Arc<RwLock<HashMap<PeerKey, PeerSession>>>,
    raw_topic: Arc<RwLock<Option<[u8; 32]>>>,
}

impl TransferSwarm {
    pub async fn start() -> Result<Self, peeroxide::SwarmError> {
        let config = SwarmConfig::with_public_bootstrap();
        let (join_handle, handle, conn_rx) = spawn(config).await?;
        Ok(Self {
            handle,
            _join_handle: join_handle,
            conn_rx,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            raw_topic: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn generate_topic_hex(&self) -> Result<String, peeroxide::SwarmError> {
        let mut topic = [0u8; 32];
        rand::Rng::fill(&mut rand::rng(), &mut topic);
        self.join_raw_topic(topic).await?;
        *self.raw_topic.write().await = Some(topic);
        Ok(hex::encode(topic))
    }

    pub async fn join_topic_hex(&self, topic_hex: &str) -> Result<(), peeroxide::SwarmError> {
        let topic = parse_topic_hex(topic_hex).map_err(|_| peeroxide::SwarmError::Destroyed)?;
        self.join_raw_topic(topic).await?;
        *self.raw_topic.write().await = Some(topic);
        Ok(())
    }

    async fn join_raw_topic(&self, topic: [u8; 32]) -> Result<(), peeroxide::SwarmError> {
        let discovery = discovery_key(&topic);
        self.handle
            .join(
                discovery,
JoinOpts::default(),
            )
            .await?;
        self.handle.flush().await?;
        Ok(())
    }

    pub async fn destroy(&self) {
        if let Some(topic) = self.raw_topic.write().await.take() {
            let discovery = discovery_key(&topic);
            let _ = self.handle.leave(discovery).await;
        }
        let _ = self.handle.destroy().await;
    }

    pub async fn peer_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    pub async fn broadcast(&self, message: &PeerControlMessage) {
        let sessions = self.sessions.read().await;
        for session in sessions.values() {
            let _ = session.outbound.send(message.clone());
        }
    }

    pub async fn recv_connection(&mut self) -> Option<SwarmConnection> {
        self.conn_rx.recv().await
    }

    pub async fn register_connection(
        &self,
        conn: SwarmConnection,
        on_control: Arc<dyn Fn(PeerKey, PeerControlMessage) + Send + Sync>,
    ) {
        let peer_key = hex::encode(conn.remote_public_key());
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<PeerControlMessage>();

        self.sessions.write().await.insert(
            peer_key.clone(),
            PeerSession {
                peer_key: peer_key.clone(),
                outbound: outbound_tx,
            },
        );

        let mut stream = conn.peer.stream;
        let sessions = self.sessions.clone();
        let peer_key_task = peer_key.clone();

        tokio::spawn(async move {
            let mut buffer = Vec::new();
            loop {
                tokio::select! {
                    msg = outbound_rx.recv() => {
                        match msg {
                            Some(m) => {
                                let frame = encode_control(&m);
                                if stream.write(&frame).await.is_err() {
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
                                while let Some(len) = frame_length(&buffer) {
                                    if buffer.len() < len {
                                        break;
                                    }
                                    let frame = buffer[..len].to_vec();
                                    buffer.drain(..len);
                                    if let Some(msg) = decode_control(&frame) {
                                        on_control(peer_key_task.clone(), msg);
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                warn!("control read ended: {err}");
                                break;
                            }
                        }
                    }
                }
            }
            sessions.write().await.remove(&peer_key_task);
        });
    }
}

fn frame_length(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(buf[..4].try_into().ok()?) as usize;
    Some(4 + len)
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
