use std::sync::Arc;

use altersend_mux::{BinaryMessageCallback, MuxError, PeerMux};
use hypercore::Hypercore;
use hypercore_protocol::{Message, schema::*};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::replication::capability::{decode_handshake, expected_remote_capability};
use crate::replication::wire::{decode_message, encode_body};

#[derive(Debug)]
pub enum ReplicationOutbound {
    Send {
        remote_id: u64,
        message: Message,
    },
}

#[derive(Default)]
struct PeerState {
    remote_fork: u64,
    remote_length: u64,
    remote_synced: bool,
    can_upgrade: bool,
}

pub struct HypercoreReplicationPeer {
    remote_id: u64,
    core: Arc<Mutex<Hypercore>>,
    outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    peer_state: PeerState,
}

impl HypercoreReplicationPeer {
    pub fn attach(
        mux: &mut PeerMux,
        remote_id: u64,
        handshake: &[u8],
        local_is_initiator: bool,
        handshake_hash: &[u8; 64],
        core_public_key: [u8; 32],
        core: Arc<Mutex<Hypercore>>,
        outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    ) -> Result<(), String> {
        let (_, remote_capability) =
            decode_handshake(handshake).ok_or("invalid hypercore handshake")?;
        let expected =
            expected_remote_capability(local_is_initiator, &core_public_key, handshake_hash);
        if remote_capability != expected {
            return Err("invalid hypercore replication capability".into());
        }

        let peer = Arc::new(Mutex::new(Self {
            remote_id,
            core,
            outbound_tx: outbound_tx.clone(),
            peer_state: PeerState {
                can_upgrade: true,
                ..PeerState::default()
            },
        }));

        info!(
            "hypercore/alpha replication server on channel {remote_id} for {}",
            hex::encode(core_public_key)
        );

        {
            let peer_for_open = peer.clone();
            tokio::spawn(async move {
                if let Err(err) = peer_for_open.lock().await.send_initial_sync().await {
                    warn!("hypercore replication initial sync failed: {err}");
                }
            });
        }

        let handler_peer = peer.clone();
        let handler: BinaryMessageCallback = Box::new(move |msg_type, payload| {
            let peer = handler_peer.clone();
            let payload = payload.to_vec();
            tokio::spawn(async move {
                let mut peer_guard = peer.lock().await;
                if let Some(message) = decode_message(msg_type, &payload) {
                    if let Err(err) = peer_guard.on_message(message).await {
                        warn!("hypercore replication message error: {err}");
                    }
                }
            });
        });

        mux.set_remote_binary_handler(remote_id, handler)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn send_initial_sync(&self) -> Result<(), String> {
        let (fork, length, contiguous_length) = {
            let core = self.core.lock().await;
            let info = core.info();
            (info.fork, info.length, info.contiguous_length)
        };

        let sync_msg = Synchronize {
            fork,
            length,
            remote_length: 0,
            can_upgrade: self.peer_state.can_upgrade,
            uploading: true,
            downloading: true,
        };

        let mut messages = vec![Message::Synchronize(sync_msg)];
        if contiguous_length > 0 {
            messages.push(Message::Range(Range {
                drop: false,
                start: 0,
                length: contiguous_length,
            }));
        }

        for message in messages {
            self.send_message(message)?;
        }
        Ok(())
    }

    fn send_message(&self, message: Message) -> Result<(), String> {
        self.outbound_tx
            .send(ReplicationOutbound::Send {
                remote_id: self.remote_id,
                message,
            })
            .map_err(|_| "replication outbound closed".to_string())
    }

    async fn on_message(&mut self, message: Message) -> Result<(), String> {
        match message {
            Message::Synchronize(msg) => self.on_synchronize(msg).await,
            Message::Request(msg) => self.on_request(msg).await,
            Message::Data(_) => Ok(()),
            _ => Ok(()),
        }
    }

    async fn on_synchronize(&mut self, message: Synchronize) -> Result<(), String> {
        self.peer_state.remote_fork = message.fork;
        self.peer_state.remote_length = message.length;
        self.peer_state.remote_synced = true;
        Ok(())
    }

    async fn on_request(&mut self, message: Request) -> Result<(), String> {
        let (fork, proof) = {
            let mut core = self.core.lock().await;
            let proof = core
                .create_proof(message.block, message.hash, message.seek, message.upgrade)
                .await
                .map_err(|e| e.to_string())?;
            (core.info().fork, proof)
        };

        if let Some(proof) = proof {
            self.send_message(Message::Data(Data {
                request: message.id,
                fork,
                hash: proof.hash,
                block: proof.block,
                seek: proof.seek,
                upgrade: proof.upgrade,
            }))?;
        }
        Ok(())
    }
}

pub trait ReplicationMux {
    fn send_message(&mut self, remote_id: u64, message: &Message) -> Result<(), MuxError>;
}

impl ReplicationMux for PeerMux {
    fn send_message(&mut self, remote_id: u64, message: &Message) -> Result<(), MuxError> {
        let (msg_type, body) = encode_body(message).ok_or(MuxError::InvalidFrame)?;
        self.send_on_channel(remote_id, msg_type, &body)
    }
}

pub fn send_replication_outbound(
    mux: &mut PeerMux,
    outbound: ReplicationOutbound,
) -> Result<(), MuxError> {
    match outbound {
        ReplicationOutbound::Send { remote_id, message } => mux.send_message(remote_id, &message),
    }
}
