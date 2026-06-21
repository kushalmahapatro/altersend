use std::sync::Arc;

use altersend_mux::{BinaryMessageCallback, PeerMux};
use hypercore::Hypercore;
use hypercore_protocol::{Message, schema::*};
use hypercore_schema::RequestBlock;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::hyperdrive::{HypercoreManifest, ManifestSlot};
use crate::replication::capability::{decode_handshake, expected_remote_capability};
use crate::replication::wire::{decode_data_message, decode_message};

use super::peer::ReplicationOutbound;

#[derive(Default)]
struct ClientPeerState {
    remote_fork: u64,
    remote_length: u64,
    remote_synced: bool,
    can_upgrade: bool,
    length_acked: u64,
}

pub struct HypercoreReplicationClient {
    channel_id: u64,
    core: Arc<Mutex<Hypercore>>,
    manifest_slot: Option<ManifestSlot>,
    outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    peer_state: ClientPeerState,
    next_request_id: u64,
}

impl HypercoreReplicationClient {
    pub fn attach_inbound(
        mux: &mut PeerMux,
        remote_id: u64,
        handshake: &[u8],
        local_is_initiator: bool,
        handshake_hash: &[u8; 64],
        core_public_key: [u8; 32],
        core: Arc<Mutex<Hypercore>>,
        manifest_slot: Option<ManifestSlot>,
        outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    ) -> Result<(), String> {
        Self::attach_inner(
            mux,
            remote_id,
            true,
            handshake,
            local_is_initiator,
            handshake_hash,
            core_public_key,
            core,
            manifest_slot,
            outbound_tx,
        )
    }

    pub fn attach_outbound(
        mux: &mut PeerMux,
        local_channel_id: u64,
        local_is_initiator: bool,
        handshake_hash: &[u8; 64],
        core_public_key: [u8; 32],
        core: Arc<Mutex<Hypercore>>,
        manifest_slot: Option<ManifestSlot>,
        outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    ) -> Result<(), String> {
        Self::attach_inner(
            mux,
            local_channel_id,
            false,
            &[],
            local_is_initiator,
            handshake_hash,
            core_public_key,
            core,
            manifest_slot,
            outbound_tx,
        )
    }

    fn attach_inner(
        mux: &mut PeerMux,
        channel_id: u64,
        verify_handshake: bool,
        handshake: &[u8],
        local_is_initiator: bool,
        handshake_hash: &[u8; 64],
        core_public_key: [u8; 32],
        core: Arc<Mutex<Hypercore>>,
        manifest_slot: Option<ManifestSlot>,
        outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    ) -> Result<(), String> {
        if verify_handshake {
            let (_, remote_capability) =
                decode_handshake(handshake).ok_or("invalid hypercore handshake")?;
            let expected =
                expected_remote_capability(local_is_initiator, &core_public_key, handshake_hash);
            if remote_capability != expected {
                return Err("invalid hypercore replication capability".into());
            }
        }

        let client = Arc::new(Mutex::new(Self {
            channel_id,
            core,
            manifest_slot,
            outbound_tx: outbound_tx.clone(),
            peer_state: ClientPeerState {
                can_upgrade: true,
                ..ClientPeerState::default()
            },
            next_request_id: 1,
        }));

        info!(
            "hypercore/alpha replication client on channel {channel_id} for {}",
            hex::encode(core_public_key)
        );

        let handler_client = client.clone();
        let handler: BinaryMessageCallback = Box::new(move |msg_type, payload| {
            let client = handler_client.clone();
            let payload = payload.to_vec();
            tokio::spawn(async move {
                let mut guard = client.lock().await;
                if msg_type == 3 {
                    if let Some((data, manifest)) = decode_data_message(&payload) {
                        if let Err(err) = guard.on_data(data, manifest).await {
                            warn!("hypercore replication client error: {err}");
                        }
                    }
                    return;
                }
                if let Some(message) = decode_message(msg_type, &payload) {
                    if let Err(err) = guard.on_message(message).await {
                        warn!("hypercore replication client error: {err}");
                    }
                }
            });
        });

        if verify_handshake {
            mux.set_remote_binary_handler(channel_id, handler)
                .map_err(|e| e.to_string())?;
        } else {
            mux.set_local_binary_handler(channel_id, handler);
        }
        Ok(())
    }

    // Legacy name used by replicate registry for remote-initiated channels.
    pub fn attach(
        mux: &mut PeerMux,
        remote_id: u64,
        handshake: &[u8],
        local_is_initiator: bool,
        handshake_hash: &[u8; 64],
        core_public_key: [u8; 32],
        core: Arc<Mutex<Hypercore>>,
        manifest_slot: Option<ManifestSlot>,
        outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    ) -> Result<(), String> {
        Self::attach_inbound(
            mux,
            remote_id,
            handshake,
            local_is_initiator,
            handshake_hash,
            core_public_key,
            core,
            manifest_slot,
            outbound_tx,
        )
    }

    fn needs_manifest(&self) -> bool {
        self.manifest_slot.is_some()
            && self
                .manifest_slot
                .as_ref()
                .and_then(|slot| slot.try_lock().ok().and_then(|g| if g.is_none() { Some(()) } else { None }))
                .is_some()
    }

    async fn store_manifest(&self, manifest: HypercoreManifest) {
        if let Some(slot) = &self.manifest_slot {
            *slot.lock().await = Some(manifest);
        }
    }

    fn send_message(&self, message: Message) -> Result<(), String> {
        self.outbound_tx
            .send(ReplicationOutbound::Send {
                remote_id: self.channel_id,
                message,
            })
            .map_err(|_| "replication outbound closed".to_string())
    }

    async fn on_message(&mut self, message: Message) -> Result<(), String> {
        match message {
            Message::Synchronize(msg) => self.on_synchronize(msg).await,
            Message::Data(msg) => self.on_data(msg, None).await,
            Message::Range(_) => Ok(()),
            _ => Ok(()),
        }
    }

    async fn on_synchronize(&mut self, message: Synchronize) -> Result<(), String> {
        let length_changed = message.length != self.peer_state.remote_length;
        let first_sync = !self.peer_state.remote_synced;
        let info = {
            let core = self.core.lock().await;
            core.info()
        };
        let same_fork = message.fork == info.fork;

        self.peer_state.remote_fork = message.fork;
        self.peer_state.remote_length = message.length;
        self.peer_state.remote_synced = true;
        self.peer_state.length_acked = if same_fork {
            message.remote_length
        } else {
            0
        };

        if first_sync {
            self.send_message(Message::Synchronize(Synchronize {
                fork: info.fork,
                length: info.length,
                remote_length: self.peer_state.remote_length,
                can_upgrade: self.peer_state.can_upgrade,
                uploading: true,
                downloading: true,
            }))?;
        }

        if first_sync && self.needs_manifest() {
            self.next_request_id += 1;
            self.send_message(Message::Request(Request {
                id: self.next_request_id,
                fork: message.fork,
                hash: None,
                block: None,
                seek: None,
                upgrade: None,
                manifest: true,
                priority: 0,
            }))?;
        }

        if self.peer_state.remote_length > info.length
            && self.peer_state.length_acked == info.length
            && length_changed
        {
            self.next_request_id += 1;
            self.send_message(Message::Request(Request {
                id: self.next_request_id,
                fork: info.fork,
                hash: None,
                block: None,
                seek: None,
                upgrade: Some(hypercore_schema::RequestUpgrade {
                    start: info.length,
                    length: self.peer_state.remote_length - info.length,
                }),
                manifest: self.needs_manifest(),
                priority: 0,
            }))?;
        }
        Ok(())
    }

    async fn on_data(&mut self, message: Data, manifest: Option<HypercoreManifest>) -> Result<(), String> {
        if let Some(manifest) = manifest {
            self.store_manifest(manifest).await;
        }

        let (old_info, new_info, follow_up) = {
            let mut core = self.core.lock().await;
            let old_info = core.info();
            let proof = message.clone().into_proof();
            let _applied = core.verify_and_apply_proof(&proof).await.map_err(|e| e.to_string())?;
            let new_info = core.info();

            let follow_up = if let Some(upgrade) = &message.upgrade {
                if old_info.length < upgrade.length {
                    let request_index = old_info.length;
                    let nodes = core.missing_nodes(request_index).await.map_err(|e| e.to_string())?;
                    Some(RequestBlock {
                        index: request_index,
                        nodes,
                    })
                } else {
                    None
                }
            } else if let Some(block) = &message.block {
                if block.index < self.peer_state.remote_length.saturating_sub(1) {
                    let request_index = block.index + 1;
                    let nodes = core.missing_nodes(request_index).await.map_err(|e| e.to_string())?;
                    Some(RequestBlock {
                        index: request_index,
                        nodes,
                    })
                } else {
                    None
                }
            } else {
                None
            };

            (old_info, new_info, follow_up)
        };

        if let Some(upgrade) = &message.upgrade {
            let remote_length = if new_info.fork == self.peer_state.remote_fork {
                self.peer_state.remote_length
            } else {
                0
            };
            self.send_message(Message::Synchronize(Synchronize {
                fork: new_info.fork,
                length: upgrade.length,
                remote_length,
                can_upgrade: false,
                uploading: true,
                downloading: true,
            }))?;
        }

        if let Some(request_block) = follow_up {
            self.next_request_id += 1;
            self.send_message(Message::Request(Request {
                id: self.next_request_id,
                fork: new_info.fork,
                hash: None,
                block: Some(request_block),
                seek: None,
                upgrade: None,
                manifest: false,
                priority: 0,
            }))?;
        }

        let _ = old_info;
        Ok(())
    }
}
