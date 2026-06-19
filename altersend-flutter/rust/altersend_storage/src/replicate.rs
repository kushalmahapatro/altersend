use std::collections::HashMap;
use std::sync::Arc;

use altersend_mux::PeerMux;
use hypercore::Hypercore;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::info;

use crate::drive::OutgoingDrive;
use crate::replication::{
    HypercoreReplicationClient, HypercoreReplicationPeer, ReplicationOutbound,
};

/// Hypercore registered for replication with a connected peer.
#[derive(Clone)]
pub struct RegisteredCore {
    pub public_key_hex: String,
    pub discovery_key_hex: String,
    pub upload: bool,
}

struct RegisteredCoreState {
    meta: RegisteredCore,
    core: Arc<Mutex<Hypercore>>,
}

/// Tracks active outgoing drive keys and registered cores for replication with connected peers.
pub struct ReplicationRegistry {
    active_drive_keys: RwLock<HashMap<String, String>>,
    registered_cores: RwLock<HashMap<String, RegisteredCoreState>>,
    outgoing_drive: RwLock<Option<Arc<Mutex<OutgoingDrive>>>>,
}

impl ReplicationRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active_drive_keys: RwLock::new(HashMap::new()),
            registered_cores: RwLock::new(HashMap::new()),
            outgoing_drive: RwLock::new(None),
        })
    }

    pub async fn set_outgoing_drive(&self, drive: Arc<Mutex<OutgoingDrive>>) {
        let (metadata_key, metadata, blobs_key, blobs) = {
            let guard = drive.lock().await;
            (
                guard.key_hex().await,
                guard.metadata_core(),
                guard.blobs_key_hex().await,
                guard.blobs_core(),
            )
        };
        self.register_core(&metadata_key, metadata, true).await;
        self.register_core(&blobs_key, blobs, true).await;
        *self.outgoing_drive.write().await = Some(drive);
    }

    pub async fn register_incoming_core(
        &self,
        public_key_hex: &str,
        core: Arc<Mutex<Hypercore>>,
    ) {
        self.register_core(public_key_hex, core, false).await;
    }

    async fn register_core(
        &self,
        public_key_hex: &str,
        core: Arc<Mutex<Hypercore>>,
        upload: bool,
    ) {
        let discovery = discovery_key_hex(public_key_hex);
        let meta = RegisteredCore {
            public_key_hex: public_key_hex.to_string(),
            discovery_key_hex: discovery.clone(),
            upload,
        };
        self.registered_cores.write().await.insert(
            discovery,
            RegisteredCoreState {
                meta,
                core,
            },
        );
    }

    pub async fn clear_outgoing_drive(&self) {
        self.registered_cores
            .write()
            .await
            .retain(|_, state| !state.meta.upload);
        *self.outgoing_drive.write().await = None;
    }

    pub async fn clear_incoming_cores(&self) {
        self.registered_cores
            .write()
            .await
            .retain(|_, state| state.meta.upload);
    }

    pub async fn registered_core_for_discovery(
        &self,
        discovery_key_hex: &str,
    ) -> Option<RegisteredCore> {
        self.registered_cores
            .read()
            .await
            .get(&discovery_key_hex.to_lowercase())
            .map(|state| state.meta.clone())
    }

    pub async fn download_targets(&self) -> Vec<(RegisteredCore, Arc<Mutex<Hypercore>>)> {
        self.registered_cores
            .read()
            .await
            .values()
            .filter(|state| !state.meta.upload)
            .map(|state| (state.meta.clone(), state.core.clone()))
            .collect()
    }

    pub async fn registered_cores(&self) -> Vec<RegisteredCore> {
        self.registered_cores
            .read()
            .await
            .values()
            .map(|state| state.meta.clone())
            .collect()
    }

    pub async fn outgoing_drive(&self) -> Option<Arc<Mutex<OutgoingDrive>>> {
        self.outgoing_drive.read().await.clone()
    }

    pub async fn set_active_drive(&self, peer_key: &str, drive_key_hex: &str) {
        self.active_drive_keys
            .write()
            .await
            .insert(peer_key.to_string(), drive_key_hex.to_string());
    }

    pub async fn clear_peer(&self, peer_key: &str) {
        self.active_drive_keys.write().await.remove(peer_key);
    }

    pub async fn clear_all(&self) {
        self.active_drive_keys.write().await.clear();
        self.registered_cores.write().await.clear();
        *self.outgoing_drive.write().await = None;
    }
}

/// Attached when a peer connects. Hypercore protomux replication plugs in here.
pub struct ReplicationHandle {
    peer_key: String,
    registry: Arc<ReplicationRegistry>,
}

impl ReplicationHandle {
    pub fn attach_peer(peer_key: &str, registry: Arc<ReplicationRegistry>) -> Self {
        info!("replication hook attached for peer {peer_key}");
        Self {
            peer_key: peer_key.to_string(),
            registry,
        }
    }

    pub async fn attach_hypercore_channel(
        &self,
        mux: &mut PeerMux,
        remote_id: u64,
        discovery_key_hex: &str,
        handshake: &[u8],
        local_is_initiator: bool,
        handshake_hash: &[u8; 64],
        outbound_tx: mpsc::UnboundedSender<ReplicationOutbound>,
    ) -> Result<(), String> {
        let (meta, core) = {
            let guard = self.registry.registered_cores.read().await;
            let state = guard
                .get(&discovery_key_hex.to_lowercase())
                .ok_or_else(|| format!("unknown discovery key {discovery_key_hex}"))?;
            (state.meta.clone(), state.core.clone())
        };

        let public_key = hex::decode(&meta.public_key_hex)
            .map_err(|_| "invalid core public key".to_string())?;
        if public_key.len() != 32 {
            return Err("core public key must be 32 bytes".into());
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&public_key);

        info!(
            "peer {} hypercore/alpha channel {} for core {} ({})",
            self.peer_key,
            remote_id,
            discovery_key_hex,
            if meta.upload { "upload" } else { "download" }
        );

        if meta.upload {
            HypercoreReplicationPeer::attach(
                mux,
                remote_id,
                handshake,
                local_is_initiator,
                handshake_hash,
                key,
                core,
                outbound_tx,
            )
        } else {
            HypercoreReplicationClient::attach(
                mux,
                remote_id,
                handshake,
                local_is_initiator,
                handshake_hash,
                key,
                core,
                outbound_tx,
            )
        }
    }

    pub async fn detach(self) {
        self.registry.clear_peer(&self.peer_key).await;
    }
}

fn discovery_key_hex(public_key_hex: &str) -> String {
    let bytes = hex::decode(public_key_hex).unwrap_or_default();
    if bytes.len() != 32 {
        return public_key_hex.to_lowercase();
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    hex::encode(hypercore_protocol::discovery_key(&key))
}
