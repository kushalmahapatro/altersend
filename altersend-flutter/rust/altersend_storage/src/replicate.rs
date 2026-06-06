use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::drive::OutgoingDrive;

/// Local Hypercore registered for outgoing replication to legacy peers.
#[derive(Clone)]
pub struct RegisteredCore {
    pub public_key_hex: String,
    pub discovery_key_hex: String,
}

/// Tracks active outgoing drive keys and registered cores for replication with connected peers.
pub struct ReplicationRegistry {
    active_drive_keys: RwLock<HashMap<String, String>>,
    registered_cores: RwLock<HashMap<String, RegisteredCore>>,
    outgoing_drive: RwLock<Option<Arc<tokio::sync::Mutex<OutgoingDrive>>>>,
}

impl ReplicationRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active_drive_keys: RwLock::new(HashMap::new()),
            registered_cores: RwLock::new(HashMap::new()),
            outgoing_drive: RwLock::new(None),
        })
    }

    pub async fn set_outgoing_drive(&self, drive: Arc<tokio::sync::Mutex<OutgoingDrive>>) {
        let key_hex = {
            let guard = drive.lock().await;
            guard.key_hex()
        };
        let discovery = discovery_key_hex(&key_hex);
        let core = RegisteredCore {
            public_key_hex: key_hex.clone(),
            discovery_key_hex: discovery.clone(),
        };
        self.registered_cores
            .write()
            .await
            .insert(discovery, core);
        *self.outgoing_drive.write().await = Some(drive);
    }

    pub async fn clear_outgoing_drive(&self) {
        self.registered_cores.write().await.clear();
        *self.outgoing_drive.write().await = None;
    }

    pub async fn registered_core_for_discovery(
        &self,
        discovery_key_hex: &str,
    ) -> Option<RegisteredCore> {
        self.registered_cores
            .read()
            .await
            .get(&discovery_key_hex.to_lowercase())
            .cloned()
    }

    pub async fn registered_cores(&self) -> Vec<RegisteredCore> {
        self.registered_cores.read().await.values().cloned().collect()
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
        self.clear_outgoing_drive().await;
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

    pub async fn on_hypercore_channel_open(&self, discovery_key_hex: &str) {
        if self
            .registry
            .registered_core_for_discovery(discovery_key_hex)
            .await
            .is_some()
        {
            info!(
                "legacy peer {} opened hypercore/alpha for local core {}",
                self.peer_key, discovery_key_hex
            );
            return;
        }

        warn!(
            "legacy peer {} opened hypercore/alpha for unknown discovery key {}",
            self.peer_key, discovery_key_hex
        );
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
