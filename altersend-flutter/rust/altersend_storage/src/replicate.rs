use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

/// Tracks active outgoing drive keys for replication with connected peers.
#[derive(Default)]
pub struct ReplicationRegistry {
    active_drive_keys: RwLock<HashMap<String, String>>,
}

impl ReplicationRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
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
    }
}

/// Attached when a peer connects. Hypercore protomux replication will plug in here.
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

    pub async fn detach(self) {
        self.registry.clear_peer(&self.peer_key).await;
    }
}
