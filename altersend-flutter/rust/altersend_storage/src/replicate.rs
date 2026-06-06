use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{info, warn};

/// Placeholder for `corestore.replicate(socket, { live: true })`.
///
/// When Hyperdrive interop lands, this will run hypercore-protocol replication
/// on the same encrypted peer stream that carries Protomux channels.
pub struct ReplicationHandle {
    _inner: Arc<Mutex<()>>,
}

impl ReplicationHandle {
    pub fn attach_peer(_peer_key: &str, _is_initiator: bool) -> Self {
        info!("replication hook attached (hyperdrive staging pending)");
        Self {
            _inner: Arc::new(Mutex::new(())),
        }
    }

    pub async fn detach(&self) {
        warn!("replication hook detached");
    }
}
