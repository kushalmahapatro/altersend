mod capability;
pub mod peer;
mod wire;

pub use capability::{decode_handshake, expected_remote_capability};
pub use peer::{
    send_replication_outbound, HypercoreReplicationPeer, ReplicationMux, ReplicationOutbound,
};
