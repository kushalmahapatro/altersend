mod capability;
mod client;
pub mod peer;
mod wire;

pub use capability::{decode_handshake, encode_handshake, expected_remote_capability, local_capability};
pub use client::HypercoreReplicationClient;
pub use peer::{
    send_replication_outbound, HypercoreReplicationPeer, ReplicationMux, ReplicationOutbound,
};
