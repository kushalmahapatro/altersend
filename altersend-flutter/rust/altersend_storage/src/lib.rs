//! Corestore / Hyperdrive storage for JS interop.
//!
//! Outgoing transfers stage files into real Hyperdrive metadata + blobs cores.
//! Incoming replication reads remote drives via [`IncomingHyperdrive`].

mod corestore;
mod drive;
mod hyperdrive;
mod replicate;
mod replication;

pub use corestore::CoreStore;
pub use drive::{OutgoingDrive, StagedFileMeta, CHUNK_SIZE as DRIVE_CHUNK_SIZE};
pub use hyperdrive::{IncomingHyperdrive, IncomingHyperdriveError};
pub use replicate::{ReplicationHandle, ReplicationRegistry, RegisteredCore};
pub use replication::{
    encode_handshake, local_capability, send_replication_outbound, HypercoreReplicationClient,
    HypercoreReplicationPeer, ReplicationOutbound,
};
