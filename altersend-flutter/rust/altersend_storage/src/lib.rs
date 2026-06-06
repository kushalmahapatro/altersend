//! Corestore / Hyperdrive storage scaffolding for JS interop.
//!
//! Full Hyperdrive replication requires a Rust Hyperdrive port (not yet available).
//! This crate wires [`hypercore`] staging for outgoing transfers and tracks replication
//! hooks on peer connections.

mod corestore;
mod drive;
mod replicate;

pub use corestore::CoreStore;
pub use drive::{OutgoingDrive, StagedFileMeta, CHUNK_SIZE as DRIVE_CHUNK_SIZE};
pub use replicate::{ReplicationHandle, ReplicationRegistry};
