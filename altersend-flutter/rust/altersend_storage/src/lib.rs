//! Corestore / Hyperdrive storage scaffolding for JS interop.
//!
//! Full Hyperdrive replication requires a Rust Hyperdrive port (not yet available).
//! This crate wires [`hypercore-protocol`] replication onto peer connections alongside
//! Protomux control channels — matching the Electron/RN worklet layout.

mod corestore;
mod replicate;

pub use corestore::CoreStore;
pub use replicate::ReplicationHandle;
