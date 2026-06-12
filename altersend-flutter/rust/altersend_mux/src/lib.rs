//! Minimal [Protomux](https://github.com/holepunchto/protomux) implementation for AlterSend.
//!
//! Wire-compatible with the JavaScript `protomux` package used by the Electron/RN worklet.
//! Supports opening named channels and sending JSON messages — enough for the
//! `altersend/control` coordination channel.

mod channel;
mod codec;
mod session;

pub use channel::{Channel, MessageHandle};
pub use session::{
    BinaryMessageCallback, MuxError, PeerMux, PeerMuxBuilder, RemoteChannelOpen,
};

pub const CONTROL_PROTOCOL: &str = "altersend/control";
pub const CHUNK_PROTOCOL: &str = "altersend/chunks";
/// Hypercore replication channel used by the JS worklet (`corestore.replicate`).
pub const HYPERCORE_ALPHA_PROTOCOL: &str = "hypercore/alpha";
/// Alias accepted by holepunchto/hypercore replicator.
pub const HYPERCORE_PROTOCOL_ALIAS: &str = "hypercore";
