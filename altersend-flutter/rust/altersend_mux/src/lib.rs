//! Minimal [Protomux](https://github.com/holepunchto/protomux) implementation for AlterSend.
//!
//! Wire-compatible with the JavaScript `protomux` package used by the Electron/RN worklet.
//! Supports opening named channels and sending JSON messages — enough for the
//! `altersend/control` coordination channel.

mod channel;
mod codec;
mod session;

pub use channel::{Channel, MessageHandle};
pub use session::{MuxError, PeerMux, PeerMuxBuilder};

pub const CONTROL_PROTOCOL: &str = "altersend/control";
pub const CHUNK_PROTOCOL: &str = "altersend/chunks";
