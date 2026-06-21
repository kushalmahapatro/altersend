//! Application spine: owns session state (domain reducer) and drives the P2P orchestrator.

mod commands;
mod session;

pub use commands::*;
pub use session::*;
