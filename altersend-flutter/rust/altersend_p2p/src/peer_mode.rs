use altersend_mux::{CHUNK_PROTOCOL, HYPERCORE_ALPHA_PROTOCOL, HYPERCORE_PROTOCOL_ALIAS};

/// How a connected peer transfers file bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerInteropMode {
    /// Flutter+Rust peer — uses `altersend/chunks` after control negotiation.
    Rust,
    /// Original AlterSend worklet — Hyperdrive over `hypercore/alpha` protomux channels.
    Legacy,
    /// Not enough signal yet (only control channel observed).
    Unknown,
}

impl PeerInteropMode {
    pub fn observe_remote_protocol(self, protocol: &str) -> Self {
        if is_hypercore_replication_protocol(protocol) {
            return Self::Legacy;
        }
        if protocol == CHUNK_PROTOCOL {
            return Self::Rust;
        }
        self
    }

    pub fn uses_chunk_channel(self) -> bool {
        matches!(self, Self::Rust)
    }

    pub fn uses_hypercore_replication(self) -> bool {
        matches!(self, Self::Legacy)
    }
}

pub fn is_hypercore_replication_protocol(protocol: &str) -> bool {
    protocol == HYPERCORE_ALPHA_PROTOCOL || protocol == HYPERCORE_PROTOCOL_ALIAS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_legacy_hypercore_channel() {
        let mode = PeerInteropMode::Unknown.observe_remote_protocol(HYPERCORE_ALPHA_PROTOCOL);
        assert_eq!(mode, PeerInteropMode::Legacy);
    }

    #[test]
    fn detects_rust_chunk_channel() {
        let mode = PeerInteropMode::Unknown.observe_remote_protocol(CHUNK_PROTOCOL);
        assert_eq!(mode, PeerInteropMode::Rust);
    }
}
