use blake2::{
    Blake2bMac,
    digest::{FixedOutput, Update, typenum::U32},
};

const REPLICATE_INITIATOR: [u8; 32] = [
    0x51, 0x81, 0x2A, 0x2A, 0x35, 0x9B, 0x50, 0x36, 0x95, 0x36, 0x77, 0x5D, 0xF8, 0x9E, 0x18, 0xE4,
    0x77, 0x40, 0xF3, 0xDB, 0x72, 0xAC, 0x0A, 0xE7, 0x0B, 0x29, 0x59, 0x4C, 0x19, 0x4D, 0xC3, 0x16,
];
const REPLICATE_RESPONDER: [u8; 32] = [
    0x04, 0x38, 0x49, 0x2D, 0x02, 0x97, 0x0C, 0xC1, 0x35, 0x28, 0xAC, 0x02, 0x62, 0xBC, 0xA0, 0x07,
    0x4E, 0x09, 0x26, 0x26, 0x02, 0x56, 0x86, 0x5A, 0xCC, 0xC0, 0xBF, 0x15, 0xBD, 0x79, 0x12, 0x7D,
];

/// Decode protomux hypercore handshake `{ seeks, capability }`.
pub fn decode_handshake(data: &[u8]) -> Option<(bool, [u8; 32])> {
    use compact_encoding::CompactEncoding;
    let mut slice = data;
    let (flags, rest) = u64::decode(&mut slice).ok()?;
    let slice = rest;
    if slice.len() < 32 {
        return None;
    }
    let mut capability = [0u8; 32];
    capability.copy_from_slice(&slice[..32]);
    Some(((flags & 1) != 0, capability))
}

/// Capability we send when locally opening a hypercore replication channel.
pub fn local_capability(
    local_is_initiator: bool,
    core_public_key: &[u8; 32],
    handshake_hash: &[u8; 64],
) -> [u8; 32] {
    replicate_capability(local_is_initiator, core_public_key, handshake_hash)
}

/// Encode protomux hypercore handshake `{ seeks, capability }`.
pub fn encode_handshake(seeks: bool, capability: &[u8; 32]) -> Vec<u8> {
    use compact_encoding::CompactEncoding;
    let flags = if seeks { 1u64 } else { 0u64 };
    let size = flags.encoded_size().unwrap();
    let mut buf = Vec::with_capacity(size + 32);
    buf.resize(size, 0);
    flags.encode(&mut buf).unwrap();
    buf.extend_from_slice(capability);
    buf
}

/// Expected remote capability for a hypercore public key.
pub fn expected_remote_capability(
    local_is_initiator: bool,
    core_public_key: &[u8; 32],
    handshake_hash: &[u8; 64],
) -> [u8; 32] {
    replicate_capability(!local_is_initiator, core_public_key, handshake_hash)
}

fn replicate_capability(is_initiator: bool, key: &[u8], handshake_hash: &[u8]) -> [u8; 32] {
    let seed = if is_initiator {
        REPLICATE_INITIATOR
    } else {
        REPLICATE_RESPONDER
    };

    let mut hasher =
        Blake2bMac::<U32>::new_with_salt_and_personal(handshake_hash, &[], &[]).unwrap();
    hasher.update(&seed);
    hasher.update(key);
    let hash = hasher.finalize_fixed();
    let mut out = [0u8; 32];
    out.copy_from_slice(hash.as_slice());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_handshake() {
        use compact_encoding::CompactEncoding;
        let mut buf = Vec::new();
        let size = 1u64.encoded_size().unwrap();
        buf.resize(size, 0);
        1u64.encode(&mut buf).unwrap();
        buf.extend_from_slice(&[0xAB; 32]);
        let (seeks, cap) = decode_handshake(&buf).unwrap();
        assert!(seeks);
        assert_eq!(cap, [0xAB; 32]);
    }
}
