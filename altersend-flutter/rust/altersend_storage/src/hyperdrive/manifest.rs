use blake2::digest::{consts::U32, Digest};
use blake2::Blake2b;
use compact_encoding::{CompactEncoding, EncodingError, decode_usize, take_array};
use std::sync::Arc;
use tokio::sync::Mutex;

pub const MANIFEST_CAP: [u8; 32] = [
    0xe6, 0x4b, 0x71, 0x08, 0xea, 0xcc, 0xe4, 0x7c, 0xfc, 0x61, 0xac, 0x85, 0x05, 0x68, 0xf5, 0x5f,
    0x8b, 0x15, 0xb8, 0x2e, 0xc5, 0xed, 0x78, 0xc4, 0xec, 0x59, 0x7b, 0x03, 0x6e, 0x2a, 0x14, 0x98,
];

pub const BLOBS_NAMESPACE: [u8; 32] = [
    0xf2, 0x17, 0xb7, 0x47, 0x16, 0x17, 0x84, 0xb4, 0x95, 0xce, 0xee, 0x3c, 0x42, 0x16, 0x3b, 0x07,
    0xdd, 0x62, 0xcb, 0x06, 0xdf, 0x26, 0xe2, 0x0e, 0x95, 0xba, 0x4d, 0x43, 0x81, 0x23, 0xae, 0xeb,
];

pub const DEFAULT_SIGNER_NAMESPACE: [u8; 32] = [
    0x41, 0x44, 0xee, 0xa5, 0x31, 0xe4, 0x83, 0xd5, 0x4e, 0x0c, 0x14, 0xf4, 0xca, 0x68, 0xe0, 0x64,
    0x4f, 0x35, 0x53, 0x43, 0xff, 0x6f, 0xcb, 0x0f, 0x00, 0x52, 0x00, 0xe1, 0x2c, 0xd7, 0x47, 0xcb,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HypercoreSigner {
    pub namespace: [u8; 32],
    pub public_key: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HypercoreManifest {
    pub version: u64,
    pub allow_patch: bool,
    pub quorum: u64,
    pub signers: Vec<HypercoreSigner>,
}

impl HypercoreManifest {
    pub fn decode(buffer: &[u8]) -> Result<(Self, &[u8]), EncodingError> {
        let (version, rest) = u64::decode(buffer)?;
        if version == 0 {
            return Err(EncodingError::invalid_data("manifest v0 not supported"));
        }
        if version > 2 {
            return Err(EncodingError::invalid_data("unknown manifest version"));
        }

        let (flags, rest) = u64::decode(rest)?;
        let (hash_id, rest) = u64::decode(rest)?;
        if hash_id != 0 {
            return Err(EncodingError::invalid_data("unknown manifest hash"));
        }
        let (quorum, rest) = u64::decode(rest)?;
        let (signers, rest) = decode_signer_array(rest)?;

        let allow_patch = flags & 0b0000_0001 != 0;
        let mut cursor = rest;
        if flags & 0b0000_0010 != 0 {
            let (_, next) = take_array::<32>(cursor)?;
            let (_, next) = u64::decode(next)?;
            cursor = next;
        }
        if flags & 0b0000_0100 != 0 {
            let (len, next) = decode_usize(cursor)?;
            cursor = &next[len..];
        }
        if flags & 0b0000_1000 != 0 {
            let (len, next) = decode_usize(cursor)?;
            cursor = &next[len..];
        }

        Ok((
            Self {
                version,
                allow_patch,
                quorum,
                signers,
            },
            cursor,
        ))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        write_u64(self.version, &mut body);
        let mut flags = 0u64;
        if self.allow_patch {
            flags |= 0b0000_0001;
        }
        write_u64(flags, &mut body);
        write_u64(0, &mut body); // blake2b
        write_u64(self.quorum, &mut body);
        encode_signer_array(&self.signers, &mut body);

        let mut out = Vec::with_capacity(32 + body.len());
        out.extend_from_slice(&MANIFEST_CAP);
        out.extend_from_slice(&body);
        out
    }
}

pub fn manifest_hash(manifest: &HypercoreManifest) -> [u8; 32] {
    hash_bytes(&manifest.encode())
}

pub fn derive_content_manifest(
    metadata_manifest: &HypercoreManifest,
    drive_key: &[u8; 32],
) -> HypercoreManifest {
    let signers = metadata_manifest
        .signers
        .iter()
        .map(|signer| HypercoreSigner {
            namespace: hash_batch(&[&BLOBS_NAMESPACE, drive_key, &signer.namespace]),
            public_key: signer.public_key,
        })
        .collect();
    HypercoreManifest {
        version: metadata_manifest.version,
        allow_patch: metadata_manifest.allow_patch,
        quorum: metadata_manifest.quorum,
        signers,
    }
}

pub fn derive_blobs_public_key(
    metadata_manifest: &HypercoreManifest,
    drive_key: &[u8; 32],
) -> [u8; 32] {
    manifest_hash(&derive_content_manifest(metadata_manifest, drive_key))
}

fn decode_signer_array(buffer: &[u8]) -> Result<(Vec<HypercoreSigner>, &[u8]), EncodingError> {
    let (len, rest) = decode_usize(buffer)?;
    let mut signers = Vec::with_capacity(len);
    let mut cursor = rest;
    for _ in 0..len {
        let (signer, next) = decode_signer(cursor)?;
        signers.push(signer);
        cursor = next;
    }
    Ok((signers, cursor))
}

fn decode_signer(buffer: &[u8]) -> Result<(HypercoreSigner, &[u8]), EncodingError> {
    let (signature_id, rest) = u64::decode(buffer)?;
    if signature_id != 0 {
        return Err(EncodingError::invalid_data("unknown signer signature"));
    }
    let (namespace, rest) = take_array::<32>(rest)?;
    let (public_key, rest) = take_array::<32>(rest)?;
    Ok((
        HypercoreSigner {
            namespace,
            public_key,
        },
        rest,
    ))
}

fn encode_signer_array(signers: &[HypercoreSigner], out: &mut Vec<u8>) {
    write_usize(signers.len(), out);
    for signer in signers {
        encode_signer(signer, out);
    }
}

fn encode_signer(signer: &HypercoreSigner, out: &mut Vec<u8>) {
    write_u64(0, out); // ed25519
    out.extend_from_slice(&signer.namespace);
    out.extend_from_slice(&signer.public_key);
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    let mut v = value;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn write_usize(value: usize, out: &mut Vec<u8>) {
    write_u64(value as u64, out);
}

fn hash_bytes(data: &[u8]) -> [u8; 32] {
    let digest = Blake2b::<U32>::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn hash_batch(parts: &[&[u8; 32]]) -> [u8; 32] {
    let mut hasher = Blake2b::<U32>::new();
    for part in parts {
        hasher.update(*part);
    }
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

pub type ManifestSlot = Arc<Mutex<Option<HypercoreManifest>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_hash_matches_js_fixture() {
        let manifest = HypercoreManifest {
            version: 1,
            allow_patch: false,
            quorum: 1,
            signers: vec![HypercoreSigner {
                namespace: DEFAULT_SIGNER_NAMESPACE,
                public_key: [1u8; 32],
            }],
        };
        let hash = manifest_hash(&manifest);
        assert_eq!(
            hex::encode(hash),
            "1ec23a2b047862aa88526b18b8c0c096f6438ecf2a652410ca55c35259dc6e4d"
        );
    }

    #[test]
    fn derives_blobs_key_from_wire_manifest() {
        let wire = hex::decode(
            "0100000101004144eea531e483d54e0c14f4ca68e0644f355343ff6fcb0f005200e12cd747cb20425c7285fca12b968a546470308a593e0898100bb4d41c6f6f14332dac8f61",
        )
        .unwrap();
        let (manifest, _) = HypercoreManifest::decode(&wire).expect("decode");
        let drive_key_bytes = hex::decode(
            "76e0eb5236991405a53c2321f96b29936a8b67ef7db06598e9e3d5a701c22f33",
        )
        .unwrap();
        let mut drive_key = [0u8; 32];
        drive_key.copy_from_slice(&drive_key_bytes);
        let blobs = derive_blobs_public_key(&manifest, &drive_key);
        assert_eq!(
            hex::encode(blobs),
            "8a96e441d0f41e50c218f9e4f155e89ca6424d75b116989c8f201006bbe2966d"
        );
    }
}
