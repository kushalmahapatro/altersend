use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hypercore::{Hypercore, HypercoreBuilder, PartialKeypair, Storage, VerifyingKey};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::timeout;

use super::bee::{find_entry_value, parse_header_content_key, BlobRef};
use super::manifest::{derive_blobs_public_key, ManifestSlot};

#[derive(Debug, Error)]
pub enum IncomingHyperdriveError {
    #[error("hypercore: {0}")]
    Hypercore(#[from] hypercore::HypercoreError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid drive key")]
    InvalidDriveKey,
    #[error("replication timed out")]
    ReplicationTimeout,
    #[error("entry not found: {0}")]
    EntryNotFound(String),
    #[error("blob not available")]
    BlobNotAvailable,
}

pub struct IncomingHyperdrive {
    drive_key: [u8; 32],
    metadata: Arc<Mutex<Hypercore>>,
    manifest_slot: ManifestSlot,
    blobs: Option<Arc<Mutex<Hypercore>>>,
}

impl IncomingHyperdrive {
    pub async fn open(dir: impl AsRef<Path>, drive_key_hex: &str) -> Result<Self, IncomingHyperdriveError> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;
        let public_key = parse_key_hex(drive_key_hex)?;
        let verifying = VerifyingKey::from_bytes(&public_key).map_err(|_| IncomingHyperdriveError::InvalidDriveKey)?;
        let db_dir = dir.join("db");
        let storage = Storage::new_disk(&db_dir, true).await?;
        let metadata = HypercoreBuilder::new(storage)
            .key_pair(PartialKeypair {
                public: verifying,
                secret: None,
            })
            .build()
            .await?;
        Ok(Self {
            drive_key: public_key,
            metadata: Arc::new(Mutex::new(metadata)),
            manifest_slot: Arc::new(Mutex::new(None)),
            blobs: None,
        })
    }

    pub fn metadata_core(&self) -> Arc<Mutex<Hypercore>> {
        self.metadata.clone()
    }

    pub fn manifest_slot(&self) -> ManifestSlot {
        self.manifest_slot.clone()
    }

    pub async fn metadata_public_key_async(&self) -> [u8; 32] {
        self.metadata.lock().await.key_pair().public.to_bytes()
    }

    pub async fn blobs_core(
        &mut self,
        dir: impl AsRef<Path>,
        wait: Duration,
    ) -> Result<Arc<Mutex<Hypercore>>, IncomingHyperdriveError> {
        if let Some(core) = &self.blobs {
            return Ok(core.clone());
        }
        let header = {
            let mut core = self.metadata.lock().await;
            core.get(0).await?
        };
        let header = header.ok_or(IncomingHyperdriveError::BlobNotAvailable)?;
        let content_key = if let Some(key) = parse_header_content_key(&header) {
            key
        } else {
            self.wait_for_manifest(wait).await?
        };
        let verifying = VerifyingKey::from_bytes(&content_key).map_err(|_| IncomingHyperdriveError::InvalidDriveKey)?;
        let blobs_dir = dir.as_ref().join("blobs");
        let storage = Storage::new_disk(&blobs_dir, true).await?;
        let blobs = HypercoreBuilder::new(storage)
            .key_pair(PartialKeypair {
                public: verifying,
                secret: None,
            })
            .build()
            .await?;
        let arc = Arc::new(Mutex::new(blobs));
        self.blobs = Some(arc.clone());
        Ok(arc)
    }

    async fn wait_for_manifest(&self, wait: Duration) -> Result<[u8; 32], IncomingHyperdriveError> {
        timeout(wait, async {
            loop {
                if let Some(manifest) = self.manifest_slot.lock().await.clone() {
                    return Ok(derive_blobs_public_key(&manifest, &self.drive_key));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| IncomingHyperdriveError::ReplicationTimeout)?
    }

    pub async fn wait_until_contiguous(
        &self,
        core: &Arc<Mutex<Hypercore>>,
        min_length: u64,
        wait: Duration,
    ) -> Result<(), IncomingHyperdriveError> {
        timeout(wait, async {
            loop {
                let info = core.lock().await.info();
                if info.contiguous_length >= min_length.max(1) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| IncomingHyperdriveError::ReplicationTimeout)?
    }

    pub async fn read_file(
        &self,
        path: &str,
        wait: Duration,
    ) -> Result<Vec<u8>, IncomingHyperdriveError> {
        self.wait_until_contiguous(&self.metadata, 2, wait).await?;
        let blocks = {
            let mut core = self.metadata.lock().await;
            let len = core.info().length;
            let mut blocks = Vec::with_capacity(len as usize);
            for i in 0..len {
                if let Some(block) = core.get(i).await? {
                    blocks.push(block);
                }
            }
            blocks
        };
        let entry = find_entry_value(&blocks, path).ok_or_else(|| IncomingHyperdriveError::EntryNotFound(path.to_string()))?;
        let blob = entry.blob.ok_or(IncomingHyperdriveError::BlobNotAvailable)?;
        let blobs_core = self
            .blobs
            .as_ref()
            .ok_or(IncomingHyperdriveError::BlobNotAvailable)?;
        self.wait_until_contiguous(
            blobs_core,
            blob.block_offset + blob.block_length,
            wait,
        )
        .await?;
        read_blob(blobs_core, &blob).await
    }
}

async fn read_blob(
    core: &Arc<Mutex<Hypercore>>,
    blob: &BlobRef,
) -> Result<Vec<u8>, IncomingHyperdriveError> {
    let mut out = Vec::with_capacity(blob.byte_length as usize);
    let mut core = core.lock().await;
    for index in blob.block_offset..blob.block_offset + blob.block_length {
        let block = core.get(index).await?.ok_or(IncomingHyperdriveError::BlobNotAvailable)?;
        out.extend_from_slice(&block);
    }
    let start = blob.byte_offset as usize;
    let end = start + blob.byte_length as usize;
    if end > out.len() {
        return Err(IncomingHyperdriveError::BlobNotAvailable);
    }
    Ok(out[start..end].to_vec())
}

fn parse_key_hex(hex_str: &str) -> Result<[u8; 32], IncomingHyperdriveError> {
    let bytes = hex::decode(hex_str).map_err(|_| IncomingHyperdriveError::InvalidDriveKey)?;
    if bytes.len() != 32 {
        return Err(IncomingHyperdriveError::InvalidDriveKey);
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}
