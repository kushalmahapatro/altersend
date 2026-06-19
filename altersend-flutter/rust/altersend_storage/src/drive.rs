use std::path::Path;
use std::sync::Arc;

use hypercore::{Hypercore, HypercoreBuilder, Storage};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;
use tokio::sync::Mutex;

use crate::hyperdrive::{
    encode_drive_path, encode_entry_value, encode_hyperbee_header, encode_hyperbee_node, BlobRef,
    BLOB_BLOCK_SIZE,
};

pub const CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Error)]
pub enum DriveError {
    #[error("hypercore: {0}")]
    Hypercore(#[from] hypercore::HypercoreError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown file id: {0}")]
    UnknownFile(String),
    #[error("file not staged")]
    NotStaged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedFileMeta {
    pub id: String,
    pub name: String,
    pub path: String,
    pub size: u64,
    pub block_start: u64,
    pub block_count: u64,
}

struct StagedEntry {
    seq: u64,
    path: String,
    blob: BlobRef,
}

/// Writable Hyperdrive for an outgoing transfer session.
///
/// Files are staged into a metadata hyperbee core plus a separate blobs core.
/// The metadata public key hex is the `driveKey` in control messages.
pub struct OutgoingDrive {
    metadata: Arc<Mutex<Hypercore>>,
    blobs: Arc<Mutex<Hypercore>>,
    files: Vec<StagedFileMeta>,
    entries: Vec<StagedEntry>,
    total_blob_bytes: u64,
    next_seq: u64,
}

impl OutgoingDrive {
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self, DriveError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;

        let blobs_dir = dir.join("blobs");
        let blobs_storage = Storage::new_disk(&blobs_dir, true).await?;
        let blobs = HypercoreBuilder::new(blobs_storage).build().await?;
        let blobs_key = blobs.key_pair().public.to_bytes();

        let db_dir = dir.join("db");
        let metadata_storage = Storage::new_disk(&db_dir, true).await?;
        let mut metadata = HypercoreBuilder::new(metadata_storage).build().await?;
        metadata
            .append(&encode_hyperbee_header(&blobs_key))
            .await?;

        Ok(Self {
            metadata: Arc::new(Mutex::new(metadata)),
            blobs: Arc::new(Mutex::new(blobs)),
            files: Vec::new(),
            entries: Vec::new(),
            total_blob_bytes: 0,
            next_seq: 0,
        })
    }

    pub fn metadata_core(&self) -> Arc<Mutex<Hypercore>> {
        self.metadata.clone()
    }

    pub fn blobs_core(&self) -> Arc<Mutex<Hypercore>> {
        self.blobs.clone()
    }

    pub fn shared_core(&self) -> Arc<Mutex<Hypercore>> {
        self.metadata_core()
    }

    pub async fn key_hex(&self) -> String {
        let core = self.metadata.lock().await;
        hex::encode(core.key_pair().public.as_bytes())
    }

    pub async fn blobs_key_hex(&self) -> String {
        let core = self.blobs.lock().await;
        hex::encode(core.key_pair().public.as_bytes())
    }

    pub fn files(&self) -> &[StagedFileMeta] {
        &self.files
    }

    pub fn file_meta(&self, file_id: &str) -> Option<&StagedFileMeta> {
        self.files.iter().find(|f| f.id == file_id)
    }

    pub async fn stage_file(
        &mut self,
        file_id: &str,
        name: &str,
        path: &str,
        disk_path: &Path,
    ) -> Result<(), DriveError> {
        let data = fs::read(disk_path).await?;
        let blob = self.append_blob(&data).await?;
        self.next_seq += 1;
        let seq = self.next_seq;

        self.entries.push(StagedEntry {
            seq,
            path: path.to_string(),
            blob: blob.clone(),
        });

        let mut sorted = self.entries.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|entry| encode_drive_path(&entry.path));
        let sorted_seqs = sorted.iter().map(|entry| entry.seq).collect::<Vec<_>>();

        let node = encode_hyperbee_node(
            &sorted_seqs,
            &encode_drive_path(path),
            &encode_entry_value(&blob),
        );
        self.metadata.lock().await.append(&node).await?;

        self.files.push(StagedFileMeta {
            id: file_id.to_string(),
            name: name.to_string(),
            path: path.to_string(),
            size: data.len() as u64,
            block_start: blob.block_offset,
            block_count: blob.block_length,
        });

        Ok(())
    }

    async fn append_blob(&mut self, data: &[u8]) -> Result<BlobRef, DriveError> {
        let mut blobs = self.blobs.lock().await;
        let block_offset = blobs.info().length;
        let byte_offset = self.total_blob_bytes;

        for chunk in data.chunks(BLOB_BLOCK_SIZE) {
            blobs.append(chunk).await?;
        }

        let block_length = blobs.info().length - block_offset;
        self.total_blob_bytes += data.len() as u64;

        Ok(BlobRef {
            block_offset,
            block_length,
            byte_offset,
            byte_length: data.len() as u64,
        })
    }

    pub async fn read_file_range(
        &mut self,
        file_id: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, DriveError> {
        let meta = self
            .files
            .iter()
            .find(|f| f.id == file_id)
            .cloned()
            .ok_or_else(|| DriveError::UnknownFile(file_id.to_string()))?;

        if offset >= meta.size {
            return Ok(0);
        }

        let blob = self
            .entries
            .iter()
            .find(|entry| entry.path == meta.path)
            .map(|entry| entry.blob.clone())
            .ok_or(DriveError::NotStaged)?;

        read_blob_range(&self.blobs, &blob, offset, buf).await
    }

    pub async fn core_length(&self) -> u64 {
        self.metadata.lock().await.info().length
    }

    pub async fn core_info(&self) -> hypercore::Info {
        self.metadata.lock().await.info()
    }

    pub async fn public_key_bytes(&self) -> [u8; 32] {
        self.metadata.lock().await.key_pair().public.to_bytes()
    }

    pub async fn create_proof(
        &self,
        block: Option<hypercore_schema::RequestBlock>,
        hash: Option<hypercore_schema::RequestBlock>,
        seek: Option<hypercore_schema::RequestSeek>,
        upgrade: Option<hypercore_schema::RequestUpgrade>,
    ) -> Result<Option<hypercore_schema::Proof>, DriveError> {
        let mut core = self.metadata.lock().await;
        Ok(core
            .create_proof(block, hash, seek, upgrade)
            .await?)
    }
}

async fn read_blob_range(
    blobs: &Arc<Mutex<Hypercore>>,
    blob: &BlobRef,
    offset: u64,
    buf: &mut [u8],
) -> Result<usize, DriveError> {
    if offset >= blob.byte_length {
        return Ok(0);
    }

    let mut out = Vec::with_capacity(blob.byte_length as usize);
    let mut core = blobs.lock().await;
    for index in blob.block_offset..blob.block_offset + blob.block_length {
        let block = core
            .get(index)
            .await?
            .ok_or(DriveError::NotStaged)?;
        out.extend_from_slice(&block);
    }

    let start = blob.byte_offset as usize + offset as usize;
    let want = (blob.byte_length - offset) as usize;
    let take = want.min(buf.len()).min(out.len().saturating_sub(start));
    if take == 0 {
        return Ok(0);
    }
    buf[..take].copy_from_slice(&out[start..start + take]);
    Ok(take)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperdrive::find_entry_value;

    #[tokio::test]
    async fn stages_and_reads_back() {
        let dir = std::env::temp_dir().join(format!(
            "altersend-drive-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut drive = OutgoingDrive::open(&dir).await.unwrap();
        let key = drive.key_hex().await;
        assert_eq!(key.len(), 64);

        let file_path = dir.join("sample.txt");
        fs::write(&file_path, b"hello hyperdrive staging").await.unwrap();
        drive
            .stage_file("f1", "sample.txt", "/sample.txt", &file_path)
            .await
            .unwrap();

        let mut buf = [0u8; 64];
        let n = drive.read_file_range("f1", 0, &mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello hyperdrive staging");

        let blocks = {
            let mut core = drive.metadata.lock().await;
            let len = core.info().length;
            let mut blocks = Vec::with_capacity(len as usize);
            for i in 0..len {
                blocks.push(core.get(i).await.unwrap().unwrap());
            }
            blocks
        };
        let entry = find_entry_value(&blocks, "/sample.txt").expect("metadata entry");
        assert_eq!(entry.blob.as_ref().map(|b| b.byte_length), Some(24));
    }
}
