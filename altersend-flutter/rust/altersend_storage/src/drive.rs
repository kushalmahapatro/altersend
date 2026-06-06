use std::path::Path;

use hypercore::{Hypercore, HypercoreBuilder, Storage};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;

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

/// Writable Hypercore used as the outgoing "drive" for a transfer session.
///
/// Files are staged as fixed-size Hypercore blocks. The public key hex is used as
/// `driveKey` in control messages — matching the JS worklet shape. Full Hyperdrive
/// metadata replication for Electron/RN interop is still pending.
pub struct OutgoingDrive {
    core: Hypercore,
    files: Vec<StagedFileMeta>,
}

impl OutgoingDrive {
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self, DriveError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;
        let storage = Storage::new_disk(&dir, true).await?;
        let core = HypercoreBuilder::new(storage).build().await?;
        Ok(Self {
            core,
            files: Vec::new(),
        })
    }

    pub fn key_hex(&self) -> String {
        hex::encode(self.core.key_pair().public.as_bytes())
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
        let block_start = self.core.info().length;
        let block_count = data.len().div_ceil(CHUNK_SIZE) as u64;

        for chunk in data.chunks(CHUNK_SIZE) {
            self.core.append(chunk).await?;
        }

        self.files.push(StagedFileMeta {
            id: file_id.to_string(),
            name: name.to_string(),
            path: path.to_string(),
            size: data.len() as u64,
            block_start,
            block_count,
        });

        Ok(())
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

        let mut written = 0usize;
        let mut pos = offset;

        while written < buf.len() && pos < meta.size {
            let block_index = meta.block_start + (pos as usize / CHUNK_SIZE) as u64;
            let block_offset = (pos as usize) % CHUNK_SIZE;
            let block = self
                .core
                .get(block_index)
                .await?
                .ok_or(DriveError::NotStaged)?;
            let available = block.len().saturating_sub(block_offset);
            let want = (meta.size - pos) as usize;
            let take = available.min(want).min(buf.len() - written);
            buf[written..written + take].copy_from_slice(&block[block_offset..block_offset + take]);
            written += take;
            pos += take as u64;
        }

        Ok(written)
    }

    pub fn core_length(&self) -> u64 {
        self.core.info().length
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let key = drive.key_hex();
        assert_eq!(key.len(), 64);

        let file_path = dir.join("sample.txt");
        fs::write(&file_path, b"hello hypercore staging").await.unwrap();
        drive
            .stage_file("f1", "sample.txt", "/sample.txt", &file_path)
            .await
            .unwrap();

        let mut buf = [0u8; 64];
        let n = drive.read_file_range("f1", 0, &mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello hypercore staging");
    }
}
