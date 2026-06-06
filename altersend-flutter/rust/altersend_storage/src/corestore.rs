use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage not ready")]
    NotReady,
}

/// Minimal Corestore-shaped storage root matching the JS worklet layout.
pub struct CoreStore {
    root: PathBuf,
    ready: bool,
}

impl CoreStore {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root).await?;
        tokio::fs::create_dir_all(root.join("outgoing-drive")).await?;
        tokio::fs::create_dir_all(root.join("incoming-drives")).await?;
        Ok(Self { root, ready: true })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn outgoing_namespace(&self) -> PathBuf {
        self.root.join("outgoing-drive")
    }

    pub fn incoming_namespace(&self) -> PathBuf {
        self.root.join("incoming-drives")
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub async fn wipe(&mut self) -> Result<(), StoreError> {
        if self.root.exists() {
            tokio::fs::remove_dir_all(&self.root).await?;
        }
        *self = Self::open(&self.root).await?;
        Ok(())
    }
}
