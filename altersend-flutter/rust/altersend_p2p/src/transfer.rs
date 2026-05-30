use std::path::{Path, PathBuf};

use altersend_domain::IncomingFileOffer;
use rand::Rng;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::warn;

use crate::control::FileOffer;

#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub file_name: String,
    pub input_path: PathBuf,
    pub size: u64,
}

pub struct ScanResult {
    pub files: Vec<ScannedFile>,
    pub total_bytes: u64,
    pub errors: Vec<String>,
}

pub async fn scan_files(paths: &[String]) -> ScanResult {
    let mut files = Vec::new();
    let mut errors = Vec::new();
    let mut total_bytes = 0u64;

    for path in paths {
        let path_buf = PathBuf::from(path);
        let meta = match fs::metadata(&path_buf).await {
            Ok(m) => m,
            Err(_) => {
                errors.push(format!("Could not read file: {}", path_buf.display()));
                continue;
            }
        };
        if meta.is_dir() {
            errors.push(format!("Folders are not supported: {}", path_buf.display()));
            continue;
        }
        let file_name = path_buf
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        let size = meta.len();
        total_bytes += size;
        files.push(ScannedFile {
            file_name,
            input_path: path_buf,
            size,
        });
    }

    ScanResult {
        files,
        total_bytes,
        errors,
    }
}

pub fn create_file_id() -> String {
    hex::encode(rand::rng().random::<[u8; 12]>())
}

pub fn create_transfer_id() -> String {
    hex::encode(rand::rng().random::<[u8; 16]>())
}

pub fn build_file_offers(
    transfer_id: &str,
    drive_key: &str,
    scanned: &[ScannedFile],
) -> Vec<FileOffer> {
    scanned
        .iter()
        .map(|f| FileOffer {
            id: create_file_id(),
            transfer_id: transfer_id.to_string(),
            name: f.file_name.clone(),
            path: format!("/{}", f.file_name),
            size: f.size,
            drive_key: drive_key.to_string(),
        })
        .collect()
}

pub fn offers_to_domain(files: &[FileOffer]) -> Vec<IncomingFileOffer> {
    files
        .iter()
        .map(|f| IncomingFileOffer {
            id: f.id.clone(),
            transfer_id: f.transfer_id.clone(),
            name: f.name.clone(),
            path: f.path.clone(),
            size: f.size,
            drive_key: f.drive_key.clone(),
        })
        .collect()
}

const CHUNK_SIZE: usize = 64 * 1024;

pub async fn send_file(path: &Path, mut write: impl AsyncWriteExt + Unpin) -> Result<u64, String> {
    let mut file = File::open(path).await.map_err(|e| e.to_string())?;
    let mut total = 0u64;
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        write.write_all(&buf[..n]).await.map_err(|e| e.to_string())?;
        total += n as u64;
    }
    Ok(total)
}

pub async fn receive_file(
    path: &Path,
    expected_size: u64,
    mut read: impl AsyncReadExt + Unpin,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await.map_err(|e| e.to_string())?;
    }
    let mut file = File::create(path).await.map_err(|e| e.to_string())?;
    let mut received = 0u64;
    let mut buf = vec![0u8; CHUNK_SIZE];
    while received < expected_size {
        let n = read.read(&mut buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n]).await.map_err(|e| e.to_string())?;
        received += n as u64;
    }
    if received < expected_size {
        warn!("received {received} of {expected_size} bytes");
    }
    Ok(())
}
