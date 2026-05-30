use std::path::{Path, PathBuf};

use altersend_domain::IncomingFileOffer;
use rand::Rng;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::control::FileOffer;

pub const CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_FILE_SIZE: u64 = 50 * 1024 * 1024 * 1024; // 50 GB

#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub file_id: String,
    pub file_name: String,
    pub input_path: PathBuf,
    pub size: u64,
}

pub struct ScanResult {
    pub files: Vec<ScannedFile>,
    pub total_bytes: u64,
    pub errors: Vec<String>,
}

pub fn is_safe_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.contains('/')
        && !name.contains('\\')
        && name != "."
        && name != ".."
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
        let size = meta.len();
        if size > MAX_FILE_SIZE {
            errors.push(format!("File too large: {}", path_buf.display()));
            continue;
        }
        let file_name = path_buf
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        if !is_safe_file_name(&file_name) {
            errors.push(format!("Unsafe file name: {file_name}"));
            continue;
        }
        total_bytes += size;
        files.push(ScannedFile {
            file_id: create_file_id(),
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

pub fn build_file_offers(transfer_id: &str, drive_key: &str, scanned: &[ScannedFile]) -> Vec<FileOffer> {
    scanned
        .iter()
        .map(|f| FileOffer {
            id: f.file_id.clone(),
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

pub async fn send_file_chunks<F, Fut>(
    path: &Path,
    total_size: u64,
    mut on_chunk: F,
) -> Result<(), String>
where
    F: FnMut(u64, &[u8]) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let mut file = File::open(path).await.map_err(|e| e.to_string())?;
    let mut offset = 0u64;
    let mut buf = vec![0u8; CHUNK_SIZE];
    while offset < total_size {
        let n = file.read(&mut buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        on_chunk(offset, &buf[..n]).await?;
        offset += n as u64;
    }
    Ok(())
}

pub struct IncomingDownload {
    pub path: PathBuf,
    pub total_bytes: u64,
    pub file: Option<File>,
}

impl IncomingDownload {
    pub async fn open(path: PathBuf, total_bytes: u64) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| e.to_string())?;
        }
        let file = File::create(&path).await.map_err(|e| e.to_string())?;
        Ok(Self {
            path,
            total_bytes,
            file: Some(file),
        })
    }

    pub async fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), String> {
        let file = self.file.as_mut().ok_or("download closed")?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| e.to_string())?;
        file.write_all(data).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<(), String> {
        if let Some(file) = self.file.take() {
            file.sync_all().await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
