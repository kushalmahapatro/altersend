use serde::{Deserialize, Serialize};

use crate::page_ui::SendDraftPhase;
use crate::types::SharingStatusEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedFile {
    pub path: String,
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderUploadItem {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub status: UploadItemStatus,
    pub bytes_transferred: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UploadItemStatus {
    Waiting,
    Uploading,
    Completed,
}

pub fn get_phase_from_selection(count: usize) -> SendDraftPhase {
    if count == 0 {
        SendDraftPhase::Empty
    } else {
        SendDraftPhase::Selected
    }
}

pub fn merge_selected_files(
    current: &[SelectedFile],
    incoming: &[SelectedFile],
) -> Vec<SelectedFile> {
    let mut next = current.to_vec();
    for file in incoming {
        if !next.iter().any(|f| f.path == file.path) {
            next.push(file.clone());
        }
    }
    next
}

pub fn create_initial_upload_items(files: &[SelectedFile]) -> Vec<SenderUploadItem> {
    files
        .iter()
        .map(|f| SenderUploadItem {
            path: f.path.clone(),
            name: f.name.clone(),
            size: f.size,
            status: UploadItemStatus::Waiting,
            bytes_transferred: 0,
        })
        .collect()
}

pub fn apply_sharing_progress(
    items: &[SenderUploadItem],
    event: &SharingStatusEvent,
) -> Vec<SenderUploadItem> {
    items
        .iter()
        .map(|item| {
            if item.name != event.file_name {
                return item.clone();
            }
            let status = if event.bytes_transferred >= event.total_bytes && event.total_bytes > 0
            {
                UploadItemStatus::Completed
            } else {
                UploadItemStatus::Uploading
            };
            SenderUploadItem {
                status,
                bytes_transferred: event.bytes_transferred,
                ..item.clone()
            }
        })
        .collect()
}
