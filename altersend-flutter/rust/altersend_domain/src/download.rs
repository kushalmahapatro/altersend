use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::format::format_file_size;
use crate::types::{IncomingFileOffer, ReceiveDownloadStatusEvent, SaveDestination};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DownloadStatus {
    Idle,
    Downloading,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadItemState {
    pub status: DownloadStatus,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
    pub saved_to: Option<String>,
    pub destination: Option<SaveDestination>,
    pub intended_destination: Option<SaveDestination>,
    pub message: Option<String>,
}

pub fn get_offer_key(file: &IncomingFileOffer) -> String {
    file.id.clone()
}

pub fn create_download_state_map(
    current: &HashMap<String, DownloadItemState>,
    files: &[IncomingFileOffer],
) -> HashMap<String, DownloadItemState> {
    let mut next = HashMap::new();
    for file in files {
        let key = get_offer_key(file);
        next.insert(
            key.clone(),
            current.get(&key).cloned().unwrap_or(DownloadItemState {
                status: DownloadStatus::Idle,
                bytes_transferred: 0,
                total_bytes: file.size,
                saved_to: None,
                destination: None,
                intended_destination: None,
                message: None,
            }),
        );
    }
    next
}

pub fn all_downloads_completed(states: &HashMap<String, DownloadItemState>) -> bool {
    !states.is_empty() && states.values().all(|s| s.status == DownloadStatus::Completed)
}

pub fn resolve_offer_key(
    offers: &[IncomingFileOffer],
    event: &ReceiveDownloadStatusEvent,
) -> Option<String> {
    if let Some(file_id) = &event.file_id {
        if offers.iter().any(|o| o.id == *file_id) {
            return Some(file_id.clone());
        }
    }
    if let Some(name) = &event.file_name {
        return offers.iter().find(|o| o.name == *name).map(get_offer_key);
    }
    None
}

pub fn apply_download_message(
    current: &HashMap<String, DownloadItemState>,
    offer_key: &str,
    event: &ReceiveDownloadStatusEvent,
) -> HashMap<String, DownloadItemState> {
    let mut next = current.clone();
    let entry = next.entry(offer_key.to_string()).or_insert(DownloadItemState {
        status: DownloadStatus::Idle,
        bytes_transferred: 0,
        total_bytes: event.total_bytes.unwrap_or(0),
        saved_to: None,
        destination: None,
        intended_destination: None,
        message: None,
    });

    match event.state.as_str() {
        "downloading" => {
            entry.status = DownloadStatus::Downloading;
            entry.bytes_transferred = event.bytes_transferred.unwrap_or(0);
            if let Some(total) = event.total_bytes {
                entry.total_bytes = total;
            }
        }
        "download-progress" => {
            entry.status = DownloadStatus::Downloading;
            entry.bytes_transferred = event.bytes_transferred.unwrap_or(entry.bytes_transferred);
        }
        "downloaded" => {
            entry.status = DownloadStatus::Completed;
            entry.bytes_transferred = entry.total_bytes;
            entry.saved_to = event.saved_to.clone();
        }
        "download-failed" => {
            entry.status = DownloadStatus::Failed;
            entry.message = event.message.clone();
        }
        _ => {}
    }
    next
}

pub fn apply_download_routed(
    current: &HashMap<String, DownloadItemState>,
    offer_key: &str,
    destination: SaveDestination,
    intended_destination: SaveDestination,
    saved_to: Option<String>,
) -> HashMap<String, DownloadItemState> {
    let mut next = current.clone();
    if let Some(entry) = next.get_mut(offer_key) {
        entry.destination = Some(destination);
        entry.intended_destination = Some(intended_destination);
        if saved_to.is_some() {
            entry.saved_to = saved_to;
        }
    }
    next
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTotals {
    pub total_bytes: u64,
    pub bytes_transferred: u64,
    pub completed_count: u32,
    pub active_count: u32,
    pub percent: u32,
}

pub fn get_download_totals(
    offers: &[IncomingFileOffer],
    states: &HashMap<String, DownloadItemState>,
) -> DownloadTotals {
    let mut total_bytes = 0u64;
    let mut bytes_transferred = 0u64;
    let mut completed_count = 0u32;
    let mut active_count = 0u32;

    for offer in offers {
        total_bytes += offer.size;
        let key = get_offer_key(offer);
        if let Some(state) = states.get(&key) {
            bytes_transferred += state.bytes_transferred;
            match state.status {
                DownloadStatus::Completed => completed_count += 1,
                DownloadStatus::Downloading => active_count += 1,
                _ => {}
            }
        }
    }

    let percent = if total_bytes == 0 {
        0
    } else {
        ((bytes_transferred as f64 / total_bytes as f64) * 100.0).round() as u32
    };

    DownloadTotals {
        total_bytes,
        bytes_transferred,
        completed_count,
        active_count,
        percent: percent.min(100),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRowDisplay {
    pub description: Option<String>,
    pub progress_percent: Option<u32>,
    pub status_label: String,
    pub status_tone: String,
    pub percent: u32,
    pub is_active: bool,
    pub is_completed: bool,
}

pub fn get_download_row_display(
    file: &IncomingFileOffer,
    state: Option<&DownloadItemState>,
) -> DownloadRowDisplay {
    let total_bytes = state.map(|s| s.total_bytes).unwrap_or(file.size);
    let transferred = state.map(|s| s.bytes_transferred).unwrap_or(0);
    let percent = if total_bytes == 0 {
        0
    } else {
        ((transferred as f64 / total_bytes as f64) * 100.0).round() as u32
    };

    match state.map(|s| s.status) {
        Some(DownloadStatus::Completed) => DownloadRowDisplay {
            description: None,
            progress_percent: Some(100),
            status_label: "Saved".to_string(),
            status_tone: "success".to_string(),
            percent: 100,
            is_active: false,
            is_completed: true,
        },
        Some(DownloadStatus::Failed) => DownloadRowDisplay {
            description: None,
            progress_percent: if transferred > 0 { Some(percent) } else { None },
            status_label: state
                .and_then(|s| s.message.clone())
                .unwrap_or_else(|| "Failed".to_string()),
            status_tone: "muted".to_string(),
            percent,
            is_active: false,
            is_completed: false,
        },
        Some(DownloadStatus::Downloading) if total_bytes > 0 => DownloadRowDisplay {
            description: Some(format!(
                "{} / {}",
                format_file_size(transferred),
                format_file_size(total_bytes)
            )),
            progress_percent: Some(percent),
            status_label: format!("{percent}%"),
            status_tone: "active".to_string(),
            percent,
            is_active: true,
            is_completed: false,
        },
        _ => DownloadRowDisplay {
            description: Some(format_file_size(total_bytes)),
            progress_percent: None,
            status_label: "Ready".to_string(),
            status_tone: "muted".to_string(),
            percent: 0,
            is_active: false,
            is_completed: false,
        },
    }
}
