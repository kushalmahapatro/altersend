use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum PeerControlMessage {
    TransferStart {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "totalFiles")]
        total_files: u32,
        #[serde(rename = "totalBytes")]
        total_bytes: u64,
    },
    TransferReady {
        #[serde(rename = "transferId")]
        transfer_id: String,
        files: Vec<FileOffer>,
    },
    DownloadRequest {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "fileId")]
        file_id: String,
        #[serde(rename = "fileName")]
        file_name: String,
        path: String,
        #[serde(rename = "totalBytes")]
        total_bytes: u64,
    },
    DownloadProgress {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "fileId")]
        file_id: String,
        #[serde(rename = "fileName")]
        file_name: String,
        #[serde(rename = "bytesTransferred")]
        bytes_transferred: u64,
        #[serde(rename = "totalBytes")]
        total_bytes: u64,
    },
    DownloadComplete {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "fileId")]
        file_id: String,
        #[serde(rename = "fileName")]
        file_name: String,
        #[serde(rename = "savedTo")]
        saved_to: String,
    },
    DownloadFailed {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "fileId")]
        file_id: String,
        #[serde(rename = "fileName")]
        file_name: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOffer {
    pub id: String,
    #[serde(rename = "transferId")]
    pub transfer_id: String,
    pub name: String,
    pub path: String,
    pub size: u64,
    #[serde(rename = "driveKey")]
    pub drive_key: String,
}

pub fn encode_control(msg: &PeerControlMessage) -> Vec<u8> {
    let value = serde_json::to_value(msg).expect("serialize control");
    let mut obj = match value {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    obj.insert(
        "protocolVersion".to_string(),
        json!(PROTOCOL_VERSION),
    );
    let json = serde_json::to_vec(&Value::Object(obj)).expect("serialize");
    let mut out = (json.len() as u32).to_be_bytes().to_vec();
    out.extend(json);
    out
}

pub fn decode_control(bytes: &[u8]) -> Option<PeerControlMessage> {
    if bytes.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(bytes[..4].try_into().ok()?) as usize;
    if bytes.len() < 4 + len {
        return None;
    }
    let value: Value = serde_json::from_slice(&bytes[4..4 + len]).ok()?;
    let version = value.get("protocolVersion")?.as_u64()? as u32;
    if version != PROTOCOL_VERSION {
        return None;
    }
    serde_json::from_value(value).ok()
}
