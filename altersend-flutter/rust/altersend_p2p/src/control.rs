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

pub fn encode_control_payload(msg: &PeerControlMessage) -> Vec<u8> {
    let value = serde_json::to_value(msg).expect("serialize control");
    let mut obj = match value {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    obj.insert("protocolVersion".to_string(), json!(PROTOCOL_VERSION));
    serde_json::to_vec(&Value::Object(obj)).expect("serialize")
}

pub fn decode_control_payload(bytes: &[u8]) -> Option<PeerControlMessage> {
    let mut value: Value = serde_json::from_slice(bytes).ok()?;
    let version = value.get("protocolVersion")?.as_u64()? as u32;
    if version != PROTOCOL_VERSION {
        return None;
    }
    if let Value::Object(ref mut map) = value {
        map.remove("protocolVersion");
    }
    serde_json::from_value(value).ok()
}
