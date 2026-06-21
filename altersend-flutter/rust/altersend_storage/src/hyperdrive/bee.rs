use serde::{Deserialize, Serialize};

pub const BLOB_BLOCK_SIZE: usize = 64 * 1024;

/// Hyperdrive path key encoding: `files\0` + utf-8 path.
pub fn encode_drive_path(path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + path.len());
    key.extend_from_slice(b"files");
    key.push(0);
    key.extend_from_slice(path.as_bytes());
    key
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveEntryValue {
    pub executable: Option<bool>,
    pub linkname: Option<String>,
    pub blob: Option<BlobRef>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobRef {
    #[serde(rename = "blockOffset")]
    pub block_offset: u64,
    #[serde(rename = "blockLength")]
    pub block_length: u64,
    #[serde(rename = "byteOffset")]
    pub byte_offset: u64,
    #[serde(rename = "byteLength")]
    pub byte_length: u64,
}

/// Parse hyperbee block 0 header and return content feed public key if present.
pub fn parse_header_content_key(block: &[u8]) -> Option<[u8; 32]> {
    let mut offset = 0;
    while offset < block.len() {
        let (tag, consumed) = read_varint(&block[offset..])?;
        offset += consumed;
        let field = tag >> 3;
        let wire = tag & 0x07;
        if field == 2 && wire == 2 {
            let (len, consumed) = read_varint(&block[offset..])?;
            offset += consumed;
            let end = offset + len as usize;
            if end > block.len() {
                return None;
            }
            return parse_metadata_content_feed(&block[offset..end]);
        }
        offset += skip_field(wire, &block[offset..])?;
    }
    None
}

fn parse_metadata_content_feed(meta: &[u8]) -> Option<[u8; 32]> {
    let mut offset = 0;
    while offset < meta.len() {
        let (tag, consumed) = read_varint(&meta[offset..])?;
        offset += consumed;
        let field = tag >> 3;
        let wire = tag & 0x07;
        if field == 1 && wire == 2 {
            let (len, consumed) = read_varint(&meta[offset..])?;
            offset += consumed;
            if len < 32 || offset + len as usize > meta.len() {
                return None;
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&meta[offset..offset + 32]);
            return Some(key);
        }
        offset += skip_field(wire, &meta[offset..])?;
    }
    None
}

/// Scan hyperbee node blocks for a drive path entry (small drives only).
pub fn find_entry_value(blocks: &[Vec<u8>], path: &str) -> Option<DriveEntryValue> {
    let want = encode_drive_path(path);
    for block in blocks.iter().skip(1) {
        if let Some(value_bytes) = parse_node_value(block, &want) {
            if let Ok(value) = serde_json::from_slice::<DriveEntryValue>(&value_bytes) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_node_value(block: &[u8], want_key: &[u8]) -> Option<Vec<u8>> {
    let mut offset = 0;
    let mut key: Option<Vec<u8>> = None;
    let mut value: Option<Vec<u8>> = None;
    while offset < block.len() {
        let (tag, consumed) = read_varint(&block[offset..])?;
        offset += consumed;
        let field = tag >> 3;
        let wire = tag & 0x07;
        match field {
            2 if wire == 2 => {
                let (len, consumed) = read_varint(&block[offset..])?;
                offset += consumed;
                let end = offset + len as usize;
                if end > block.len() {
                    return None;
                }
                key = Some(block[offset..end].to_vec());
                offset = end;
            }
            3 if wire == 2 => {
                let (len, consumed) = read_varint(&block[offset..])?;
                offset += consumed;
                let end = offset + len as usize;
                if end > block.len() {
                    return None;
                }
                value = Some(block[offset..end].to_vec());
                offset = end;
            }
            _ => {
                offset += skip_field(wire, &block[offset..])?;
            }
        }
    }
    if key.as_deref() == Some(want_key) {
        value
    } else {
        None
    }
}

fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, &byte) in buf.iter().enumerate() {
        value |= ((byte & 0x7f) as u64) << (i * 7);
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
        if i >= 9 {
            return None;
        }
    }
    None
}

fn skip_field(wire: u64, buf: &[u8]) -> Option<usize> {
    match wire {
        0 => {
            let (_, n) = read_varint(buf)?;
            Some(n)
        }
        2 => {
            let (len, n) = read_varint(buf)?;
            Some(n + len as usize)
        }
        _ => None,
    }
}

/// Hyperbee block 0: protocol + optional content feed for the blobs core.
pub fn encode_hyperbee_header(content_feed: &[u8; 32]) -> Vec<u8> {
    let mut metadata = Vec::new();
    write_bytes_field(1, content_feed, &mut metadata);

    let mut out = Vec::new();
    write_string_field(1, "hyperbee", &mut out);
    write_key_varint(2, 2, &mut out);
    write_varint(metadata.len() as u64, &mut out);
    out.extend_from_slice(&metadata);
    out
}

/// Encode a hyperbee node block for a drive path entry.
pub fn encode_hyperbee_node(sorted_seqs: &[u64], key: &[u8], value: &[u8]) -> Vec<u8> {
    let index = encode_yolo_index(sorted_seqs);
    let mut out = Vec::new();
    write_bytes_field(1, &index, &mut out);
    write_bytes_field(2, key, &mut out);
    write_bytes_field(3, value, &mut out);
    out
}

pub fn encode_entry_value(blob: &BlobRef) -> Vec<u8> {
    serde_json::to_vec(&DriveEntryValue {
        executable: Some(false),
        linkname: None,
        blob: Some(blob.clone()),
        metadata: None,
    })
    .expect("entry value json")
}

fn encode_yolo_index(sorted_seqs: &[u64]) -> Vec<u8> {
    let mut packed = Vec::new();
    for seq in sorted_seqs {
        write_varint(*seq, &mut packed);
    }
    let mut level = Vec::new();
    write_key_varint(1, 2, &mut level);
    write_varint(packed.len() as u64, &mut level);
    level.extend_from_slice(&packed);

    let mut out = Vec::new();
    write_key_varint(1, 2, &mut out);
    write_varint(level.len() as u64, &mut out);
    out.extend_from_slice(&level);
    out
}

fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn write_key_varint(field_number: u64, wire_type: u64, out: &mut Vec<u8>) {
    write_varint((field_number << 3) | wire_type, out);
}

fn write_string_field(field: u64, value: &str, out: &mut Vec<u8>) {
    write_key_varint(field, 2, out);
    write_varint(value.len() as u64, out);
    out.extend_from_slice(value.as_bytes());
}

fn write_bytes_field(field: u64, data: &[u8], out: &mut Vec<u8>) {
    write_key_varint(field, 2, out);
    write_varint(data.len() as u64, out);
    out.extend_from_slice(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_drive_path() {
        let key = encode_drive_path("/sample.txt");
        assert_eq!(&key[..5], b"files");
        assert_eq!(key[5], 0);
        assert_eq!(&key[6..], b"/sample.txt");
    }

    #[test]
    fn encodes_first_hyperbee_node_like_js() {
        let blob = BlobRef {
            block_offset: 0,
            block_length: 1,
            byte_offset: 0,
            byte_length: 24,
        };
        let node = encode_hyperbee_node(
            &[1],
            &encode_drive_path("/sample.txt"),
            &encode_entry_value(&blob),
        );
        assert_eq!(&node[..5], &[0x0a, 0x05, 0x0a, 0x03, 0x0a]);
        let value = find_entry_value(&[vec![], node], "/sample.txt").expect("entry");
        assert_eq!(value.blob.as_ref().map(|b| b.byte_length), Some(24));
    }
}
