use serde::Deserialize;

/// Hyperdrive path key encoding: `files\0` + utf-8 path.
pub fn encode_drive_path(path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + path.len());
    key.extend_from_slice(b"files");
    key.push(0);
    key.extend_from_slice(path.as_bytes());
    key
}

#[derive(Debug, Deserialize)]
pub struct DriveEntryValue {
    pub executable: Option<bool>,
    pub linkname: Option<String>,
    pub blob: Option<BlobRef>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
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
}
