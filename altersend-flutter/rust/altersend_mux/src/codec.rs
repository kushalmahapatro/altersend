use bytes::{BufMut, BytesMut};
use compact_encoding::{encoded_size_var_u64, encode_var_u64, CompactEncoding};

pub fn encode_uint(value: u64, out: &mut BytesMut) {
    let size = encoded_size_var_u64(value);
    let start = out.len();
    out.resize(start + size, 0);
    let rest = encode_var_u64(value, &mut out[start..]).expect("uint encode");
    assert!(rest.is_empty());
}

pub fn decode_uint(input: &mut &[u8]) -> Option<u64> {
    let (value, rest) = u64::decode(input).ok()?;
    *input = rest;
    Some(value)
}

pub fn encode_string(value: &str, out: &mut BytesMut) {
    let size = value.encoded_size().expect("string size");
    let start = out.len();
    out.resize(start + size, 0);
    let rest = value.encode(&mut out[start..]).expect("string encode");
    assert!(rest.is_empty());
}

pub fn decode_string(input: &mut &[u8]) -> Option<String> {
    let (value, rest) = String::decode(input).ok()?;
    *input = rest;
    Some(value)
}

pub fn encode_optional_buffer(value: Option<&[u8]>, out: &mut BytesMut) {
    match value {
        None => out.put_u8(0),
        Some(bytes) => {
            out.put_u8(1);
            encode_buffer(bytes, out);
        }
    }
}

pub fn encode_buffer(value: &[u8], out: &mut BytesMut) {
    let bytes = value.to_vec();
    let size = bytes.encoded_size().expect("buffer size");
    let start = out.len();
    out.resize(start + size, 0);
    let rest = bytes.encode(&mut out[start..]).expect("buffer encode");
    assert!(rest.is_empty());
}

pub fn decode_buffer(input: &mut &[u8]) -> Option<Vec<u8>> {
    let (value, rest) = Vec::<u8>::decode(input).ok()?;
    *input = rest;
    Some(value)
}

pub fn decode_optional_buffer(input: &mut &[u8]) -> Option<Option<Vec<u8>>> {
    if input.is_empty() {
        return None;
    }
    match input[0] {
        0 => {
            *input = &input[1..];
            Some(None)
        }
        1 => {
            *input = &input[1..];
            decode_buffer(input).map(Some)
        }
        _ => None,
    }
}

pub fn encode_json<T: serde::Serialize>(value: &T, out: &mut BytesMut) -> Result<(), serde_json::Error> {
    let json = serde_json::to_vec(value)?;
    encode_buffer(&json, out);
    Ok(())
}

pub fn decode_json<T: serde::de::DeserializeOwned>(input: &mut &[u8]) -> Option<T> {
    let bytes = decode_buffer(input)?;
    serde_json::from_slice(&bytes).ok()
}

/// Protomux control frame: `[0][type]...`
pub fn encode_control_frame(control_type: u8, payload: &mut BytesMut) -> BytesMut {
    let mut frame = BytesMut::with_capacity(2 + payload.len());
    frame.extend_from_slice(&[0, control_type]);
    frame.extend_from_slice(payload);
    frame
}

/// Channel message frame: `[local_id][msg_type]payload`
pub fn encode_channel_message(local_id: u64, msg_type: u64, payload: &mut BytesMut) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_uint(local_id, &mut frame);
    encode_uint(msg_type, &mut frame);
    frame.extend_from_slice(payload);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint_roundtrip() {
        let mut out = BytesMut::new();
        encode_uint(42, &mut out);
        let mut slice = out.as_ref();
        assert_eq!(decode_uint(&mut slice), Some(42));
    }

    #[test]
    fn string_roundtrip() {
        let mut out = BytesMut::new();
        encode_string("altersend/control", &mut out);
        let mut slice = out.as_ref();
        assert_eq!(decode_string(&mut slice).as_deref(), Some("altersend/control"));
    }
}
