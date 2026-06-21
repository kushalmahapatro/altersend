use compact_encoding::CompactEncoding;
use hypercore_protocol::{Message, schema::*};
use hypercore_schema::{DataBlock, DataHash, DataSeek, DataUpgrade};

use crate::hyperdrive::HypercoreManifest;

fn decode_body<T>(payload: &[u8]) -> Option<T>
where
    T: CompactEncoding,
{
    let mut buf = payload;
    let (value, _rest) = T::decode(&mut buf).ok()?;
    Some(value)
}

/// Decode a replication `data` message, including optional JS manifest payloads.
pub fn decode_data_message(payload: &[u8]) -> Option<(Data, Option<HypercoreManifest>)> {
    let mut buf = payload;
    let (flags, rest) = u64::decode(&mut buf).ok()?;
    let (request, rest) = u64::decode(rest).ok()?;
    let (fork, mut rest) = u64::decode(rest).ok()?;

    let block = if flags & 1 != 0 {
        let (value, next) = DataBlock::decode(rest).ok()?;
        rest = next;
        Some(value)
    } else {
        None
    };
    let hash = if flags & 2 != 0 {
        let (value, next) = DataHash::decode(rest).ok()?;
        rest = next;
        Some(value)
    } else {
        None
    };
    let seek = if flags & 4 != 0 {
        let (value, next) = DataSeek::decode(rest).ok()?;
        rest = next;
        Some(value)
    } else {
        None
    };
    let upgrade = if flags & 8 != 0 {
        let (value, next) = DataUpgrade::decode(rest).ok()?;
        rest = next;
        Some(value)
    } else {
        None
    };
    let manifest = if flags & 16 != 0 {
        let (value, next) = HypercoreManifest::decode(rest).ok()?;
        rest = next;
        Some(value)
    } else {
        None
    };

    let data = Data {
        request,
        fork,
        block,
        hash,
        seek,
        upgrade,
    };
    let _ = rest;
    Some((data, manifest))
}

pub fn decode_message(msg_type: u64, payload: &[u8]) -> Option<Message> {
    match msg_type {
        0 => decode_body::<Synchronize>(payload).map(Message::Synchronize),
        1 => decode_body::<Request>(payload).map(Message::Request),
        2 => decode_body::<Cancel>(payload).map(Message::Cancel),
        3 => decode_body::<Data>(payload).map(Message::Data),
        4 => decode_body::<NoData>(payload).map(Message::NoData),
        5 => decode_body::<Want>(payload).map(Message::Want),
        6 => decode_body::<Unwant>(payload).map(Message::Unwant),
        7 => decode_body::<Bitfield>(payload).map(Message::Bitfield),
        8 => decode_body::<Range>(payload).map(Message::Range),
        9 => decode_body::<Extension>(payload).map(Message::Extension),
        _ => None,
    }
}

pub fn encode_body(message: &Message) -> Option<(u64, Vec<u8>)> {
    let (msg_type, size) = match message {
        Message::Synchronize(value) => (0u64, value.encoded_size().ok()?),
        Message::Request(value) => (1, value.encoded_size().ok()?),
        Message::Cancel(value) => (2, value.encoded_size().ok()?),
        Message::Data(value) => (3, value.encoded_size().ok()?),
        Message::NoData(value) => (4, value.encoded_size().ok()?),
        Message::Want(value) => (5, value.encoded_size().ok()?),
        Message::Unwant(value) => (6, value.encoded_size().ok()?),
        Message::Bitfield(value) => (7, value.encoded_size().ok()?),
        Message::Range(value) => (8, value.encoded_size().ok()?),
        Message::Extension(value) => (9, value.encoded_size().ok()?),
        _ => return None,
    };

    let mut buf = vec![0u8; size];
    match message {
        Message::Synchronize(value) => value.encode(&mut buf).ok()?,
        Message::Request(value) => value.encode(&mut buf).ok()?,
        Message::Cancel(value) => value.encode(&mut buf).ok()?,
        Message::Data(value) => value.encode(&mut buf).ok()?,
        Message::NoData(value) => value.encode(&mut buf).ok()?,
        Message::Want(value) => value.encode(&mut buf).ok()?,
        Message::Unwant(value) => value.encode(&mut buf).ok()?,
        Message::Bitfield(value) => value.encode(&mut buf).ok()?,
        Message::Range(value) => value.encode(&mut buf).ok()?,
        Message::Extension(value) => value.encode(&mut buf).ok()?,
        _ => return None,
    };
    Some((msg_type, buf))
}
