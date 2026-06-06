use std::future::Future;
use std::pin::Pin;

use bytes::BytesMut;
use serde::Serialize;

use crate::codec::{decode_json, encode_json};

pub type MessageCallback = Box<dyn FnMut(serde_json::Value) + Send>;

pub struct MessageHandle {
    pub send_json: Box<dyn FnMut(serde_json::Value) -> bool + Send>,
}

pub struct Channel {
    pub protocol: String,
    pub local_id: u64,
    pub remote_id: u64,
    pub opened: bool,
    pub on_message: Option<MessageCallback>,
}

impl Channel {
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            local_id: 0,
            remote_id: 0,
            opened: false,
            on_message: None,
        }
    }

    pub fn on_json_message<F>(&mut self, handler: F)
    where
        F: FnMut(serde_json::Value) + Send + 'static,
    {
        self.on_message = Some(Box::new(handler));
    }

    pub fn recv_json(&mut self, payload: &[u8]) {
        let mut slice = payload;
        if let Some(value) = decode_json::<serde_json::Value>(&mut slice) {
            if let Some(handler) = self.on_message.as_mut() {
                handler(value);
            }
        }
    }

    pub fn encode_json_message<T: Serialize>(&self, msg_type: u64, value: &T) -> Option<BytesMut> {
        let mut payload = BytesMut::new();
        encode_json(value, &mut payload).ok()?;
        Some(payload)
    }
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
