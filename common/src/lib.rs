use serde::{Deserialize, Serialize};

pub type Offset = u64;

pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:9000";

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Publish {
        topic: String,
        payload: String,
    },
    Consume {
        topic: String,
        offset: Offset,
        max_messages: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    PublishAck {
        offset: Offset,
    },
    ConsumeResult {
        messages: Vec<(Offset, String)>,
    },
    Error {
        message: String,
    },
}

