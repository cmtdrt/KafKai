use std::collections::HashMap;
use std::sync::Arc;

use common::{Offset, Request, Response, DEFAULT_BIND_ADDR};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Default)]
struct Broker {
    topics: HashMap<String, Vec<String>>,
}

impl Broker {
    fn publish(&mut self, topic: String, payload: String) -> Offset {
        let log = self.topics.entry(topic).or_insert_with(Vec::new);
        log.push(payload);
        // offset is index in the vector
        (log.len() - 1) as Offset
    }

    fn consume(
        &self,
        topic: &str,
        offset: Offset,
        max_messages: usize,
    ) -> Vec<(Offset, String)> {
        let log = match self.topics.get(topic) {
            Some(log) => log,
            None => return Vec::new(),
        };

        let start = offset as usize;
        if start >= log.len() {
            return Vec::new();
        }

        let end = (start + max_messages).min(log.len());
        (start..end)
            .map(|idx| (idx as Offset, log[idx].clone()))
            .collect()
    }
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let listener = TcpListener::bind(DEFAULT_BIND_ADDR).await?;
    println!("Broker listening on {}", DEFAULT_BIND_ADDR);

    let broker = Arc::new(Mutex::new(Broker::default()));

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);

        let broker = broker.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, broker).await {
                eprintln!("Connection error from {}: {}", addr, e);
            }
        });
    }
}

async fn handle_connection(
    socket: TcpStream,
    broker: Arc<Mutex<Broker>>,
) -> Result<(), DynError> {
    let (reader, mut writer) = tokio::io::split(socket);
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break;
        }

        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }

        let request: Request = match serde_json::from_str(trimmed) {
            Ok(req) => req,
            Err(e) => {
                let resp = Response::Error {
                    message: format!("Invalid request: {}", e),
                };
                send_response(&mut writer, &resp).await?;
                continue;
            }
        };

        let response = match request {
            Request::Publish { topic, payload } => {
                let mut broker = broker.lock().await;
                let offset = broker.publish(topic, payload);
                Response::PublishAck { offset }
            }
            Request::Consume {
                topic,
                offset,
                max_messages,
            } => {
                let broker = broker.lock().await;
                let messages = broker.consume(&topic, offset, max_messages);
                Response::ConsumeResult { messages }
            }
        };

        send_response(&mut writer, &response).await?;
    }

    Ok(())
}

async fn send_response(
    writer: &mut (impl AsyncWriteExt + Unpin),
    response: &Response,
) -> Result<(), DynError> {
    let json = serde_json::to_string(response)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

