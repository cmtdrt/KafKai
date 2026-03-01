use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use common::{Offset, Request, Response, DEFAULT_BIND_ADDR};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

type DynError = Box<dyn std::error::Error + Send + Sync>;

const LOGS_DIR: &str = "logs";

/// Sanitize topic name for use in filenames (alphanumeric, dash, underscore).
fn sanitize_topic(topic: &str) -> String {
    topic
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        }).collect()
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LogEntry {
    payload: String,
}

/// Index: offset i → byte position of message i in the log file (Kafka-like).
struct TopicState {
    path: PathBuf,
    /// offsets[i] = byte position in file where message i starts
    offsets: Vec<u64>,
}

struct Broker {
    /// Topic key (sanitized) → index only; data lives on disk.
    topics: HashMap<String, TopicState>,
    logs_dir: PathBuf,
}

impl Broker {
    /// Build index from existing log files (no message content loaded in memory).
    async fn load_from_disk() -> Result<Self, DynError> {
        let logs_dir = PathBuf::from(LOGS_DIR);
        tokio::fs::create_dir_all(&logs_dir).await?;

        let mut topics: HashMap<String, TopicState> = HashMap::new();

        let mut entries = tokio::fs::read_dir(&logs_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("kafkai-") && name.ends_with("-logs") {
                let topic = name
                    .trim_start_matches("kafkai-")
                    .trim_end_matches("-logs")
                    .to_string();
                let path = entry.path();
                let offsets = Self::build_offset_index(&path).await?;
                if !offsets.is_empty() {
                    topics.insert(
                        topic,
                        TopicState {
                            path,
                            offsets,
                        },
                    );
                }
            }
        }

        Ok(Broker { topics, logs_dir })
    }

    /// Scan file and record byte offset of each line start (one message per line).
    async fn build_offset_index(path: &PathBuf) -> Result<Vec<u64>, DynError> {
        let file = tokio::fs::File::open(path).await?;
        let mut reader = BufReader::new(file);
        let mut offsets = Vec::new();
        let mut pos: u64 = 0;
        let mut line = String::new();

        loop {
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            offsets.push(pos);
            pos += n as u64;
            line.clear();
        }

        Ok(offsets)
    }

    /// Append to log file and update index only (no payload stored in memory).
    async fn publish(&mut self, topic: String, payload: String) -> Result<Offset, DynError> {
        let key = sanitize_topic(&topic);
        let path = self.logs_dir.join(format!("kafkai-{}-logs", key));

        let state = self
            .topics
            .entry(key)
            .or_insert_with(|| TopicState {
                path: path.clone(),
                offsets: Vec::new(),
            });

        let line = serde_json::to_string(&LogEntry { payload })?;
        let line = format!("{}\n", line);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&state.path)
            .await?;

        let position = file.seek(SeekFrom::End(0)).await?;
        state.offsets.push(position);

        file.write_all(line.as_bytes()).await?;
        file.flush().await?;

        Ok((state.offsets.len() - 1) as Offset)
    }

    /// Read messages from disk using the offset index (Kafka-style: disk is source of truth).
    /// Returns (path, offsets, start, end) so the actual read can be done without holding the lock.
    fn consume_params(
        &self,
        topic: &str,
        offset: Offset,
        max_messages: usize,
    ) -> Option<(PathBuf, Vec<u64>, usize, usize)> {
        let key = sanitize_topic(topic);
        let state = self.topics.get(&key)?;
        let start = offset as usize;
        if start >= state.offsets.len() {
            return None;
        }
        let end = (start + max_messages).min(state.offsets.len());
        Some((
            state.path.clone(),
            state.offsets.clone(),
            start,
            end,
        ))
    }
}

/// Read messages from log file without holding broker lock (Kafka-style: disk is source of truth).
async fn read_from_log(
    path: PathBuf,
    offsets: Vec<u64>,
    start: usize,
    end: usize,
    offset: Offset,
) -> Result<Vec<(Offset, String)>, DynError> {
    if start >= end {
        return Ok(Vec::new());
    }

    let start_off = offsets[start];
    let end_off = if end < offsets.len() {
        offsets[end]
    } else {
        tokio::fs::metadata(&path).await?.len()
    };
    let to_read = (end_off - start_off) as usize;

    if to_read == 0 {
        return Ok(Vec::new());
    }

    let mut file = tokio::fs::File::open(&path).await?;
    file.seek(SeekFrom::Start(start_off)).await?;

    let mut buf = vec![0u8; to_read];
    file.read_exact(&mut buf).await?;

    let s = std::str::from_utf8(&buf).map_err(|e| format!("Invalid UTF-8 in log: {}", e))?;
    let mut result = Vec::with_capacity(end - start);
    let mut idx = 0u64;
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<LogEntry>(line) {
            result.push((offset + idx, entry.payload));
            idx += 1;
        }
    }

    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let listener = TcpListener::bind(DEFAULT_BIND_ADDR).await?;
    println!("Broker listening on {}", DEFAULT_BIND_ADDR);

    let broker = Arc::new(Mutex::new(Broker::load_from_disk().await?));

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
                match broker.publish(topic, payload).await {
                    Ok(offset) => Response::PublishAck { offset },
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            Request::Consume {
                topic,
                offset,
                max_messages,
            } => {
                let params = {
                    let broker = broker.lock().await;
                    broker.consume_params(&topic, offset, max_messages)
                };
                match params {
                    None => Response::ConsumeResult {
                        messages: Vec::new(),
                    },
                    Some((path, offsets, start, end)) => {
                        match read_from_log(path, offsets, start, end, offset).await {
                            Ok(messages) => Response::ConsumeResult { messages },
                            Err(e) => Response::Error {
                                message: e.to_string(),
                            },
                        }
                    }
                }
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
