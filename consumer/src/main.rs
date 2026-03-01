use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

use common::{Offset, Request, Response, DEFAULT_BIND_ADDR};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);

    let topic = match args.next() {
        Some(t) => t,
        None => {
            eprintln!("Usage: consumer <topic> <offset> [max_messages]");
            std::process::exit(1);
        }
    };

    let offset: Offset = match args.next() {
        Some(o) => o.parse().map_err(|e| format!("Invalid offset: {}", e))?,
        None => {
            eprintln!("Usage: consumer <topic> <offset> [max_messages]");
            std::process::exit(1);
        }
    };

    let max_messages: usize = match args.next() {
        Some(m) => m
            .parse()
            .map_err(|e| format!("Invalid max_messages: {}", e))?,
        None => 10,
    };

    let stream = TcpStream::connect(DEFAULT_BIND_ADDR)?;
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    let request = Request::Consume {
        topic,
        offset,
        max_messages,
    };

    let json = serde_json::to_string(&request)?;
    writer.write_all(json.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    let mut response_line = String::new();
    reader.read_line(&mut response_line)?;

    if response_line.trim().is_empty() {
        eprintln!("No response from broker");
        std::process::exit(1);
    }

    let response: Response = serde_json::from_str(response_line.trim_end())?;

    match response {
        Response::ConsumeResult { messages } => {
            if messages.is_empty() {
                println!("No messages available");
            } else {
                for (offset, payload) in messages {
                    println!("[{}] {}", offset, payload);
                }
            }
        }
        Response::Error { message } => {
            eprintln!("Broker error: {}", message);
            std::process::exit(1);
        }
        other => {
            eprintln!("Unexpected response from broker: {:?}", other);
            std::process::exit(1);
        }
    }

    Ok(())
}