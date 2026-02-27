use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

use common::{Request, Response, DEFAULT_BIND_ADDR};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();

    if args.len() < 2 {
        eprintln!("Usage: publisher <topic> <message>");
        quit_from_error();
    }

    let topic = args.remove(0);
    let message = args.join(" ");

    let stream = TcpStream::connect(DEFAULT_BIND_ADDR)?;
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    let request = Request::Publish {
        topic,
        payload: message,
    };
    let json = serde_json::to_string(&request)?;
    writer.write_all(json.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    let mut response_line = String::new();
    reader.read_line(&mut response_line)?;

    if response_line.trim().is_empty() {
        eprintln!("No response from broker");
        quit_from_error();
    }

    let response: Response = serde_json::from_str(response_line.trim_end())?;

    match response {
        Response::PublishAck { offset } => {
            println!("Message published at offset {}", offset);
        }
        Response::Error { message } => {
            eprintln!("Broker error: {}", message);
            quit_from_error();
        }
        other => {
            eprintln!("Unexpected response from broker: {:?}", other);
            quit_from_error();
        }
    }
    Ok(())
}

pub fn quit_from_error() {
    println!("Exiting...");
    std::process::exit(1);
}