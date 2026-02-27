use std::io::Write;
use std::net::TcpStream;

use common::{Request, DEFAULT_BIND_ADDR};

fn main() {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();

    if args.len() < 2 {
        eprintln!("Usage: publisher <topic> <message>");
        std::process::exit(1);
    }

    let topic = args.remove(0);
    let message = args.join(" ");

    let stream = TcpStream::connect(DEFAULT_BIND_ADDR).unwrap();
    let mut writer = stream.try_clone().unwrap();

    let request = Request::Publish {
        topic,
        payload: message,
    };
    let json = serde_json::to_string(&request).unwrap();
    writer.write_all(json.as_bytes()).unwrap();
    writer.write_all(b"\n").unwrap();
    writer.flush().unwrap();
}

