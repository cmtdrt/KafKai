use common::{Offset, DEFAULT_BIND_ADDR};

fn main() {
    let mut args = std::env::args().skip(1);

    let topic = match args.next() {
        Some(t) => t,
        None => {
            eprintln!("Usage: consumer <topic> <offset> [max_messages]");
            std::process::exit(1);
        }
    };

    let offset: Offset = match args.next() {
        Some(o) => o.parse().map_err(|e| format!("Invalid offset: {}", e)).unwrap(),
        None => {
            eprintln!("Usage: consumer <topic> <offset> [max_messages]");
            std::process::exit(1);
        }
    };

    let max_messages: usize = match args.next() {
        Some(m) => m
            .parse()
            .map_err(|e| format!("Invalid max_messages: {}", e)).unwrap(),
        None => 10,
    };

    println!("topic: {}, offset: {}, max_messages: {}", topic, offset, max_messages);
    println!("Connecting to broker at {}", DEFAULT_BIND_ADDR);
}

