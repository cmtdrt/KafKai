use common::DEFAULT_BIND_ADDR;
fn main() {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();

    if args.len() < 2 {
        eprintln!("Usage: publisher <topic> <message>");
        std::process::exit(1);
    }

    let topic = args.remove(0);
    let message = args.join(" ");

    println!("Connecting to broker at: {}", DEFAULT_BIND_ADDR);
    println!("Publishing message '{}' to topic: '{}'", message, topic);
}

