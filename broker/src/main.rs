use std::collections::HashMap;

use common::{DEFAULT_BIND_ADDR};
use tokio::net::TcpListener;

#[derive(Default, Debug)]
struct Broker {
    // name of the topic, list of messages
    topics: HashMap<String, Vec<String>>,
}

#[tokio::main]
async fn main() {
    let listener = match TcpListener::bind(DEFAULT_BIND_ADDR).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Failed to bind to {}: {}", DEFAULT_BIND_ADDR, e);
            std::process::exit(1);
        }
    };
    println!("Listener: {:?}", listener);
    println!("Broker listening on {}", DEFAULT_BIND_ADDR);

    let messages: Vec<String> = vec![String::from("Hello, world!")];
    let test_topic = "test-topic".to_string();
    let broker: Broker = Broker {
        topics: HashMap::from([(test_topic, messages)]),
    };
    println!("Broker: {:?}", broker);
}
