# KafKai

> A minimal, lightweight Kafka-like message broker.

KafKai is a simplified distributed log built in 
![Rust](https://img.shields.io/badge/Rust-%23f84c00.svg?logo=rust&logoColor=white) 
to explore the core concepts behind event streaming systems.

---

## What's KafKai?

KafKai is a small message broker based on three core ideas:

- **Append-only log**  
  Messages are never modified or deleted. They are only appended to a log.

- **Offsets**  
  Each message receives an incremental offset within a topic.

- **Pull-based consumption**  
  Consumers request messages starting from a specific offset.

This design allows consumers to read at their own pace and replay messages if needed.

---

## Architecture

The project is structured as a Rust workspace:

```
kafkai/
├── broker/       # The message broker (server)
├── publisher/    # CLI tool to publish messages
├── consumer/   # CLI tool to consume messages
└── common/       # Shared types and utilities
```

### Components

#### Broker
- Manages topics
- Stores messages in an append-only log
- Assigns offsets
- Serves consumer requests

#### Publisher
- Sends messages to a topic
- Does not need to know about consumers

#### Subscriber
- Reads messages from a topic
- Keeps track of its last processed offset
- Can resume from a previous position