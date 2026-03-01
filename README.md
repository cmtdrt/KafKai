# KafKai

> A minimal, lightweight Kafka-like message broker written in ![Rust](https://img.shields.io/badge/Rust-%23f84c00.svg?logo=rust&logoColor=white)

KafKai is a simplified log-based message broker in Rust. It explores the core ideas behind event streaming: append-only logs, offsets, and pull-based consumption.

---

## How it works

- **Append-only log** — Messages are never modified or deleted; they are only appended to a per-topic log file.
- **Offsets** — Each message gets an incremental offset within its topic. Consumers use offsets to say “give me messages starting here.”
- **Pull-based consumption** — Consumers request messages from a given offset; they are not pushed. This allows reading at your own pace and replaying from any position.

**Storage (Kafka-style):** The broker keeps messages on **disk** only. In memory it maintains a small **index** (offset → byte position in the log file). Reads are served by seeking in the file and reading the required byte range, so topics can grow larger than RAM.

---

## Architecture

The project is a Rust workspace with four crates:

```
kafkai/
├── broker/       # Message broker (TCP server)
├── publisher/    # CLI to publish messages
├── consumer/     # CLI to consume messages
└── common/       # Shared types and protocol
```

| Component   | Role |
|------------|------|
| **Broker** | Listens on TCP, manages topics, appends to log files, maintains the offset index, serves publish and consume requests. |
| **Publisher** | Sends messages to a topic via the broker. |
| **Consumer** | Requests messages from a topic from a given offset; can resume from any offset. |

Log files are stored under a `logs/` directory (created by the broker), one file per topic: `kafkai-{topic}-logs`.

---

## Quick start

**1. Start the broker**

```bash
make start broker
```

**2. Publish a message** (in another terminal)

```bash
make start publisher topic=hello message="Hello KafKai"
```

**3. Consume from offset 0**

```bash
make start consumer topic=hello offset=0 max=10
```

Or run the binaries directly:

```bash
cargo run -p broker
cargo run -p publisher -- hello "My message"
cargo run -p consumer -- hello 0 10
```
