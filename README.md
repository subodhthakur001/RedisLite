#  Redis-Inspired In-Memory Data Store (Java)

A high-performance, concurrent, in-memory data store built from scratch in Java, inspired by core Redis primitives.

This project implements a subset of Redis functionality including **key-value storage, list operations, blocking queues, and expiry handling**, along with a custom RESP protocol parser and multi-threaded request handling.

---

## 🧠 Why this project?

Most engineers *use* Redis. Very few understand how it actually works under the hood.

This project is an attempt to deeply understand and reimplement:
- Network protocol parsing (RESP)
- Concurrent data structures
- Blocking operations (`BLPOP`)
- Thread coordination (`wait/notify`)
- Expiry semantics

---

## ⚙️ Features

### 🧩 Core Data Structures

#### Key-Value Store
- `SET key value [PX ms]`
- `GET key`
- Lazy expiration handling

#### List Operations
- `RPUSH key element [element ...]`
- `LPUSH key element [element ...]`
- `LPOP key [count]`
- `LRANGE key start stop`
- `LLEN key`

---

### ⏳ Expiry Support
- Millisecond precision expiry using `PX`
- Lazy eviction strategy (checked during access)
- No background thread → minimal overhead

---

### 🔒 Concurrency Model
- Thread-per-connection using `ExecutorService`
- Shared state via `ConcurrentHashMap`
- Fine-grained locking (per-key locking)

---

### 🧵 Blocking Operations (Advanced)

#### `BLPOP key timeout`

- Blocks client until:
  - Element is available OR
  - Timeout expires
- Uses:
  - `wait()` for blocking
  - `notifyAll()` for wake-up
- Supports:
  - Infinite blocking (`timeout = 0`)
  - Timed blocking

---

### 🔌 Protocol Support

Implements **RESP (Redis Serialization Protocol)**:

- Bulk Strings (`$`)
- Arrays (`*`)
- Integers (`:`)
- Simple Strings (`+`)

