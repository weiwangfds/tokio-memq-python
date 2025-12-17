# tokio-memq-python

[![PyPI version](https://badge.fury.io/py/tokio-memq-python.svg)](https://badge.fury.io/py/tokio-memq-python)
[![Build Status](https://github.com/weiwangfds/tokio-memq-python/actions/workflows/CI.yml/badge.svg)](https://github.com/weiwangfds/tokio-memq-python/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/pypi/pyversions/tokio-memq-python.svg)](https://pypi.org/project/tokio-memq-python/)

**tokio-memq-python** is a high-performance, asynchronous in-memory message queue library for Python, powered by the robust Rust [tokio-memq](https://crates.io/crates/tokio-memq) crate.

It bridges the gap between Python's ease of use and Rust's performance, providing a seamless `asyncio` interface for building high-throughput, low-latency messaging applications. Whether you are building a local event bus, a job queue, or a high-speed data pipeline, `tokio-memq-python` offers the speed and reliability you need.

## Features

*   **🚀 High Performance**: Built on Rust's Tokio runtime, offering exceptional throughput and low latency.
*   **⚡ Async/Await Native**: Fully integrated with Python's `asyncio` for non-blocking I/O. Supports `async for` loops.
*   **� Consumer Groups**: Built-in support for competing consumers pattern with automatic load balancing.
*   **� Partition Support**: Scale horizontally with built-in partitioned topics and flexible routing strategies (RoundRobin, Hash, Random, Fixed).
*   **🛠 Type Safety**: Leverages Rust's strong type system to ensure stability and correctness.
*   **🔧 Cross-Platform**: Pre-compiled wheels available for Linux, Windows, and macOS (Intel & Apple Silicon).
*   **⚙️ Configurable**: Fine-tune queue depth, message TTL (Time-To-Live), and eviction policies (LRU).

## � Use Cases

- **High-Performance Event Bus**: Decouple components in your Python application with minimal latency overhead.
- **Async Job Processing**: Efficiently distribute tasks to worker coroutines or threads using consumer groups.
- **Data Streaming Pipelines**: Handle high-velocity data ingestion and processing with backpressure support.
- **Local Microservices Communication**: Fast inter-process-like communication within a single application instance.
- **Testing & Simulation**: Simulate complex distributed messaging scenarios (like Kafka/RabbitMQ) locally without infrastructure overhead.

## �📊 Benchmarks

**Environment:**
- **OS**: macOS
- **Device**: Apple M4 (24GB RAM)
- **Python**: 3.12

**Results (Run with `python3 examples/benchmark.py --count 10000`):**

| Scenario | Metric | Result | Resource Usage (Avg CPU / Peak Mem) |
| :--- | :--- | :--- | :--- |
| **Latency (1 Pub → 1 Sub)** | Avg Latency | **120.17 µs** | N/A |
| | P99 Latency | **174.55 µs** | |
| **Throughput (1 Pub → 1 Sub)** | Publish Rate | **12,883 msg/s** | **148% / 37 MB** |
| | Consume Rate | **12,882 msg/s** | |
| **Fan-Out (1 Pub → 5 Sub)** | Publish Rate | **6,647 msg/s** | **216% / 40 MB** |
| | Total Consume Rate | **28,306 msg/s** | |
| **Fan-In (5 Pub → 1 Sub)** | Publish Rate | **32,974 msg/s** | **143% / 44 MB** |
| | Consume Rate | **13,662 msg/s** | |

*Note: Benchmarks include Python object serialization overhead. Raw Rust performance is significantly higher. Resource usage reflects the Python process overhead.*

## Installation

```bash
pip install tokio-memq-python
```

Requires Python 3.8 or later.

## Examples

### 1. Basic Publish-Subscribe

A simple example demonstrating how to create a queue, publish messages, and consume them asynchronously.

```python
import asyncio
from tokio_memq import MessageQueue

async def main():
    # Initialize the Message Queue
    mq = MessageQueue()
    
    # Create a publisher for "notifications"
    publisher = mq.publisher("notifications")
    
    # Create a subscriber for the same topic
    # This automatically creates the topic if it doesn't exist
    subscriber = await mq.subscriber("notifications")
    
    # Publish messages (any JSON-serializable data)
    await publisher.send({"type": "email", "to": "user@example.com"})
    await publisher.send({"type": "sms", "to": "+1234567890"})
    
    # Process messages using async iterator
    async for msg in subscriber:
        print(f"Processed notification: {msg}")
        # For this example, break after 2 messages
        if msg["type"] == "sms":
            break

if __name__ == "__main__":
    asyncio.run(main())
```

### 2. Consumer Groups (Load Balancing)

Multiple consumers can join a group to share the workload. Messages are distributed among active consumers.

```python
import asyncio
from tokio_memq import MessageQueue

async def worker(mq, topic, group_id, name):
    # Join a consumer group with "Earliest" mode to get existing messages
    sub = await mq.subscriber_group(topic, group_id, mode="Earliest")
    print(f"[{name}] Joined group '{group_id}' (ID: {sub.consumer_id})")
    
    async for msg in sub:
        print(f"[{name}] Processing: {msg}")

async def main():
    mq = MessageQueue()
    topic = "jobs"
    group = "workers"
    
    # Start two workers
    w1 = asyncio.create_task(worker(mq, topic, group, "Worker-1"))
    w2 = asyncio.create_task(worker(mq, topic, group, "Worker-2"))
    
    # Publish tasks
    pub = mq.publisher(topic)
    for i in range(10):
        await pub.send({"task_id": i})
        
    # Keep running...

if __name__ == "__main__":
    asyncio.run(main())
```

### 3. Partitioned Topics with Hash Routing

This example shows how to use partitioned topics to distribute load while ensuring message ordering for specific keys (e.g., user IDs).

```python
import asyncio
from tokio_memq import MessageQueue

async def main():
    mq = MessageQueue()
    topic = "user_activity"
    
    # Create a topic with 4 partitions for parallel processing
    await mq.create_partitioned_topic(topic, partition_count=4)
    
    # Configure "Hash" routing: messages with the same key go to the same partition
    await mq.set_partition_routing(topic, "Hash", key="message")
    
    pub = mq.publisher(topic)
    
    # Simulate user events
    # Events for 'user_123' will always land in the same partition
    await pub.send({"event": "login"}, key="user_123")
    await pub.send({"event": "view_item"}, key="user_123")
    
    # Events for 'user_456' will land in a different partition (likely)
    await pub.send({"event": "logout"}, key="user_456")
    
    # Consume from a specific partition (e.g., Partition 0)
    # Use mode="Earliest" to ensure we get messages published before we subscribed
    sub_p0 = await mq.subscribe_partition(topic, 0, mode="Earliest")
    
    print("Listening on Partition 0...")
    try:
        msg = await asyncio.wait_for(sub_p0.recv(), timeout=1.0)
        print(f"Partition 0 received: {msg}")
    except asyncio.TimeoutError:
        print("No messages on Partition 0 for this run.")

if __name__ == "__main__":
    asyncio.run(main())
```

### 4. Advanced Configuration (TTL & Queue Limits)

Control memory usage and message lifecycle using `TopicOptions`.

```python
import asyncio
from tokio_memq import MessageQueue, TopicOptions

async def main():
    mq = MessageQueue()
    
    # Configure topic options
    opts = TopicOptions()
    opts.max_messages = 100        # Limit queue to 100 messages
    opts.message_ttl_ms = 5000     # Messages expire after 5 seconds
    opts.lru_enabled = True        # Drop oldest messages when full
    
    # Create subscriber with custom options
    sub = await mq.subscriber_with_options("fast_logs", opts)
    pub = mq.publisher("fast_logs")
    
    # Publish a message
    await pub.send("This message will self-destruct in 5s")
    
    msg = await sub.recv()
    print(f"Got: {msg}")

if __name__ == "__main__":
    asyncio.run(main())
```

## More Examples
Check the `examples/` directory for ready-to-run scripts:
*   [basic_usage.py](examples/basic_usage.py): Complete pub/sub workflow.
*   [consumer_group.py](examples/consumer_group.py): Load balancing with consumer groups.
*   [async_iteration.py](examples/async_iteration.py): Pythonic usage with `async for` and `send()`.
*   [partitioned_topic.py](examples/partitioned_topic.py): Working with partitions and Hash routing.
*   [advanced_config.py](examples/advanced_config.py): Using TTL and LRU eviction.
*   [performance_test.py](examples/performance_test.py): Simple throughput benchmark.
*   [benchmark.py](examples/benchmark.py): Comprehensive latency and throughput scenarios.

## API Reference

### `MessageQueue`

The central manager for topics, publishers, and subscribers.

*   `publisher(topic: str) -> Publisher`: Get a publisher for a topic.
*   `subscriber(topic: str) -> Subscriber`: Subscribe to a topic (auto-creates it).
*   `subscriber_with_options(topic: str, options: TopicOptions) -> Subscriber`: Subscribe with specific configuration.
*   `subscriber_group(topic: str, consumer_id: str, mode: str = "LastOffset", offset: int = None) -> Subscriber`: Join a consumer group.
    *   `consumer_id`: Unique ID for the consumer group member.
    *   `mode`: `"Earliest"`, `"Latest"`, `"LastOffset"`, or `"Offset"`.
*   `create_partitioned_topic(topic: str, partition_count: int, options: TopicOptions = None)`: Create a topic with `n` partitions.
*   `set_partition_routing(topic: str, strategy: str, key: str = None, fixed_id: int = None)`: Set routing logic.
    *   `strategy`: `"RoundRobin"`, `"Random"`, `"Hash"`, `"Fixed"`.
    *   `key`: Required for `"Hash"` routing (e.g., `"message"`).
    *   `fixed_id`: Required for `"Fixed"` routing (target partition ID).
*   `subscribe_partition(topic: str, partition_id: int, consumer_id: str = None, mode: str = "LastOffset", offset: int = None) -> Subscriber`: Listen to a specific partition.
*   `list_partitioned_topics() -> List[str]`: List active partitioned topics.
*   `delete_partitioned_topic(topic: str)`: Delete a topic.

### `Publisher`

*   `send(data: Any, key: str = None)`: Send a message (alias for `publish`).
    *   `data`: JSON-serializable payload (dict, list, str, int, etc.).
    *   `key`: Routing key (used only if topic is partitioned with Hash routing).
*   `publish(data: Any, key: str = None)`: Send a message.
*   `topic() -> str`: Get the topic name.

### `Subscriber`

*   `recv() -> Any`: Await the next message. Raises exception if connection is lost.
*   `try_recv() -> Any | None`: Non-blocking receive. Returns `None` if no message available.
*   `current_offset() -> int`: Get current offset.
*   `reset_offset()`: Reset offset to 0.
*   `consumer_id -> str | None`: (Property) Get the consumer ID if applicable.
*   `topic -> str`: (Property) Get the topic name.
*   **Async Iterator**: Supports `async for msg in subscriber:` syntax.

### `TopicOptions`

*   `max_messages` (int): Max messages per topic/partition.
*   `message_ttl_ms` (int): Message lifetime in milliseconds.
*   `lru_enabled` (bool): If True, drops oldest message when full; otherwise rejects new messages.
*   `partitions` (int): (Read-only) Number of partitions if applicable.

## License

This project is licensed under the MIT License.
