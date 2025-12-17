# tokio-memq-python

[![PyPI version](https://badge.fury.io/py/tokio-memq-python.svg)](https://badge.fury.io/py/tokio-memq-python)
[![Build Status](https://github.com/weiwangfds/tokio-memq-python/actions/workflows/CI.yml/badge.svg)](https://github.com/weiwangfds/tokio-memq-python/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/pypi/pyversions/tokio-memq-python.svg)](https://pypi.org/project/tokio-memq-python/)

High-performance, asynchronous, in-memory message queue bindings for Python, powered by Rust's `tokio` and `tokio-memq`.

## Features

*   **High Performance**: Built on Rust's Tokio runtime for ultra-low latency and high throughput.
*   **Async/Await Support**: Fully integrated with Python's `asyncio`.
*   **Partition Support**: Built-in support for partitioned topics (RoundRobin, Hash, Random, Fixed routing).
*   **Type Safety**: Leverages Rust's safety guarantees under the hood.
*   **Cross-Platform**: Pre-compiled wheels for Linux, Windows, and macOS (Intel & Apple Silicon).

## Installation

```bash
pip install tokio-memq-python
```

Requires Python 3.8 or later.

## Usage

### Basic Usage

Simple publish-subscribe pattern using default topic settings.

```python
import asyncio
from tokio_memq import MessageQueue

async def main():
    # Initialize the Message Queue
    mq = MessageQueue()
    
    # Create a publisher for "test_topic"
    publisher = mq.publisher("test_topic")
    
    # Create a subscriber
    subscriber = await mq.subscriber("test_topic")
    
    # Publish a message (can be any JSON-serializable object)
    await publisher.publish({"id": 1, "content": "Hello World"})
    
    # Receive the message
    msg = await subscriber.recv()
    print(f"Received: {msg}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Partitioned Topics

Scale your application by distributing messages across multiple partitions.

```python
import asyncio
from tokio_memq import MessageQueue

async def main():
    mq = MessageQueue()
    topic = "user_events"
    
    # 1. Create a topic with 4 partitions
    #    This allows parallel consumption by up to 4 consumers
    await mq.create_partitioned_topic(topic, partition_count=4)
    
    # 2. Configure Hash routing
    #    "Hash" strategy ensures messages with the same key go to the same partition.
    #    key="message" tells the router to use the key provided in publish().
    await mq.set_partition_routing(topic, "Hash", key="message")
    
    # 3. Publish messages with keys
    pub = mq.publisher(topic)
    
    # User A's events will always go to the same partition (e.g., Partition 1)
    await pub.publish({"event": "login"}, key="user_A")
    await pub.publish({"event": "click"}, key="user_A")
    
    # User B's events will go to a different partition (e.g., Partition 3)
    await pub.publish({"event": "login"}, key="user_B")
    
    # 4. Subscribe to a specific partition
    #    In a real app, you would distribute these partition IDs across different workers
    sub_p1 = await mq.subscribe_partition(topic, 1) # Listening for User A
    
    msg = await sub_p1.recv()
    print(f"Worker for Partition 1 received: {msg}")

if __name__ == "__main__":
    asyncio.run(main())
```

## API Reference

### `MessageQueue`

The main entry point for interacting with the queue.

*   `publisher(topic: str) -> Publisher`: Get a publisher instance for a topic.
*   `subscriber(topic: str) -> Subscriber`: Subscribe to a standard topic.
*   `create_partitioned_topic(topic: str, partition_count: int, options: TopicOptions = None)`: Create a topic with multiple partitions.
*   `set_partition_routing(topic: str, strategy: str, key: str = None, fixed_id: int = None)`: Configure routing strategy.
    *   `strategy`: "RoundRobin", "Random", "Hash", "Fixed".
*   `subscribe_partition(topic: str, partition_id: int) -> Subscriber`: Subscribe to a specific partition.
*   `list_partitioned_topics() -> List[str]`: Get all partitioned topics.
*   `delete_partitioned_topic(topic: str)`: Remove a topic and its partitions.

### `Publisher`

*   `publish(data: Any, key: str = None)`: Asynchronously publish data.
    *   `data`: Any Python object serializable to JSON (dict, list, str, int, etc.).
    *   `key`: Optional string key, required only for "Hash" routing on partitioned topics.

### `Subscriber`

*   `recv() -> Any`: Asynchronously wait for and return the next message.

### `TopicOptions`

Configuration object for topic creation.

```python
from tokio_memq import TopicOptions

opts = TopicOptions()
opts.max_messages = 1000      # Max queue depth per partition
opts.message_ttl_ms = 60000   # Message time-to-live in ms
opts.lru_enabled = True       # Evict oldest when full
```

## License

This project is licensed under the MIT License.
