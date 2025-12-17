# tokio-memq-python

Python bindings for [tokio-memq](https://crates.io/crates/tokio-memq).

## Installation

```bash
pip install tokio-memq-python
```

## Usage

### Basic Usage

```python
import asyncio
from tokio_memq import MessageQueue, TopicOptions

async def main():
    mq = MessageQueue()
    publisher = mq.publisher("test_topic")
    
    # Publish
    await publisher.publish({"key": "value"})
    
    # Subscribe
    subscriber = await mq.subscriber("test_topic")
    msg = await subscriber.recv()
    print(msg)

if __name__ == "__main__":
    asyncio.run(main())
```

### Partitioned Topics

You can create topics with multiple partitions to scale consumption and manage data distribution.

```python
import asyncio
from tokio_memq import MessageQueue

async def main():
    mq = MessageQueue()
    topic = "partitioned_topic"
    
    # Create a topic with 3 partitions
    await mq.create_partitioned_topic(topic, 3)
    
    # Set routing strategy (optional, default is RoundRobin)
    # Available strategies: "RoundRobin", "Random", "Hash", "Fixed"
    
    # Example 1: Round Robin (default) - distributes messages evenly
    await mq.set_partition_routing(topic, "RoundRobin")
    
    # Example 2: Hash routing - ensures messages with same key go to same partition
    # Use "message" to use the message key provided during publish
    await mq.set_partition_routing(topic, "Hash", key="message")
    
    # Create publisher
    pub = mq.publisher(topic)
    
    # Publish with key for Hash routing
    # All messages with key="user1" will go to the same partition
    await pub.publish({"user": "user1", "data": "A"}, key="user1")
    await pub.publish({"user": "user1", "data": "B"}, key="user1")
    
    # Subscribe to a specific partition (e.g., partition 0)
    sub0 = await mq.subscribe_partition(topic, 0)
    
    # Receive messages
    while True:
        msg = await sub0.recv()
        print(f"Received from P0: {msg}")

if __name__ == "__main__":
    asyncio.run(main())
```

### API Reference

#### MessageQueue

- `publisher(topic)`: Get a publisher for a topic.
- `subscriber(topic)`: Subscribe to a topic (auto-creates if not exists).
- `subscriber_with_options(topic, options)`: Subscribe with custom options.
- `create_partitioned_topic(topic, partition_count, options=None)`: Create a partitioned topic.
- `set_partition_routing(topic, strategy, key=None, fixed_id=None)`: Set routing strategy.
- `subscribe_partition(topic, partition_id)`: Subscribe to a specific partition.
- `get_partition_stats(topic, partition_id)`: Get stats for a partition.
- `list_partitioned_topics()`: List all partitioned topics.
- `delete_partitioned_topic(topic)`: Delete a partitioned topic.

#### Publisher

- `publish(data, key=None)`: Publish a message (dict/object). `key` is used for Hash routing.

#### Subscriber

- `recv()`: Receive next message (async).
- `try_recv()`: Try to receive (non-blocking).
