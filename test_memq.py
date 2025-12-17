import asyncio
from tokio_memq import MessageQueue, TopicOptions

async def main():
    print("Creating MessageQueue...")
    mq = MessageQueue()
    
    topic = "test_topic"
    print(f"Creating Publisher for {topic}...")
    publisher = mq.publisher(topic)
    
    print(f"Creating Subscriber for {topic}...")
    subscriber = await mq.subscriber(topic)
    
    print("Publishing message...")
    await publisher.publish({"hello": "world", "count": 1})
    
    print("Receiving message...")
    msg = await subscriber.recv()
    print(f"Received: {msg}")
    
    assert msg == {"hello": "world", "count": 1}
    
    print("Getting stats...")
    stats = await mq.get_topic_stats(topic)
    print(f"Stats: {stats}")
    
    print("Test passed!")

if __name__ == "__main__":
    asyncio.run(main())
