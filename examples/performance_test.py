import asyncio
import time
from tokio_memq import MessageQueue

async def main():
    mq = MessageQueue()
    topic = "batch_processing"
    
    pub = mq.publisher(topic)
    sub = await mq.subscriber(topic)
    
    message_count = 1000
    print(f"Publishing {message_count} messages...")
    
    start_time = time.time()
    
    # Batch publish
    # Currently we don't have a specific batch_publish API exposed, 
    # but async publish is fast.
    for i in range(message_count):
        await pub.publish({"id": i, "data": "x" * 100})
        
    pub_duration = time.time() - start_time
    print(f"Published in {pub_duration:.4f}s ({message_count/pub_duration:.2f} msg/s)")
    
    print("Consuming messages...")
    start_time = time.time()
    
    received = 0
    while received < message_count:
        msg = await sub.recv()
        received += 1
        if received % 100 == 0:
            # print(f"Received {received}...")
            pass
            
    cons_duration = time.time() - start_time
    print(f"Consumed in {cons_duration:.4f}s ({message_count/cons_duration:.2f} msg/s)")

if __name__ == "__main__":
    asyncio.run(main())
