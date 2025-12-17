import asyncio
import time
import statistics
import argparse
from tokio_memq import MessageQueue, TopicOptions

async def run_throughput_test(message_count, payload_size, num_publishers, num_subscribers):
    mq = MessageQueue()
    topic = f"bench_throughput_{time.time()}"
    
    # Ensure topic is created with enough capacity to prevent LRU drops during benchmark
    # We want to measure throughput, not drop rate.
    opts = TopicOptions(max_messages=message_count + 1000, lru_enabled=False)
    
    payload = "x" * payload_size
    msg_data = {"data": payload}
    
    print(f"--- Preparing Throughput Test ({num_publishers} Pub -> {num_subscribers} Sub) ---")
    
    # Pre-create subscribers
    subscribers = []
    # Create first subscriber with options to initialize the topic
    if num_subscribers > 0:
        sub = await mq.subscriber_with_options(topic, opts)
        subscribers.append(sub)
        for i in range(num_subscribers - 1):
            sub = await mq.subscriber(topic)
            subscribers.append(sub)
            
    # Create publishers
    publishers = []
    for i in range(num_publishers):
        pub = mq.publisher(topic)
        publishers.append(pub)
        
    start_time = time.time()
    
    async def publisher_task(pub, count, pid):
        # print(f"Pub {pid} started")
        for _ in range(count):
            await pub.publish(msg_data)
        # print(f"Pub {pid} finished")
            
    async def subscriber_task(sub, expected_count, sid):
        # print(f"Sub {sid} started")
        count = 0
        while count < expected_count:
            await sub.recv()
            count += 1
        # print(f"Sub {sid} finished")
        return count

    # Distribute messages among publishers
    msgs_per_pub = message_count // num_publishers
    # Handle remainder if any (though usually we pass multiples)
    remainder = message_count % num_publishers
    
    pub_tasks = []
    for i, p in enumerate(publishers):
        count = msgs_per_pub + (1 if i < remainder else 0)
        pub_tasks.append(asyncio.create_task(publisher_task(p, count, i)))
    
    # Subscribers receive ALL messages (broadcast)
    sub_tasks = []
    for i, s in enumerate(subscribers):
        sub_tasks.append(asyncio.create_task(subscriber_task(s, message_count, i)))
    
    print(f"Running test with {message_count} messages...")
    
    # Wait for all publishers to finish
    await asyncio.gather(*pub_tasks)
    pub_end_time = time.time()
    print("Publishers finished.")
    
    # Wait for all subscribers to finish
    # We use wait_for to detect hangs
    try:
        await asyncio.wait_for(asyncio.gather(*sub_tasks), timeout=60.0)
    except asyncio.TimeoutError:
        print("TIMEOUT: Subscribers failed to receive all messages.")
        # Determine how many were received
        # Note: We can't easily get count from cancelled tasks here without shared state
        print(f"Warning: Test timed out. Results may be incomplete.")
        
    end_time = time.time()
    
    total_time = end_time - start_time
    total_messages = message_count * num_subscribers 
    
    print(f"Messages: {message_count}, Payload: {payload_size} bytes")
    print(f"Total Time: {total_time:.4f}s")
    print(f"Publish Rate: {message_count / (pub_end_time - start_time):.2f} msg/s")
    print(f"Consume Rate: {total_messages / total_time:.2f} msg/s (Total delivered)")
    print("-" * 40)

async def main():
    print("Starting Hang Test...\n")
    
    # 3 Pub -> 1 Sub Throughput
    await run_throughput_test(100000, 100, 3, 1)

if __name__ == "__main__":
    asyncio.run(main())
