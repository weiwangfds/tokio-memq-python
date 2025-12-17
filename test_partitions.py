import asyncio
from tokio_memq import MessageQueue, TopicOptions

async def main():
    print("Creating MessageQueue...")
    mq = MessageQueue()
    
    topic = "p_topic"
    partition_count = 2
    
    print(f"Creating Partitioned Topic {topic} with {partition_count} partitions...")
    await mq.create_partitioned_topic(topic, partition_count, None)
    
    # Default is RoundRobin, but let's set it explicitly to be sure
    print("Setting routing strategy to RoundRobin...")
    await mq.set_partition_routing(topic, "RoundRobin")
    
    print("Subscribing to partitions...")
    sub0 = await mq.subscribe_partition(topic, 0)
    sub1 = await mq.subscribe_partition(topic, 1)
    
    print("Creating Publisher...")
    publisher = mq.publisher(topic)
    
    print("Publishing 10 messages...")
    for i in range(10):
        await publisher.publish(f"msg{i}")
        
    print("Consuming messages...")
    # We expect messages to be distributed.
    # Since it's RoundRobin, P0 gets msg0, P1 gets msg1, P0 gets msg2, etc.
    
    count0 = 0
    count1 = 0
    
    # Try to receive from both. We need to be careful not to block forever if one is empty.
    # We can use wait_for with timeout.
    
    for _ in range(10):
        # Try P0
        try:
            msg = await asyncio.wait_for(sub0.recv(), timeout=0.1)
            if msg:
                print(f"P0 received: {msg}")
                count0 += 1
        except asyncio.TimeoutError:
            pass
            
        # Try P1
        try:
            msg = await asyncio.wait_for(sub1.recv(), timeout=0.1)
            if msg:
                print(f"P1 received: {msg}")
                count1 += 1
        except asyncio.TimeoutError:
            pass
            
    print(f"P0 count: {count0}, P1 count: {count1}")
    
    print("Getting partition stats...")
    stats0 = await mq.get_partition_stats(topic, 0)
    stats1 = await mq.get_partition_stats(topic, 1)
    
    print(f"Stats P0: {stats0}")
    print(f"Stats P1: {stats1}")
    
    assert count0 > 0
    assert count1 > 0
    assert count0 + count1 == 10
    
    print("All tests passed!")

if __name__ == "__main__":
    asyncio.run(main())
