import asyncio
from tokio_memq import MessageQueue

async def main():
    mq = MessageQueue()
    topic = "user_activity"
    partition_count = 4
    
    print(f"Creating partitioned topic '{topic}' with {partition_count} partitions...")
    await mq.create_partitioned_topic(topic, partition_count=partition_count)
    
    # Configure "Hash" routing: messages with the same key go to the same partition
    print("Setting partition routing to 'Hash' based on key...")
    await mq.set_partition_routing(topic, "Hash", key="message")
    
    pub = mq.publisher(topic)
    
    # Simulate user events
    user_id_1 = "user_123"
    user_id_2 = "user_456"
    
    print(f"Publishing events for {user_id_1} (should go to same partition)...")
    await pub.publish({"event": "login", "user": user_id_1}, key=user_id_1)
    await pub.publish({"event": "view_item", "user": user_id_1}, key=user_id_1)
    
    print(f"Publishing events for {user_id_2} (should go to different partition)...")
    await pub.publish({"event": "logout", "user": user_id_2}, key=user_id_2)
    
    # In this example, we'll try to find which partition 'user_123' landed on.
    # In a real app, you would have consumers for all partitions.
    
    print("Checking partitions for messages...")
    
    # Debug: Check stats
    stats = await mq.get_all_partition_stats(topic)
    print(f"Partition stats: {stats}")
    
    found_count = 0
    expected_count = 3 # 2 for user_123, 1 for user_456
    
    for p_id in range(partition_count):
        # Use "Earliest" to ensure we get messages published before subscription
        sub = await mq.subscribe_partition(topic, p_id, mode="Earliest")
        
        # We need a non-blocking way or a short timeout to check if there are messages
        # Since we just published, they should be there.
        
        while True:
            try:
                # Very short timeout just to check availability
                msg = await asyncio.wait_for(sub.recv(), timeout=0.1)
                print(f"[Partition {p_id}] Received: {msg}")
                found_count += 1
            except asyncio.TimeoutError:
                # No more messages in this partition right now
                break
                
    print(f"Total messages received: {found_count}/{expected_count}")

if __name__ == "__main__":
    asyncio.run(main())
