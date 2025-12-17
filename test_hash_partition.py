import asyncio
import tokio_memq
import json

async def test_hash_partition():
    print("Creating MessageQueue...")
    mq = tokio_memq.MessageQueue()
    
    topic = "hash_topic_3"
    partitions = 3
    
    # 1. Create partitioned topic
    print(f"Creating Partitioned Topic {topic} with {partitions} partitions...")
    await mq.create_partitioned_topic(topic, partitions, None)
    
    # 2. Set routing strategy to Hash
    print("Setting routing strategy to Hash with key 'message' (using message key)...")
    await mq.set_partition_routing(topic, "Hash", key="message")
    
    # 3. Subscribe to partitions
    print("Subscribing to partitions...")
    sub0 = await mq.subscribe_partition(topic, 0)
    sub1 = await mq.subscribe_partition(topic, 1)
    sub2 = await mq.subscribe_partition(topic, 2)
    
    # 4. Publish messages
    print("Creating Publisher...")
    pub = mq.publisher(topic)
    
    users = [f"user{i}" for i in range(20)]
    user_partition_map = {}
    
    print("Publishing messages...")
    for user in users:
        msg = {"user_id": user, "data": "test"}
        # Pass user_id as key for hashing
        await pub.publish(msg, key=user)
    
    # 5. Consume and verify
    print("Consuming messages...")
    
    # Helper to collect messages
    async def collect(sub, pid):
        msgs = []
        try:
            while True:
                msg = await asyncio.wait_for(sub.recv(), timeout=0.2)
                msgs.append(msg)
        except asyncio.TimeoutError:
            pass
        return msgs

    msgs0 = await collect(sub0, 0)
    msgs1 = await collect(sub1, 1)
    msgs2 = await collect(sub2, 2)
    
    print(f"P0 received {len(msgs0)} messages")
    print(f"P1 received {len(msgs1)} messages")
    print(f"P2 received {len(msgs2)} messages")
    
    # Verify consistency
    for msg in msgs0:
        uid = msg['user_id']
        if uid not in user_partition_map:
            user_partition_map[uid] = 0
        else:
            assert user_partition_map[uid] == 0
            
    for msg in msgs1:
        uid = msg['user_id']
        if uid not in user_partition_map:
            user_partition_map[uid] = 1
        else:
            assert user_partition_map[uid] == 1
            
    for msg in msgs2:
        uid = msg['user_id']
        if uid not in user_partition_map:
            user_partition_map[uid] = 2
        else:
            assert user_partition_map[uid] == 2

    print("User partition mapping:")
    for u, p in user_partition_map.items():
        print(f"  {u} -> P{p}")
        
    print("All tests passed!")

if __name__ == "__main__":
    asyncio.run(test_hash_partition())
