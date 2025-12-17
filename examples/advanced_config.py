import asyncio
from tokio_memq import MessageQueue, TopicOptions

async def main():
    mq = MessageQueue()
    
    # Example 1: Bounded Queue with LRU Eviction
    # When the queue exceeds 10 messages, the oldest ones will be dropped (LRU).
    topic_bounded = "bounded_topic"
    print(f"\n--- Example 1: Bounded Topic '{topic_bounded}' ---")
    
    # Configure options
    opts = TopicOptions(
        max_messages=10, 
        lru_enabled=True
    )
    
    # Create subscriber with options (this creates the topic if needed)
    print(f"Creating subscriber with max_messages=10, lru_enabled=True")
    sub = await mq.subscriber_with_options(topic_bounded, opts)
    pub = mq.publisher(topic_bounded)
    
    # Publish 15 messages (5 more than capacity)
    print("Publishing 15 messages...")
    for i in range(15):
        await pub.publish({"seq": i})
        
    # Check stats
    stats = await mq.get_topic_stats(topic_bounded)
    if stats:
        print(f"Topic stats: {stats}")
        # Note: 'message_count' might still show total messages ever published or current depth depending on implementation.
        # But 'dropped_messages' should ideally reflect drops if tracked.
    
    # Consume. Since it's LRU, we expect to see the *latest* 10 messages (seq 5 to 14), 
    # OR if the subscriber was slow, it might have missed the dropped ones.
    # Actually, in tokio-memq, the subscriber reads from the buffer. If the buffer is circular/LRU,
    # the subscriber offset might be invalidated or adjusted.
    # Let's see what we get.
    
    print("Consuming messages...")
    count = 0
    while True:
        msg = await sub.try_recv()
        if msg is None:
            break
        print(f"Received: {msg}")
        count += 1
        
    print(f"Total received: {count}")
    
    
    # Example 2: TTL (Time To Live)
    topic_ttl = "ttl_topic"
    print(f"\n--- Example 2: TTL Topic '{topic_ttl}' ---")
    
    # Configure options: 500ms TTL
    opts_ttl = TopicOptions(
        message_ttl_ms=500
    )
    
    sub_ttl = await mq.subscriber_with_options(topic_ttl, opts_ttl)
    pub_ttl = mq.publisher(topic_ttl)
    
    print("Publishing message 1 (immediate)...")
    await pub_ttl.publish({"msg": "immediate"})
    
    print("Publishing message 2 (will expire)...")
    await pub_ttl.publish({"msg": "expired"})
    
    # Receive immediate one
    msg1 = await sub_ttl.recv()
    print(f"Received immediate: {msg1}")
    
    print("Sleeping for 1 second...")
    await asyncio.sleep(1.0)
    
    # Try receive expired one
    print("Attempting to receive expired message...")
    msg2 = await sub_ttl.try_recv()
    if msg2 is None:
        print("Received None (Message expired as expected)")
    else:
        print(f"Received: {msg2} (Unexpected)")

if __name__ == "__main__":
    asyncio.run(main())
