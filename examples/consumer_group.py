import asyncio
import tokio_memq

async def consumer_task(mq, topic, group_id, consumer_name):
    # Join a consumer group
    print(f"[{consumer_name}] Joining group '{group_id}'...")
    sub = await mq.subscriber_group(topic, group_id, mode="Earliest")
    
    print(f"[{consumer_name}] Joined. Consumer ID: {sub.consumer_id}")

    try:
        while True:
            # Wait for message with timeout
            try:
                msg = await asyncio.wait_for(sub.recv(), timeout=3.0)
                print(f"[{consumer_name}] Received: {msg}")
            except asyncio.TimeoutError:
                print(f"[{consumer_name}] Idle (no messages).")
                break
    except Exception as e:
        print(f"[{consumer_name}] Error: {e}")

async def main():
    mq = tokio_memq.MessageQueue()
    topic_name = "shared_tasks"
    
    # Create topic
    await mq.create_topic(topic_name)
    print(f"Topic '{topic_name}' created.")

    # Start two consumers in the same group
    group_id = "worker_group"
    c1 = asyncio.create_task(consumer_task(mq, topic_name, group_id, "Worker-1"))
    c2 = asyncio.create_task(consumer_task(mq, topic_name, group_id, "Worker-2"))

    # Give consumers time to start
    await asyncio.sleep(0.5)

    # Publish messages
    pub = await mq.publisher(topic_name)
    print("Publishing 10 tasks...")
    for i in range(10):
        await pub.send({"task_id": i, "payload": f"data-{i}"})
    print("Publishing done.")

    # Wait for consumers to finish
    await asyncio.gather(c1, c2)

if __name__ == "__main__":
    asyncio.run(main())
