import asyncio
import tokio_memq

async def main():
    mq = tokio_memq.MessageQueue()
    topic_name = "pythonic_topic"
    
    # 1. Test Repr and Send
    pub = mq.publisher(topic_name)
    print(f"Publisher Repr: {pub}")  # Should show <Publisher topic='pythonic_topic'>
    
    sub = await mq.subscriber(topic_name)
    print(f"Subscriber Repr: {sub}") # Should show <Subscriber topic='pythonic_topic' consumer_id='None'>

    # 2. Test async for
    print("Publishing 5 messages via pub.send()...")
    for i in range(5):
        await pub.send({"id": i, "msg": f"hello-{i}"})
    
    # Send a poison pill to stop the consumer
    await pub.send({"action": "stop"})
    
    print("Consuming via 'async for'...")
    count = 0
    async for msg in sub:
        print(f"Received: {msg}")
        count += 1
        if isinstance(msg, dict) and msg.get("action") == "stop":
            print("Stop signal received. Breaking loop.")
            break
            
    print(f"Total consumed: {count}")

if __name__ == "__main__":
    asyncio.run(main())
