import asyncio
from tokio_memq import MessageQueue

async def main():
    # Initialize the Message Queue
    print("Initializing MessageQueue...")
    mq = MessageQueue()
    
    topic_name = "notifications"
    
    # Create a publisher
    print(f"Creating publisher for topic '{topic_name}'...")
    publisher = mq.publisher(topic_name)
    
    # Create a subscriber
    # This automatically creates the topic if it doesn't exist
    print(f"Creating subscriber for topic '{topic_name}'...")
    subscriber = await mq.subscriber(topic_name)
    
    # Publish messages
    messages = [
        {"type": "email", "to": "user@example.com", "content": "Welcome!"},
        {"type": "sms", "to": "+1234567890", "content": "Your code is 1234"}
    ]
    
    print("Publishing messages...")
    for msg in messages:
        await publisher.publish(msg)
        print(f"Published: {msg}")
    
    # Process messages
    print("Consuming messages...")
    for i in range(len(messages)):
        # Wait for a message with a timeout to ensure the script finishes
        try:
            msg = await asyncio.wait_for(subscriber.recv(), timeout=2.0)
            print(f"Received [{i+1}/{len(messages)}]: {msg}")
        except asyncio.TimeoutError:
            print(f"Timeout waiting for message {i+1}")

if __name__ == "__main__":
    asyncio.run(main())
