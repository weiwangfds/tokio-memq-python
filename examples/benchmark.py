import asyncio
import time
import statistics
import argparse
import psutil
import os
from tokio_memq import MessageQueue, TopicOptions

class ResourceMonitor:
    def __init__(self, interval=0.1):
        self.interval = interval
        self.running = False
        self.process = psutil.Process(os.getpid())
        self.cpu_samples = []
        self.memory_samples = []
        self.peak_memory = 0
        self._task = None

    async def start(self):
        self.running = True
        self.cpu_samples = []
        self.memory_samples = []
        self.peak_memory = 0
        # Reset CPU percent counter
        self.process.cpu_percent(interval=None)
        self._task = asyncio.create_task(self._monitor())

    async def stop(self):
        self.running = False
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _monitor(self):
        while self.running:
            try:
                # CPU percent since last call
                cpu = self.process.cpu_percent(interval=None)
                # Memory info (RSS in MB)
                mem = self.process.memory_info().rss / 1024 / 1024
                
                self.cpu_samples.append(cpu)
                self.memory_samples.append(mem)
                self.peak_memory = max(self.peak_memory, mem)
                
                await asyncio.sleep(self.interval)
            except Exception:
                break

    def get_stats(self):
        if not self.cpu_samples:
            return {"avg_cpu": 0, "peak_mem": 0, "avg_mem": 0}
            
        return {
            "avg_cpu": statistics.mean(self.cpu_samples),
            "peak_mem": self.peak_memory,
            "avg_mem": statistics.mean(self.memory_samples)
        }

async def run_throughput_test(message_count, payload_size, num_publishers, num_subscribers):
    mq = MessageQueue()
    topic = f"bench_throughput_{time.time()}"
    
    # Ensure topic is created with enough capacity to prevent LRU drops during benchmark
    # We want to measure throughput, not drop rate.
    opts = TopicOptions(max_messages=message_count + 1000, lru_enabled=False)
    
    payload = "x" * payload_size
    msg_data = {"data": payload}
    
    print(f"--- Preparing Throughput Test ({num_publishers} Pub -> {num_subscribers} Sub) ---")
    
    monitor = ResourceMonitor(interval=0.1)
    
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
    await monitor.start()
    
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
    try:
        await asyncio.wait_for(asyncio.gather(*pub_tasks), timeout=60.0)
        pub_end_time = time.time()
        print("Publishers finished.")
    except asyncio.TimeoutError:
        print("TIMEOUT: Publishers failed to finish.")
        pub_end_time = time.time()
    
    # Wait for all subscribers to finish
    # We use wait_for to detect hangs
    try:
        await asyncio.wait_for(asyncio.gather(*sub_tasks), timeout=60.0)
    except asyncio.TimeoutError:
        print("TIMEOUT: Subscribers failed to receive all messages.")
        # Determine how many were received
        total_recvd = 0
        for task in sub_tasks:
            if task.done():
                 total_recvd += task.result()
            else:
                 task.cancel()
                 # We can't easily get the partial count from a cancelled task unless we used a shared counter
                 # For now, we assume 0 for cancelled tasks in this count, which explains the "Loss"
        print(f"Warning: Test timed out. Results may be incomplete.")
        
    end_time = time.time()
    await monitor.stop()
    stats = monitor.get_stats()
    
    total_time = end_time - start_time
    total_messages = message_count * num_subscribers 
    # Calculate actual received if we tracked it, but here we just use what we expected for rate calc
    # unless we want to be precise.
    
    print(f"Messages: {message_count}, Payload: {payload_size} bytes")
    print(f"Total Time: {total_time:.4f}s")
    print(f"Publish Rate: {message_count / (pub_end_time - start_time):.2f} msg/s")
    # Note: Consume rate is only valid if we finished.
    print(f"Consume Rate: {total_messages / total_time:.2f} msg/s (Total delivered)")
    print(f"Resource Usage:")
    print(f"  Avg CPU: {stats['avg_cpu']:.1f}%")
    print(f"  Peak Memory: {stats['peak_mem']:.2f} MB")
    print(f"  Avg Memory: {stats['avg_mem']:.2f} MB")
    print("-" * 40)

async def run_latency_test(message_count, payload_size):
    mq = MessageQueue()
    topic = f"bench_latency_{time.time()}"
    
    pub = mq.publisher(topic)
    sub = await mq.subscriber(topic)
    
    payload = "x" * payload_size
    msg_data = {"data": payload}
    
    latencies = []
    
    print(f"--- Latency Test (1 Pub -> 1 Sub) ---")
    print(f"Messages: {message_count}, Payload: {payload_size} bytes")
    
    for _ in range(message_count):
        t0 = time.perf_counter()
        await pub.publish(msg_data)
        await sub.recv()
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1_000_000) # Microseconds
        
    avg_lat = statistics.mean(latencies)
    p50 = statistics.median(latencies)
    p99 = statistics.quantiles(latencies, n=100)[98]
    
    print(f"Avg Latency: {avg_lat:.2f} µs")
    print(f"P50 Latency: {p50:.2f} µs")
    print(f"P99 Latency: {p99:.2f} µs")
    print("-" * 40)

async def main():
    parser = argparse.ArgumentParser(description="Tokio-MemQ Python Benchmark")
    parser.add_argument("--count", type=int, default=10000, help="Number of messages")
    parser.add_argument("--size", type=int, default=100, help="Payload size in bytes")
    args = parser.parse_args()
    
    print("Starting Benchmarks...\n")
    
    # 1. Latency Test
    await run_latency_test(1000, args.size) # Run fewer messages for latency to be precise
    
    # 2. 1 Pub -> 1 Sub Throughput
    await run_throughput_test(args.count, args.size, 1, 1)
    
    # 3. 1 Pub -> 5 Sub Throughput (Fan-out)
    await run_throughput_test(args.count, args.size, 1, 5)
    
    # 4. 5 Pub -> 1 Sub Throughput (Fan-in)
    await run_throughput_test(args.count, args.size, 5, 1)

if __name__ == "__main__":
    asyncio.run(main())
