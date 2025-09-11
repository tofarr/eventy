#!/usr/bin/env python3
"""
Distributed Task Manager Example for Eventy Queue System

This example demonstrates a distributed factorial calculation system using the eventy
queue system. It showcases:

1. Distributed processing with worker and broker modes
2. Inter-process communication using event queues
3. Task distribution and result collection
4. Factorial calculation as a compute-intensive example task

The system operates in two modes:
- Broker mode: Generates factorial requests, distributes them to workers, and collects results
- Worker mode: Processes factorial requests and returns results

Architecture:
- FactorialRequestEvent: Contains the number to calculate factorial for
- FactorialResponseEvent: Contains the calculated factorial result
- FactorialRequestSubscriber: Worker that calculates factorials
- FactorialResponseSubscriber: Broker that displays results
- NonceSubscriber: Ensures each task is processed only once across multiple workers

Usage:
  python 3_distributed_task_manager.py                    # Run as broker (default)
  python 3_distributed_task_manager.py --mode worker      # Run as worker
  python 3_distributed_task_manager.py --workers 5 --interval 2 --max-value 50
"""

import argparse
import asyncio
import math
import random
import subprocess
import sys
from dataclasses import dataclass
from typing import List

from eventy.event_queue import EventQueue
from eventy.mem.memory_queue_manager import MemoryQueueManager
from eventy.queue_event import QueueEvent
from eventy.queue_manager import get_default_queue_manager
from eventy.subscribers.nonce_subscriber import NonceSubscriber
from eventy.subscribers.subscriber import Subscriber
from eventy.subscribers.worker_match_subscriber import WorkerMatchSubscriber


@dataclass
class FactorialRequestEvent:
    """Event containing a number for which to calculate the factorial."""
    value: int


@dataclass
class FactorialResponseEvent:
    """Event containing the calculated factorial result."""
    value: int


request_queue: EventQueue[FactorialRequestEvent] | None = None
response_queue: EventQueue[FactorialResponseEvent] | None = None


@dataclass
class FactorialRequestSubscriber(Subscriber[FactorialRequestEvent]):
    """Worker subscriber that calculates factorials and publishes results."""
    
    async def on_event(
        self, 
        event: QueueEvent[FactorialRequestEvent], 
        event_queue: EventQueue[FactorialRequestEvent]
    ) -> None:
        """Calculate factorial and publish result."""
        request_value = event.payload.value
        print(f"🔢 Worker calculating factorial of {request_value}")
        
        # Calculate factorial
        try:
            result = math.factorial(request_value)
            print(f"✅ Worker completed: {request_value}! = {result}")
            
            # Publish the result
            response_event = FactorialResponseEvent(value=result)
            await response_queue.publish(response_event)
            
        except Exception as e:
            print(f"❌ Worker error calculating factorial of {request_value}: {e}")


@dataclass
class FactorialResponseSubscriber(Subscriber[FactorialResponseEvent]):
    """Broker subscriber that displays factorial results."""
    
    async def on_event(
        self, 
        event: QueueEvent[FactorialResponseEvent], 
        event_queue: EventQueue[FactorialResponseEvent]
    ) -> None:
        """Display the factorial result."""
        result = event.payload.value
        print(f"📊 Broker received result: {result}")


async def run_worker():
    global request_queue, response_queue
    """Run in worker mode - process factorial requests."""
    print("🔧 Starting worker mode...")
    
    # Get the default queue manager
    queue_manager = await get_default_queue_manager()
    
    # Check if it's a MemoryQueueManager (doesn't support clustering)
    if isinstance(queue_manager, MemoryQueueManager):
        print("❌ MemoryQueueManager does not support clustering. Use a different queue manager for distributed processing.")
        sys.exit(1)
    
    async with queue_manager:
        # Register our event types
        await queue_manager.register(FactorialRequestEvent)
        await queue_manager.register(FactorialResponseEvent)
        
        # Get the queues
        request_queue = await queue_manager.get_event_queue(FactorialRequestEvent)
        response_queue = await queue_manager.get_event_queue(FactorialResponseEvent)
        
        async with request_queue, response_queue:
            print(f"✅ Worker connected with ID: {request_queue.get_worker_id()}")
            
            # Create the factorial request subscriber wrapped in NonceSubscriber
            factorial_subscriber = FactorialRequestSubscriber()
            nonce_subscriber = NonceSubscriber(subscriber=factorial_subscriber)
            
            # Subscribe to the request queue
            subscription = await request_queue.subscribe(nonce_subscriber, True)
            print(f"✅ Worker subscribed to requests with subscription ID: {subscription.id}")
            
            try:
                # Wait forever, processing requests as they come
                print("⏳ Worker waiting for factorial requests...")
                while True:
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                print("\n🛑 Worker shutting down...")
                await request_queue.unsubscribe(subscription.id)
                print("✅ Worker unsubscribed and shut down")


async def run_broker(num_workers: int, interval: float, max_value: int):
    """Run in broker mode - generate requests and collect results."""
    global request_queue, response_queue
    print(f"🎯 Starting broker mode with {num_workers} workers...")
    
    # Get the default queue manager
    queue_manager = await get_default_queue_manager()
    
    # Check if it's a MemoryQueueManager (doesn't support clustering)
    if isinstance(queue_manager, MemoryQueueManager):
        print("❌ MemoryQueueManager does not support clustering. Use a different queue manager for distributed processing.")
        sys.exit(1)
    
    async with queue_manager:
        # Register our event types
        await queue_manager.register(FactorialRequestEvent)
        await queue_manager.register(FactorialResponseEvent)
        
        # Get the queues
        request_queue = await queue_manager.get_event_queue(FactorialRequestEvent)
        response_queue = await queue_manager.get_event_queue(FactorialResponseEvent)
        
        async with request_queue, response_queue:
            print(f"✅ Broker connected with ID: {request_queue.get_worker_id()}")
            
            # Create and subscribe the response subscriber
            response_subscriber = FactorialResponseSubscriber()
            worker_match_subscriber = WorkerMatchSubscriber(response_subscriber, response_queue.get_worker_id())
            response_subscription = await response_queue.subscribe(worker_match_subscriber, True)
            print(f"✅ Broker subscribed to responses with subscription ID: {response_subscription.id}")
            
            # Start worker subprocesses
            worker_processes: List[subprocess.Popen] = []
            try:
                print(f"🚀 Starting {num_workers} worker processes...")
                for i in range(num_workers):
                    process = subprocess.Popen([
                        sys.executable, __file__, "--mode", "worker"
                    ])
                    worker_processes.append(process)
                    print(f"✅ Started worker process {i+1} with PID: {process.pid}")
                
                # Give workers time to start up
                await asyncio.sleep(2)
                
                print(f"📤 Broker will generate requests every {interval} seconds...")
                print(f"🎲 Random numbers will be between 1 and {max_value}")
                print("Press Ctrl+C to stop\n")
                
                request_count = 0
                while True:
                    # Generate a random number for factorial calculation
                    number = random.randint(1, max_value)
                    request_count += 1
                    
                    print(f"📤 Broker request #{request_count}: Calculate factorial of {number}")
                    
                    # Publish the request
                    request_event = FactorialRequestEvent(value=number)
                    await request_queue.publish(request_event)
                    
                    # Wait for the specified interval
                    await asyncio.sleep(interval)
                    
            except asyncio.CancelledError:
                print("\n🛑 Broker shutting down...")
                
                # Unsubscribe from responses
                await response_queue.unsubscribe(response_subscription.id)
                print("✅ Broker unsubscribed from responses")
                
                # Terminate all worker processes
                print("🔄 Terminating worker processes...")
                for i, process in enumerate(worker_processes):
                    if process.poll() is None:  # Process is still running
                        process.terminate()
                        print(f"✅ Terminated worker process {i+1} (PID: {process.pid})")
                
                # Wait for processes to terminate
                for process in worker_processes:
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        print("⚠️  Force killed a worker process")
                
                print(f"📊 Total requests generated: {request_count}")
                print("👋 Broker shut down complete")

        await queue_manager.reset(FactorialRequestEvent)
        await queue_manager.reset(FactorialResponseEvent)

def main():
    """Main function that parses arguments and runs the appropriate mode."""
    parser = argparse.ArgumentParser(
        description="Distributed Factorial Task Manager using Eventy Queue System"
    )
    parser.add_argument(
        "--mode", 
        choices=["broker", "worker"], 
        default="broker",
        help="Run mode: 'broker' generates tasks and collects results, 'worker' processes tasks (default: broker)"
    )
    parser.add_argument(
        "--workers", 
        type=int, 
        default=3,
        help="Number of worker processes to start in broker mode (default: 3)"
    )
    parser.add_argument(
        "--interval", 
        type=float, 
        default=1.0,
        help="Interval in seconds between generating new requests in broker mode (default: 1.0)"
    )
    parser.add_argument(
        "--max-value", 
        type=int, 
        default=100,
        help="Maximum value for random factorial requests in broker mode (default: 100)"
    )
    
    args = parser.parse_args()
    
    print("🎯 Distributed Factorial Task Manager")
    print("=" * 50)
    
    try:
        if args.mode == "worker":
            asyncio.run(run_worker())
        else:
            asyncio.run(run_broker(args.workers, args.interval, args.max_value))
    except KeyboardInterrupt:
        print("\n👋 Exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()