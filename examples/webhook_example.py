#!/usr/bin/env python3
"""
Example demonstrating how to use WebhookSubscriber with EventQueue.
"""
import asyncio
from datetime import datetime
from typing import Dict, Any

from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.subscriber.webhook_subscriber import WebhookSubscriber
from eventy.serializers.json_serializer import JsonSerializer


async def main():
    """Demonstrate webhook subscriber usage."""
    print("🚀 WebhookSubscriber Example")
    print("=" * 40)

    # Create an event queue for dictionary payloads
    queue = MemoryEventQueue[Dict[str, Any]](event_type=dict)

    # Create a webhook subscriber that sends to httpbin.org (for testing)
    webhook = WebhookSubscriber(
        url="https://httpbin.org/post",  # Test endpoint that echoes back the request
        payload_type=dict,
        serializer=JsonSerializer[Dict[str, Any]](),
        headers={
            "Authorization": "Bearer your-api-token",
            "X-Source": "eventy-webhook-example",
        },
        request_method="POST",
        retry_attempts=2,
        timeout=10.0,
    )

    print(f"Created webhook subscriber: {webhook}")

    # Subscribe to the queue - returns a UUID for managing the subscription
    subscriber_id = await queue.subscribe(webhook)
    print(f"Subscribed with UUID: {subscriber_id}")
    print(f"Subscriber ID type: {type(subscriber_id)}")

    # Publish some test events
    test_events = [
        {
            "event_type": "user_registered",
            "user_id": "user123",
            "email": "user@example.com",
            "timestamp": datetime.now().isoformat(),
        },
        {
            "event_type": "order_placed",
            "order_id": "order456",
            "amount": 99.99,
            "currency": "USD",
            "timestamp": datetime.now().isoformat(),
        },
        {
            "event_type": "payment_processed",
            "payment_id": "pay789",
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
        },
    ]

    print(f"\nPublishing {len(test_events)} events...")

    for i, event_data in enumerate(test_events, 1):
        await queue.publish(event_data)
        print(f"  {i}. Published: {event_data['event_type']}")

    # Give some time for webhooks to be sent
    print("\nWaiting for webhooks to be sent...")
    await asyncio.sleep(2)

    # Unsubscribe using the UUID returned from subscribe
    success = await queue.unsubscribe(subscriber_id)
    print(f"\nUnsubscribed using UUID {subscriber_id}: {success}")

    print("\n✅ Example completed!")
    print("\nKey Features Demonstrated:")
    print("• EventQueue.subscribe() returns a UUID for subscriber management")
    print("• EventQueue.unsubscribe() accepts the UUID and returns success status")
    print(
        "• WebhookSubscriber sends HTTP requests with configurable methods and headers"
    )
    print("• JsonSerializer handles payload conversion with UTF-8 encoding")
    print(
        "\nNote: Check the httpbin.org response to see the webhook data that was sent."
    )


if __name__ == "__main__":
    asyncio.run(main())
