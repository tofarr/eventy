import asyncio
import os
import json
import fcntl
import base64
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import TypeVar, Optional, List, AsyncIterator
from uuid import UUID

from eventy.event_queue import EventQueue
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscriber import Subscriber

T = TypeVar("T")


@dataclass
class FilesystemEventQueue(EventQueue[T]):
    """
    Filesystem-based event queue implementation.
    
    Events are stored as individual files in an 'events' directory.
    When a threshold is reached (by count or size), events are consolidated
    into numbered page files in a 'pages' directory.
    """

    event_type: type[T]
    root_path: Path
    max_age: timedelta | None = None
    max_events_per_page: int = 25
    max_page_size_bytes: int = 1024 * 1024  # 1MB
    serializer: Serializer[QueueEvent[T]] = field(default_factory=get_default_serializer)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self):
        """Initialize directory structure"""
        self.events_dir = self.root_path / "events"
        self.pages_dir = self.root_path / "pages"
        self.lock_file = self.root_path / ".lock"
        
        # Create directories if they don't exist
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.pages_dir.mkdir(parents=True, exist_ok=True)

    def _deserialize_json_data(self, json_data):
        """Convert JSON data back to serialized format"""
        if isinstance(json_data, dict) and "type" in json_data:
            if json_data["type"] == "bytes":
                return base64.b64decode(json_data["data"])
            else:
                return json_data["data"]
        else:
            # Legacy format - assume it's the raw data
            return json_data

    async def _cleanup_old_events(self) -> None:
        """Remove events older than max_age if specified"""
        if self.max_age is None:
            return
            
        cutoff_time = datetime.now(UTC) - self.max_age
        
        # Clean up individual event files
        for event_file in self.events_dir.glob("*.json"):
            try:
                with open(event_file, 'r') as f:
                    json_data = json.load(f)
                event_data = self._deserialize_json_data(json_data)
                event = self.serializer.deserialize(event_data)
                if event.created_at < cutoff_time:
                    event_file.unlink()
            except Exception:
                # Skip corrupted files
                continue
                
        # Clean up old pages
        for page_file in self.pages_dir.glob("*.json"):
            try:
                with open(page_file, 'r') as f:
                    page_data = json.load(f)
                # Check if all events in the page are old
                all_old = True
                for json_event_data in page_data:
                    event_data = self._deserialize_json_data(json_event_data)
                    event = self.serializer.deserialize(event_data)
                    if event.created_at >= cutoff_time:
                        all_old = False
                        break
                if all_old:
                    page_file.unlink()
            except Exception:
                # Skip corrupted files
                continue

    async def _consolidate_events_to_page(self) -> None:
        """Consolidate events into a page file if thresholds are met"""
        event_files = list(self.events_dir.glob("*.json"))
        
        if len(event_files) < self.max_events_per_page:
            # Check total size
            total_size = sum(f.stat().st_size for f in event_files)
            if total_size < self.max_page_size_bytes:
                return
        
        # Use file locking to coordinate between processes
        with open(self.lock_file, 'w') as lock_fd:
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Re-check after acquiring lock (another process might have done this)
                event_files = list(self.events_dir.glob("*.json"))
                if len(event_files) == 0:
                    return
                
                # Load and sort events by creation time
                events_data = []
                for event_file in event_files:
                    try:
                        with open(event_file, 'r') as f:
                            json_data = json.load(f)
                        events_data.append((json_data, event_file))
                    except Exception:
                        # Skip corrupted files
                        continue
                
                # Sort by created_at timestamp (need to deserialize to get timestamp)
                def get_created_at(item):
                    try:
                        json_data, _ = item
                        event_data = self._deserialize_json_data(json_data)
                        event = self.serializer.deserialize(event_data)
                        return event.created_at.isoformat()
                    except:
                        return ''
                
                events_data.sort(key=get_created_at)
                
                # Find next page number
                existing_pages = list(self.pages_dir.glob("*.json"))
                page_numbers = []
                for page_file in existing_pages:
                    try:
                        page_numbers.append(int(page_file.stem))
                    except ValueError:
                        continue
                
                next_page_num = max(page_numbers, default=0) + 1
                page_file = self.pages_dir / f"{next_page_num:06d}.json"
                
                # Write page file
                page_events = [json_data for json_data, _ in events_data]
                with open(page_file, 'w') as f:
                    json.dump(page_events, f)
                
                # Delete individual event files
                for _, event_file in events_data:
                    try:
                        event_file.unlink()
                    except FileNotFoundError:
                        # Another process might have deleted it
                        pass
                        
            except BlockingIOError:
                # Another process is consolidating, skip
                pass

    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Subscribe to events"""
        async with self.lock:
            await self._cleanup_old_events()
            # For filesystem implementation, we don't maintain active subscribers
            # This would need to be implemented with file watching or polling
            pass

    async def publish(self, event: QueueEvent[T]) -> None:
        """Publish an event to the queue"""
        async with self.lock:
            await self._cleanup_old_events()
            
            # Serialize and save event to individual file
            event_data = self.serializer.serialize(event)
            event_file = self.events_dir / f"{event.id}.json"
            
            # Convert bytes to base64 for JSON storage
            if isinstance(event_data, bytes):
                json_data = {"data": base64.b64encode(event_data).decode('utf-8'), "type": "bytes"}
            else:
                json_data = {"data": event_data, "type": "object"}
            
            with open(event_file, 'w') as f:
                json.dump(json_data, f)
            
            # Check if we need to consolidate events
            await self._consolidate_events_to_page()

    async def get_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        page_size: int = 10,
        page_id: Optional[str] = None,
    ) -> Page[QueueEvent[T]]:
        """Get events matching the criteria"""
        async with self.lock:
            await self._cleanup_old_events()
            
            all_events = []
            
            # First, load events from individual files
            for event_file in self.events_dir.glob("*.json"):
                try:
                    with open(event_file, 'r') as f:
                        json_data = json.load(f)
                    event_data = self._deserialize_json_data(json_data)
                    event = self.serializer.deserialize(event_data)
                    all_events.append(event)
                except Exception:
                    # Skip corrupted files
                    continue
            
            # Then, load events from page files
            page_files = sorted(self.pages_dir.glob("*.json"))
            for page_file in page_files:
                try:
                    with open(page_file, 'r') as f:
                        page_data = json.load(f)
                    for json_event_data in page_data:
                        event_data = self._deserialize_json_data(json_event_data)
                        event = self.serializer.deserialize(event_data)
                        all_events.append(event)
                except Exception:
                    # Skip corrupted files
                    continue
            
            # Sort events by creation time
            all_events.sort(key=lambda e: e.created_at)
            
            # Apply filters
            filtered_events = []
            for event in all_events:
                if created_at__min and event.created_at < created_at__min:
                    continue
                if created_at__max and event.created_at > created_at__max:
                    continue
                filtered_events.append(event)
            
            # Handle pagination
            start_index = 0
            if page_id:
                try:
                    start_index = int(page_id)
                except ValueError:
                    start_index = 0
            
            end_index = start_index + page_size
            page_events = filtered_events[start_index:end_index]
            
            # Determine next page ID
            next_page_id = None
            if end_index < len(filtered_events):
                next_page_id = str(end_index)
            
            return Page(items=page_events, next_page_id=next_page_id)

    async def count_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
    ) -> int:
        """Get the number of events matching the criteria"""
        async with self.lock:
            await self._cleanup_old_events()
            
            count = 0
            
            # Count events from individual files
            for event_file in self.events_dir.glob("*.json"):
                try:
                    with open(event_file, 'r') as f:
                        json_data = json.load(f)
                    event_data = self._deserialize_json_data(json_data)
                    event = self.serializer.deserialize(event_data)
                    
                    # Apply datetime filters
                    if created_at__min and event.created_at < created_at__min:
                        continue
                    if created_at__max and event.created_at > created_at__max:
                        continue
                    
                    count += 1
                except Exception:
                    # Skip corrupted files
                    continue
            
            # Count events from page files
            page_files = sorted(self.pages_dir.glob("*.json"))
            for page_file in page_files:
                try:
                    with open(page_file, 'r') as f:
                        page_data = json.load(f)
                    for json_event_data in page_data:
                        event_data = self._deserialize_json_data(json_event_data)
                        event = self.serializer.deserialize(event_data)
                        
                        # Apply datetime filters
                        if created_at__min and event.created_at < created_at__min:
                            continue
                        if created_at__max and event.created_at > created_at__max:
                            continue
                        
                        count += 1
                except Exception:
                    # Skip corrupted files
                    continue
            
            return count

    async def get_event(self, id: UUID) -> QueueEvent[T]:
        """Get an event by its ID"""
        async with self.lock:
            await self._cleanup_old_events()
            
            # First check individual event files
            event_file = self.events_dir / f"{id}.json"
            if event_file.exists():
                try:
                    with open(event_file, 'r') as f:
                        json_data = json.load(f)
                    event_data = self._deserialize_json_data(json_data)
                    return self.serializer.deserialize(event_data)
                except Exception:
                    pass
            
            # Then check page files
            page_files = sorted(self.pages_dir.glob("*.json"))
            for page_file in page_files:
                try:
                    with open(page_file, 'r') as f:
                        page_data = json.load(f)
                    for json_event_data in page_data:
                        event_data = self._deserialize_json_data(json_event_data)
                        event = self.serializer.deserialize(event_data)
                        if event.id == id:
                            return event
                except Exception:
                    # Skip corrupted files
                    continue
            
            raise ValueError(f"Event with id {id} not found")