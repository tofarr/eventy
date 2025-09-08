import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

from eventy.fs.abstract_file_event_queue import AbstractFileEventQueue

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class PollingFileEventQueue(AbstractFileEventQueue[T]):
    """
    Polling-based file event queue implementation.
    
    This implementation uses periodic polling to check for new events and process them.
    It runs background tasks that periodically scan the events directory for new files.
    """
    
    # Polling configuration
    polling_interval: float = field(default=1.0)  # seconds between polls
    
    # Background task management
    _polling_task: asyncio.Task = field(default=None, init=False)
    _stop_polling: bool = field(default=False, init=False)
    
    async def __aenter__(self):
        """Start the polling event queue"""
        if self.running:
            return self
        
        self.running = True
        self._stop_polling = False
        
        # Start background polling task
        self._polling_task = asyncio.create_task(self._polling_loop())
        
        _LOGGER.info(f"Started polling file event queue at {self.root_dir} with interval {self.polling_interval}s")
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the polling event queue"""
        if not self.running:
            return
        
        self.running = False
        self._stop_polling = True
        
        # Cancel and wait for polling task to complete
        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
        
        _LOGGER.info(f"Stopped polling file event queue at {self.root_dir}")
    
    async def _polling_loop(self):
        """Main polling loop that runs in the background"""
        _LOGGER.debug("Starting polling loop")
        
        try:
            while not self._stop_polling:
                try:
                    await self._poll_for_events()
                except Exception as e:
                    _LOGGER.error(f"Error during polling: {e}", exc_info=True)
                
                # Wait for next polling interval
                await asyncio.sleep(self.polling_interval)
        except asyncio.CancelledError:
            _LOGGER.debug("Polling loop cancelled")
            raise
        except Exception as e:
            _LOGGER.error(f"Unexpected error in polling loop: {e}", exc_info=True)
        finally:
            _LOGGER.debug("Polling loop ended")
    
    async def _poll_for_events(self):
        """Poll for new events and process them"""
        if not self.events_dir.exists():
            return
        
        # Get all event files
        event_files = list(self.events_dir.iterdir())
        if not event_files:
            return
        
        _LOGGER.debug(f"Found {len(event_files)} event files to process")
        
        # Process each event file
        for event_file in event_files:
            try:
                await self._process_event_file(event_file)
            except Exception as e:
                _LOGGER.error(f"Error processing event file {event_file}: {e}", exc_info=True)
    
    async def _process_event_file(self, event_file: Path):
        """Process a single event file"""
        try:
            event_id = int(event_file.name)
        except ValueError:
            _LOGGER.warning(f"Skipping non-numeric event file: {event_file}")
            return
        
        # Skip if already processed
        if self._is_event_processed(event_id):
            return
        
        # Load the event
        try:
            with open(event_file, 'rb') as f:
                event_data = f.read()
            event = self.event_serializer.deserialize(event_data)
        except Exception as e:
            _LOGGER.error(f"Failed to deserialize event {event_id}: {e}")
            return
        
        # Process event with all subscribers
        await self._notify_subscribers(event)
        
        # Mark as processed
        self._mark_event_processed(event_id)
    
    async def _notify_subscribers(self, event):
        """Notify all subscribers about an event"""
        subscriber_count = 0
        
        async for subscription in self._iter_subscriptions():
            subscriber_count += 1
            try:
                await subscription.subscriber.on_event(event)
                _LOGGER.debug(f"Notified subscriber {subscription.id} about event {event.id}")
            except Exception as e:
                _LOGGER.error(f"Error notifying subscriber {subscription.id} about event {event.id}: {e}", exc_info=True)
        
        if subscriber_count > 0:
            _LOGGER.info(f"Notified {subscriber_count} subscribers about event {event.id}")