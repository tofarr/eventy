import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    Observer = None
    FileSystemEventHandler = None

from eventy.fs.abstract_file_event_queue import AbstractFileEventQueue

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


if FileSystemEventHandler is not None:
    class EventFileHandler(FileSystemEventHandler):
        """File system event handler for watchdog"""

        def __init__(self, queue, loop: asyncio.AbstractEventLoop):
            super().__init__()
            self.queue = queue
            self._pending_events = asyncio.Queue()
            self.loop = loop

        def on_created(self, event):
            """Handle file creation events"""
            if not event.is_directory:
                # Add to pending events queue for async processing
                try:
                    self.loop.call_soon_threadsafe(
                        self._pending_events.put_nowait, event.src_path
                    )
                except RuntimeError:
                    # If no event loop is running, log a warning
                    _LOGGER.warning(
                        f"No event loop running when trying to queue event: {event.src_path}"
                    )

        async def get_pending_event(self):
            """Get the next pending event"""
            return await self._pending_events.get()

        def has_pending_events(self):
            """Check if there are pending events"""
            return not self._pending_events.empty()


    class SubscriptionFileHandler(FileSystemEventHandler):
        """File system event handler for subscription directory"""

        def __init__(self, queue):
            super().__init__()
            self.queue = queue

        def on_created(self, event):
            """Handle subscription file creation"""
            if not event.is_directory:
                _LOGGER.debug(f"Subscription file created: {event.src_path}")
                self.queue._mark_subscription_cache_dirty()  # pylint: disable=protected-access

        def on_deleted(self, event):
            """Handle subscription file deletion"""
            if not event.is_directory:
                _LOGGER.debug(f"Subscription file deleted: {event.src_path}")
                self.queue._mark_subscription_cache_dirty()  # pylint: disable=protected-access

        def on_modified(self, event):
            """Handle subscription file modification"""
            if not event.is_directory:
                _LOGGER.debug(f"Subscription file modified: {event.src_path}")
                self.queue._mark_subscription_cache_dirty()  # pylint: disable=protected-access

else:
    # Fallback classes when watchdog is not available
    class EventFileHandler:
        """Dummy event handler when watchdog is not available"""
        pass  # pylint: disable=unnecessary-pass

    class SubscriptionFileHandler:
        """Dummy subscription handler when watchdog is not available"""
        pass  # pylint: disable=unnecessary-pass


@dataclass
class WatchdogFileEventQueue(AbstractFileEventQueue[T]):
    """
    Watchdog-based file event queue implementation.

    This implementation uses the watchdog library to monitor the events directory
    for new files and processes them immediately when they are created.
    Requires the watchdog library to be installed.
    """

    # Watchdog configuration
    _observer: Observer = field(default=None, init=False)  # type: ignore
    _subscription_observer: Observer = field(default=None, init=False)  # type: ignore
    _event_handler: EventFileHandler = field(default=None, init=False)
    _subscription_handler: SubscriptionFileHandler = field(default=None, init=False)
    _processing_task: asyncio.Task = field(default=None, init=False)
    _stop_processing: bool = field(default=False, init=False)

    async def __aenter__(self):
        """Start the watchdog event queue"""
        if Observer is None:
            raise ImportError("watchdog library is required for WatchdogFileEventQueue")
        
        if self.running:
            return self

        self.running = True
        self._stop_processing = False

        await self._start_watchdog()

        _LOGGER.info(f"Started watchdog file event queue at {self.root_dir}")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the watchdog event queue"""
        if not self.running:
            return

        self.running = False
        self._stop_processing = True

        await self._stop_watchdog()

        _LOGGER.info(f"Stopped watchdog file event queue at {self.root_dir}")

    async def _start_watchdog(self):
        """Start watchdog file monitoring"""
        # Set up event monitoring
        self._event_handler = EventFileHandler(self, asyncio.get_running_loop())
        self._observer = Observer()
        self._observer.schedule(
            self._event_handler, str(self.events_dir), recursive=False
        )
        self._observer.start()

        # Set up subscription monitoring
        self._subscription_handler = SubscriptionFileHandler(self)
        self._subscription_observer = Observer()
        self._subscription_observer.schedule(
            self._subscription_handler, str(self.subscriptions_dir), recursive=False
        )
        self._subscription_observer.start()

        # Start background task to process events
        self._processing_task = asyncio.create_task(self._event_processing_loop())

        # Process any existing events
        await self._process_existing_events()

        _LOGGER.info(
            f"Started watchdog monitoring on {self.events_dir} and {self.subscriptions_dir}"
        )

    async def _stop_watchdog(self):
        """Stop watchdog file monitoring"""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None

        if self._subscription_observer:
            self._subscription_observer.stop()
            self._subscription_observer.join()
            self._subscription_observer = None

        # Cancel and wait for processing task
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

        _LOGGER.info("Stopped watchdog monitoring")

    async def _event_processing_loop(self):
        """Main event processing loop for watchdog events"""
        _LOGGER.debug("Starting event processing loop")

        try:
            while not self._stop_processing:
                try:
                    # Wait for new events from watchdog
                    event_path = await asyncio.wait_for(
                        self._event_handler.get_pending_event(), timeout=1.0
                    )
                    await self._process_event_file(Path(event_path))
                except asyncio.TimeoutError:
                    # No events received, continue loop
                    continue
                except Exception as e:
                    _LOGGER.error(f"Error in event processing loop: {e}", exc_info=True)
        except asyncio.CancelledError:
            _LOGGER.debug("Event processing loop cancelled")
            raise
        except Exception as e:
            _LOGGER.error(
                f"Unexpected error in event processing loop: {e}", exc_info=True
            )
        finally:
            _LOGGER.debug("Event processing loop ended")

    async def _process_existing_events(self):
        """Process any events that already exist in the events directory"""
        if not self.events_dir.exists():
            return

        event_files = list(self.events_dir.iterdir())
        if event_files:
            _LOGGER.info(f"Processing {len(event_files)} existing event files")
            for event_file in event_files:
                try:
                    await self._process_event_file(event_file)
                except Exception as e:
                    _LOGGER.error(
                        f"Error processing existing event file {event_file}: {e}",
                        exc_info=True,
                    )

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
            with open(event_file, "rb") as f:
                event_data = f.read()
            event = self.event_serializer.deserialize(event_data)
        except Exception as e:
            _LOGGER.error(f"Failed to deserialize event {event_id}: {e}")
            return

        # Process event with all subscribers using cache
        await self._notify_subscribers_cached(event)

        # Mark as processed
        self._mark_event_processed(event_id)
