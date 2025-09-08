import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileCreatedEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False
    Observer = None
    FileSystemEventHandler = object  # Use object as base class when not available
    FileCreatedEvent = None

from eventy.fs.abstract_file_event_queue import AbstractFileEventQueue

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


class EventFileHandler(FileSystemEventHandler if WATCHDOG_AVAILABLE else object):
    """File system event handler for watchdog"""
    
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self._pending_events = asyncio.Queue()
    
    def on_created(self, event):
        """Handle file creation events"""
        if WATCHDOG_AVAILABLE and not event.is_directory:
            # Add to pending events queue for async processing
            try:
                asyncio.create_task(self._pending_events.put(event.src_path))
            except RuntimeError:
                # If no event loop is running, we'll handle this in the polling fallback
                pass
    
    async def get_pending_event(self):
        """Get the next pending event"""
        return await self._pending_events.get()
    
    def has_pending_events(self):
        """Check if there are pending events"""
        return not self._pending_events.empty()


class SubscriptionFileHandler(FileSystemEventHandler if WATCHDOG_AVAILABLE else object):
    """File system event handler for subscription directory"""
    
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
    
    def on_created(self, event):
        """Handle subscription file creation"""
        if WATCHDOG_AVAILABLE and not event.is_directory:
            _LOGGER.debug(f"Subscription file created: {event.src_path}")
            self.queue._mark_subscription_cache_dirty()
    
    def on_deleted(self, event):
        """Handle subscription file deletion"""
        if WATCHDOG_AVAILABLE and not event.is_directory:
            _LOGGER.debug(f"Subscription file deleted: {event.src_path}")
            self.queue._mark_subscription_cache_dirty()
    
    def on_modified(self, event):
        """Handle subscription file modification"""
        if WATCHDOG_AVAILABLE and not event.is_directory:
            _LOGGER.debug(f"Subscription file modified: {event.src_path}")
            self.queue._mark_subscription_cache_dirty()


@dataclass
class WatchdogFileEventQueue(AbstractFileEventQueue[T]):
    """
    Watchdog-based file event queue implementation.
    
    This implementation uses the watchdog library to monitor the events directory
    for new files and processes them immediately when they are created.
    Falls back to polling if watchdog is not available.
    """
    
    # Watchdog configuration
    _observer: Observer = field(default=None, init=False)
    _subscription_observer: Observer = field(default=None, init=False)
    _event_handler: EventFileHandler = field(default=None, init=False)
    _subscription_handler: SubscriptionFileHandler = field(default=None, init=False)
    _processing_task: asyncio.Task = field(default=None, init=False)
    _stop_processing: bool = field(default=False, init=False)
    
    # Fallback polling configuration (used if watchdog is not available)
    polling_interval: float = field(default=1.0)
    _polling_task: asyncio.Task = field(default=None, init=False)
    _subscription_polling_task: asyncio.Task = field(default=None, init=False)
    
    def __post_init__(self):
        super().__post_init__()
        
        if not WATCHDOG_AVAILABLE:
            _LOGGER.warning("Watchdog library not available, falling back to polling mode")
    
    async def __aenter__(self):
        """Start the watchdog event queue"""
        if self.running:
            return self
        
        self.running = True
        self._stop_processing = False
        
        # Start heartbeat system
        await self._start_heartbeat()
        
        if WATCHDOG_AVAILABLE:
            await self._start_watchdog()
        else:
            await self._start_polling_fallback()
        
        _LOGGER.info(f"Started watchdog file event queue at {self.root_dir}")
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the watchdog event queue"""
        if not self.running:
            return
        
        self.running = False
        self._stop_processing = True
        
        if WATCHDOG_AVAILABLE and self._observer:
            await self._stop_watchdog()
        else:
            await self._stop_polling_fallback()
        
        # Stop heartbeat system
        await self._stop_heartbeat()
        
        _LOGGER.info(f"Stopped watchdog file event queue at {self.root_dir}")
    
    async def _start_watchdog(self):
        """Start watchdog file monitoring"""
        # Set up event monitoring
        self._event_handler = EventFileHandler(self)
        self._observer = Observer()
        self._observer.schedule(self._event_handler, str(self.events_dir), recursive=False)
        self._observer.start()
        
        # Set up subscription monitoring
        self._subscription_handler = SubscriptionFileHandler(self)
        self._subscription_observer = Observer()
        self._subscription_observer.schedule(self._subscription_handler, str(self.subscriptions_dir), recursive=False)
        self._subscription_observer.start()
        
        # Start background task to process events
        self._processing_task = asyncio.create_task(self._event_processing_loop())
        
        # Process any existing events
        await self._process_existing_events()
        
        _LOGGER.info(f"Started watchdog monitoring on {self.events_dir} and {self.subscriptions_dir}")
    
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
    
    async def _start_polling_fallback(self):
        """Start polling fallback when watchdog is not available"""
        self._polling_task = asyncio.create_task(self._polling_loop())
        self._subscription_polling_task = asyncio.create_task(self._subscription_polling_loop())
        _LOGGER.info(f"Started polling fallback with interval {self.polling_interval}s")
    
    async def _stop_polling_fallback(self):
        """Stop polling fallback"""
        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
        
        if self._subscription_polling_task and not self._subscription_polling_task.done():
            self._subscription_polling_task.cancel()
            try:
                await self._subscription_polling_task
            except asyncio.CancelledError:
                pass
        
        _LOGGER.info("Stopped polling fallback")
    
    async def _event_processing_loop(self):
        """Main event processing loop for watchdog events"""
        _LOGGER.debug("Starting event processing loop")
        
        try:
            while not self._stop_processing:
                try:
                    # Wait for new events from watchdog
                    event_path = await asyncio.wait_for(
                        self._event_handler.get_pending_event(), 
                        timeout=1.0
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
            _LOGGER.error(f"Unexpected error in event processing loop: {e}", exc_info=True)
        finally:
            _LOGGER.debug("Event processing loop ended")
    
    async def _polling_loop(self):
        """Fallback polling loop when watchdog is not available"""
        _LOGGER.debug("Starting polling fallback loop")
        
        try:
            while not self._stop_processing:
                try:
                    await self._poll_for_events()
                except Exception as e:
                    _LOGGER.error(f"Error during polling: {e}", exc_info=True)
                
                await asyncio.sleep(self.polling_interval)
        except asyncio.CancelledError:
            _LOGGER.debug("Polling fallback loop cancelled")
            raise
        except Exception as e:
            _LOGGER.error(f"Unexpected error in polling fallback loop: {e}", exc_info=True)
        finally:
            _LOGGER.debug("Polling fallback loop ended")
    
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
                    _LOGGER.error(f"Error processing existing event file {event_file}: {e}", exc_info=True)
    
    async def _poll_for_events(self):
        """Poll for new events (fallback method)"""
        if not self.events_dir.exists():
            return
        
        event_files = list(self.events_dir.iterdir())
        if not event_files:
            return
        
        _LOGGER.debug(f"Found {len(event_files)} event files to process")
        
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
        
        # Process event with all subscribers using cache
        await self._notify_subscribers_cached(event)
        
        # Mark as processed
        self._mark_event_processed(event_id)
    
    async def _subscription_polling_loop(self):
        """Polling loop that monitors subscription directory for changes (fallback)"""
        _LOGGER.debug("Starting subscription polling loop (fallback)")
        
        # Track last modification times of subscription files
        last_subscription_mtimes = {}
        
        try:
            while not self._stop_processing:
                try:
                    await self._poll_for_subscription_changes(last_subscription_mtimes)
                except Exception as e:
                    _LOGGER.error(f"Error during subscription polling: {e}", exc_info=True)
                
                # Wait for next polling interval
                await asyncio.sleep(self.polling_interval)
        except asyncio.CancelledError:
            _LOGGER.debug("Subscription polling loop cancelled")
            raise
        except Exception as e:
            _LOGGER.error(f"Unexpected error in subscription polling loop: {e}", exc_info=True)
        finally:
            _LOGGER.debug("Subscription polling loop ended")
    
    async def _poll_for_subscription_changes(self, last_mtimes: dict):
        """Poll for changes in the subscription directory (fallback)"""
        if not self.subscriptions_dir.exists():
            return
        
        current_files = set()
        cache_needs_refresh = False
        
        # Check all subscription files
        for subscription_file in self.subscriptions_dir.iterdir():
            if not subscription_file.is_file():
                continue
                
            current_files.add(subscription_file.name)
            
            try:
                current_mtime = subscription_file.stat().st_mtime
                last_mtime = last_mtimes.get(subscription_file.name, 0)
                
                if current_mtime > last_mtime:
                    _LOGGER.debug(f"Detected change in subscription file: {subscription_file.name}")
                    last_mtimes[subscription_file.name] = current_mtime
                    cache_needs_refresh = True
                    
            except OSError as e:
                _LOGGER.warning(f"Error checking subscription file {subscription_file}: {e}")
        
        # Check for deleted files
        deleted_files = set(last_mtimes.keys()) - current_files
        if deleted_files:
            _LOGGER.debug(f"Detected deleted subscription files: {deleted_files}")
            for deleted_file in deleted_files:
                del last_mtimes[deleted_file]
            cache_needs_refresh = True
        
        # Refresh cache if needed
        if cache_needs_refresh:
            _LOGGER.info("Refreshing subscription cache due to detected changes")
            self._mark_subscription_cache_dirty()
    
