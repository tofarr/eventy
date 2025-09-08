import asyncio
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Awaitable, Optional, Set

# Optional Watchdog import
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileSystemEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    Observer = None
    FileSystemEventHandler = None
    FileSystemEvent = None
    WATCHDOG_AVAILABLE = False


class _WatchdogEventHandler(FileSystemEventHandler if WATCHDOG_AVAILABLE else object):
    """Event handler for Watchdog filesystem events"""
    
    def __init__(self, callback: Callable[[], Awaitable[None]], monitor_deletes: bool = False):
        if WATCHDOG_AVAILABLE:
            super().__init__()
        self.callback = callback
        self.monitor_deletes = monitor_deletes
        self._loop = None
        self._callback_lock = threading.Lock()
    
    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        """Set the event loop for async callback execution"""
        self._loop = loop
    
    def _schedule_callback(self):
        """Schedule the callback to run in the event loop"""
        if self._loop and not self._loop.is_closed():
            with self._callback_lock:
                # Use call_soon_threadsafe to schedule the callback from a different thread
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(self.callback())
                )
    
    def on_created(self, event: FileSystemEvent):
        """Handle file/directory creation events"""
        if not event.is_directory:  # Only trigger on file creation
            self._schedule_callback()
    
    def on_deleted(self, event: FileSystemEvent):
        """Handle file/directory deletion events"""
        if self.monitor_deletes and not event.is_directory:  # Only trigger on file deletion
            self._schedule_callback()
    
    def on_moved(self, event: FileSystemEvent):
        """Handle file/directory move events"""
        if not event.is_directory:  # Treat moves as creation/deletion
            self._schedule_callback()


@dataclass
class FilesystemWatch:
    """
    A filesystem watcher that monitors a directory for changes and triggers a callback.

    This class is designed to be used as an async context manager. When Watchdog is available,
    it uses efficient filesystem events. Otherwise, it falls back to polling the directory
    at the specified frequency. By default, the callback is only triggered when new files 
    are added. If monitor_deletes is True, the callback is triggered on any directory 
    change (additions or deletions).
    """

    path: Path
    """The directory path to monitor"""

    callback: Callable[[], Awaitable[None]]
    """Async callback function to execute when changes are detected"""

    previous_contents: Optional[Set[str]] = field(default=None)
    """Set containing the previous directory contents (used for polling mode)"""

    polling_frequency: float = 1.0
    """Polling frequency in seconds (default: 1.0 second, used when Watchdog unavailable)"""

    monitor_deletes: bool = False
    """If True, callback is triggered on any directory change (additions or deletions).
    If False, callback is only triggered when new files are added (default: False)"""

    use_watchdog: bool = True
    """If True, use Watchdog when available. If False, always use polling (default: True)"""

    _task: Optional[asyncio.Task] = field(default=None, init=False)
    """Background polling task (used in polling mode)"""

    _stop_event: Optional[asyncio.Event] = field(default=None, init=False)
    """Event to signal the polling task to stop (used in polling mode)"""

    _observer: Optional[Observer] = field(default=None, init=False)
    """Watchdog observer instance (used in Watchdog mode)"""

    _event_handler: Optional[_WatchdogEventHandler] = field(default=None, init=False)
    """Watchdog event handler (used in Watchdog mode)"""

    _using_watchdog: bool = field(default=False, init=False)
    """Flag indicating whether Watchdog is being used"""

    def __post_init__(self):
        """Initialize the path as a Path object if it's a string"""
        if isinstance(self.path, str):
            self.path = Path(self.path)

    async def __aenter__(self):
        """Enter the async context manager and start monitoring"""
        # Determine which monitoring method to use
        self._using_watchdog = WATCHDOG_AVAILABLE and self.use_watchdog
        
        if self._using_watchdog:
            # Use Watchdog for filesystem monitoring
            try:
                # Ensure the directory exists before trying to watch it
                if not self.path.exists():
                    # If directory doesn't exist, fall back to polling
                    self._using_watchdog = False
                else:
                    self._event_handler = _WatchdogEventHandler(self.callback, self.monitor_deletes)
                    self._event_handler.set_event_loop(asyncio.get_running_loop())
                    
                    self._observer = Observer()
                    self._observer.schedule(self._event_handler, str(self.path), recursive=False)
                    self._observer.start()
            except (OSError, FileNotFoundError):
                # If Watchdog fails for any reason, fall back to polling
                self._using_watchdog = False
                if self._observer:
                    try:
                        self._observer.stop()
                        self._observer.join()
                    except:
                        pass
                    self._observer = None
                self._event_handler = None
        
        if not self._using_watchdog:
            # Fall back to polling
            self._stop_event = asyncio.Event()

            # Initialize previous contents if not set
            if self.previous_contents is None:
                self.previous_contents = self._get_directory_contents()

            # Start the background polling task
            self._task = asyncio.create_task(self._poll_directory())

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager and stop monitoring"""
        if self._using_watchdog:
            # Stop Watchdog observer
            if self._observer:
                self._observer.stop()
                self._observer.join()  # Wait for the observer thread to finish
                self._observer = None
            self._event_handler = None
        else:
            # Stop polling
            if self._stop_event:
                self._stop_event.set()

            if self._task:
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

            self._task = None
            self._stop_event = None
        
        self._using_watchdog = False

    @property
    def is_using_watchdog(self) -> bool:
        """Return True if currently using Watchdog, False if using polling"""
        return self._using_watchdog

    def _get_directory_contents(self) -> Set[str]:
        """Get the current contents of the directory"""
        try:
            if not self.path.exists():
                return set()

            if not self.path.is_dir():
                return set()

            return set(os.listdir(self.path))
        except (OSError, PermissionError):
            return set()

    async def _poll_directory(self):
        """Background task that polls the directory for changes"""
        while not self._stop_event.is_set():
            try:
                current_contents = self._get_directory_contents()

                # Check for changes based on monitor_deletes flag
                if self.previous_contents is not None:
                    if self.monitor_deletes:
                        # Monitor any changes (additions or deletions)
                        if current_contents != self.previous_contents:
                            # Update previous contents before calling callback
                            self.previous_contents = current_contents
                            # Await the callback function
                            await self.callback()
                    else:
                        # Only monitor new files (files present now that weren't before)
                        new_files = current_contents - self.previous_contents
                        if new_files:
                            # Update previous contents before calling callback
                            self.previous_contents = current_contents
                            # Await the callback function
                            await self.callback()
                else:
                    # First time - just update previous contents without calling callback
                    self.previous_contents = current_contents

                # Wait for the polling frequency or until stop is signaled
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self.polling_frequency
                    )
                    # If we reach here, stop was signaled
                    break
                except asyncio.TimeoutError:
                    # Timeout is expected - continue polling
                    continue

            except Exception as e:
                # Log the error but continue polling
                # In a real application, you might want to use proper logging
                print(f"Error during directory polling: {e}")

                # Wait before retrying
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self.polling_frequency
                    )
                    break
                except asyncio.TimeoutError:
                    continue
