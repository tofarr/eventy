import asyncio
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Awaitable, Optional, Set


@dataclass
class FilesystemWatch:
    """
    A filesystem watcher that monitors a directory for changes and triggers a callback.

    This class is designed to be used as an async context manager. When entered,
    it starts a background task that polls the directory at the specified frequency
    and compares the current file list with the previous one. By default, the callback
    is only triggered when new files are added. If monitor_deletes is True, the callback
    is triggered on any directory change (additions or deletions).
    """

    path: Path
    """The directory path to monitor"""

    callback: Callable[[], Awaitable[None]]
    """Async callback function to execute when changes are detected"""

    previous_contents: Optional[Set[str]] = field(default=None)
    """Set containing the previous directory contents"""

    polling_frequency: float = 1.0
    """Polling frequency in seconds (default: 1.0 second)"""

    monitor_deletes: bool = False
    """If True, callback is triggered on any directory change (additions or deletions).
    If False, callback is only triggered when new files are added (default: False)"""

    _task: Optional[asyncio.Task] = field(default=None, init=False)
    """Background polling task"""

    _stop_event: Optional[asyncio.Event] = field(default=None, init=False)
    """Event to signal the polling task to stop"""

    def __post_init__(self):
        """Initialize the path as a Path object if it's a string"""
        if isinstance(self.path, str):
            self.path = Path(self.path)

    async def __aenter__(self):
        """Enter the async context manager and start monitoring"""
        self._stop_event = asyncio.Event()

        # Initialize previous contents if not set
        if self.previous_contents is None:
            self.previous_contents = self._get_directory_contents()

        # Start the background polling task
        self._task = asyncio.create_task(self._poll_directory())

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager and stop monitoring"""
        if self._stop_event:
            self._stop_event.set()

        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        self._task = None
        self._stop_event = None

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
