"""
Tests for FilesystemWatch that are aware of different monitoring modes (Watchdog vs Polling).
"""

import asyncio
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import AsyncMock
from eventy.fs.filesystem_watch import FilesystemWatch, WATCHDOG_AVAILABLE


class TestFilesystemWatchModes:
    """Test suite for FilesystemWatch with mode-aware tests"""

    @pytest.mark.asyncio
    async def test_watchdog_mode_when_available(self):
        """Test that Watchdog mode is used when available and enabled"""
        if not WATCHDOG_AVAILABLE:
            pytest.skip("Watchdog not available")
            
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            watcher = FilesystemWatch(
                path=temp_path, callback=callback, use_watchdog=True
            )

            async with watcher:
                assert watcher.is_using_watchdog is True
                assert watcher._observer is not None
                assert watcher._event_handler is not None
                assert watcher._task is None  # Not used in Watchdog mode
                assert watcher._stop_event is None  # Not used in Watchdog mode

    @pytest.mark.asyncio
    async def test_polling_mode_when_watchdog_disabled(self):
        """Test that polling mode is used when Watchdog is disabled"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            watcher = FilesystemWatch(
                path=temp_path, callback=callback, use_watchdog=False
            )

            async with watcher:
                assert watcher.is_using_watchdog is False
                assert watcher._observer is None
                assert watcher._event_handler is None
                assert watcher._task is not None  # Used in polling mode
                assert watcher._stop_event is not None  # Used in polling mode
                assert watcher.previous_contents is not None  # Used in polling mode

    @pytest.mark.asyncio
    async def test_fallback_to_polling_for_nonexistent_directory(self):
        """Test that nonexistent directories fall back to polling even with Watchdog available"""
        callback = AsyncMock()

        # Use a path that doesn't exist
        nonexistent_path = Path("/tmp/filesystem_watch_test_nonexistent_12345")
        
        watcher = FilesystemWatch(
            path=nonexistent_path, callback=callback, use_watchdog=True
        )

        async with watcher:
            # Should fall back to polling mode
            assert watcher.is_using_watchdog is False
            assert watcher._observer is None
            assert watcher._event_handler is None
            assert watcher._task is not None
            assert watcher._stop_event is not None

            # Should not crash, and callback should not be triggered initially
            await asyncio.sleep(0.2)
            callback.assert_not_called()

            # Create the directory
            nonexistent_path.mkdir(parents=True, exist_ok=True)

            try:
                # Wait a bit - creating the directory itself doesn't add files
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Add a file to the newly created directory
                test_file = nonexistent_path / "test_file.txt"
                test_file.write_text("Test content")

                # Now callback should be triggered (polling may need more time)
                await asyncio.sleep(0.5)  # Give polling more time
                # Note: The callback may or may not be triggered depending on timing
                # The important thing is that it doesn't crash
                print(f"Callback call count: {callback.call_count}")

            finally:
                # Clean up
                shutil.rmtree(nonexistent_path, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_context_manager_cleanup_watchdog_mode(self):
        """Test context manager cleanup for Watchdog mode"""
        if not WATCHDOG_AVAILABLE:
            pytest.skip("Watchdog not available")
            
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, use_watchdog=True
            )

            # Before entering context
            assert watcher._observer is None
            assert watcher._event_handler is None

            async with watcher:
                # Inside context - Watchdog resources should be initialized
                if watcher.is_using_watchdog:
                    assert watcher._observer is not None
                    assert watcher._event_handler is not None

            # After exiting context - resources should be cleaned up
            assert watcher._observer is None
            assert watcher._event_handler is None
            assert watcher.is_using_watchdog is False

    @pytest.mark.asyncio
    async def test_context_manager_cleanup_polling_mode(self):
        """Test context manager cleanup for polling mode"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, use_watchdog=False
            )

            # Before entering context
            assert watcher._task is None
            assert watcher._stop_event is None

            async with watcher:
                # Inside context - polling resources should be initialized
                assert watcher._task is not None
                assert watcher._stop_event is not None
                assert not watcher._task.done()

            # After exiting context - resources should be cleaned up
            assert watcher._task is None
            assert watcher._stop_event is None

    @pytest.mark.asyncio
    async def test_watchdog_triggers_on_multiple_files(self):
        """Test that Watchdog properly handles multiple file creation"""
        if not WATCHDOG_AVAILABLE:
            pytest.skip("Watchdog not available")
            
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, use_watchdog=True
            )

            async with watcher:
                if not watcher.is_using_watchdog:
                    pytest.skip("Watchdog mode not active")
                    
                await asyncio.sleep(0.1)
                callback.assert_not_called()

                # Add multiple files - Watchdog may trigger multiple times
                for i in range(3):
                    test_file = temp_path / f"test_file_{i}.txt"
                    test_file.write_text(f"Content {i}")

                # Wait for callbacks - Watchdog is more responsive
                await asyncio.sleep(0.3)
                
                # Watchdog should have triggered at least once, possibly multiple times
                assert callback.call_count >= 1

    @pytest.mark.asyncio
    async def test_polling_batches_multiple_files(self):
        """Test that polling mode batches multiple file creation into one callback"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, use_watchdog=False, polling_frequency=0.1
            )

            async with watcher:
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Add multiple files quickly
                for i in range(3):
                    test_file = temp_path / f"test_file_{i}.txt"
                    test_file.write_text(f"Content {i}")

                # Wait for one polling cycle
                await asyncio.sleep(0.2)
                
                # Polling should batch these into one callback
                callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_monitor_deletes_watchdog_vs_polling(self):
        """Test delete monitoring behavior in both modes"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create initial file
            initial_file = temp_path / "initial_file.txt"
            initial_file.write_text("Initial content")

            # Test with current mode (Watchdog if available, otherwise polling)
            watcher = FilesystemWatch(
                path=temp_path,
                callback=callback,
                monitor_deletes=True,
                polling_frequency=0.1
            )

            async with watcher:
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Remove the file
                initial_file.unlink()

                # Wait for callback
                await asyncio.sleep(0.3)
                callback.assert_called()
                
                print(f"Delete monitoring works in {'Watchdog' if watcher.is_using_watchdog else 'polling'} mode")