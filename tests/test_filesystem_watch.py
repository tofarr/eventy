"""
Unit tests for FilesystemWatch class.

This module provides comprehensive tests for the FilesystemWatch class,
including testing the callback execution when new files are added to
a monitored directory.
"""

import asyncio
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import AsyncMock
from eventy.fs.filesystem_watch import FilesystemWatch


class TestFilesystemWatch:
    """Test suite for FilesystemWatch class"""

    @pytest.mark.asyncio
    async def test_filesystem_watch_initialization(self):
        """Test FilesystemWatch initialization with different path types"""
        callback = AsyncMock()

        # Test with Path object
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            watcher = FilesystemWatch(
                path=temp_path, callback=callback, polling_frequency=0.1
            )

            assert watcher.path == temp_path
            assert watcher.callback == callback
            assert watcher.polling_frequency == 0.1
            assert watcher.monitor_deletes is False  # Default value
            assert watcher.previous_contents is None
            assert watcher._task is None
            assert watcher._stop_event is None

        # Test with string path
        with tempfile.TemporaryDirectory() as temp_dir:
            watcher = FilesystemWatch(path=temp_dir, callback=callback)  # String path

            assert watcher.path == Path(temp_dir)
            assert isinstance(watcher.path, Path)

    @pytest.mark.asyncio
    async def test_callback_triggered_on_new_files(self):
        """Test that callback is triggered when new files are added"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path,
                callback=callback,
                polling_frequency=0.1,  # Fast polling for testing
            )

            async with watcher:
                # Initially, no callback should be triggered
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Create a new file
                test_file = temp_path / "test_file.txt"
                test_file.write_text("Hello, World!")

                # Wait for the callback to be triggered
                await asyncio.sleep(0.2)
                callback.assert_called_once()

                # Reset the mock
                callback.reset_mock()

                # Create another file
                test_file2 = temp_path / "test_file2.txt"
                test_file2.write_text("Another file")

                # Wait for the callback to be triggered again
                await asyncio.sleep(0.2)
                callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_callback_not_triggered_on_file_removal(self):
        """Test that callback is NOT triggered when files are removed"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create initial files
            test_file1 = temp_path / "existing_file1.txt"
            test_file2 = temp_path / "existing_file2.txt"
            test_file1.write_text("File 1")
            test_file2.write_text("File 2")

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, polling_frequency=0.1
            )

            async with watcher:
                # Wait a bit to ensure initial state is captured
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Remove a file
                test_file1.unlink()

                # Wait and verify callback was NOT triggered
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Remove another file
                test_file2.unlink()

                # Wait and verify callback was still NOT triggered
                await asyncio.sleep(0.2)
                callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_callback_triggered_after_file_removal_and_addition(self):
        """Test that callback is triggered when files are added after some were removed"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create initial file
            initial_file = temp_path / "initial_file.txt"
            initial_file.write_text("Initial content")

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, polling_frequency=0.1
            )

            async with watcher:
                # Wait for initial state
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Remove the initial file
                initial_file.unlink()
                await asyncio.sleep(0.2)
                callback.assert_not_called()  # Should not trigger

                # Add a new file
                new_file = temp_path / "new_file.txt"
                new_file.write_text("New content")

                # Wait for callback
                await asyncio.sleep(0.2)
                callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_files_added_simultaneously(self):
        """Test callback behavior when multiple files are added at once"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, polling_frequency=0.1
            )

            async with watcher:
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Add multiple files at once
                for i in range(3):
                    test_file = temp_path / f"test_file_{i}.txt"
                    test_file.write_text(f"Content {i}")

                # Wait for callback
                await asyncio.sleep(0.2)
                
                # Behavior depends on monitoring mode:
                # - Watchdog: Individual events for each file (3 calls)
                # - Polling: Batched into one callback (1 call)
                if watcher.is_using_watchdog:
                    # Watchdog triggers individual events
                    assert callback.call_count >= 1  # At least one call, possibly more
                else:
                    # Polling batches changes
                    callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_nonexistent_directory(self):
        """Test behavior with non-existent directory"""
        callback = AsyncMock()

        # Use a path that doesn't exist
        nonexistent_path = Path("/tmp/filesystem_watch_test_nonexistent_12345")

        watcher = FilesystemWatch(
            path=nonexistent_path, callback=callback, polling_frequency=0.1
        )

        async with watcher:
            # Should not crash, and callback should not be triggered
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

                # Now callback should be triggered
                await asyncio.sleep(0.2)
                callback.assert_called_once()

            finally:
                # Clean up
                shutil.rmtree(nonexistent_path, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_context_manager_cleanup(self):
        """Test that context manager properly cleans up resources"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, polling_frequency=0.1
            )

            # Before entering context - no resources should be initialized
            assert watcher._task is None
            assert watcher._stop_event is None
            assert watcher._observer is None
            assert watcher._event_handler is None

            async with watcher:
                # Inside context - appropriate resources should be initialized based on mode
                if watcher.is_using_watchdog:
                    # Watchdog mode uses observer
                    assert watcher._observer is not None
                    assert watcher._event_handler is not None
                    assert watcher._task is None  # Not used in Watchdog mode
                    assert watcher._stop_event is None  # Not used in Watchdog mode
                else:
                    # Polling mode uses task
                    assert watcher._task is not None
                    assert watcher._stop_event is not None
                    assert not watcher._task.done()
                    assert watcher._observer is None  # Not used in polling mode
                    assert watcher._event_handler is None  # Not used in polling mode

                # Add a file to verify it's working
                test_file = temp_path / "test_file.txt"
                test_file.write_text("Test")
                await asyncio.sleep(0.2)
                callback.assert_called()

            # After exiting context - all resources should be cleaned up
            assert watcher._task is None
            assert watcher._stop_event is None
            assert watcher._observer is None
            assert watcher._event_handler is None

    @pytest.mark.asyncio
    async def test_previous_contents_initialization(self):
        """Test that previous_contents is properly initialized"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create some initial files
            for i in range(2):
                test_file = temp_path / f"initial_file_{i}.txt"
                test_file.write_text(f"Initial content {i}")

            watcher = FilesystemWatch(
                path=temp_path, callback=callback, polling_frequency=0.1
            )

            async with watcher:
                # previous_contents behavior depends on monitoring mode
                if watcher.is_using_watchdog:
                    # Watchdog mode doesn't use previous_contents
                    assert watcher.previous_contents is None
                else:
                    # Polling mode initializes previous_contents with existing files
                    assert watcher.previous_contents is not None
                    assert len(watcher.previous_contents) == 2
                    assert "initial_file_0.txt" in watcher.previous_contents
                    assert "initial_file_1.txt" in watcher.previous_contents

                # No callback should be triggered for existing files
                await asyncio.sleep(0.2)
                callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_custom_polling_frequency(self):
        """Test that custom polling frequency is respected"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Use a longer polling frequency
            watcher = FilesystemWatch(
                path=temp_path, callback=callback, polling_frequency=0.5  # 500ms
            )

            async with watcher:
                # Add a file
                test_file = temp_path / "test_file.txt"
                test_file.write_text("Test")

                # Wait less than polling frequency - callback might not be triggered yet
                await asyncio.sleep(0.2)
                # Don't assert here as timing can be variable

                # Wait longer than polling frequency - callback should definitely be triggered
                await asyncio.sleep(0.4)
                callback.assert_called()

    @pytest.mark.asyncio
    async def test_monitor_deletes_flag_initialization(self):
        """Test that monitor_deletes flag can be set during initialization"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test with monitor_deletes=True
            watcher = FilesystemWatch(
                path=temp_path, callback=callback, monitor_deletes=True
            )

            assert watcher.monitor_deletes is True

            # Test with monitor_deletes=False (explicit)
            watcher2 = FilesystemWatch(
                path=temp_path, callback=callback, monitor_deletes=False
            )

            assert watcher2.monitor_deletes is False

    @pytest.mark.asyncio
    async def test_monitor_deletes_true_triggers_on_file_removal(self):
        """Test that callback is triggered on file removal when monitor_deletes=True"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create initial files
            test_file1 = temp_path / "existing_file1.txt"
            test_file2 = temp_path / "existing_file2.txt"
            test_file1.write_text("File 1")
            test_file2.write_text("File 2")

            watcher = FilesystemWatch(
                path=temp_path,
                callback=callback,
                polling_frequency=0.1,
                monitor_deletes=True,  # Enable deletion monitoring
            )

            async with watcher:
                # Wait a bit to ensure initial state is captured
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Remove a file - should trigger callback
                test_file1.unlink()

                # Wait and verify callback was triggered
                await asyncio.sleep(0.2)
                callback.assert_called_once()

                # Reset the mock
                callback.reset_mock()

                # Remove another file - should trigger callback again
                test_file2.unlink()

                # Wait and verify callback was triggered again
                await asyncio.sleep(0.2)
                callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_monitor_deletes_true_triggers_on_file_addition(self):
        """Test that callback is still triggered on file addition when monitor_deletes=True"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            watcher = FilesystemWatch(
                path=temp_path,
                callback=callback,
                polling_frequency=0.1,
                monitor_deletes=True,
            )

            async with watcher:
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Add a file - should trigger callback
                test_file = temp_path / "new_file.txt"
                test_file.write_text("New content")

                await asyncio.sleep(0.2)
                callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_monitor_deletes_false_does_not_trigger_on_removal(self):
        """Test that callback is NOT triggered on file removal when monitor_deletes=False (default)"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create initial files
            test_file1 = temp_path / "existing_file1.txt"
            test_file2 = temp_path / "existing_file2.txt"
            test_file1.write_text("File 1")
            test_file2.write_text("File 2")

            watcher = FilesystemWatch(
                path=temp_path,
                callback=callback,
                polling_frequency=0.1,
                monitor_deletes=False,  # Explicit False (same as default)
            )

            async with watcher:
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Remove files - should NOT trigger callback
                test_file1.unlink()
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                test_file2.unlink()
                await asyncio.sleep(0.2)
                callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_monitor_deletes_mixed_operations(self):
        """Test mixed file operations with monitor_deletes=True"""
        callback = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create initial file
            initial_file = temp_path / "initial_file.txt"
            initial_file.write_text("Initial content")

            watcher = FilesystemWatch(
                path=temp_path,
                callback=callback,
                polling_frequency=0.1,
                monitor_deletes=True,
            )

            async with watcher:
                await asyncio.sleep(0.2)
                callback.assert_not_called()

                # Add a file - should trigger callback
                new_file1 = temp_path / "new_file1.txt"
                new_file1.write_text("New content 1")
                await asyncio.sleep(0.2)
                callback.assert_called_once()
                callback.reset_mock()

                # Remove a file - should trigger callback
                initial_file.unlink()
                await asyncio.sleep(0.2)
                callback.assert_called_once()
                callback.reset_mock()

                # Add multiple files
                new_file2 = temp_path / "new_file2.txt"
                new_file3 = temp_path / "new_file3.txt"
                new_file2.write_text("New content 2")
                new_file3.write_text("New content 3")
                await asyncio.sleep(0.2)
                
                # Behavior depends on monitoring mode:
                # - Watchdog: Individual events for each file (2 calls)
                # - Polling: Batched into one callback (1 call)
                if watcher.is_using_watchdog:
                    assert callback.call_count >= 1  # At least one call, possibly more
                else:
                    callback.assert_called_once()
                callback.reset_mock()

                # Remove multiple files
                new_file1.unlink()
                new_file2.unlink()
                await asyncio.sleep(0.2)
                
                # Same behavior difference applies to deletions
                if watcher.is_using_watchdog:
                    assert callback.call_count >= 1  # At least one call, possibly more
                else:
                    callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_monitor_deletes_comparison_behavior(self):
        """Test that the same operations behave differently based on monitor_deletes flag"""
        callback_with_deletes = AsyncMock()
        callback_without_deletes = AsyncMock()

        with (
            tempfile.TemporaryDirectory() as temp_dir1,
            tempfile.TemporaryDirectory() as temp_dir2,
        ):
            temp_path1 = Path(temp_dir1)
            temp_path2 = Path(temp_dir2)

            # Create identical initial files in both directories
            for path in [temp_path1, temp_path2]:
                initial_file = path / "initial_file.txt"
                initial_file.write_text("Initial content")

            watcher_with_deletes = FilesystemWatch(
                path=temp_path1,
                callback=callback_with_deletes,
                polling_frequency=0.1,
                monitor_deletes=True,
            )

            watcher_without_deletes = FilesystemWatch(
                path=temp_path2,
                callback=callback_without_deletes,
                polling_frequency=0.1,
                monitor_deletes=False,
            )

            async with watcher_with_deletes, watcher_without_deletes:
                await asyncio.sleep(0.2)
                callback_with_deletes.assert_not_called()
                callback_without_deletes.assert_not_called()

                # Remove files from both directories
                (temp_path1 / "initial_file.txt").unlink()
                (temp_path2 / "initial_file.txt").unlink()

                await asyncio.sleep(0.2)

                # Only the watcher with monitor_deletes=True should trigger
                callback_with_deletes.assert_called_once()
                callback_without_deletes.assert_not_called()

                # Reset mocks
                callback_with_deletes.reset_mock()
                callback_without_deletes.reset_mock()

                # Add files to both directories
                (temp_path1 / "new_file.txt").write_text("New content")
                (temp_path2 / "new_file.txt").write_text("New content")

                await asyncio.sleep(0.2)

                # Both watchers should trigger for additions
                callback_with_deletes.assert_called_once()
                callback_without_deletes.assert_called_once()
