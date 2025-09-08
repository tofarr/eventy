#!/usr/bin/env python3
"""
Example demonstrating FilesystemWatch with optional Watchdog integration.

This example shows how FilesystemWatch automatically uses Watchdog when available
for efficient filesystem monitoring, and gracefully falls back to polling when
Watchdog is not installed.
"""

import asyncio
import tempfile
from pathlib import Path
from eventy.fs.filesystem_watch import FilesystemWatch, WATCHDOG_AVAILABLE


async def file_change_callback():
    """Callback function that gets triggered when files change"""
    print("📁 File system change detected!")


async def demonstrate_watchdog_integration():
    """Demonstrate the Watchdog integration"""
    print("🔍 FilesystemWatch with Watchdog Integration Demo")
    print("=" * 50)
    print(f"Watchdog available: {WATCHDOG_AVAILABLE}")
    print()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        print(f"Monitoring directory: {temp_path}")
        
        # Create a watcher with default settings (uses Watchdog if available)
        watcher = FilesystemWatch(
            path=temp_path,
            callback=file_change_callback,
            monitor_deletes=True  # Monitor both additions and deletions
        )
        
        async with watcher:
            print(f"Using {'Watchdog' if watcher.is_using_watchdog else 'polling'} for monitoring")
            print()
            
            # Wait a moment for initialization
            await asyncio.sleep(0.1)
            
            print("Creating files...")
            
            # Create some files
            for i in range(3):
                file_path = temp_path / f"test_file_{i}.txt"
                file_path.write_text(f"Content for file {i}")
                print(f"  Created: {file_path.name}")
                await asyncio.sleep(0.2)  # Small delay to see individual events
            
            print("\nModifying files...")
            
            # Modify a file
            modify_file = temp_path / "test_file_1.txt"
            modify_file.write_text("Modified content")
            print(f"  Modified: {modify_file.name}")
            await asyncio.sleep(0.2)
            
            print("\nDeleting files...")
            
            # Delete a file
            delete_file = temp_path / "test_file_0.txt"
            delete_file.unlink()
            print(f"  Deleted: {delete_file.name}")
            await asyncio.sleep(0.2)
            
            print("\nWaiting for final events...")
            await asyncio.sleep(0.5)
            
        print("\n✅ Demo completed!")


async def demonstrate_polling_fallback():
    """Demonstrate the polling fallback by explicitly disabling Watchdog"""
    print("\n🔄 Polling Mode Demo (Watchdog disabled)")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        print(f"Monitoring directory: {temp_path}")
        
        # Create a watcher with Watchdog explicitly disabled
        watcher = FilesystemWatch(
            path=temp_path,
            callback=file_change_callback,
            use_watchdog=False,  # Force polling mode
            polling_frequency=0.5,  # Poll every 500ms
            monitor_deletes=True
        )
        
        async with watcher:
            print(f"Using {'Watchdog' if watcher.is_using_watchdog else 'polling'} for monitoring")
            print("(Polling frequency: 500ms)")
            print()
            
            # Wait a moment for initialization
            await asyncio.sleep(0.6)
            
            print("Creating files...")
            
            # Create files quickly (polling will batch them)
            for i in range(3):
                file_path = temp_path / f"batch_file_{i}.txt"
                file_path.write_text(f"Batch content {i}")
                print(f"  Created: {file_path.name}")
            
            print("\nWaiting for polling to detect changes...")
            await asyncio.sleep(1.0)  # Wait for polling cycle
            
            print("Deleting a file...")
            delete_file = temp_path / "batch_file_1.txt"
            delete_file.unlink()
            print(f"  Deleted: {delete_file.name}")
            
            print("Waiting for polling to detect deletion...")
            await asyncio.sleep(1.0)  # Wait for polling cycle
            
        print("\n✅ Polling demo completed!")


async def demonstrate_error_handling():
    """Demonstrate error handling for nonexistent directories"""
    print("\n🚫 Error Handling Demo")
    print("=" * 50)
    
    # Try to watch a nonexistent directory
    nonexistent_path = Path("/tmp/nonexistent_directory_12345")
    print(f"Attempting to watch nonexistent directory: {nonexistent_path}")
    
    watcher = FilesystemWatch(
        path=nonexistent_path,
        callback=file_change_callback,
        use_watchdog=True  # Try to use Watchdog first
    )
    
    async with watcher:
        print(f"Fallback mode: {'Watchdog' if watcher.is_using_watchdog else 'polling'}")
        print("(Should fall back to polling for nonexistent directories)")
        
        await asyncio.sleep(0.5)
        
        print("Creating the directory...")
        nonexistent_path.mkdir(parents=True, exist_ok=True)
        
        try:
            await asyncio.sleep(0.5)
            
            print("Adding a file to the newly created directory...")
            test_file = nonexistent_path / "test.txt"
            test_file.write_text("Test content")
            
            await asyncio.sleep(1.0)  # Wait for detection
            
        finally:
            # Clean up
            import shutil
            shutil.rmtree(nonexistent_path, ignore_errors=True)
    
    print("✅ Error handling demo completed!")


async def main():
    """Run all demonstrations"""
    await demonstrate_watchdog_integration()
    await demonstrate_polling_fallback()
    await demonstrate_error_handling()
    
    print("\n🎉 All demos completed successfully!")
    print("\nKey features demonstrated:")
    print("• Automatic Watchdog detection and usage")
    print("• Graceful fallback to polling when Watchdog unavailable")
    print("• Real-time file system event detection")
    print("• Error handling for edge cases")
    print("• Both file creation and deletion monitoring")


if __name__ == "__main__":
    asyncio.run(main())