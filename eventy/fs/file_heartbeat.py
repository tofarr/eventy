import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import UUID

_LOGGER = logging.getLogger(__name__)


@dataclass
class FileHeartbeat:
    """
    File-based heartbeat system for tracking worker liveness in a cluster.
    
    Each worker creates and periodically updates a heartbeat file containing
    its worker ID, timestamp, and metadata. Other workers can scan these files
    to discover active workers and detect dead workers based on stale timestamps.
    """
    
    worker_id: UUID
    workers_dir: Path
    heartbeat_timeout: int = field(default=300)  # 5 minutes in seconds
    heartbeat_interval: int = field(default=60)  # 1 minute in seconds
    enable_cleanup: bool = field(default=True)
    
    # Internal state
    _running: bool = field(default=False, init=False)
    _heartbeat_task: Optional[asyncio.Task] = field(default=None, init=False)
    
    def __post_init__(self):
        """Initialize the workers directory"""
        self.workers_dir = Path(self.workers_dir)
        self.workers_dir.mkdir(parents=True, exist_ok=True)
    
    async def start(self):
        """Start the heartbeat system"""
        if self._running:
            _LOGGER.warning(f"Heartbeat for worker {self.worker_id} is already running")
            return
        
        self._running = True
        
        # Create initial heartbeat file
        await self._update_heartbeat()
        
        # Start periodic heartbeat updates
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        _LOGGER.info(f"Started heartbeat for worker {self.worker_id}")
    
    async def stop(self):
        """Stop the heartbeat system and clean up"""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel the heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        
        # Remove our heartbeat file
        heartbeat_file = self.workers_dir / str(self.worker_id)
        try:
            heartbeat_file.unlink(missing_ok=True)
            _LOGGER.info(f"Removed heartbeat file for worker {self.worker_id}")
        except OSError as e:
            _LOGGER.warning(f"Failed to remove heartbeat file for worker {self.worker_id}: {e}")
        
        _LOGGER.info(f"Stopped heartbeat for worker {self.worker_id}")
    
    async def get_active_worker_ids(self) -> set[UUID]:
        """
        Get the IDs of all workers that have recent heartbeat files.
        
        Returns:
            set[UUID]: Set of worker IDs that are considered active
        """
        # Update our own heartbeat first
        if self._running:
            await self._update_heartbeat()
        
        worker_ids = set()
        current_time = datetime.now()
        
        if not self.workers_dir.exists():
            # If no workers directory exists, only return our own ID if we're running
            return {self.worker_id} if self._running else set()
        
        for heartbeat_file in self.workers_dir.iterdir():
            try:
                worker_id = UUID(heartbeat_file.name)
                
                # Read heartbeat data to get timestamp
                heartbeat_data = self._read_heartbeat_file(heartbeat_file)
                if heartbeat_data is None:
                    continue
                
                # Check if heartbeat is recent enough
                heartbeat_time = datetime.fromisoformat(heartbeat_data['timestamp'])
                age_seconds = (current_time - heartbeat_time).total_seconds()
                
                if age_seconds <= self.heartbeat_timeout:
                    worker_ids.add(worker_id)
                    _LOGGER.debug(f"Found active worker {worker_id} (age: {age_seconds:.1f}s)")
                else:
                    _LOGGER.debug(f"Found stale worker {worker_id} (age: {age_seconds:.1f}s)")
                    if self.enable_cleanup:
                        try:
                            heartbeat_file.unlink(missing_ok=True)
                            _LOGGER.debug(f"Cleaned up stale heartbeat file for worker {worker_id}")
                        except OSError as e:
                            _LOGGER.warning(f"Failed to clean up stale heartbeat file for worker {worker_id}: {e}")
                            
            except (ValueError, OSError) as e:
                _LOGGER.warning(f"Failed to process heartbeat file {heartbeat_file}: {e}")
                # Optionally clean up malformed files
                if self.enable_cleanup:
                    try:
                        heartbeat_file.unlink(missing_ok=True)
                        _LOGGER.debug(f"Cleaned up malformed heartbeat file {heartbeat_file}")
                    except OSError:
                        pass
        
        # Always include our own worker ID if we're running
        if self._running:
            worker_ids.add(self.worker_id)
        
        _LOGGER.debug(f"Found {len(worker_ids)} active workers: {worker_ids}")
        return worker_ids
    
    async def _heartbeat_loop(self):
        """Background task that periodically updates the heartbeat file"""
        try:
            while self._running:
                await asyncio.sleep(self.heartbeat_interval)
                if self._running:  # Check again in case we were stopped during sleep
                    await self._update_heartbeat()
        except asyncio.CancelledError:
            _LOGGER.debug(f"Heartbeat loop cancelled for worker {self.worker_id}")
            raise
        except Exception as e:
            _LOGGER.error(f"Heartbeat loop failed for worker {self.worker_id}: {e}", exc_info=True)
            raise
    
    async def _update_heartbeat(self):
        """Update this worker's heartbeat file with current timestamp and metadata"""
        heartbeat_file = self.workers_dir / str(self.worker_id)
        
        heartbeat_data = {
            'worker_id': str(self.worker_id),
            'timestamp': datetime.now().isoformat(),
            'pid': os.getpid(),
            'hostname': self._get_hostname(),
        }
        
        try:
            # Write atomically by writing to a temp file and then renaming
            temp_file = heartbeat_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(heartbeat_data, f, indent=2)
            
            temp_file.replace(heartbeat_file)
            _LOGGER.debug(f"Updated heartbeat for worker {self.worker_id}")
            
        except OSError as e:
            _LOGGER.error(f"Failed to update heartbeat for worker {self.worker_id}: {e}")
            raise
    
    def _read_heartbeat_file(self, heartbeat_file: Path) -> Optional[dict]:
        """
        Read and parse a heartbeat file.
        
        Args:
            heartbeat_file: Path to the heartbeat file
            
        Returns:
            dict: Parsed heartbeat data, or None if file is invalid
        """
        try:
            with open(heartbeat_file, 'r') as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            _LOGGER.warning(f"Failed to read heartbeat file {heartbeat_file}: {e}")
            return None
    
    def _get_hostname(self) -> str:
        """Get the hostname of the current machine"""
        try:
            if hasattr(os, 'uname'):
                return os.uname().nodename
            else:
                # Fallback for Windows
                import socket
                return socket.gethostname()
        except Exception:
            return 'unknown'
    
    async def cleanup_stale_heartbeats(self):
        """
        Manually clean up all stale heartbeat files.
        
        This can be called periodically or on-demand to clean up heartbeat files
        from workers that have died without proper cleanup.
        """
        if not self.workers_dir.exists():
            return
        
        current_time = datetime.now()
        cleaned_count = 0
        
        for heartbeat_file in self.workers_dir.iterdir():
            try:
                worker_id = UUID(heartbeat_file.name)
                
                # Skip our own heartbeat file
                if worker_id == self.worker_id:
                    continue
                
                heartbeat_data = self._read_heartbeat_file(heartbeat_file)
                if heartbeat_data is None:
                    # Malformed file, clean it up
                    heartbeat_file.unlink(missing_ok=True)
                    cleaned_count += 1
                    continue
                
                # Check if heartbeat is stale
                heartbeat_time = datetime.fromisoformat(heartbeat_data['timestamp'])
                age_seconds = (current_time - heartbeat_time).total_seconds()
                
                if age_seconds > self.heartbeat_timeout:
                    heartbeat_file.unlink(missing_ok=True)
                    cleaned_count += 1
                    _LOGGER.info(f"Cleaned up stale heartbeat for worker {worker_id} (age: {age_seconds:.1f}s)")
                    
            except (ValueError, OSError) as e:
                _LOGGER.warning(f"Failed to process heartbeat file {heartbeat_file} during cleanup: {e}")
                # Try to clean up the problematic file
                try:
                    heartbeat_file.unlink(missing_ok=True)
                    cleaned_count += 1
                except OSError:
                    pass
        
        if cleaned_count > 0:
            _LOGGER.info(f"Cleaned up {cleaned_count} stale heartbeat files")