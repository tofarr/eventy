"""Environment variable constants for the Eventy library.

This module centralizes all environment variable keys used throughout the Eventy library
to avoid hardcoded strings and provide better maintainability.
"""

# Queue Manager Configuration
EVENTY_QUEUE_MANAGER = "EVENTY_QUEUE_MANAGER"
"""Environment variable to override the default queue manager implementation.
Set this to a fully qualified class name to use a custom QueueManager implementation.
Default: eventy.mem.memory_queue_manager.MemoryQueueManager
"""

# Serializer Configuration  
EVENTY_SERIALIZER = "EVENTY_SERIALIZER"
"""Environment variable to override the default serializer implementation.
Set this to a fully qualified class name to use a custom Serializer implementation.
Default: eventy.serializers.pickle_serializer.PickleSerializer
"""

# Configuration
EVENTY_CONFIG = "EVENTY_CONFIG"
"""Environment variable to specify the EventyConfig implementation.
Set this to a fully qualified class name to use a custom EventyConfig implementation.
No default - this must be set to use the configuration system.
"""

# Filesystem configuration
EVENTY_ROOT_DIR = "EVENTY_ROOT_DIR"
"""Environment variable to specify the root directory for file system based implementation
"""