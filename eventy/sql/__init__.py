"""SQL-based event queue implementation using SQLAlchemy."""

try:
    from .sql_event_queue import SqlEventQueue
    from .sql_queue_manager import SqlQueueManager
    from .models import Base, SqlEvent, SqlEventResult, SqlSubscriber, SqlClaim
    from .migrate import (
        create_migration,
        upgrade_database,
        downgrade_database,
        show_current_revision,
        show_migration_history,
    )
    
    __all__ = [
        "SqlEventQueue",
        "SqlQueueManager", 
        "Base",
        "SqlEvent",
        "SqlEventResult",
        "SqlSubscriber",
        "SqlClaim",
        "create_migration",
        "upgrade_database",
        "downgrade_database",
        "show_current_revision",
        "show_migration_history",
    ]
except ImportError:
    # SQLAlchemy/Alembic not available
    __all__ = []