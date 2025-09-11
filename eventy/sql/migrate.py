"""Database migration utilities for eventy SQL backend."""

import os
import sys
from pathlib import Path

try:
    from alembic import command
    from alembic.config import Config
except ImportError as e:
    raise ImportError(
        "Alembic is required for SQL migrations. Install with: pip install eventy[sql]"
    ) from e


def get_alembic_config(database_url: str = None) -> Config:
    """Get Alembic configuration."""
    # Get the directory containing this file
    sql_dir = Path(__file__).parent
    alembic_ini_path = sql_dir / "alembic.ini"
    
    config = Config(str(alembic_ini_path))
    
    # Override database URL if provided
    if database_url:
        config.set_main_option("sqlalchemy.url", database_url)
    elif os.getenv("EVENTY_DATABASE_URL"):
        config.set_main_option("sqlalchemy.url", os.getenv("EVENTY_DATABASE_URL"))
    
    return config


def create_migration(message: str, database_url: str = None, autogenerate: bool = True):
    """Create a new migration."""
    config = get_alembic_config(database_url)
    command.revision(config, message=message, autogenerate=autogenerate)


def upgrade_database(database_url: str = None, revision: str = "head"):
    """Upgrade database to a specific revision."""
    config = get_alembic_config(database_url)
    command.upgrade(config, revision)


def downgrade_database(database_url: str = None, revision: str = "-1"):
    """Downgrade database to a specific revision."""
    config = get_alembic_config(database_url)
    command.downgrade(config, revision)


def show_current_revision(database_url: str = None):
    """Show current database revision."""
    config = get_alembic_config(database_url)
    command.current(config)


def show_migration_history(database_url: str = None):
    """Show migration history."""
    config = get_alembic_config(database_url)
    command.history(config)


def main():
    """Command-line interface for database migrations."""
    if len(sys.argv) < 2:
        print("Usage: python -m eventy.sql.migrate <command> [args...]")
        print("Commands:")
        print("  create <message>     - Create a new migration")
        print("  upgrade [revision]   - Upgrade database (default: head)")
        print("  downgrade [revision] - Downgrade database (default: -1)")
        print("  current              - Show current revision")
        print("  history              - Show migration history")
        sys.exit(1)
    
    command_name = sys.argv[1]
    database_url = os.getenv("EVENTY_DATABASE_URL")
    
    try:
        if command_name == "create":
            if len(sys.argv) < 3:
                print("Error: Migration message required")
                sys.exit(1)
            message = sys.argv[2]
            create_migration(message, database_url)
            print(f"Created migration: {message}")
            
        elif command_name == "upgrade":
            revision = sys.argv[2] if len(sys.argv) > 2 else "head"
            upgrade_database(database_url, revision)
            print(f"Upgraded database to: {revision}")
            
        elif command_name == "downgrade":
            revision = sys.argv[2] if len(sys.argv) > 2 else "-1"
            downgrade_database(database_url, revision)
            print(f"Downgraded database to: {revision}")
            
        elif command_name == "current":
            show_current_revision(database_url)
            
        elif command_name == "history":
            show_migration_history(database_url)
            
        else:
            print(f"Unknown command: {command_name}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()