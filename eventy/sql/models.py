"""SQLAlchemy models for event queue storage."""

from datetime import datetime
from typing import Optional
from uuid import UUID

try:
    from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, LargeBinary
    from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
    from sqlalchemy.orm import declarative_base
    from sqlalchemy.sql import func
    from sqlalchemy.types import TypeDecorator, CHAR
except ImportError as e:
    raise ImportError(
        "SQLAlchemy is required for SQL event queue. Install with: pip install eventy[sql]"
    ) from e


class GUID(TypeDecorator):
    """Platform-independent GUID type.
    
    Uses PostgreSQL's UUID type when available, otherwise uses CHAR(36).
    """
    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(PostgresUUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, UUID):
                return str(value)
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, UUID):
                return UUID(value)
            return value


Base = declarative_base()


class SqlEvent(Base):
    """SQLAlchemy model for queue events."""
    
    __tablename__ = 'eventy_events'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    payload_type = Column(String(255), nullable=False, index=True)
    payload_data = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    
    def __repr__(self):
        return f"<SqlEvent(id={self.id}, payload_type='{self.payload_type}')>"


class SqlEventResult(Base):
    """SQLAlchemy model for event results."""
    
    __tablename__ = 'eventy_results'
    
    id = Column(GUID(), primary_key=True)
    worker_id = Column(GUID(), nullable=False, index=True)
    event_id = Column(Integer, nullable=False, index=True)
    success = Column(Boolean, nullable=False)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    
    def __repr__(self):
        return f"<SqlEventResult(id={self.id}, event_id={self.event_id}, success={self.success})>"


class SqlSubscriber(Base):
    """SQLAlchemy model for subscribers."""
    
    __tablename__ = 'eventy_subscribers'
    
    id = Column(GUID(), primary_key=True)
    payload_type = Column(String(255), nullable=False, index=True)
    subscriber_data = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    
    def __repr__(self):
        return f"<SqlSubscriber(id={self.id}, payload_type='{self.payload_type}')>"


class SqlClaim(Base):
    """SQLAlchemy model for claims."""
    
    __tablename__ = 'eventy_claims'
    
    id = Column(String(255), primary_key=True)
    worker_id = Column(GUID(), nullable=False, index=True)
    payload_type = Column(String(255), nullable=False, index=True)
    data = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    
    def __repr__(self):
        return f"<SqlClaim(id='{self.id}', worker_id={self.worker_id})>"