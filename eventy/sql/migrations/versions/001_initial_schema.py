"""Initial schema for eventy SQL backend

Revision ID: 001
Revises: 
Create Date: 2025-01-11 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.types import TypeDecorator, CHAR
from uuid import UUID


class GUID(TypeDecorator):
    """Platform-independent GUID type."""
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


# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create eventy_events table
    op.create_table(
        'eventy_events',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('payload_type', sa.String(255), nullable=False),
        sa.Column('payload_data', sa.LargeBinary(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_eventy_events_payload_type', 'eventy_events', ['payload_type'])

    # Create eventy_results table
    op.create_table(
        'eventy_results',
        sa.Column('id', GUID(), nullable=False),
        sa.Column('worker_id', GUID(), nullable=False),
        sa.Column('event_id', sa.Integer(), nullable=False),
        sa.Column('success', sa.Boolean(), nullable=False),
        sa.Column('details', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_eventy_results_worker_id', 'eventy_results', ['worker_id'])
    op.create_index('ix_eventy_results_event_id', 'eventy_results', ['event_id'])

    # Create eventy_subscribers table
    op.create_table(
        'eventy_subscribers',
        sa.Column('id', GUID(), nullable=False),
        sa.Column('payload_type', sa.String(255), nullable=False),
        sa.Column('subscriber_data', sa.LargeBinary(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_eventy_subscribers_payload_type', 'eventy_subscribers', ['payload_type'])

    # Create eventy_claims table
    op.create_table(
        'eventy_claims',
        sa.Column('id', sa.String(255), nullable=False),
        sa.Column('worker_id', GUID(), nullable=False),
        sa.Column('payload_type', sa.String(255), nullable=False),
        sa.Column('data', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_eventy_claims_worker_id', 'eventy_claims', ['worker_id'])
    op.create_index('ix_eventy_claims_payload_type', 'eventy_claims', ['payload_type'])


def downgrade() -> None:
    op.drop_table('eventy_claims')
    op.drop_table('eventy_subscribers')
    op.drop_table('eventy_results')
    op.drop_table('eventy_events')