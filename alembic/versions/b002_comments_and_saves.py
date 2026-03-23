"""add comments and saved_detections tables

Revision ID: b002
Revises: b001
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = 'b002'
down_revision = 'b001'


def upgrade():
    op.create_table(
        'comments',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('detection_id', UUID(as_uuid=True), sa.ForeignKey('detections.id'), nullable=False),
        sa.Column('text', sa.Text(), nullable=False),
        sa.Column('author', sa.String(100), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )

    op.create_table(
        'saved_detections',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('detection_id', UUID(as_uuid=True), sa.ForeignKey('detections.id'), nullable=False),
        sa.Column('label', sa.String(255), nullable=True),
        sa.Column('color', sa.String(7), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )


def downgrade():
    op.drop_table('saved_detections')
    op.drop_table('comments')
