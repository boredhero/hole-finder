"""add new state enums for NC, MD, MA, LA, CA

Revision ID: b003
Revises: b002
"""
from alembic import op

revision = 'b003'
down_revision = 'b002'


def upgrade():
    # PostgreSQL ALTER TYPE ADD VALUE cannot run inside a transaction
    op.execute("ALTER TYPE datasourceenum ADD VALUE IF NOT EXISTS 'nc'")
    op.execute("ALTER TYPE datasourceenum ADD VALUE IF NOT EXISTS 'md'")

    op.execute("ALTER TYPE featuretype ADD VALUE IF NOT EXISTS 'lava_tube'")
    op.execute("ALTER TYPE featuretype ADD VALUE IF NOT EXISTS 'salt_dome_collapse'")

    op.execute("ALTER TYPE groundtruthsource ADD VALUE IF NOT EXISTS 'nc_cave_survey'")
    op.execute("ALTER TYPE groundtruthsource ADD VALUE IF NOT EXISTS 'md_karst_survey'")
    op.execute("ALTER TYPE groundtruthsource ADD VALUE IF NOT EXISTS 'ma_usgs_mines'")
    op.execute("ALTER TYPE groundtruthsource ADD VALUE IF NOT EXISTS 'la_subsidence'")
    op.execute("ALTER TYPE groundtruthsource ADD VALUE IF NOT EXISTS 'ca_blm_aml'")


def downgrade():
    # PostgreSQL does not support removing enum values
    pass
