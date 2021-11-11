"""Initial image and audio

Revision ID: 399417106d8c
Revises:
Create Date: 2021-11-11 22:11:50.645399

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "399417106d8c"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    ####################################################################################
    # Set up database
    ####################################################################################
    op.execute(
        """
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';
"""
    )
    ####################################################################################
    # Audio tables
    ####################################################################################
    op.create_table(
        "audio",
        sa.Column(
            "identifier",
            postgresql.UUID(),
            server_default=sa.text("uuid_generate_v4()"),
            nullable=False,
        ),
        sa.Column("created_on", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_on", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingestion_type", sa.String(length=80), nullable=True),
        sa.Column("provider", sa.String(length=80), nullable=True),
        sa.Column("source", sa.String(length=80), nullable=True),
        sa.Column("foreign_identifier", sa.String(length=3000), nullable=True),
        sa.Column("foreign_landing_url", sa.String(length=1000), nullable=True),
        sa.Column("url", sa.String(length=3000), nullable=False),
        sa.Column("thumbnail", sa.String(length=3000), nullable=True),
        sa.Column("filetype", sa.String(length=5), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.Column("bit_rate", sa.Integer(), nullable=True),
        sa.Column("sample_rate", sa.Integer(), nullable=True),
        sa.Column("category", sa.String(length=80), nullable=True),
        sa.Column("genres", sa.ARRAY(sa.String(length=80)), nullable=True),
        sa.Column("audio_set", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("set_position", sa.Integer(), nullable=True),
        sa.Column("alt_files", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("filesize", sa.Integer(), nullable=True),
        sa.Column("license", sa.String(length=50), nullable=False),
        sa.Column("license_version", sa.String(length=25), nullable=True),
        sa.Column("creator", sa.String(length=2000), nullable=True),
        sa.Column("creator_url", sa.String(length=2000), nullable=True),
        sa.Column("title", sa.String(length=5000), nullable=True),
        sa.Column("meta_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("tags", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("watermarked", sa.Boolean(), nullable=True),
        sa.Column("last_synced_with_source", sa.DateTime(timezone=True), nullable=True),
        sa.Column("removed_from_source", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("identifier"),
    )
    # Add index manually
    op.create_index(
        index_name="audio_provider_fid_idx",
        table_name="audio",
        columns=[
            "provider",
            sa.func.md5("foreign_identifier"),
        ],
        unique=True,
    )
    audio_pop_metrics = op.create_table(
        "audio_popularity_metrics",
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("metric", sa.String(length=80), nullable=True),
        sa.Column("percentile", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("provider"),
    )
    # Insert initial values
    op.bulk_insert(
        audio_pop_metrics,
        [
            {
                "provider": "wikimedia_audio",
                "metric": "global_usage_count",
                "percentile": 0.85,
            },
            {
                "provider": "jamendo",
                "metric": "listens",
                "percentile": 0.85,
            },
        ],
    )
    ####################################################################################
    # Image tables
    ####################################################################################
    op.create_table(
        "image",
        sa.Column(
            "identifier",
            postgresql.UUID(),
            server_default=sa.text("uuid_generate_v4()"),
            nullable=False,
        ),
        sa.Column("created_on", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_on", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingestion_type", sa.String(length=80), nullable=True),
        sa.Column("provider", sa.String(length=80), nullable=True),
        sa.Column("source", sa.String(length=80), nullable=True),
        sa.Column("foreign_identifier", sa.String(length=3000), nullable=True),
        sa.Column("foreign_landing_url", sa.String(length=1000), nullable=True),
        sa.Column("url", sa.String(length=3000), nullable=False),
        sa.Column("thumbnail", sa.String(length=3000), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("filesize", sa.Integer(), nullable=True),
        sa.Column("license", sa.String(length=50), nullable=False),
        sa.Column("license_version", sa.String(length=25), nullable=True),
        sa.Column("creator", sa.String(length=2000), nullable=True),
        sa.Column("creator_url", sa.String(length=2000), nullable=True),
        sa.Column("title", sa.String(length=5000), nullable=True),
        sa.Column("meta_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("tags", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("watermarked", sa.Boolean(), nullable=True),
        sa.Column("last_synced_with_source", sa.DateTime(timezone=True), nullable=True),
        sa.Column("removed_from_source", sa.Boolean(), nullable=False),
        sa.Column("filetype", sa.String(length=5), nullable=True),
        sa.Column("category", sa.String(length=80), nullable=True),
        sa.PrimaryKeyConstraint("identifier"),
    )
    # Add index manually
    op.create_index(
        index_name="image_provider_fid_idx",
        table_name="image",
        columns=[
            "provider",
            sa.func.md5("foreign_identifier"),
        ],
        unique=True,
    )
    image_pop_metrics = op.create_table(
        "image_popularity_metrics",
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("metric", sa.String(length=80), nullable=True),
        sa.Column("percentile", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("provider"),
    )
    # Insert initial values
    op.bulk_insert(
        image_pop_metrics,
        [
            {
                "provider": "flickr",
                "metric": "views",
                "percentile": 0.85,
            },
            {
                "provider": "wikimedia",
                "metric": "global_usage_count",
                "percentile": 0.85,
            },
            {
                "provider": "stocksnap",
                "metric": "downloads_raw",
                "percentile": 0.85,
            },
        ],
    )


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("image_popularity_metrics")
    op.drop_table("image")
    op.drop_table("audio_popularity_metrics")
    op.drop_table("audio")
    # ### end Alembic commands ###
