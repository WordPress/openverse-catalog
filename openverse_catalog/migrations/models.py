import sqlalchemy as sa
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_materialized_view import PGMaterializedView
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


########################################################################################
# Images
########################################################################################


class Image(Base):
    __tablename__ = "image"
    __table_args__ = (
        sa.Index(
            "image_provider_fid_idx",
            "provider",
            sa.func.md5("foreign_identifier"),
            unique=True,
        ),
    )

    identifier = sa.Column(
        UUID, primary_key=True, server_default=sa.text("uuid_generate_v4()")
    )
    created_on = sa.Column(sa.DateTime(timezone=True), nullable=False)
    updated_on = sa.Column(sa.DateTime(timezone=True), nullable=False)
    ingestion_type = sa.Column(sa.String(80))
    provider = sa.Column(sa.String(80))
    source = sa.Column(sa.String(80))
    foreign_identifier = sa.Column(sa.String(3000))
    foreign_landing_url = sa.Column(sa.String(1000))
    url = sa.Column(sa.String(3000), nullable=False)
    thumbnail = sa.Column(sa.String(3000))
    width = sa.Column(sa.Integer)
    height = sa.Column(sa.Integer)
    filesize = sa.Column(sa.Integer)
    license = sa.Column(sa.String(50), nullable=False)
    license_version = sa.Column(sa.String(25))
    creator = sa.Column(sa.String(2000))
    creator_url = sa.Column(sa.String(2000))
    title = sa.Column(sa.String(5000))
    meta_data = sa.Column(JSONB)
    tags = sa.Column(JSONB)
    watermarked = sa.Column(sa.Boolean)
    last_synced_with_source = sa.Column(sa.DateTime(timezone=True))
    removed_from_source = sa.Column(sa.Boolean, nullable=False)
    filetype = sa.Column(sa.String(5))
    category = sa.Column(sa.String(80))


class ImagePopularityMetric(Base):
    __tablename__ = "image_popularity_metrics"

    provider = sa.Column(sa.String(80), primary_key=True)
    metric = sa.Column(sa.String(80))
    percentile = sa.Column(sa.Float)


ImagePopularityPercentileFunction = PGFunction(
    schema="public",
    signature="image_popularity_percentile(provider text, pop_field text, percentile float)",  # noqa
    definition="""
RETURNS FLOAT AS $$
  SELECT percentile_disc($3) WITHIN GROUP (
    ORDER BY (meta_data->>$2)::float
  )
  FROM image WHERE provider=$1;
$$
LANGUAGE SQL
STABLE
RETURNS NULL ON NULL INPUT;
""",
)

# TODO: Add unique index!!
ImagePopularityConstantsMatView = PGMaterializedView(
    schema="public",
    signature="image_popularity_constants",
    definition="""
WITH popularity_metric_values AS (
    SELECT
    *,
    image_popularity_percentile(provider, metric, percentile) AS val
    FROM image_popularity_metrics
)
SELECT *, ((1 - percentile) / percentile) * val AS constant
FROM popularity_metric_values;
""",
)

StandardizedImagePopularityFunction = PGFunction(
    schema="public",
    signature="standardized_image_popularity(provider text, meta_data jsonb)",
    definition="""
RETURNS FLOAT AS $$
  SELECT ($2->>metric)::FLOAT / (($2->>metric)::FLOAT + constant)
  FROM image_popularity_constants WHERE provider=$1;
$$
LANGUAGE SQL
STABLE
RETURNS NULL ON NULL INPUT;
""",
)

# TODO: Add unique index!!
ImageViewMatView = PGMaterializedView(
    schema="public",
    signature="image_view",
    definition="""
SELECT
identifier,
created_on,
updated_on,
ingestion_type,
provider,
source,
foreign_identifier,
foreign_landing_url,
url,
thumbnail,
width,
height,
filesize,
license,
license_version,
creator,
creator_url,
title,
meta_data,
tags,
watermarked,
last_synced_with_source,
removed_from_source,
filetype,
category,
standardized_image_popularity(
  image.provider, image.meta_data
) AS standardized_popularity
FROM image;
""",
)


########################################################################################
# Audio
########################################################################################


class Audio(Base):
    __tablename__ = "audio"
    __table_args__ = (
        sa.Index(
            "audio_provider_fid_idx",
            "provider",
            sa.func.md5("foreign_identifier"),
            unique=True,
        ),
    )

    identifier = sa.Column(
        UUID, primary_key=True, server_default=sa.text("uuid_generate_v4()")
    )
    created_on = sa.Column(sa.DateTime(timezone=True), nullable=False)
    updated_on = sa.Column(sa.DateTime(timezone=True), nullable=False)
    ingestion_type = sa.Column(sa.String(80))
    provider = sa.Column(sa.String(80))
    source = sa.Column(sa.String(80))
    foreign_identifier = sa.Column(sa.String(3000))
    foreign_landing_url = sa.Column(sa.String(1000))
    url = sa.Column(sa.String(3000), nullable=False)
    thumbnail = sa.Column(sa.String(3000))
    filetype = sa.Column(sa.String(5))
    duration = sa.Column(sa.Integer)
    bit_rate = sa.Column(sa.Integer)
    sample_rate = sa.Column(sa.Integer)
    category = sa.Column(sa.String(80))
    genres = sa.Column(sa.ARRAY(sa.String(length=80)))
    audio_set = sa.Column(JSONB)
    set_position = sa.Column(sa.Integer)
    alt_files = sa.Column(JSONB)
    filesize = sa.Column(sa.Integer)
    license = sa.Column(sa.String(50), nullable=False)
    license_version = sa.Column(sa.String(25))
    creator = sa.Column(sa.String(2000))
    creator_url = sa.Column(sa.String(2000))
    title = sa.Column(sa.String(5000))
    meta_data = sa.Column(JSONB)
    tags = sa.Column(JSONB)
    watermarked = sa.Column(sa.Boolean)
    last_synced_with_source = sa.Column(sa.DateTime(timezone=True))
    removed_from_source = sa.Column(sa.Boolean, nullable=False)


class AudioPopularityMetric(Base):
    __tablename__ = "audio_popularity_metrics"

    provider = sa.Column(sa.String(80), primary_key=True)
    metric = sa.Column(sa.String(80))
    percentile = sa.Column(sa.Float)


#
#
# t_audio_popularity_constants = sa.Table(
#     'audio_popularity_constants', metadata,
#     sa.Column('provider', sa.String(80), unique=True),
#     sa.Column('metric', sa.String(80)),
#     sa.Column('percentile', sa.Float(53)),
#     sa.Column('val', sa.Float(53)),
#     sa.Column('constant', sa.Float(53))
# )
#
#
# t_audio_view = sa.Table(
#     'audio_view', metadata,
#     sa.Column('identifier', UUID, unique=True),
#     sa.Column('created_on', sa.DateTime(True)),
#     sa.Column('updated_on', sa.DateTime(True)),
#     sa.Column('ingestion_type', sa.String(80)),
#     sa.Column('provider', sa.String(80)),
#     sa.Column('source', sa.String(80)),
#     sa.Column('foreign_identifier', sa.String(3000)),
#     sa.Column('foreign_landing_url', sa.String(1000)),
#     sa.Column('url', sa.String(3000)),
#     sa.Column('thumbnail', sa.String(3000)),
#     sa.Column('filetype', sa.String(5)),
#     sa.Column('duration', sa.Integer),
#     sa.Column('bit_rate', sa.Integer),
#     sa.Column('sample_rate', sa.Integer),
#     sa.Column('category', sa.String(80)),
#     sa.Column('genres', sa.ARRAY(sa.String(length=80))),
#     sa.Column('audio_set', JSONB(astext_type=sa.Text())),
#     sa.Column('alt_files', JSONB(astext_type=sa.Text())),
#     sa.Column('filesize', sa.Integer),
#     sa.Column('license', sa.String(50)),
#     sa.Column('license_version', sa.String(25)),
#     sa.Column('creator', sa.String(2000)),
#     sa.Column('creator_url', sa.String(2000)),
#     sa.Column('title', sa.String(5000)),
#     sa.Column('meta_data', JSONB(astext_type=sa.Text())),
#     sa.Column('tags', JSONB(astext_type=sa.Text())),
#     sa.Column('watermarked', sa.Boolean),
#     sa.Column('last_synced_with_source', sa.DateTime(True)),
#     sa.Column('removed_from_source', sa.Boolean),
#     sa.Column('standardized_popularity', sa.Float(53))
# )
#
#
# t_image_popularity_constants = sa.Table(
#     'image_popularity_constants', metadata,
#     sa.Column('provider', sa.String(80), unique=True),
#     sa.Column('metric', sa.String(80)),
#     sa.Column('percentile', sa.Float(53)),
#     sa.Column('val', sa.Float(53)),
#     sa.Column('constant', sa.Float(53))
# )
#
#
# t_image_view = sa.Table(
#     'image_view', metadata,
#     sa.Column('identifier', UUID, unique=True),
#     sa.Column('created_on', sa.DateTime(True)),
#     sa.Column('updated_on', sa.DateTime(True)),
#     sa.Column('ingestion_type', sa.String(80)),
#     sa.Column('provider', sa.String(80)),
#     sa.Column('source', sa.String(80)),
#     sa.Column('foreign_identifier', sa.String(3000)),
#     sa.Column('foreign_landing_url', sa.String(1000)),
#     sa.Column('url', sa.String(3000)),
#     sa.Column('thumbnail', sa.String(3000)),
#     sa.Column('width', sa.Integer),
#     sa.Column('height', sa.Integer),
#     sa.Column('filesize', sa.Integer),
#     sa.Column('license', sa.String(50)),
#     sa.Column('license_version', sa.String(25)),
#     sa.Column('creator', sa.String(2000)),
#     sa.Column('creator_url', sa.String(2000)),
#     sa.Column('title', sa.String(5000)),
#     sa.Column('meta_data', JSONB(astext_type=sa.Text())),
#     sa.Column('tags', JSONB(astext_type=sa.Text())),
#     sa.Column('watermarked', sa.Boolean),
#     sa.Column('last_synced_with_source', sa.DateTime(True)),
#     sa.Column('removed_from_source', sa.Boolean),
#     sa.Column('filetype', sa.String(5)),
#     sa.Column('category', sa.String(80)),
#     sa.Column('standardized_popularity', sa.Float(53))
# )
