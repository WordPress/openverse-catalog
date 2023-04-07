foreign_id_description = """The unique ID of the media item from the provider."""

landing_url_description = """The URL of the media item on the provider's website."""

direct_url_description = """### Description
The direct URL of the media file.

### API

### Frontend

"""

thumbnail_url_description = """### Description
The URL of the media item's thumbnail image.

### API
Thumbnail field is not used directly in the API response. The `/thumbnail` endpoint
uses the `thumbnail` if it is of an acceptable size, or the direct `url` field, to
get the thumbnail from `photon` server. When adding a new provider, we need to
update the `photon` Openverse provider settings if the url contains query parameters.

### Frontend
The frontend does not use the thumbnail from the Catalog directly, and, instead,
uses the `thumbnail` created by the API. This image is used in the search results
page, and before the main image on the single result page is loaded.
"""

filesize_description = """### Description
The size of the main media file in bytes.

### API

### Frontend

"""

license_description = """### Description
The license of the media item, e.g. "cc-by" for the CC Attribution license.

### API

### Frontend

"""

license_version_description = """### Description
The version of the license of the media item.

### API

### Frontend

"""

creator_description = """### Description
The name of the creator of the media item.

### API

### Frontend
"""

creator_url_description = """### Description
The URL of the creator's website.

### API

### Frontend

"""

title_description = """### Description
The title of the media item.

### API

### Frontend
"""

meta_data_description = """### Description
The metadata of the media item.

### API

### Frontend
"""

tags_description = """### Description
The tags of the media item.

### API

### Frontend
"""

watermarked_description = """### Description
Whether the media item is watermarked.

### API

### Frontend
"""

last_synced_description = """### Description
The date and time when the media item was last synced with the provider's website.

### API

### Frontend
"""


foreign_identifier_description = """### Description
The unique ID of the media item from the provider.

### API

### Frontend
"""

width_description = """### Description
The width of the media item in pixels.

### API

### Frontend
"""

height_description = """### Description
The height of the media item in pixels.

### API

### Frontend
"""

duration_description = """### Description
The duration of the media item in seconds.

### API

### Frontend
"""

bit_rate_description = """### Description
The bit rate of the media item in kilobits per second.

### API

### Frontend
"""

sample_rate_description = """### Description
The sample rate of the media item in kilohertz.

### API

### Frontend
"""

format_description = """### Description
The format of the media item."""

provider_description = """### Description
The provider of the media item."""

source_description = """### Description
The source of the media item."""

ingestion_type_description = """### Description
The ingestion type of the media item."""

category_description = """### Description
The category of the media item."""

genres_description = """### Description
The genre of the media item."""

audio_set_description = """### Description
The audio set of the media item."""

set_position_description = """### Description
The position of the media item in the audio set."""

alt_files_description = """### Description
The alternative files of the media item."""

created_on_description = """### Description
The date and time when the media item was created."""

updated_on_description = """### Description
The date and time when the media item was last updated."""

removed_description = """### Description
Whether the media item has been removed."""

filetype_description = """### Description
The filetype of the media item."""
