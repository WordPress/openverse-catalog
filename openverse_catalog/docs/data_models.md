TODO: This documentation is temporary and should be replaced by more thorough documentation of our DB fields in https://github.com/WordPress/openverse-catalog/issues/783>

# Data Models

The following is temporary, limited documentation of the columns for each of our Catalog data models.

*Required Fields*
| field name | description |
| --- | --- |
| *foreign_landing_url* | URL of page where the record lives on the source website. |
| *audio_url* / *image_url* | Direct link to the media file. Note that the field name differs depending on media type. |
| *license_info* | LicenseInfo object that has (1) the URL of the license for the record, (2) string representation of the license, (3) version of the license, (4) raw license URL that was by provider, if different from canonical URL |

The following fields are optional, but it is highly encouraged to populate as much data as possible:

| field name | description |
| --- | --- |
| *foreign_identifier* | Unique identifier for the record on the source site. |
| *thumbnail_url* | Direct link to a thumbnail-sized version of the record. |
| *filesize* | Size of the main file in bytes. |
| *filetype* | The filetype of the main file, eg. 'mp3', 'jpg', etc. |
| *creator* | The creator of the image. |
| *creator_url* | The user page, or home page of the creator. |
| *title* | Title of the record. |
| *meta_data* | Dictionary of metadata about the record. Currently, a key we prefer to have is `description`. |
| *raw_tags* | List of tags associated with the record. |
| *watermarked* | Boolean, true if the record has a watermark. |

#### Image-specific fields

Image also has the following fields:

| field_name | description |
| --- | --- |
| *width* | Image width in pixels. |
| *height* | Image height in pixels. |

#### Audio-specific fields

Audio has the following fields:

| field_name | description |
| --- | --- |
| *duration* | Audio duration in milliseconds. |
| *bit_rate* | Audio bit rate as int. |
| *sample_rate* | Audio sample rate as int. |
| *category* | Category such as 'music', 'sound', 'audio_book', or 'podcast'. |
| *genres* | List of genres. |
| *set_foreign_id* | Unique identifier for the audio set on the source site. |
| *audio_set* | The name of the set (album, pack, etc) the audio is part of. |
| *set_position* | Position of the audio in the audio_set. |
| *set_thumbnail* | URL of the audio_set thumbnail. |
| *set_url* | URL of the audio_set. |
| *alt_files* | A dictionary with information about alternative files for the audio (different formats/quality). Dict should have the following keys: url, filesize, bit_rate, sample_rate.
