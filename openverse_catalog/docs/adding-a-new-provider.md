# How to Add a New Provider to Openverse

The Openverse Catalog collects data from the APIs of sites that share openly-licensed media,and saves them in our Catalog database. We call the scripts that pull data from these APIs "Provider API scripts". You can find examples in [`provider_api_scripts` folder](../dags/providers/provider_api_scripts).

To add a new Provider to Openverse, you'll need to do the following:

1. **Create a new ProviderDataIngester class.** Each provider should implement a subclass of the [`ProviderDataIngester` base class](../dags/providers/provider_api_scripts/provider_data_ingester.py), which is responsible for actually pulling the data from the API and committing it locally. Because much of this logic is implemented in the base class, all you need to do is fill out a handful of abstract methods. We provide a script
which can be used to generate the files you'll need and get you started.

    You'll need the `name` of your provider, the API `endpoint` to fetch data from, and the `media_types`
    for which you expect to pull data (for example "image" or "audio").
    ```
    > python3 openverse_catalog/dags/templates/create_provider_data_ingester.py "Foo Museum" -e "https://foo-museum.org/api/v1/" -m image audio
    ```

    You should see output similar to this:
    ```
    Creating files in /Users/staci/projects/openverse-projects/openverse-catalog
    API script:        openverse-catalog/openverse_catalog/dags/providers/provider_api_scripts/foo_museum.py
    API script test:   openverse-catalog/tests/dags/providers/provider_api_scripts/test_foo_museum.py

    NOTE: You will also need to add a new ProviderWorkflow dataclass configuration to the PROVIDER_WORKFLOWS list in `openverse-catalog/dags/providers/provider_workflows.py`.
    ```

    Complete the TODOs detailed in the generated files. For more detailed instructions and tips, read the documentation in the [section below](#implementing-a-providerdataingester).
2. **Add a `provider` string to the [common.loader.provider_details](../dags/common/loader/provider_details.py) module.** This is the string that will be used to populate the `provider` column in the database for records ingested by your provider.
    ```
    FOO_MUSEUM_IMAGE_PROVIDER = "foo_museum"
    ```
3. **Define a `ProviderWorkflow` configuration in order to generate a DAG.** We use Airflow DAGs (link to Airflow docs) to automate data ingestion. All you need to do to create a DAG for your provider is to define a `ProviderWorkflow` dataclass and add it to the `PROVIDER_WORKFLOWS` list in [`provider_workflows.py`](../dags/providers/provider_workflows.py). Our DAG factories will pick up the configuration and generate a new DAG in Airflow!

    At minimum, you'll need to provide the following in your configuration:
    * `provider_script`: the name of the file where you defined your `ProviderDataIngester` class
    * `ingestion_callable`: the `ProviderDataIngester` class
    * `media_types`: the media types your provider handles
    ```
    ProviderWorkflow(
        provider_script='foo_museum',
        ingestion_callable=FooMuseumDataIngester,
        media_types=("image", "audio",)

    )
    ```
    There are many other options that allow you to tweak the `schedule` (when and how often your DAG is run), timeouts for individual steps of the DAG, and more. See the documentation for details. <TODO: add docs for other options.>


You should now have a fully functioning provider DAG. <TODO: add and link to docs for how to run provider DAGs locally, preferably with images.> *NOTE*: when your code is merged, the DAG will become available in production but will be disabled by default. A contributor with Airflow access will need to manually turn the DAG on in production.

# Implementing a ProviderDataIngester

Implementing a ProviderDataIngester class is the bulk of the work for adding a new Provider to Openverse. Luckily, it's pretty simple! You'll need to subclass [`ProviderDataIngester`](../dags/providers/provider_api_scripts/provider_data_ingester.py), which takes care of much of the work for you, including all of the logic for actually making requests and iterating over batches. At a high level, the `ProviderDataIngester` iteratively:

* Builds the next set of `query_parameters`
* Makes a `GET` request to the configured API endpoint, and receives a batch of data
* Iterates over each item in the batch and extracts just the data we want to store
* Commits these records to an appropriate `MediaStore`

From there, the `load` steps of the generated provider DAG handle loading the records into the actual Catalog database.

The base class also automatically adds support for some extra features that may be useful for testing and debugging. <TODO: Link to documentation for features like ingestion_limit, skip_ingestion_errors, initial_query_params>

In the simplest case, all you need to do is implement the abstact methods on your child class. For more advanced options see [below](#advanced-options).

## Define Class variables

### `providers`

This is a dictionary mapping each media type your provider supports to their corresponding `provider` string (the string that will populate the `provider` field in the DB). These strings
should be defined as constants in [common.loader.provider_details.py](../dags/common/loader/provider_details.py).

By convention, when a provider supports multiple media types we set separate provider strings for each type. For example, `wikimedia` and `wikimedia_audio`.

```
from common.loader import provider_details as prov

providers = {
    "image": prov.FOO_MUSEUM_IMAGE_PROVIDER,
    "audio": prov.FOO_MUSEUM_AUDIO_PROVIDER
}
```

### `endpoint`

This is the main API endpoint from which batches of data will be requested. The ProviderDataIngester assumes that the same endpoint will be hit each batch, with different query params. If you have a more complicated use case, you may find help in the following sections:
* [Implementing a computed endpoint with variable path](#endpoint-2)
* [Overriding get_response_json to make a more complex request](#get_response_json)


### `batch_limit`

The number of records to retrieve in a given batch. _Default: 100_

### `delay`

Integer number of seconds to wait between consecutive requests to the `endpoint`. You may need to increase this to respect certain rate limits. _Default: 1_

### `retries`

Integer number of times to retry requests to `endpoint` on error. _Default: 3_

### `headers`

Dictionary of headers passed to `GET` requests. _Default: {}_

## Implement Required Methods

### `get_next_query_params`

Given the set of query params used in the previous request, generate the query params to be used for the next request. Commonly, this is used to increment an `offset` or `page` parameter between requests.

```
def get_next_query_params(self, prev_query_params, **kwargs):
    if not prev_query_params:
        # On the first request, `prev_query_params` is None. Return default params
        return {
            "api_key": Variable.get("API_KEY_MYPROVIDER")
            "limit": self.batch_limit,
            "skip": 0
        }
    else:
        # Update only the parameters that require changes on subsequent requests
        return {
            **prev_query_params,
            "skip": prev_query_params["skip"] + self.batch_limit,
        }
```

TIPS:
* `batch_limit` is not added automatically to the params, so if your API needs it make sure to add it yourself.
* If you need to pass an API key, set is an Airflow variable rather than hardcoding it.

### `get_media_type`

For a given record, return its media type. If your provider only returns one type of media, this may be hardcoded.

```
def get_media_type(self, record):
    # This provider only supports Images.
    return constants.IMAGE
```

### `get_record_data`

This is the bulk of the work for a `ProviderDataIngester`. This method takes the json representation of a single record from your provider API, and returns a dictionary of all the necessary data.

```
def get_record_data(self, data):
    # Return early if required fields are not present
    if (foreign_landing_url := data.get('landing_url')) is None:
        return None

    if (image_url := data.get('url')) is None:
        return None

    if (license_url := data.get('license)) is None:
        return None
    license_info = get_license_info(license_url=license_url)

    ...

    return {
        "foreign_landing_url": foreign_landing_url,
        "image_url": image_url,
        "license_info": license_info,
        ...
    }
```

TIPS:
* `get_record_data` may also return a `List` of dictionaries, if it is possible to extract multiple records from `data`. For example, this may be useful if a record contains information about associated/related records.
* Sometimes you may need to make additional API requests for individual records to acquire all the necessary data. You can use `self.get_response_json` to do so, by passing in a different `endpoint` argument.
* When adding items to the `meta_data` JSONB column, avoid storing any keys that have a value of `None` - this provides little value for Elasticsearch and bloats the space required to store the record.
* If a required field is missing, immediately return None to skip further processing of the record as it will be discarded regardless.

<TODO: Remove the following and link to more extensive documentation about DB fields when it is made available in https://github.com/WordPress/openverse-catalog/issues/783>

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

## Advanced Options

Some provider APIs may not neatly fit into this ingestion model. It's possible to further extend the `ProviderDataIngester` by overriding a few additional methods.

### __init__

The `__init__` method may be overwritten in order to declare instance variables. When doing so, it is critical to call `super` and to pass through `kwargs`. Failing to do so may break fundamental behavior of the class.

```
def __init__(self, *args, **kwargs):
    # Accept and pass through args/kwargs
    super().__init__(*args, **kwargs)

    self.my_instance_variable = None
```

### endpoint

Sometimes the `endpoint` may need to be computed, rather than declared statically. This may happen when the endpoint itself changes from one request to the next, rather than passing all required data through query params. In this case, you may implement `endpoint` as a `property`.

```
@property
def endpoint(self) -> str:
    # In this example, the endpoint path includes an instance variable that may be modified
    # by other parts of the code
    return f"{BASE_ENDPOINT}/{self.my_instance_variable}"
```

### ingest_records

This is the main ingestion function. It accepts optional `kwargs`, which it passes through on each call to `get_next_query_params`. This makes it possible to override `ingest_records` in order to iterate through a discrete set of query params for which you'd like to run ingestion. This is best demonstrated with an example:

```
CATEGORIES = ['music', 'audio_book', 'podcast']

def ingest_records(self, **kwargs):
    for category in CATEGORIES:
        super().ingest_records(category=category)

def get_next_query_params(self, prev_query_params, **kwargs):
    # Our param will be passed in to kwargs, and can be used to
    # build the query params
    category = kwargs.get("category")
    return {
        "category": category,
        ...
    }
```

### get_response_json

This is the function that actually makes the `GET` requests to your endpoint. If a single `GET` request isn't sufficient, you may override this. For example, [Wikimedia](../dags/common/providers/provider_api_scripts/wikimedia_commons.py) makes multiple requests until it finds `batchcomplete`.

### get_should_continue

Each time `get_batch` is called, this method is called with the result of the batch. If it returns `False`, ingestion is halted immediately.

This can be used to halt ingestion early based on some condition, for example the presence of a particular token in the response. By default it always returns `True`, and processing is only halted when no more data is available (or an error is encountered).
