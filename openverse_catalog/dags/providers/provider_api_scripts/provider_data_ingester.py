import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from airflow.models import Variable
from common.requester import DelayedRequester
from common.storage.audio import AudioStore
from common.storage.image import ImageStore
from common.storage.media import MediaStore


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class ProviderDataIngester(ABC):
    """
    An abstract base class that initializes media stores and ingests records
    from a given provider.

    Required Init Arguments:
    providers:   a dictionary whose keys are the supported `media_types`, and values are
                 the `provider` string in the `media` table of the DB for that type.
                 Used to initialize the media stores.
    endpoint:    the URL with which to request records from the API
    delay:       integer giving the minimimum number of seconds to wait between
                 consecutive requests via the `get` method
    batch_limit: integer giving the number of records to get in each batch
    retries:     integer number of times to retry the request on error

    Optional Init Arguments:
    headers: dictionary to be passed as headers to the request
    """

    def __init__(
        self,
        providers: dict[str, str],
        endpoint: str,
        delay: int,
        batch_limit: int,
        retries: int,
        headers: Optional[Dict],
    ):
        self.providers = providers
        self.endpoint = endpoint
        self.delay = delay
        self.retries = retries
        self.headers = headers

        # An airflow variable used to cap the amount of records to be ingested.
        # This can be used for testing purposes to ensure a provider script
        # processes some data but still returns quickly.
        self.limit = Variable.get("ingestion_limit", None)

        # If a test limit is imposed, ensure that the `batch_limit` does not
        # exceed this.
        self.batch_limit = (
            batch_limit if self.limit is None else min(batch_limit, int(self.limit))
        )

        # Initialize the DelayedRequester and all necessary Media Stores.
        self.delayed_requester = DelayedRequester(self.delay)
        self.media_stores = self.init_media_stores()

    def init_media_stores(self) -> dict[str, MediaStore]:
        """
        Initialize a media store for each media type supported by this
        provider.
        """
        media_stores = {}

        for media_type, provider in self.providers.items():
            media_stores[media_type] = self.create_media_store(media_type, provider)

        return media_stores

    def create_media_store(self, media_type: str, provider: str) -> MediaStore:
        """
        A factory method to create the appropriate MediaStore for the given
        media type.
        """
        if media_type == "image":
            return ImageStore(provider)
        if media_type == "audio":
            return AudioStore(provider)

        raise Exception(f"No MediaStore is configured for type: {media_type}")

    def ingest_records(self, **kwargs) -> None:
        """
        The main ingestion function that is called during the `pull_data` task.

        Optional Arguments:
        **kwargs: Optional arguments to be passed to `get_next_query_params`.
        """
        should_continue = True
        record_count = 0
        query_params = None

        logger.info(f"Begin ingestion for {self.__class__.__name__}")

        while should_continue:
            query_params = self.get_next_query_params(query_params, **kwargs)

            batch, should_continue = self.get_batch(query_params)

            if batch and len(batch) > 0:
                record_count = self.process_batch(batch)
                logger.info(f"{record_count} records ingested so far.")
            else:
                logger.info("Batch complete.")
                should_continue = False

            if self.limit and record_count >= int(self.limit):
                logger.info(f"Ingestion limit of {self.limit} has been reached.")
                should_continue = False

        self.commit_records()

    @abstractmethod
    def get_next_query_params(self, old_query_params: Optional[Dict], **kwargs) -> Dict:
        """
        Given the last set of query params, return the query params
        for the next request. Depending on the API, this may involve incrementing
        an `offset` or `page` param, for example.

        Required arguments:
        old_query_params: Dictionary of query string params used in the previous
                          request. If None, this is the first request.
        **kwargs:         Optional kwargs passed through from `ingest_records`.

        """
        pass

    def get_batch(self, query_params: Dict) -> Tuple[Optional[List], bool]:
        """
        Given query params, request the next batch of records from the API and
        return them in a list.

        Required Arguments:
        query_params: query string parameters to be used in the request

        Return:
        batch:           list of records in the batch
        should_continue: boolean indicating whether ingestion should continue after
                         this batch has been processed
        """
        batch = None
        should_continue = True

        try:
            # Get the API response
            response_json = self.get_response_json(query_params)

            # Build a list of records from the response
            batch = self.get_batch_data(response_json)

            # Optionally, apply some logic to the response to determine whether
            # ingestion should continue or if should be short-circuited. By default
            # this will return True and ingestion continues.
            should_continue = self.get_should_continue(response_json)

        except Exception as e:
            logger.error(f"Error due to {e}")

        return batch, should_continue

    def get_response_json(self, query_params: Dict):
        """
        Make the actual API requests needed to ingest a batch. This can be overridden
        in order to support APIs that require multiple requests, for example.
        """
        return self.delayed_requester.get_response_json(
            self.endpoint, self.retries, query_params, headers=self.headers
        )

    def get_should_continue(self, response_json):
        """
        This method should be overridden when an API has additional logic for
        determining whether ingestion should continue. For example, this
        can be used to check for the existence of a `continue_token` in the
        response.
        """
        return True

    def get_batch_data(self, response_json):
        """
        Takes an API response and returns the list of records. This method
        can be overridden to accomodate different response formats.
        """
        if response_json:
            return response_json.get("data")
        return None

    def process_batch(self, media_batch):
        """
        Process a batch of records by adding them to the appropriate MediaStore.
        Returns the total count of records ingested up to this point, for all
        media types.
        """
        record_count = 0

        for record in media_batch:
            record_data = self.get_record_data(record)

            if record_data is not None:
                # We need to know what type of record we're handling in
                # order to add it to the correct store
                media_type = self.get_media_type(record)

                # Get the store for that media_type
                store = self.media_stores[media_type]
                record_count = store.add_item(**record_data)

        return record_count

    @abstractmethod
    def get_media_type(self, record):
        """
        For a given record, return the media type it represents (eg "image", "audio",
        etc.) If a provider only supports a single media type, this may be hard-coded
        to return that type.
        """
        pass

    @abstractmethod
    def get_record_data(self, record):
        """
        Parse out the necessary information (license info, urls, etc) from the record.
        """
        pass

    def commit_records(self):
        for store in self.media_stores.values():
            store.commit()
