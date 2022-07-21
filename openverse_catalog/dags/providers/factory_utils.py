import inspect
import logging
from types import FunctionType
from typing import Callable, Sequence

from airflow.models import TaskInstance
from common.constants import MediaType
from common.storage.media import MediaStore


logger = logging.getLogger(__name__)


def _load_provider_script(
    ingestion_callable: Callable,
    media_types: list[MediaType],
) -> dict[str, MediaStore]:
    # Stores exist at the module level, so in order to retrieve the output values we
    # must first pull the stores from the module.
    module = inspect.getmodule(ingestion_callable)
    stores = {}
    for media_type in media_types:
        store = getattr(module, f"{media_type}_store", None)
        if not store:
            continue
        stores[media_type] = store

    if len(stores) != len(media_types):
        raise ValueError(
            f"Expected stores in {module.__name__} were missing: "
            f"{list(set(media_types) - set(stores))}"
        )
    return stores


def generate_tsv_filenames(
    ingestion_callable: Callable,
    media_types: list[MediaType],
    ti: TaskInstance,
    args: Sequence = None,
) -> None:
    args = args or []
    logger.info("Pushing available store paths to XComs")

    # TODO: This entire branch can be removed when all of the provider scripts have been
    # TODO: refactored to subclass ProviderDataIngester.
    if isinstance(ingestion_callable, FunctionType):
        stores = _load_provider_script(ingestion_callable, media_types)

    else:
        # A ProviderDataIngester class was passed instead. First we initialize the
        # class, which will initialize the media stores and DelayedRequester.
        logger.info(
            f"Initializing ProviderIngester {ingestion_callable.__name__} in"
            f"order to generate store filenames."
        )
        ingester = ingestion_callable(*args)
        stores = ingester.media_stores

    # Push the media store output paths to XComs.
    for store in stores.values():
        logger.info(
            f"{store.media_type.capitalize()} store location: {store.output_path}"
        )
        ti.xcom_push(key=f"{store.media_type}_tsv", value=store.output_path)
