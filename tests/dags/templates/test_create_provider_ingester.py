from pathlib import Path

import pytest

from openverse_catalog.dags.templates import create_provider_ingester


@pytest.mark.parametrize(
    "provider",
    [
        ("Foobar Industries"),
        ("FOOBAR INDUSTRIES"),
        ("Foobar_industries"),
        ("FoobarIndustries"),
        ("foobar industries"),
        ("foobar_industries"),
    ],
)
def test_files_created(provider):
    endpoint = "https://myfakeapi/v1"
    media_type = "image"
    dags_path = create_provider_ingester.TEMPLATES_PATH.parent / "providers"
    expected_provider = dags_path / "provider_api_scripts" / "foobar_industries.py"
    expected_test = (
        Path(__file__).parents[2]
        / "dags"
        / "providers"
        / "provider_api_scripts"
        / "test_foobar_industries.py"
    )
    try:
        create_provider_ingester.fill_template(provider, endpoint, media_type)
        assert expected_provider.exists()
        assert expected_test.exists()
    finally:
        # Clean up
        expected_provider.unlink(missing_ok=True)
        expected_test.unlink(missing_ok=True)
