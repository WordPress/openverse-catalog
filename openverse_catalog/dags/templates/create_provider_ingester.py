"""
Script used to generate a templated ProviderDataIngester.
"""

import argparse
from pathlib import Path

import inflection


TEMPLATES_PATH = Path(__file__).parent
REPO_PATH = TEMPLATES_PATH.parents[2]
PROJECT_PATH = REPO_PATH.parent
MEDIA_TYPES = ["audio", "image"]


def _render_provider_configuration(provider: str, media_type: str):
    """
    Render the provider configuration string for a particular media type.
    """
    return f'"{media_type}": prov.{provider}_{media_type.upper()}_PROVIDER,'


def _get_filled_template(
    template_path: Path, provider: str, endpoint: str, media_types: list[str]
):
    with template_path.open("r", encoding="utf-8") as template:
        camel_provider = inflection.camelize(provider)
        screaming_snake_provider = inflection.underscore(provider).upper()

        # Build provider configuration
        provider_configuration = "\n        ".join(
            _render_provider_configuration(screaming_snake_provider, media_type)
            for media_type in media_types
        )

        template_string = template.read()
        script_string = (
            template_string.replace("{provider}", camel_provider)
            .replace("{screaming_snake_provider}", screaming_snake_provider)
            .replace("{provider_underscore}", inflection.underscore(provider))
            .replace("{provider_data_ingester}", f"{camel_provider}DataIngester")
            .replace("{endpoint}", endpoint)
            .replace("{provider_configuration}", provider_configuration)
        )

        return script_string


def _render_file(
    target: Path,
    template_path: Path,
    provider: str,
    endpoint: str,
    media_types: list[str],
    name: str,
):
    with target.open("w", encoding="utf-8") as target_file:
        filled_template = _get_filled_template(
            template_path, provider, endpoint, media_types
        )
        target_file.write(filled_template)
        print(f"{name + ':':<18} {target.relative_to(PROJECT_PATH)}")


def fill_template(provider, endpoint, media_types):
    print(f"Creating files in {REPO_PATH}")
    provider = provider.replace(" ", "_")

    dags_path = TEMPLATES_PATH.parent / "providers"
    api_path = dags_path / "provider_api_scripts"
    filename = inflection.underscore(provider)

    # Render the API file itself
    script_template_path = TEMPLATES_PATH / "template_provider.py_template"
    api_script_path = api_path / f"{filename}.py"
    _render_file(
        api_script_path,
        script_template_path,
        provider,
        endpoint,
        media_types,
        "API script",
    )

    # Render the tests
    script_template_path = TEMPLATES_PATH / "template_test.py_template"
    tests_path = REPO_PATH / "tests"
    # Mirror the directory structure, but under the "tests" top level directory
    test_script_path = tests_path.joinpath(*api_path.parts[-3:]) / f"test_{filename}.py"
    _render_file(
        test_script_path,
        script_template_path,
        provider,
        endpoint,
        media_types,
        "API script test",
    )

    print(
        """
NOTE: You will also need to add a new ProviderWorkflow dataclass configuration to the \
PROVIDER_WORKFLOWS list in `openverse-catalog/dags/providers/provider_workflows.py`.
"""
    )


def main():
    parser = argparse.ArgumentParser(
        description="Create a new provider API ProviderDataIngester",
        add_help=True,
    )
    parser.add_argument(
        "provider", help='Create the ingester for this provider (eg. "Cleveland").'
    )
    parser.add_argument(
        "-e",
        "--endpoint",
        required=True,
        help="API endpoint to fetch data from"
        '(eg. "https://commons.wikimedia.org/w/api.php").',
    )
    parser.add_argument(
        "-m",
        "--media",
        type=str,
        nargs="*",
        help="Ingester will collect media of these types"
        " ('audio'/'image'). Default value is ['image',]",
    )
    args = parser.parse_args()
    provider = args.provider
    endpoint = args.endpoint

    # Get valid media types
    media_types = []
    for media_type in args.media:
        if media_type in MEDIA_TYPES:
            media_types.append(media_type)
        else:
            print(f"Ignoring invalid type {media_type}")

    # Default to image if no valid media types given
    if not media_types:
        print('No media type given, defaulting to ["image",]')
        media_types = [
            "image",
        ]

    fill_template(provider, endpoint, media_types)


if __name__ == "__main__":
    main()
