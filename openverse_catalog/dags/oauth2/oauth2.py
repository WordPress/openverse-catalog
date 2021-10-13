import logging
from typing import Any, Collection, NamedTuple

from airflow.models import Variable
from requests_oauthlib import OAuth2Session


log = logging.getLogger(__name__)


class OauthProvider(NamedTuple):
    name: str
    auth_url: str
    refresh_url: str
    # Note: As of now these are specific to Freesound's flow. It appears that other


OAUTH2_TOKEN_KEY = "OAUTH2_ACCESS_TOKENS"
OAUTH2_AUTH_KEY = "OAUTH2_AUTH_KEYS"
OAUTH2_PROVIDERS_KEY = "OAUTH2_PROVIDER_SECRETS"
OAUTH_PROVIDERS = (
    OauthProvider(
        name="freesound",
        auth_url="https://freesound.org/apiv2/oauth2/access_token/",
        refresh_url="https://freesound.org/apiv2/oauth2/access_token/",
    ),
)


def _var_get(key: str) -> dict[str, Any]:
    """Helper function for Variable retrieval with dict default."""
    return Variable.get(key, default_var={}, deserialize_json=True)


def _update_tokens(
    provider_name: str,
    tokens: dict[str, str],
) -> None:
    log.info(f"Updating tokens for provider: {provider_name}")
    current_tokens = _var_get(OAUTH2_TOKEN_KEY)
    current_tokens[provider_name] = {
        "access_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
    }
    Variable.set(OAUTH2_TOKEN_KEY, current_tokens, serialize_json=True)


def _get_provider_secrets(
    name: str, provider_secrets: dict[str, dict] = None
) -> dict[str, str]:
    if provider_secrets is None:
        provider_secrets = _var_get(OAUTH2_PROVIDERS_KEY)
    secrets = provider_secrets.get(name)
    if secrets is None or "client_id" not in secrets:
        raise ValueError(
            f"Authorization requested for provider {name} but no secrets "
            f"were provided! Add secrets to the {OAUTH2_PROVIDERS_KEY} Variable and"
            f" ensure the provider has a client_id."
        )
    return secrets


def get_oauth_client(provider_name: str) -> OAuth2Session:
    secrets = _get_provider_secrets(provider_name)
    tokens = _var_get(OAUTH2_TOKEN_KEY)
    if provider_name not in tokens:
        raise KeyError(f"Access token not found for provider {provider_name}")
    return OAuth2Session(
        client_id=secrets["client_id"],
        token={**tokens[provider_name], "token_type": "Bearer"},
    )


def authorize_providers(providers: Collection[OauthProvider] = OAUTH_PROVIDERS) -> None:
    provider_secrets = _var_get(OAUTH2_PROVIDERS_KEY)
    auth_tokens = _var_get(OAUTH2_AUTH_KEY)
    for provider in providers:
        # Only authorize if a token was provided
        if provider.name not in auth_tokens:
            continue
        auth_token = auth_tokens[provider.name]
        log.info(f"Attempting to authorize provider: {provider.name}")
        secrets = _get_provider_secrets(provider.name, provider_secrets)
        client = OAuth2Session(secrets["client_id"])
        # NOTE: It's possible that the secrets being stored might not all be needed
        # here, and may in fact be rejected. We won't know until we add more providers.
        tokens = client.fetch_token(provider.auth_url, code=auth_token, **secrets)
        _update_tokens(provider.name, tokens)
        # Remove the auth token since it is no longer needed nor accurate
        auth_tokens.pop(provider.name)
        Variable.set(OAUTH2_AUTH_KEY, auth_tokens, serialize_json=True)


def refresh(provider: OauthProvider) -> None:
    current_tokens = _var_get(OAUTH2_TOKEN_KEY)
    if provider.name not in current_tokens:
        raise KeyError(
            f"Provider {provider.name} had no stored tokens, it may need to be "
            f"authorized first."
        )
    refresh_token = current_tokens[provider.name]["refresh_token"]
    secrets = _get_provider_secrets(provider.name)
    client = OAuth2Session(secrets["client_id"])
    log.info(f"Attempting token refresh for provider: {provider.name}")
    # NOTE: Same as above, **secrets may be too much info for some requests.
    new_tokens = client.refresh_token(
        provider.refresh_url, refresh_token=refresh_token, **secrets
    )
    _update_tokens(provider.name, new_tokens)
