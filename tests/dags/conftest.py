from unittest import mock

import pytest
from oauth2 import oauth2


FAKE_OAUTH_PROVIDER = "fakeprovider"


def _var_get_replacement(*args, **kwargs):
    values = {
        oauth2.OAUTH2_TOKEN_KEY: {
            FAKE_OAUTH_PROVIDER: {
                "access_token": "fakeaccess",
                "refresh_token": "fakerefresh",
            }
        },
        oauth2.OAUTH2_AUTH_KEY: {FAKE_OAUTH_PROVIDER: "fakeauthtoken"},
        oauth2.OAUTH2_PROVIDERS_KEY: {
            FAKE_OAUTH_PROVIDER: {
                "client_id": "fakeclient",
                "client_secret": "fakesecret",
            }
        },
    }
    return values[args[0]]


@pytest.fixture
def oauth_provider_vars():
    with mock.patch("oauth2.oauth2.Variable") as MockVariable:
        MockVariable.get.side_effect = _var_get_replacement
        yield
