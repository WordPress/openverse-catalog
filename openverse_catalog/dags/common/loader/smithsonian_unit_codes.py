"""
This program helps identify smithsonian unit codes which are not yet added to
the smithsonian sub-provider dictionary
"""

import logging

import requests
from common.loader import provider_details as prov
from common.slack import send_alert
from providers.provider_api_scripts import smithsonian


logger = logging.getLogger(__name__)

API_KEY = smithsonian.API_KEY
UNITS_ENDPOINT = smithsonian.UNITS_ENDPOINT
PARAMS = {"api_key": API_KEY, "q": "online_media_type:Images"}
SUB_PROVIDERS = prov.SMITHSONIAN_SUB_PROVIDERS


def get_new_and_outdated_unit_codes(unit_code_set, sub_prov_dict=SUB_PROVIDERS):
    sub_provider_unit_code_set = set()

    for sub_prov, unit_code_sub_set in sub_prov_dict.items():
        sub_provider_unit_code_set = sub_provider_unit_code_set.union(unit_code_sub_set)

    new_unit_codes = unit_code_set - sub_provider_unit_code_set
    outdated_unit_codes = sub_provider_unit_code_set - unit_code_set

    return new_unit_codes, outdated_unit_codes


def alert_unit_codes_from_api(
    units_endpoint=UNITS_ENDPOINT,
    query_params=PARAMS,
):
    """
    Alert in Slack if human intervention is needed to update the
    SMITHSONIAN_SUB_PROVIDERS dictionary
    """
    response = requests.get(units_endpoint, params=query_params)
    unit_code_set = set(response.json().get("response", {}).get("terms", []))
    new_unit_codes, outdated_unit_codes = get_new_and_outdated_unit_codes(unit_code_set)

    if bool(new_unit_codes) or bool(outdated_unit_codes):
        message = "\n*Updates needed to the SMITHSONIAN_SUB_PROVIDERS dictionary*:\n\n"

        if bool(new_unit_codes):
            codes_string = "\n".join(f"  - `{code}`" for code in new_unit_codes)
            message += "New unit codes must be added:\n"
            message += codes_string
            message += "\n"

        if bool(outdated_unit_codes):
            codes_string = "\n".join(f"  - `{code}`" for code in outdated_unit_codes)
            message += "Outdated unit codes must be deleted:\n"
            message += codes_string

        logger.info(message)
        send_alert(message, username="Smithsonian Unit Codes Check")
