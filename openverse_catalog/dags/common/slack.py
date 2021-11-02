"""
# Slack Block Message Builder

TODO:
    - track number of characters, raise error after 4k
    - attach text, fields

## Usage

This class is intended to be used with a channel-specific slack webhook.
More information can be found here: https://app.slack.com/block-kit-builder.

### Send multiple messages - payload is reset after sending

    slack = SlackMessage(username="Multi-message Test")

    slack.add_text("message 1")
    slack.send()

    slack.add_text("message 2")
    slack.send()

### Embed images, plus context

    slack = SlackMessage(username="Blocks - Referenced Images")

    slack.add_context(":pika-love: context stuff *here*")

    msg = "Example message with new method of embedding images and divider below."

    slack.add_text(msg)
    slack.add_divider()
    slack.add_image(url1, title=img1_title, alt_text="img #1")
    slack.add_image(url2, title=img2_title, alt_text="img #2")
    slack.send()

### Attached images - displayed below inline text

    slack = SlackMessage(channel, username="Blocks - Attached Images")

    slack.add_text("Example of attaching images to a section.")
    slack.attach_image(url1, title=img1_title, fallback="img #1")
    slack.attach_image(url2, title=img2_title, fallback="img #2")
    slack.send()


## Dev Tools
>>> # prints current payload
>>> slack.display()

>>> # get payload dict
>>> payload = slack.payload

"""

import json
from os.path import basename
from time import perf_counter, sleep
from typing import Any, Optional

from airflow.providers.http.hooks.http import HttpHook
from requests import Response


SLACK_CONN_ID = "slack"
JsonDict = dict[str, Any]


def _text_section(msg: str, plain_text: bool) -> JsonDict:
    text_type = "plain_text" if plain_text else "mrkdwn"
    return {"type": "section", "text": {"type": text_type, "text": msg}}


def _image_section(
    url: str, title: Optional[str] = None, alt_text: Optional[str] = None
) -> JsonDict:
    img = {"type": "image", "image_url": url}
    if title:
        img.update({"title": {"type": "plain_text", "text": title}})
    if alt_text:
        img["alt_text"] = alt_text
    else:
        img["alt_text"] = basename(url)
    return img


class SlackMessage:
    """Slack Block Message Builder"""

    def __init__(
        self,
        username: str = "Airflow",
        icon_emoji: str = ":airflow:",
        unfurl_links: bool = True,
        unfurl_media: bool = True,
        rate_limit: bool = True,
        http_conn_id: str = SLACK_CONN_ID,
    ):

        self._time_sent = 0
        self.rate_limit = rate_limit
        self.http = HttpHook(method="POST", http_conn_id=http_conn_id)
        self.blocks = []
        self._context = {}
        self._payload: dict[str, Any] = {
            "username": username,
            "unfurl_links": unfurl_links,
            "unfurl_media": unfurl_media,
        }

        if icon_emoji:
            self._payload["icon_emoji"] = icon_emoji

        self.__base_payload = self._payload.copy()

    def clear(self) -> None:
        """Clear all stored data to prime the instance for a new message."""
        self.blocks = []
        self._context = {}
        self._payload = self.__base_payload.copy()

    def display(self) -> None:
        """Prints current payload, intended for local development only."""
        if self._context:
            self._append_context()
        self._payload.update({"blocks": self.blocks})
        print(json.dumps(self._payload, indent=4))

    @property
    def payload(self) -> JsonDict:
        payload = self._payload.copy()
        payload.update({"blocks": self.blocks})
        return payload

    def _set_payload(self, payload: JsonDict, blocks: list[JsonDict] = None) -> None:
        """For debug and testing. Use carefully."""

        self._payload = payload
        self.blocks = blocks or []

    ####################################################################################
    # Context
    ####################################################################################

    def _append_context(self) -> None:
        self.blocks.append(self._context.copy())
        self._context = None

    def add_context(self, msg: str, plain_text: bool = False) -> None:
        """Display context above or below a text block"""

        if not self._context:
            self._context = {"type": "context", "elements": []}

        text = _text_section(msg, plain_text)
        if len(self._context["elements"]) < 10:
            self._context["elements"].append(text["text"])
        else:
            raise ValueError("Unable to include more than 10 context elements")

    def add_context_image(self, url: str, alt_text: Optional[str] = None) -> None:
        """Display context image inline within a text block"""

        if not self._context:
            self._context = {"type": "context", "elements": []}

        img = _image_section(url, alt_text=alt_text)
        self._context["elements"].append(img)

    ####################################################################################
    # Blocks
    ####################################################################################

    def _add_block(self, block: JsonDict) -> None:
        if self._context:
            self._append_context()
        self.blocks.append(block)

    def add_divider(self) -> None:
        """Add a divider between blocks."""
        self._add_block({"type": "divider"})

    def add_text(self, msg: str, plain_text: bool = False) -> None:
        """Add a text block, using markdown or plain text."""
        self._add_block(_text_section(msg, plain_text))

    def add_image(
        self, url, title: Optional[str] = None, alt_text: Optional[str] = None
    ) -> None:
        """Add an image block, with optional title and alt text."""
        self._add_block(_image_section(url, title, alt_text))

    ####################################################################################
    # Attachments
    ####################################################################################

    def attach_image(
        self, url: str, title: Optional[str] = None, fallback: Optional[str] = None
    ) -> None:
        """Appends image to message attachments (legacy)"""

        img = {"image_url": url}

        if title:
            img["title"] = title
        if fallback:
            img["fallback"] = fallback
        if "attachments" in self._payload:
            self._payload["attachments"].append(img)
        else:
            self._payload["attachments"] = [img]

    # SEND -----------------------------------------------------------------------------

    def _send_payload(self, data: str) -> Response:

        if self.rate_limit:
            if self._time_sent:
                delta = perf_counter() - self._time_sent
                if delta < 1:
                    sleep(1 - delta)

            self._time_sent = perf_counter()

        response = self.http.run(
            endpoint=None,
            data=data,
            headers={"Content-type": "application/json"},
            extra_options={"verify": True},
        )

        return response

    def send(self, notification_text: str = "Airflow notification") -> Response:
        """
        Sends message payload to the channel configured by the webhook.

        Any notification text provided will only show up as the content within
        the notification pushed to various devices.
        """

        if self._context:
            self._append_context()
        self._payload.update({"blocks": self.blocks})
        self._payload["text"] = notification_text

        response = self._send_payload(json.dumps(self._payload))

        self.clear()
        return response


def send_message(
    text: str,
    username: str = "Airflow",
    icon_emoji: str = ":airflow:",
    markdown: bool = True,
    http_conn_id: str = SLACK_CONN_ID,
) -> None:
    """Send a simple slack message, convenience message for short/simple messages."""
    s = SlackMessage(username, icon_emoji, http_conn_id=http_conn_id)
    s.add_text(text, plain_text=not markdown)
    s.send(text)
