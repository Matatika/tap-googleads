"""REST client handling, including GoogleAdsStream base class."""

from datetime import datetime
from functools import cached_property
from http import HTTPStatus
from typing import Any, Dict, Optional

import requests

from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream

from tap_googleads.auth import GoogleAdsAuthenticator, ProxyGoogleAdsAuthenticator


class ResumableAPIError(Exception):
    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


class GoogleAdsStream(RESTStream):
    """GoogleAds stream class."""

    url_base = "https://googleads.googleapis.com/v20"
    rest_method = "POST"
    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.nextPageToken"  # Or override `get_next_page_token`.
    _LOG_REQUEST_METRIC_URLS: bool = True

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A :class:`requests.Response` object.

        Returns:
            str: The error message
        """
        base_msg = super().response_error_message(response)
        try:
            error = response.json()["error"]
            main_message = (
                f"Error {error['code']}: {error['message']} ({error['status']})"
            )

            if "details" in error and error["details"]:
                detail = error["details"][0]
                if "errors" in detail and detail["errors"]:
                    error_detail = detail["errors"][0]
                    detailed_message = error_detail.get("message", "")
                    request_id = detail.get("requestId", "")

                    return f"{base_msg}. {main_message}\nDetails: {detailed_message}\nRequest ID: {request_id}"

            return base_msg + main_message
        except Exception:
            return base_msg

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            msg = self.response_error_message(response)
            raise ResumableAPIError(msg, response)

        super().validate_response(response)

    @cached_property
    def authenticator(self) -> OAuthAuthenticator:
        """Return a new authenticator object."""
        base_auth_url = "https://www.googleapis.com/oauth2/v4/token"
        # Silly way to do parameters but it works

        client_id = self.config.get("oauth_credentials", {}).get("client_id", None)
        client_secret = self.config.get("oauth_credentials", {}).get(
            "client_secret", None
        )
        refresh_token = self.config.get("oauth_credentials", {}).get(
            "refresh_token", None
        )

        auth_url = base_auth_url + f"?refresh_token={refresh_token}"
        auth_url = auth_url + f"&client_id={client_id}"
        auth_url = auth_url + f"&client_secret={client_secret}"
        auth_url = auth_url + "&grant_type=refresh_token"

        if client_id and client_secret and refresh_token:
            return GoogleAdsAuthenticator(stream=self, auth_endpoint=auth_url)

        oauth_credentials = self.config.get("oauth_credentials", {})

        auth_body = {}
        auth_headers = {}

        auth_body["refresh_token"] = oauth_credentials.get("refresh_token")
        auth_body["grant_type"] = "refresh_token"

        auth_headers["authorization"] = oauth_credentials.get("refresh_proxy_url_auth")
        auth_headers["Content-Type"] = "application/json"
        auth_headers["Accept"] = "application/json"

        return ProxyGoogleAdsAuthenticator(
            stream=self,
            auth_endpoint=oauth_credentials.get("refresh_proxy_url"),
            auth_body=auth_body,
            auth_headers=auth_headers,
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["developer-token"] = self.config["developer_token"]
        headers["login-customer-id"] = (
            self.login_customer_id
            or self.context
            and self.context.get("customer_id")
        )
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        # TODO: This doesn't appear to be valid params, need to look-into this further
        # if self.replication_key:
        #     params["sort"] = "asc"
        #     params["order_by"] = self.replication_key
        return params

    def get_records(self, context):
        try:
            yield from super().get_records(context)
        except ResumableAPIError as e:
            self.logger.warning(e)

    @property
    def gaql(self):
        raise NotImplementedError

    @property
    def path(self) -> str:
        # Paramas
        path = "/customers/{customer_id}/googleAds:search?query="
        return path + self.gaql

    @cached_property
    def start_date(self):
        return datetime.fromisoformat(self.config["start_date"]).strftime(r"'%Y-%m-%d'")

    @cached_property
    def end_date(self):
        return datetime.fromisoformat(self.config["end_date"]).strftime(r"'%Y-%m-%d'")

    @cached_property
    def customer_ids(self):
        customer_ids = self.config.get("customer_ids")
        customer_id = self.config.get("customer_id")

        if customer_ids is None:
            if customer_id is None:
                return
            customer_ids = [customer_id]

        return list(map(_sanitise_customer_id, customer_ids))

    @cached_property
    def login_customer_id(self):
        login_customer_id = self.config.get("login_customer_id")

        if login_customer_id is None:
            return

        return _sanitise_customer_id(login_customer_id)


def _sanitise_customer_id(customer_id: str):
    return customer_id.replace("-", "")