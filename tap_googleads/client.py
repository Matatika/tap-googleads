"""REST client handling, including GoogleAdsStream base class."""

from datetime import datetime
from functools import cached_property
from typing import Any, Dict, Optional

from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream

from tap_googleads.auth import GoogleAdsAuthenticator, ProxyGoogleAdsAuthenticator


class GoogleAdsStream(RESTStream):
    """GoogleAds stream class."""

    url_base = "https://googleads.googleapis.com/v18"
    rest_method = "POST"
    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.nextPageToken"  # Or override `get_next_page_token`.
    _LOG_REQUEST_METRIC_URLS: bool = True

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
            self.config.get("login_customer_id")
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
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

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
        if self.config.get("customer_ids"):
            return self.config["customer_ids"]

        return [self.config["customer_id"]] if self.config.get("customer_id") else []