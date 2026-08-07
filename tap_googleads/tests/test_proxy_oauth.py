"""Tests the tap using a mock proxy oauth config."""

import contextlib
import io
import json
import unittest
from unittest import mock

import responses

import tap_googleads.tests.utils as test_utils
from tap_googleads.tap import TapGoogleAds


class TestTapGoogleadsWithProxyOAuthCredentials(unittest.TestCase):
    """Test class for tap-googleads using proxy refresh credentials"""

    def setUp(self):
        self.mock_config = {
            "oauth_credentials": {
                "refresh_proxy_url": "http://localhost:8080/api/tokens/oauth2-google/token",
                "refresh_proxy_url_auth": "Bearer proxy_url_token",
                "refresh_token": "1234",
            },
            "customer_id": "1234567890",
            "developer_token": "1234",
        }
        responses.reset()

        responses.add(
            responses.POST,
            "http://localhost:8080/api/tokens/oauth2-google/token",
            json={"access_token": "refresh_token_updated", "expires_in": 3622},
            status=200,
        )

        patcher = mock.patch(
            "tap_googleads.dynamic_query_stream.DynamicQueryStream.get_fields_metadata"
        )

        mock_get_fields_metadata = patcher.start()
        mock_get_fields_metadata.side_effect = lambda fields: {
            f: {
                "name": f,
                "dataType": "STRING",
            }
            for f in fields
        }

        self.addCleanup(patcher.stop)

    @responses.activate
    def test_proxy_oauth_discovery(self):
        """Test basic discover sync with proxy refresh credentials"""
        catalog = TapGoogleAds(config=self.mock_config).discover_streams()

        # Assert the correct number of default streams found
        self.assertEqual(len(catalog), 28, "Total streams from default catalog")

    @responses.activate
    def test_proxy_oauth_refresh(self):
        """Test proxy oauth refresh"""
        tap = test_utils.set_up_tap_with_custom_catalog(
            self.mock_config, ["accessible_customers"]
        )

        responses.add(
            responses.GET,
            "https://googleads.googleapis.com/v22/customers:listAccessibleCustomers",
            json=test_utils.accessible_customer_return_data,
            status=200,
        )

        captured_stdout = io.StringIO()
        with contextlib.redirect_stdout(captured_stdout):
            tap.sync_all()

        # Assert first oauth token call is using pre set refresh_proxy_url_auth

        oauth_refresh_request_token = responses.calls[0].request.headers[
            "Authorization"
        ]

        self.assertEqual(oauth_refresh_request_token, "Bearer proxy_url_token")

        # Assert that returned refresh token is used in the call.

        accessible_customers_request_token = responses.calls[1].request.headers[
            "Authorization"
        ]

        self.assertEqual(
            accessible_customers_request_token, "Bearer refresh_token_updated"
        )

        # Assert that messages are output from sync (its actually working).
        singer_messages = [
            json.loads(line) for line in captured_stdout.getvalue().splitlines()
        ]

        self.assertEqual(len(singer_messages), 3)
        self.assertEqual(singer_messages[0]["type"], "SCHEMA")
        self.assertEqual(singer_messages[1]["type"], "RECORD")
        self.assertEqual(singer_messages[2]["type"], "STATE")
