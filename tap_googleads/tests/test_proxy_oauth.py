"""Tests the tap using a mock proxy oauth config."""

import unittest

import responses
import singer_sdk._singerlib as singer
import singer_sdk.io_base as io

import tap_googleads.tests.utils as test_utils
from tap_googleads.tap import TapGoogleAds


class TestTapGoogleadsWithProxyOAuthCredentials(unittest.TestCase):
    """Test class for tap-googleads using proxy refresh credentials"""

    def setUp(self):
        self.mock_config = {
            "oauth_credentials": {
                "refresh_proxy_url": "http://localhost:8080/api/tokens/oauth2-google/token",
                "refresh_proxy_url_auth": "Bearer proxy_url_token",
                "scope": "https://www.googleapis.com/auth/adwords",
                "authorization_url": "https://oauth2.googleapis.com/token",
            },
            "customer_id": "1234",
            "developer_token": "1234",
        }
        responses.reset()
        del test_utils.SINGER_MESSAGES[:]
        io.singer_write_message = test_utils.accumulate_singer_messages

    def test_proxy_oauth_discovery(self):
        """Test basic discover sync with proxy refresh credentials"""

        catalog = TapGoogleAds(config=self.mock_config).discover_streams()

        # Assert the correct number of default streams found
        self.assertEqual(len(catalog), 11, "Total streams from default catalog")

    @responses.activate
    def test_proxy_oauth_refresh(self):
        """Test proxy oauth refresh"""

        tap = test_utils.set_up_tap_with_custom_catalog(
            self.mock_config, ["stream_accessible_customers"]
        )

        responses.add(
            responses.POST,
            "http://localhost:8080/api/tokens/oauth2-google/token",
            json={"access_token": "refresh_token_updated", "expires_in": 3622},
            status=200,
        )

        responses.add(
            responses.GET,
            "https://googleads.googleapis.com/v14/customers:listAccessibleCustomers",
            json=test_utils.accessible_customer_return_data,
            status=200,
        )

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
        self.assertEqual(len(test_utils.SINGER_MESSAGES), 14)
        self.assertIsInstance(test_utils.SINGER_MESSAGES[0], singer.StateMessage)
        self.assertIsInstance(test_utils.SINGER_MESSAGES[1], singer.SchemaMessage)
        self.assertIsInstance(test_utils.SINGER_MESSAGES[2], singer.RecordMessage)

        for msg in test_utils.SINGER_MESSAGES[3:]:
            self.assertIsInstance(msg, singer.StateMessage)

