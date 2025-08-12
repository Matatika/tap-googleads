"""Tests the tap using a mock base credentials config."""

import unittest

import responses
import singer_sdk._singerlib as singer
from singer_sdk.exceptions import ConfigValidationError

import tap_googleads.tests.utils as test_utils
from tap_googleads.tap import TapGoogleAds


class TestTapGoogleadsWithBaseCredentials(unittest.TestCase):
    """Test class for tap-googleads using base credentials"""

    def setUp(self):
        self.mock_config = {
            "oauth_credentials": {
                "client_id": "1234",
                "client_secret": "1234",
                "refresh_token": "1234",
            },
            "customer_id": "1234567890",
            "developer_token": "1234",
        }
        responses.reset()
        del test_utils.SINGER_MESSAGES[:]

        TapGoogleAds.write_message = test_utils.accumulate_singer_messages

    def test_base_credentials_discovery(self):
        """Test basic discover sync with Bearer Token"""

        catalog = TapGoogleAds(config=self.mock_config).discover_streams()

        # expect valid catalog to be discovered
        self.assertEqual(len(catalog), 11, "Total streams from default catalog")

    @responses.activate
    def test_googleads_sync_accessible_customers(self):
        """Test sync."""

        tap = test_utils.set_up_tap_with_custom_catalog(
            self.mock_config, ["stream_accessible_customers"]
        )

        responses.add(
            responses.POST,
            "https://www.googleapis.com/oauth2/v4/token?refresh_token=1234&client_id=1234"
            + "&client_secret=1234&grant_type=refresh_token",
            json={"access_token": 12341234, "expires_in": 3622},
            status=200,
        )

        responses.add(
            responses.GET,
            "https://googleads.googleapis.com/v20/customers:listAccessibleCustomers",
            json=test_utils.accessible_customer_return_data,
            status=200,
        )

        tap.sync_all()

        self.assertEqual(len(test_utils.SINGER_MESSAGES), 15)
        self.assertIsInstance(test_utils.SINGER_MESSAGES[0], singer.StateMessage)
        self.assertIsInstance(test_utils.SINGER_MESSAGES[1], singer.SchemaMessage)
        self.assertIsInstance(test_utils.SINGER_MESSAGES[2], singer.RecordMessage)

        for msg in test_utils.SINGER_MESSAGES[3:]:
            self.assertIsInstance(msg, singer.StateMessage)

    def test_valid_customer_id_config(self):
        non_hypenated_customer_id = "1234567890"

        try:
            self._tap_with_customer_id_config(non_hypenated_customer_id)
        except ConfigValidationError:
            self.fail("Expected customer ID to be correctly formatted")

        hypenated_customer_id = "123-456-7890"

        try:
            self._tap_with_customer_id_config(hypenated_customer_id)
        except ConfigValidationError:
            self.fail("Expected customer ID to be correctly formatted")

    def test_invalid_customer_id_config(self):
        def customer_resource_name():
            return self._tap_with_customer_id_config("customers/1234567890")

        self.assertRaises(ConfigValidationError, customer_resource_name)

        def non_numeric_customer_id():
            return self._tap_with_customer_id_config("abcdefghij")

        self.assertRaises(ConfigValidationError, non_numeric_customer_id)

        def underscored_customer_id():
            return self._tap_with_customer_id_config("123_456_789")

        self.assertRaises(ConfigValidationError, underscored_customer_id)

    def _tap_with_customer_id_config(self, customer_id: str):
        customer_id_config = {
            "customer_ids": [customer_id],
            "customer_id": customer_id,
            "login_customer_id": customer_id,
        }

        return TapGoogleAds(config={**self.mock_config, **customer_id_config})