import unittest

from tap_googleads.client import GoogleAdsStream
from tap_googleads.tap import TapGoogleAds

CONFIG = {
    "oauth_credentials": {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh_token",
    },
    "developer_token": "test_developer_token",
    "start_date": "2025-01-01",
}


class AssertingStartDateGoogleAdsStream(GoogleAdsStream):
    name = "assert_start_date"
    schema = {
        "properties": {
            "test_replication_key": {
                "type": ["string", "null"],
                "format": "date",
            }
        }
    }

    replication_key = "test_replication_key"

    def get_records(self, context):
        assert self.start_date == f"'{self.expected_start_date}'"
        return
        yield

    expected_start_date = ...


class TestStartDate(unittest.TestCase):
    def test_start_date_from_config(self):
        catalog = {
            "streams": [{"tap_stream_id": AssertingStartDateGoogleAdsStream.name}]
        }

        tap = TapGoogleAds(config=CONFIG, catalog=catalog)
        stream = AssertingStartDateGoogleAdsStream(tap=tap)

        stream.expected_start_date = CONFIG["start_date"]
        stream.sync()

    def test_start_date_from_state(self):
        catalog = {
            "streams": [{"tap_stream_id": AssertingStartDateGoogleAdsStream.name}]
        }
        state = {
            "bookmarks": {
                AssertingStartDateGoogleAdsStream.name: {
                    "replication_key": AssertingStartDateGoogleAdsStream.replication_key,
                    "replication_key_value": "2025-12-01",
                }
            }
        }

        tap = TapGoogleAds(config=CONFIG, catalog=catalog, state=state)
        stream = AssertingStartDateGoogleAdsStream(tap=tap)

        stream.expected_start_date = state["bookmarks"][stream.name][
            "replication_key_value"
        ]
        stream.sync()

    def test_start_date_from_state_per_context(self):
        catalog = {
            "streams": [{"tap_stream_id": AssertingStartDateGoogleAdsStream.name}]
        }
        state = {
            "bookmarks": {
                AssertingStartDateGoogleAdsStream.name: {
                    "partitions": [
                        {
                            "context": {
                                "customer_id": "1",
                                "parent_customer_id": "0",
                            },
                            "replication_key": AssertingStartDateGoogleAdsStream.replication_key,
                            "replication_key_value": "2025-12-01",
                        },
                        {
                            "context": {
                                "customer_id": "2",
                                "parent_customer_id": "0",
                            },
                            "replication_key": AssertingStartDateGoogleAdsStream.replication_key,
                            "replication_key_value": "2025-12-02",
                        },
                    ]
                }
            }
        }

        tap = TapGoogleAds(config=CONFIG, catalog=catalog, state=state)
        stream = AssertingStartDateGoogleAdsStream(tap=tap)
        stream_state_partitions = iter(state["bookmarks"][stream.name]["partitions"])

        for partition in stream_state_partitions:
            stream.expected_start_date = partition["replication_key_value"]
            stream.sync(partition["context"])
