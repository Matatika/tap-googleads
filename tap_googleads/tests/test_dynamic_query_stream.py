import unittest

from tap_googleads.dynamic_query_stream import DynamicQueryStream
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

class StopAfterVerification(Exception):
    """Sentinel exception - stop once assertions are verified."""

class AssertingGAQLDateFilterGoogleAdsStream(DynamicQueryStream):
    name = "assert_gaql_date_filter"
    schema = {
        "properties": {
            "test_replication_key": {
                "type": ["string", "null"],
                "format": "date",
            }
        }
    }

    replication_key = "test_replication_key"

    gaql = """
        SELECT
            test_attribute,
            test_segment,
            test_metric
        FROM test_resource
    """

    add_date_filter_to_query = True

    def prepare_request_payload(self, context, next_page_token):
        request_payload = super().prepare_request_payload(context, next_page_token)

        assert "query" in request_payload
        assert self.expected_gaql_date_filter in request_payload["query"]

        raise StopAfterVerification

    expected_gaql_date_filter = ...


class TestDyanmicQueryStream(unittest.TestCase):
    def test_gaql_date_filter_per_context(self):
        catalog = {
            "streams": [{"tap_stream_id": AssertingGAQLDateFilterGoogleAdsStream.name}]
        }
        state = {
            "bookmarks": {
                AssertingGAQLDateFilterGoogleAdsStream.name: {
                    "partitions": [
                        {
                            "context": {
                                "customer_id": "1",
                                "parent_customer_id": "0",
                            },
                            "replication_key": AssertingGAQLDateFilterGoogleAdsStream.replication_key,
                            "replication_key_value": "2025-12-01",
                        },
                        {
                            "context": {
                                "customer_id": "2",
                                "parent_customer_id": "0",
                            },
                            "replication_key": AssertingGAQLDateFilterGoogleAdsStream.replication_key,
                            "replication_key_value": "2025-12-02",
                        },
                    ]
                }
            }
        }

        tap = TapGoogleAds(
            config=CONFIG,
            catalog=catalog,
            state=state,
        )

        stream = AssertingGAQLDateFilterGoogleAdsStream(tap=tap)
        stream_state_partitions = iter(state["bookmarks"][stream.name]["partitions"])

        for partition in stream_state_partitions:
            stream.expected_gaql_date_filter = (
                f"segments.date >= '{partition['replication_key_value']}'"
            )

            with self.assertRaises(StopAfterVerification):
                stream.sync(partition["context"])
