"""GoogleAds tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_googleads.streams import (
    AccessibleCustomers,
    AdGroupsPerformance,
    AdGroupsStream,
    CampaignPerformance,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    CampaignsStream,
    CustomerHierarchyStream,
    GeoPerformance,
    GeotargetsStream,
    Conversion,
)

STREAM_TYPES = [
    CampaignsStream,
    AdGroupsStream,
    AdGroupsPerformance,
    AccessibleCustomers,
    CustomerHierarchyStream,
    CampaignPerformance,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    GeotargetsStream,
    GeoPerformance,
    Conversion,
]


class TapGoogleAds(Tap):
    """GoogleAds tap class."""

    name = "tap-googleads"

    # TODO: Add Descriptions
    config_jsonschema = th.PropertiesList(
        th.Property(
            "oauth_credentials.client_id",
            th.StringType,
        ),
        th.Property(
            "oauth_credentials.client_secret",
            th.StringType,
        ),
        th.Property(
            "developer_token",
            th.StringType,
        ),
        th.Property(
            "oauth_credentials.refresh_token",
            th.StringType,
        ),
        th.Property(
            "customer_id",
            th.StringType,
        ),
        th.Property(
            "login_customer_id",
            th.StringType,
            description="Value to use in the login-customer-id header, if different from the customer_id to sync. Useful if you are syncing using a manager account.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
