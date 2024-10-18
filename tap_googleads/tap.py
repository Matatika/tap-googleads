"""GoogleAds tap class."""

from datetime import datetime, timedelta, timezone
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
    ClickViewReportStream,
    CustomerHierarchyStream,
    GeoPerformance,
    GeotargetsStream,
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
]


class TapGoogleAds(Tap):
    """GoogleAds tap class."""

    name = "tap-googleads"

    _refresh_token = th.Property(
        "refresh_token",
        th.StringType,
        required=True,
        secret=True,
    )
    _end_date = datetime.now(timezone.utc).date()
    _start_date = _end_date - timedelta(days=90)

    # TODO: Add Descriptions
    config_jsonschema = th.PropertiesList(
        th.Property(
            "oauth_credentials",
            th.OneOf(
                th.ObjectType(
                    th.Property(
                        "client_id",
                        th.StringType,
                        required=True,
                    ),
                    th.Property(
                        "client_secret",
                        th.StringType,
                        required=True,
                        secret=True,
                    ),
                    _refresh_token,
                ),
                th.ObjectType(
                    th.Property(
                        "refresh_proxy_url",
                        th.StringType,
                        required=True,
                    ),
                    th.Property(
                        "refresh_proxy_url_auth",
                        th.StringType,
                        secret=True,
                    ),
                    _refresh_token,
                ),
            ),
            required=True,
        ),
        th.Property(
            "developer_token",
            th.StringType,
            required=True,
            secret=True,
        ),
        th.Property(
            "login_customer_id",
            th.StringType,
            description="Value to use in the login-customer-id header if using a manager customer account. See https://developers.google.com/search-ads/reporting/concepts/login-customer-id for more info.",
        ),
        th.Property(
            "customer_ids",
            th.ArrayType(th.StringType),
            description="Get data for the provided customers only, rather than all accessible customers. Takes precedence over `customer_id`.",
        ),
        th.Property(
            "customer_id",
            th.StringType,
            description="Get data for the provided customer only, rather than all accessible customers. Superseeded by `customer_ids`.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for all of the streams that use date based filtering.",
            default=_start_date.isoformat(),
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="End date for all of the streams that use date based filtering.",
            default=_end_date.isoformat(),
        ),
        th.Property(
            "enable_click_view_report_stream",
            th.BooleanType,
            description="Enables the tap's ClickViewReportStream. This requires setting up / permission on your google ads account(s)",
            default=False,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        if self.config["enable_click_view_report_stream"]:
            STREAM_TYPES.append(ClickViewReportStream)
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
