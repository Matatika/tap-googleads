"""GoogleAds tap class."""

from datetime import datetime, timedelta, timezone
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_googleads.custom_query_stream import CustomQueryStream
from tap_googleads.dynamic_streams import (
    AdGroupAdLabelStream,
    AdGroupAdStream,
    AdGroupCriterionStream,
    AdGroupLabelStream,
    AdGroupsPerformance,
    AdGroupsStream,
    AdListingGroupCriterionStream,
    AudienceStream,
    CampaignBudgetStream,
    CampaignCriterionStream,
    CampaignLabelStream,
    CampaignPerformance,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    CampaignHistoryStream,
    ClickViewReportStream,
    CustomerLabelStream,
    GeoPerformance,
    GeotargetsStream,
    UserInterestStream,
    CustomerStream,
    LabelStream,
    KeywordViewStream,
    #GenderViewStream,
    ManagedPlacementViewStream,
    SearchTermViewStream,
    VideoStream,
)
from tap_googleads.streams import AccessibleCustomers, CustomerHierarchyStream

STREAM_TYPES = [
    CampaignHistoryStream,
    AdGroupsStream,
    AdGroupsPerformance,
    AdGroupAdStream,
    AdGroupCriterionStream,
    AdGroupLabelStream,
    AdListingGroupCriterionStream,
    AccessibleCustomers,
    CustomerHierarchyStream,
    CustomerLabelStream,
    CampaignPerformance,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    GeotargetsStream,
    GeoPerformance,
    AdGroupAdLabelStream,
    AudienceStream,
    UserInterestStream,
    CampaignCriterionStream,
    CampaignBudgetStream,
    CampaignLabelStream,
    CustomerStream,
    LabelStream,
    KeywordViewStream,
    ManagedPlacementViewStream,
    SearchTermViewStream,
    VideoStream,
]

CUSTOMER_ID_TYPE = th.StringType(pattern=r"^[0-9]{3}-?[0-9]{3}-?[0-9]{4}$")


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
            CUSTOMER_ID_TYPE,
            description="Value to use in the login-customer-id header if using a manager customer account. See https://developers.google.com/search-ads/reporting/concepts/login-customer-id for more info.",
        ),
        th.Property(
            "customer_ids",
            th.ArrayType(CUSTOMER_ID_TYPE),
            description="Get data for the provided customers only, rather than all accessible customers. Takes precedence over `customer_id`.",
        ),
        th.Property(
            "customer_id",
            CUSTOMER_ID_TYPE,
            description="Get data for the provided customer only, rather than all accessible customers. Superseeded by `customer_ids`.",
        ),
        th.Property(
            "start_date",
            th.DateType,
            description="ISO start date for all of the streams that use date-based filtering. Defaults to 90 days before the current day.",
            default=_start_date.isoformat(),
        ),
        th.Property(
            "end_date",
            th.DateType,
            description="ISO end date for all of the streams that use date-based filtering. Defaults to the current day.",
            default=_end_date.isoformat(),
        ),
        th.Property(
            "enable_click_view_report_stream",
            th.BooleanType,
            description="Enables the tap's ClickViewReportStream. This requires setting up / permission on your google ads account(s)",
            default=False,
        ),
        th.Property(
            "custom_queries",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        description="The name to assign to the query stream.",
                    ),
                    th.Property(
                        "query",
                        th.StringType,
                        description="A custom defined GAQL query for building the report. Do not include segments.date filter in the query, it is automatically added. For more information, refer to [Google's documentation](https://developers.google.com/google-ads/api/fields/v19/overview_query_builder).",
                    ),
                    th.Property(
                        "add_date_filter_to_query",
                        th.BooleanType,
                        description="Whether to add date filter to the query. Defaults to true.",
                        default=True,
                    ),
                    th.Property(
                        "replication_key",
                        th.StringType,
                        description="The field to use as the replication key for incremental replication.",
                    ),
                    th.Property(
                        "primary_keys",
                        th.ArrayType(th.StringType),
                        description="The primary keys for the stream. Defaults to an empty list, which means no primary keys are set.",
                    ),
                    th.Property(
                        "replication_method",
                        th.StringType,
                        description="The replication method to use for the stream. Defaults to 'INCREMENTAL'.",
                        default="INCREMENTAL",
                    ),
                ),
            ),
            description="A list of custom queries to run. Each query will be assigned a stream with the name specified in the `name` field.",
            default=[],
        ),
        th.Property(
            "api_version",
            th.StringType,
            description="API version to use - see [versioning](https://developers.google.com/google-ads/api/docs/concepts/versioning) and [release notes](https://developers.google.com/google-ads/api/docs/release-notes)/[upgrade your API version](https://developers.google.com/google-ads/api/docs/upgrade).",
            default="v22",  # https://developers.google.com/google-ads/api/docs/release-notes#v22_2025-10-15
        ),
    ).to_dict()

    def setup_mapper(self):
        self._config.setdefault("flattening_enabled", True)
        self._config.setdefault("flattening_max_depth", 2)

        return super().setup_mapper()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        if self.config["enable_click_view_report_stream"]:
            streams.append(ClickViewReportStream(tap=self))
        streams.extend(
            CustomQueryStream(tap=self, custom_query=q)
            for q in self.config["custom_queries"]
        )

        return streams
