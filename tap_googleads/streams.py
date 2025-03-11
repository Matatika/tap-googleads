"""Stream type classes for tap-googleads."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_googleads.client import GoogleAdsStream

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AccessibleCustomers(GoogleAdsStream):
    """Accessible Customers."""

    rest_method = "GET"
    path = "/customers:listAccessibleCustomers"
    name = "stream_accessible_customers"
    primary_keys = ["resourceNames"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("resourceNames", th.ArrayType(th.StringType)),
    ).to_dict()

    def generate_child_contexts(
        self,
        record: Record,
        context: Context | None,
    ) -> Iterable[Context | None]:
        """Generate child contexts.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Yields:
            A child context for each child stream.

        """
        for customer in record.get("resourceNames", []):
            customer_id = customer.split("/")[1]
            yield {"customer_id": customer_id}


class CustomerHierarchyStream(GoogleAdsStream):
    """Customer Hierarchy.

    Inspiration from Google here
    https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy.

    This stream is stictly to be the Parent Stream, to let all Child Streams
    know when to query the down stream apps.

    """

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM customer_client
        WHERE customer_client.level <= 1"""

    records_jsonpath = "$.results[*]"
    name = "stream_customer_hierarchy"
    primary_keys = ["customerClient__id"]
    replication_key = None
    parent_stream_type = AccessibleCustomers
    schema = th.PropertiesList(
        th.Property("customer_id", th.StringType),
        th.Property(
            "customerClient",
            th.ObjectType(
                th.Property("resourceName", th.StringType),
                th.Property("clientCustomer", th.StringType),
                th.Property("level", th.StringType),
                th.Property("status", th.StringType),
                th.Property("timeZone", th.StringType),
                th.Property("manager", th.BooleanType),
                th.Property("descriptiveName", th.StringType),
                th.Property("currencyCode", th.StringType),
                th.Property("id", th.StringType),
            ),
        ),
    ).to_dict()

    seen_customer_ids = set()

    def generate_child_contexts(self, record, context):
        customer_ids = self.customer_ids

        if customer_ids is None:
            customer = record["customerClient"]

            if customer["manager"]:
                self.logger.warning(
                    "%s is a manager, skipping",
                    customer["clientCustomer"],
                )
                return

            if customer["status"] != "ENABLED":
                self.logger.warning(
                    "%s is not enabled, skipping",
                    customer["clientCustomer"],
                )
                return

            customer_ids = {customer["id"]}

        # sync only customers we haven't seen
        customer_ids = set(customer_ids) - self.seen_customer_ids
        yield from ({"customer_id": customer_id} for customer_id in customer_ids)

        self.seen_customer_ids.update(customer_ids)


class ReportsStream(GoogleAdsStream):
    parent_stream_type = CustomerHierarchyStream

class GeotargetsStream(ReportsStream):
    """Geotargets, worldwide, constant across all customers"""

    records_jsonpath = "$.results[*]"
    name = "stream_geo_target_constant"
    primary_keys = ["geoTargetConstant__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_target_constant.json"

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM geo_target_constant"""

    def get_records(self, context: Context) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from super().get_records(context)
        self.selected = False  # sync once only


class ClickViewReportStream(ReportsStream):
    date: datetime.date

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM click_view
        WHERE segments.date = '{self.date.isoformat()}'"""

    records_jsonpath = "$.results[*]"
    name = "stream_click_view_report"
    primary_keys = [
        "clickView__gclid",
        "clickView__keyword",
        "clickView__keywordInfo__matchType",
        "customer__id",
        "adGroup__id",
        "campaign__id",
        "segments__device",
        "segments__adNetworkType",
        "segments__slot",
        "date",
    ]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "click_view_report.json"

    def post_process(self, row, context):
        row["date"] = row["segments"].pop("date")

        if row.get("clickView", {}).get("keyword") is None:
            row["clickView"]["keyword"] = "null"
            row["clickView"]["keywordInfo"] = {"matchType": "null"}

        return row

    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.

        """
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        return params

    def request_records(self, context):
        start_value = self.get_starting_replication_key_value(context)

        start_date =  datetime.date.fromisoformat(start_value)
        end_date = datetime.date.fromisoformat(self.config["end_date"])

        delta = end_date - start_date
        dates = (start_date + datetime.timedelta(days=i) for i in range(delta.days))

        for self.date in dates:
            records = list(super().request_records(context))

            if not records:
                self._increment_stream_state({"date": self.date.isoformat()}, context=self.context)

            yield from records


class CampaignsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM campaign
        ORDER BY campaign.id"""

    records_jsonpath = "$.results[*]"
    name = "stream_campaign"
    primary_keys = ["campaign__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign.json"


class AdGroupsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM ad_group"""

    records_jsonpath = "$.results[*]"
    name = "stream_adgroups"
    primary_keys = ["adGroup__id", "adGroup__campaign", "adGroup__status"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_group.json"


class AdGroupsPerformance(ReportsStream):
    """AdGroups Performance"""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM ad_group
        WHERE segments.date >= {self.start_date}
        AND segments.date <= {self.end_date}"""

    records_jsonpath = "$.results[*]"
    name = "stream_adgroupsperformance"
    primary_keys = ["campaign__id", "adGroup__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "adgroups_performance.json"


class CampaignPerformance(ReportsStream):
    """Campaign Performance"""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM campaign
        WHERE segments.date >= {self.start_date}
        AND segments.date <= {self.end_date}"""

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance"
    primary_keys = [
        "campaign__name",
        "campaign__status",
        "segments__date",
        "segments__device",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance.json"


class CampaignPerformanceByAgeRangeAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM age_range_view
        WHERE segments.date >= {self.start_date}
        AND segments.date <= {self.end_date}"""

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_age_range_and_device"
    primary_keys = [
        "adGroupCriterion__ageRange__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_age_range_and_device.json"


class CampaignPerformanceByGenderAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM gender_view
        WHERE segments.date >= {self.start_date}
        AND segments.date <= {self.end_date}"""

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_gender_and_device"
    primary_keys = [
        "adGroupCriterion__gender__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_gender_and_device.json"


class CampaignPerformanceByLocation(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM location_view
        WHERE segments.date >= {self.start_date}
        AND segments.date <= {self.end_date}
        AND campaign_criterion.status != 'REMOVED'"""

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_location"
    primary_keys = [
        "campaignCriterion__location__geoTargetConstant",
        "campaign__name",
        "segments__date",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_location.json"


class GeoPerformance(ReportsStream):
    """Geo performance"""

    @property
    def gaql(self):
        return f"""{super().gaql}
        FROM geographic_view 
        WHERE segments.date >= {self.start_date}
        AND segments.date <= {self.end_date}"""

    records_jsonpath = "$.results[*]"
    name = "stream_geo_performance"
    primary_keys = [
        "geographicView__countryCriterionId",
        "customer_id",
        "campaign__name",
        "segments__date",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_performance.json"
