"""Stream type classes for tap-googleads."""

from __future__ import annotations

import datetime
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_googleads.client import GoogleAdsStream, ResumableAPIError

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
        return """
	SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.status,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
	"""

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

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            msg = self.response_error_message(response)
            raise ResumableAPIError(msg, response)

        super().validate_response(response)

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

    gaql = """
    SELECT
        geo_target_constant.canonical_name,
        geo_target_constant.country_code,
        geo_target_constant.id,
        geo_target_constant.name,
        geo_target_constant.status,
        geo_target_constant.target_type
    FROM geo_target_constant
    """
    records_jsonpath = "$.results[*]"
    name = "stream_geo_target_constant"
    primary_keys = ["geoTargetConstant__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_target_constant.json"

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
        return f"""
        SELECT
            click_view.gclid
            , customer.id
            , click_view.ad_group_ad
            , ad_group.id
            , ad_group.name
            , campaign.id
            , campaign.name
            , segments.ad_network_type
            , segments.device
            , segments.date
            , segments.slot
            , metrics.clicks
            , segments.click_type
            , click_view.keyword
            , click_view.keyword_info.match_type
        FROM click_view
        WHERE segments.date = '{self.date.isoformat()}'
        """

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

        start_date = datetime.date.fromisoformat(start_value)
        end_date = datetime.date.fromisoformat(self.config["end_date"])

        delta = end_date - start_date
        dates = (start_date + datetime.timedelta(days=i) for i in range(delta.days))

        for self.date in dates:
            records = list(super().request_records(context))

            if not records:
                self._increment_stream_state(
                    {"date": self.date.isoformat()}, context=self.context
                )

            yield from records

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            error = response.json()["error"]["details"][0]["errors"][0]
            msg = (
                "Click view report not accessible to customer "
                f"'{self.context['customer_id']}': {error['message']}"
            )
            raise ResumableAPIError(msg, response)

        super().validate_response(response)


class CampaignsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
        SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.id
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign"
    primary_keys = ["campaign__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign.json"


class AdGroupsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
       SELECT ad_group.url_custom_parameters,
       ad_group.type,
       ad_group.tracking_url_template,
       ad_group.targeting_setting.target_restrictions,
       ad_group.target_roas,
       ad_group.target_cpm_micros,
       ad_group.status,
       ad_group.target_cpa_micros,
       ad_group.resource_name,
       ad_group.percent_cpc_bid_micros,
       ad_group.name,
       ad_group.labels,
       ad_group.id,
       ad_group.final_url_suffix,
       ad_group.excluded_parent_asset_field_types,
       ad_group.effective_target_roas_source,
       ad_group.effective_target_roas,
       ad_group.effective_target_cpa_source,
       ad_group.effective_target_cpa_micros,
       ad_group.display_custom_bid_dimension,
       ad_group.cpv_bid_micros,
       ad_group.cpm_bid_micros,
       ad_group.cpc_bid_micros,
       ad_group.campaign,
       ad_group.base_ad_group,
       ad_group.ad_rotation_mode
       FROM ad_group
       """

    records_jsonpath = "$.results[*]"
    name = "stream_adgroups"
    primary_keys = ["adGroup__id", "adGroup__campaign", "adGroup__status"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_group.json"


class AdGroupsPerformance(ReportsStream):
    """AdGroups Performance"""

    @property
    def gaql(self):
        return f"""
        SELECT campaign.id, ad_group.id, metrics.impressions, metrics.clicks,
               metrics.cost_micros
               FROM ad_group
               WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_adgroupsperformance"
    primary_keys = ["campaign__id", "adGroup__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "adgroups_performance.json"


class CampaignPerformance(ReportsStream):
    """Campaign Performance"""

    @property
    def gaql(self):
        return f"""
    SELECT campaign.name, campaign.status, segments.device, segments.date, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM campaign WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

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
        return f"""
    SELECT ad_group_criterion.age_range.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM age_range_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

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
        return f"""
    SELECT ad_group_criterion.gender.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM gender_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

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
        return f"""
    SELECT campaign_criterion.location.geo_target_constant, campaign.name, campaign_criterion.bid_modifier, segments.date, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM location_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date} AND campaign_criterion.status != 'REMOVED'
    """

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
        return f"""
    SELECT
        campaign.name,
        campaign.status,
        segments.date,
        metrics.clicks,
        metrics.cost_micros,
        metrics.impressions,
        metrics.conversions,
        geographic_view.location_type,
        geographic_view.country_criterion_id
    FROM geographic_view
    WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

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


class PerformanceMaxAssetGroupHistoricalPerformance(ReportsStream):
    """Geo performance"""

    records_jsonpath = "$.results[*]"
    name = "stream_performance_max_asset_group_historical_performance"
    primary_keys = [
        "campaign__id",
        "asset_group__id",
        "segments__date",
    ]

    replication_key = None
    schema_filepath = (
        SCHEMAS_DIR / "performance_max_asset_group_historical_performance.json"
    )

    @property
    def gaql(self):
        return f"""
        SELECT
        campaign.id,
        campaign.resource_name,
        segments.date,
        asset_group.id,
        metrics.conversions,
        metrics.conversions_value,
        metrics.cost_micros,
        metrics.clicks,
        metrics.impressions
        FROM asset_group
        where segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """


class PerformanceMaxAssetGroups(ReportsStream):
    """Asset Groups dimension"""

    records_jsonpath = "$.results[*]"
    name = "stream_performance_max_asset_groups"
    primary_keys = [
        "asset_group__id",
    ]

    replication_key = None
    schema_filepath = SCHEMAS_DIR / "performance_max_asset_groups.json"

    @property
    def gaql(self):
        return f"""
        SELECT
        asset_group.ad_strength,
        asset_group.campaign,
        asset_group.final_mobile_urls,
        asset_group.final_urls,
        asset_group.id,
        asset_group.name,
        asset_group.path1,
        asset_group.path2,
        asset_group.primary_status,
        asset_group.primary_status_reasons,
        asset_group.resource_name,
        asset_group.status
        FROM asset_group
        """


class ConversionGoals(ReportsStream):
    """Conversion Goals"""

    @property
    def gaql(self):
        return f"""
            SELECT
                conversion_action.status
                , conversion_action.type
                , conversion_action.origin
                , conversion_action.category
                , conversion_action.counting_type
                , conversion_action.id
                , conversion_action.name
                , conversion_action.primary_for_goal
                , conversion_action.owner_customer
                , conversion_action.include_in_conversions_metric
                , conversion_action.click_through_lookback_window_days
                , conversion_action.view_through_lookback_window_days
                , conversion_action.phone_call_duration_seconds
            FROM conversion_action
        """

    records_jsonpath = "$.results[*]"
    name = "stream_conversion_goals"
    primary_keys = [
        "conversion_action__id",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "conversion_goals.json"

    def post_process(self, row: Dict, context: Dict | None = None) -> Dict | None:
        return super().post_process(row, context)


class CampaignConversion(ReportsStream):
    """Campaign Conversion"""

    @property
    def gaql(self):
        return f"""

        SELECT
            campaign.id,
            segments.conversion_action,
            segments.date,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_conversion"
    primary_keys = [
        "campaign__id",
        "segments__date",
        "segments__conversion_action",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_conversion.json"


class KeywordPerformance(ReportsStream):
    """Keyword Performance Stream.

    Retrieves keyword performance data from Google Ads API including
    information about ad groups, campaigns, and performance metrics.
    """

    @property
    def gaql(self):
        return f"""
        SELECT
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.system_serving_status,
            ad_group_criterion.approval_status,
            ad_group_criterion.status,
            ad_group_criterion.quality_info.quality_score,
            metrics.clicks,
            metrics.impressions,
            metrics.ctr,
            metrics.average_cpc,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            segments.date
        FROM keyword_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_keyword_performance"
    primary_keys = [
        "campaign__id",
        "adGroup__id",
        "adGroupCriterion__keyword__text",
        "adGroupCriterion__keyword__matchType",
        "segments__date",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "keyword_performance.json"


class SearchQueryPerformance(ReportsStream):
    """Search Query Performance Stream.

    Retrieves performance data for the actual search queries that triggered
    ads, showing how user searches relate to your keywords and their performance.
    """

    @property
    def gaql(self):
        return f"""
        SELECT
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            search_term_view.search_term,
            search_term_view.status,
            segments.keyword.info.match_type,
            segments.keyword.info.text,
            metrics.clicks,
            metrics.impressions,
            metrics.ctr,
            metrics.average_cpc,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            segments.date
        FROM search_term_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_search_query_performance"
    primary_keys = [
        "campaign__id",
        "adGroup__id",
        "searchTermView__searchTerm",
        "segments__date",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "search_query_performance.json"
