"""ClickViewReportStream for Google Ads tap."""

from __future__ import annotations

import datetime
from http import HTTPStatus

from tap_googleads.client import ResumableAPIError
from tap_googleads.dynamic_query_stream import DynamicQueryStream



class ClickViewReportStream(DynamicQueryStream):
    """Stream for click view reports."""

    date: datetime.date

    def __init__(self, *args, **kwargs) -> None:
        self.date = datetime.date.today() - datetime.timedelta(days=1)
        super().__init__(*args, **kwargs)

    @property
    def gaql(self):

        return f"""
        SELECT
          click_view.gclid,
          customer.id,
          click_view.ad_group_ad,
          ad_group.id,
          ad_group.name,
          campaign.id,
          campaign.name,
          segments.ad_network_type,
          segments.device,
          segments.date,
          segments.slot,
          metrics.clicks,
          segments.click_type,
          click_view.keyword,
          click_view.keyword_info.match_type,
          campaign.target_impression_share.cpc_bid_ceiling_micros,
          campaign.target_roas.cpc_bid_floor_micros,
          customer.currency_code,
          click_view.user_list,
          customer.test_account,
          campaign.percent_cpc.cpc_bid_ceiling_micros,
          click_view.area_of_interest.city,
          campaign.percent_cpc.enhanced_cpc_enabled,
          click_view.page_number,
          campaign.targeting_setting.target_restrictions,
          campaign.bidding_strategy_type,
          customer.has_partners_badge,
          campaign.manual_cpm,
          campaign.optimization_score,
          segments.month_of_year,
          ad_group.effective_target_cpa_source,
          campaign.end_date,
          campaign.target_cpa.cpc_bid_ceiling_micros,
          campaign.manual_cpv,
          click_view.area_of_interest.metro,
          campaign.status,
          ad_group.effective_target_roas_source,
          campaign.labels,
          campaign.target_spend.target_spend_micros,
          click_view.location_of_presence.region,
          ad_group.base_ad_group,
          campaign.app_campaign_setting.bidding_strategy_goal_type,
          campaign.target_spend.cpc_bid_ceiling_micros,
          campaign.base_campaign,
          ad_group.type,
          ad_group.effective_target_cpa_micros,
          campaign.target_roas.target_roas,
          campaign.tracking_setting.tracking_url,
          campaign.keyword_match_type,
          ad_group.url_custom_parameters,
          campaign.real_time_bidding_setting.opt_in,
          customer.manager,
          ad_group.resource_name,
          ad_group.status,
          campaign.audience_setting.use_audience_grouped,
          ad_group.final_url_suffix,
          campaign.manual_cpc.enhanced_cpc_enabled,
          ad_group.labels,
          ad_group.target_cpm_micros,
          campaign.maximize_conversions.target_cpa_micros,
          ad_group.percent_cpc_bid_micros,
          click_view.gclid,
          campaign.campaign_budget,
          ad_group.id,
          customer.optimization_score,
          campaign.target_cpa.cpc_bid_floor_micros,
          campaign.name,
          customer.status,
          customer.descriptive_name,
          ad_group.tracking_url_template,
          ad_group.target_cpa_micros,
          campaign.local_campaign_setting.location_source_type,
          click_view.location_of_presence.most_specific,
          click_view.location_of_presence.country,
          ad_group.excluded_parent_asset_field_types,
          click_view.location_of_presence.metro,
          customer.time_zone,
          campaign.id,
          click_view.ad_group_ad,
          click_view.keyword,
          ad_group.name,
          campaign.target_cpa.target_cpa_micros,
          campaign.bidding_strategy,
          click_view.area_of_interest.country,
          customer.resource_name,
          ad_group.cpm_bid_micros,
          ad_group.effective_target_roas,
          campaign.final_url_suffix,
          click_view.area_of_interest.region,
          customer.tracking_url_template,
          click_view.area_of_interest.most_specific,
          click_view.resource_name,
          customer.optimization_score_weight,
          ad_group.cpv_bid_micros,
          campaign.target_roas.cpc_bid_ceiling_micros,
          ad_group.campaign,
          campaign.start_date,
          click_view.campaign_location_target,
          click_view.location_of_presence.city,
          campaign.experiment_type,
          ad_group.target_roas,
          campaign.payment_mode,
          campaign.maximize_conversion_value.target_roas,
          campaign.tracking_url_template,
          customer.final_url_suffix,
          campaign.serving_status,
          ad_group.cpc_bid_micros,
          campaign.resource_name,
          campaign.url_custom_parameters,
          campaign.maximize_conversions.target_cpa_micros,
          ad_group.targeting_setting.target_restrictions,
          campaign.targeting_setting.target_restrictions,
          click_view.keyword_info.match_type,
          click_view.keyword_info.text,
          segments.ad_network_type
        FROM click_view
        WHERE segments.date = '{self.date.isoformat()}'
        """

    name = "click_view_report"
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
        "date"
    ]
    replication_key = "date"
    replication_method = "INCREMENTAL"

    def post_process(self, row, context):
        row["date"] = row["segments"].pop("date")

        if row.get("clickView", {}).get("keyword") is None:
            row["clickView"]["keyword"] = "UNSPECIFIED"
            row["clickView"]["keywordInfo"] = {"matchType": "UNSPECIFIED"}

        return super().post_process(row, context)

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

        ninety_days_ago = datetime.date.today() - datetime.timedelta(days=90)
        start_value = datetime.date.fromisoformat(self.get_starting_replication_key_value(context))#context.get("bookmarks", {}).get(self.name, {}).get(self.replication_key)#self.get_starting_replication_key_value(context)
        if start_value < ninety_days_ago:
            start_date = ninety_days_ago
        else:
            start_date = start_value

        end_date = datetime.date.fromisoformat(self.config["end_date"])

        delta = end_date - start_date
        dates = (start_date + datetime.timedelta(days=i) for i in range(delta.days))

        for self.date in dates:
            self.logger.info(f"Requesting records for date: {self.date} | customer_id: {context.get('customer_id')}")
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
