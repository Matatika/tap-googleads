"""CampaignPerformanceByAgeRangeAndDevice for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignPerformanceByAgeRangeAndDevice(DynamicQueryStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT age_range_view.resource_name, ad_group_criterion.resource_name, ad_group_criterion.age_range.type, campaign.resource_name, campaign.name, campaign.status, ad_group.resource_name, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM age_range_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    name = "campaign_performance_by_age_range_and_device"
    primary_keys = [
        "adGroupCriterion__ageRange__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]