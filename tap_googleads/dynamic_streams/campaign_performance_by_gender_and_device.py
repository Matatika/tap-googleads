"""CampaignPerformanceByGenderAndDevice for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignPerformanceByGenderAndDevice(DynamicQueryStream):
    """Campaign Performance By Gender and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT ad_group_criterion.gender.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM gender_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    name = "campaign_performance_by_gender_and_device"
    primary_keys = [
        "adGroupCriterion__gender__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    replication_key = None 