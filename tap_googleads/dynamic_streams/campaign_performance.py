"""CampaignPerformanceByLocation for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignPerformance(DynamicQueryStream):
    """Campaign Performance"""

    @property
    def gaql(self):
        return f"""
    SELECT campaign.resource_name, campaign.name, campaign.status, segments.device, segments.date, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM campaign WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    name = "campaign_performance"
    primary_keys = [
        "campaign__name",
        "campaign__status",
        "segments__date",
        "segments__device",
    ]
