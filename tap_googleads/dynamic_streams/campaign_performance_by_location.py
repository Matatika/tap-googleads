"""CampaignPerformanceByLocation for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignPerformanceByLocation(DynamicQueryStream):
    """Campaign Performance By Location"""

    @property
    def gaql(self):
        return f"""
    SELECT campaign_criterion.location.geo_target_constant, campaign.name, campaign_criterion.bid_modifier, segments.date, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM location_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date} AND campaign_criterion.status != 'REMOVED'
    """

    name = "campaign_performance_by_location"
    primary_keys = [
        "campaignCriterion__location__geoTargetConstant",
        "campaign__name",
        "segments__date",
    ]
    replication_key = None
