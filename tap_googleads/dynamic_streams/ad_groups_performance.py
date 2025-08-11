"""AdGroupsPerformance for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupsPerformance(DynamicQueryStream):
    """AdGroups Performance"""

    @property
    def gaql(self):
        return f"""
        SELECT
            campaign.id,
            ad_group.id,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.engagements,
            metrics.interactions,
            metrics.video_views,
            metrics.conversions_value,
            metrics.all_conversions,
            metrics.all_conversions_value,
            segments.date
        FROM ad_group
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    name = "ad_groups_performance"
    primary_keys = ["campaign__id", "adGroup__id", "segments__date"]
