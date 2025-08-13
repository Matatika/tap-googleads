"""GeoPerformance for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class GeoPerformance(DynamicQueryStream):
    """Geo performance"""

    @property
    def gaql(self):
        return f"""
    SELECT 
        campaign.resource_name,
        campaign.name, 
        campaign.status, 
        segments.date, 
        metrics.clicks, 
        metrics.cost_micros,
        metrics.impressions, 
        metrics.conversions,
        geographic_view.resource_name,
        geographic_view.location_type,
        geographic_view.country_criterion_id
    FROM geographic_view 
    WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date} 
    """

    name = "geo_performance"
    primary_keys = [
        "geographicView__countryCriterionId",
        "customer_id",
        "campaign__name",
        "segments__date",
    ]