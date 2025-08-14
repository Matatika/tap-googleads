"""GenderViewStream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class GenderViewStream(DynamicQueryStream):
    """Gender view stream."""

    def _get_gaql(self):
        return """
        SELECT 
          customer.id, 
          segments.date, 
          metrics.conversions, 
          ad_group_criterion.status, 
          ad_group_criterion.gender.type, 
          customer.currency_code, 
          customer.time_zone, 
          campaign.id, 
          segments.device, 
          metrics.video_view_rate, 
          ad_group.name, 
          metrics.clicks, 
          ad_group_criterion.criterion_id, 
          ad_group.status, 
          ad_group_criterion.effective_cpc_bid_micros, 
          ad_group_criterion.negative, 
          metrics.video_quartile_p25_rate, 
          metrics.video_quartile_p75_rate, 
          metrics.video_quartile_p50_rate, 
          ad_group_criterion.bid_modifier, 
          segments.ad_network_type, 
          metrics.impressions, 
          metrics.view_through_conversions, 
          ad_group.id, 
          metrics.video_quartile_p100_rate, 
          metrics.all_conversions_value, 
          campaign.name, 
          metrics.video_views, 
          customer.descriptive_name, 
          campaign.status, 
          metrics.cost_micros 
        FROM gender_view  
        """

    name = "gender_view"
    primary_keys = ["customer__id","segments__date"]
    replication_key = "segments__date"
    add_date_filter_to_query = True