"""ManagedPlacementViewStream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class ManagedPlacementViewStream(DynamicQueryStream):
    """Managed placement view stream."""

    def _get_gaql(self):
        return """
        SELECT 
          customer.id, 
          segments.date, 
          metrics.conversions, 
          ad_group_criterion.status, 
          customer.currency_code, 
          customer.time_zone, 
          campaign.id, 
          ad_group_criterion.final_urls, 
          segments.device, 
          ad_group_criterion.final_mobile_urls, 
          ad_group_criterion.placement.url, 
          metrics.gmail_forwards, 
          ad_group.name, 
          ad_group_criterion.effective_cpc_bid_source, 
          metrics.clicks, 
          ad_group_criterion.criterion_id, 
          ad_group.status, 
          ad_group.targeting_setting.target_restrictions, 
          campaign.bidding_strategy_type, 
          ad_group_criterion.effective_cpc_bid_micros, 
          ad_group_criterion.negative, 
          metrics.video_quartile_p25_rate, 
          metrics.video_quartile_p75_rate, 
          metrics.video_quartile_p50_rate, 
          ad_group_criterion.bid_modifier, 
          segments.ad_network_type, 
          metrics.impressions, 
          ad_group.id, 
          metrics.video_quartile_p100_rate, 
          metrics.gmail_saves, 
          metrics.all_conversions_value, 
          campaign.name, 
          metrics.video_views, 
          customer.descriptive_name, 
          campaign.status, 
          metrics.gmail_secondary_clicks, 
          metrics.cost_micros, 
          ad_group_criterion.display_name 
        FROM managed_placement_view 
        """

    name = "managed_placement_view"
    primary_keys = ["customer__id", "segments__date"]
    replication_key = "segments__date"
    replication_method = "INCREMENTAL"
    add_date_filter_to_query = True