"""KeywordViewStream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class KeywordViewStream(DynamicQueryStream):
    """Keyword view stream."""

    def _get_gaql(self):
        return """
        SELECT 
          customer.id, 
          segments.date, 
          metrics.conversions, 
          ad_group_criterion.status, 
          ad_group_criterion.keyword.text, 
          customer.currency_code, 
          segments.slot, 
          campaign.keyword_match_type, 
          ad_group_criterion.labels, 
          campaign.id, 
          ad_group_criterion.final_urls, 
          segments.device, 
          ad_group_criterion.final_mobile_urls, 
          ad_group.name, 
          ad_group_criterion.effective_cpc_bid_source, 
          metrics.clicks, 
          ad_group_criterion.criterion_id, 
          ad_group_criterion.quality_info.quality_score, 
          ad_group_criterion.quality_info.post_click_quality_score, 
          ad_group_criterion.quality_info.search_predicted_ctr, 
          ad_group.status, 
          ad_group_criterion.position_estimates.first_page_cpc_micros, 
          ad_group_criterion.quality_info.creative_quality_score, 
          campaign.manual_cpc.enhanced_cpc_enabled, 
          ad_group_criterion.effective_cpc_bid_micros, 
          ad_group_criterion.approval_status, 
          ad_group_criterion.topic.topic_constant, 
          segments.ad_network_type, 
          metrics.impressions, 
          ad_group.id, 
          ad_group_criterion.position_estimates.top_of_page_cpc_micros, 
          campaign.name, 
          ad_group_criterion.position_estimates.estimated_add_cost_at_first_position_cpc, 
          customer.descriptive_name, 
          campaign.status, 
          ad_group_criterion.position_estimates.estimated_add_clicks_at_first_position_cpc, 
          metrics.cost_micros, 
          campaign.bidding_strategy_type 
        FROM keyword_view 
        """

    name = "keyword_view"
    primary_keys = ["customer__id","segments__date"]
    replication_key = "segments__date"
    replication_method = "INCREMENTAL"
    add_date_filter_to_query = True