"""SearchTermViewStream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class SearchTermViewStream(DynamicQueryStream):
    """Search term view stream."""

    def _get_gaql(self):
        return """
        SELECT 
            customer.id, 
            segments.date, 
            metrics.conversions, 
            segments.month, 
            customer.currency_code, 
            segments.year, 
            customer.time_zone, 
            metrics.engagements, 
            campaign.id, 
            metrics.engagement_rate, 
            metrics.all_conversions, 
            ad_group.name, 
            ad_group_ad.ad.tracking_url_template, 
            segments.quarter, 
            segments.day_of_week, 
            metrics.ctr, 
            ad_group_ad.ad.final_urls, 
            ad_group_ad.ad.id, 
            metrics.interaction_rate, 
            metrics.impressions, 
            metrics.view_through_conversions, 
            campaign.status, 
            metrics.top_impression_percentage, 
            metrics.cross_device_conversions, 
            metrics.conversions_from_interactions_rate, 
            metrics.cost_micros, 
            search_term_view.search_term, 
            metrics.interactions, 
            metrics.average_cpm, 
            metrics.interaction_event_types, 
            segments.search_term_match_type, 
            segments.device, 
            segments.week, 
            metrics.clicks, 
            metrics.all_conversions_from_interactions_rate, 
            ad_group.status, 
            segments.keyword.info.text, 
            metrics.cost_per_conversion, 
            metrics.average_cpc, 
            metrics.average_cpe, 
            metrics.conversions_value, 
            metrics.average_cost, 
            segments.ad_network_type, 
            metrics.value_per_all_conversions, 
            ad_group.id, 
            metrics.absolute_top_impression_percentage, 
            metrics.all_conversions_value, 
            campaign.name, 
            customer.descriptive_name, 
            metrics.value_per_conversion, 
            segments.keyword.ad_group_criterion, 
            search_term_view.status, 
            metrics.cost_per_all_conversions 
        FROM search_term_view
        """

    name = "search_term_view"
    replication_key = "segments__date"
    add_date_filter_to_query = True