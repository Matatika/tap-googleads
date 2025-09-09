"""SearchTermViewStream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class SearchTermViewStream(DynamicQueryStream):
    """Search term view stream."""

    def _get_gaql(self):
        return """
        SELECT 
            customer.resource_name,
            customer.id, 
            segments.date, 
            customer.currency_code, 
            customer.descriptive_name,
            ad_group.id,
            ad_group.name, 
            ad_group.status,
            segments.ad_network_type,
            metrics.conversions_value,
            campaign.id,
            campaign.name,
            campaign.status,
            metrics.clicks,
            metrics.conversions,
            metrics.cost_micros,
            segments.device,
            metrics.impressions, 
            segments.keyword.ad_group_criterion,
            segments.keyword.info.text, 
            search_term_view.resource_name,
            search_term_view.search_term,
            search_term_view.status,
            segments.search_term_match_type,
            campaign.resource_name,
            ad_group.resource_name,
            ad_group_ad.resource_name,
            ad_group_ad.ad.resource_name
        FROM search_term_view
        """

    name = "search_term_view"
    replication_key = "segments__date"
    add_date_filter_to_query = True