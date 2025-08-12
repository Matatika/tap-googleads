"""CampaignHistoryStream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignHistoryStream(DynamicQueryStream):
    """Campaign history stream."""

    def _get_gaql(self):
        return """
        SELECT 
          campaign.id,
          campaign.name,
          customer.id,
          campaign.ad_serving_optimization_status,
          campaign.advertising_channel_sub_type,
          campaign.advertising_channel_type,
          campaign.experiment_type,
          campaign.end_date,
          campaign.final_url_suffix,
          campaign.frequency_caps,
          campaign.optimization_score,
          campaign.payment_mode,
          campaign.serving_status,
          campaign.start_date,
          campaign.status,
          campaign.tracking_url_template,
          campaign.vanity_pharma.vanity_pharma_display_url_mode,
          campaign.vanity_pharma.vanity_pharma_text,
          campaign.video_brand_safety_suitability,
          segments.date
        FROM campaign
        """

    name = "campaign_history"
    primary_keys = ["campaign__id","segments__date"]
    replication_key = "segments__date"
    add_date_filter_to_query = True
