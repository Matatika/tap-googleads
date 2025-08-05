"""VideoStream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class VideoStream(DynamicQueryStream):
    """Video stream."""

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
          segments.device, 
          video.id, 
          segments.week, 
          metrics.video_view_rate, 
          ad_group.name, 
          metrics.clicks, 
          ad_group.status, 
          segments.day_of_week, 
          metrics.cost_per_conversion, 
          metrics.ctr, 
          metrics.video_quartile_p25_rate, 
          metrics.video_quartile_p75_rate, 
          metrics.video_quartile_p50_rate, 
          ad_group_ad.ad.id, 
          segments.ad_network_type, 
          video.title, 
          metrics.impressions, 
          metrics.view_through_conversions, 
          ad_group_ad.status, 
          ad_group.id, 
          metrics.video_quartile_p100_rate, 
          metrics.all_conversions_value, 
          campaign.name, 
          metrics.video_views, 
          video.duration_millis, 
          video.channel_id, 
          customer.descriptive_name, 
          campaign.status, 
          metrics.cross_device_conversions, 
          metrics.conversions_from_interactions_rate, 
          metrics.cost_micros 
        FROM video 
        """

    name = "video"
    replication_key = "segments__date"
    replication_method = "INCREMENTAL"
    add_date_filter_to_query = True