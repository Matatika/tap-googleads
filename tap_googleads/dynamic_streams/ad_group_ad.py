"""AdGroupAdStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupAdStream(DynamicQueryStream):
    """Ad Group Ad stream"""

    def _get_gaql(self):
        return """
        SELECT
          ad_group.resource_name,
          ad_group.id,
          ad_group_ad.ad.id,
          ad_group_ad.ad.added_by_google_ads,
          ad_group_ad.ad.app_ad.descriptions,
          ad_group_ad.ad.app_ad.headlines,
          ad_group_ad.ad.app_ad.html5_media_bundles,
          ad_group_ad.ad.app_ad.images,
          ad_group_ad.ad.app_ad.mandatory_ad_text,
          ad_group_ad.ad.app_ad.youtube_videos,
          ad_group_ad.ad.device_preference,
          ad_group_ad.ad.display_url,
          ad_group_ad.ad.final_app_urls,
          ad_group_ad.ad.final_mobile_urls,
          ad_group_ad.ad.final_url_suffix,
          ad_group_ad.ad.final_urls,
          ad_group_ad.ad.hotel_ad,
          ad_group_ad.ad.id,
          ad_group_ad.ad.name,
          ad_group_ad.ad.resource_name,
          ad_group_ad.ad.shopping_comparison_listing_ad.headline,
          ad_group_ad.ad.shopping_product_ad,
          ad_group_ad.ad.shopping_smart_ad,
          ad_group_ad.ad.smart_campaign_ad.descriptions,
          ad_group_ad.ad.smart_campaign_ad.headlines,
          ad_group_ad.ad.system_managed_resource_source,
          ad_group_ad.ad.tracking_url_template,
          ad_group_ad.ad.type,
          ad_group_ad.ad.url_collections,
          ad_group_ad.ad.url_custom_parameters,
          ad_group_ad.action_items,
          ad_group_ad.ad_group,
          ad_group_ad.ad_strength,
          ad_group_ad.labels,
          ad_group_ad.resource_name,
          ad_group_ad.status,
          ad_group.name,
          segments.date,
          customer.resource_name,
          customer.id,
          campaign.id,
          campaign.resource_name,
          campaign.status,
          ad_group.status,
          campaign.name,
          ad_group.base_ad_group
        FROM ad_group_ad
        """

    name = "ad_group_ad"
    primary_keys = ["adGroup__id","adGroupAd__ad__id","segments__date"]
    replication_key = "segments__date"
    add_date_filter_to_query = True