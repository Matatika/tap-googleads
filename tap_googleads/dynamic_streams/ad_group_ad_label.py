"""AdGroupAdLabelStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupAdLabelStream(DynamicQueryStream):
    """Ad Group Ad Label stream"""

    @property
    def gaql(self):
        return """
        SELECT
          ad_group.id,
          ad_group_ad.ad.id,
          ad_group_ad.ad.resource_name,
          ad_group_ad_label.resource_name,
          label.name,
          label.resource_name,
          label.id
        FROM ad_group_ad_label
        """

    name = "ad_group_ad_label"
    primary_keys = ["adGroup__id", "adGroupAd__ad__id", "label__id"]