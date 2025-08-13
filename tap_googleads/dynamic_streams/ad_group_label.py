"""AdGroupLabelStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupLabelStream(DynamicQueryStream):
    """Ad Group Label stream"""

    @property
    def gaql(self):
        return """
        SELECT
          ad_group.id,
          label.id,
          ad_group.resource_name,
          ad_group_label.resource_name,
          label.name,
          label.resource_name
        FROM ad_group_label
        """

    name = "ad_group_label"
    primary_keys = ["adGroup__id", "label__id"]
    replication_key = None
