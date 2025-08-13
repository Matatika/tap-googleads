"""CampaignLabelStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignLabelStream(DynamicQueryStream):
    """Campaign Label stream"""

    @property
    def gaql(self):
        return """
        SELECT
          campaign.id,
          label.id,
          campaign.resource_name,
          campaign_label.resource_name,
          label.name,
          label.resource_name
        FROM campaign_label
        """

    name = "campaign_label"
    primary_keys = ["campaign__id", "label__id"]
    replication_key = None
