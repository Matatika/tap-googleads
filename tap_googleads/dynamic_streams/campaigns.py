"""CampaignsStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignsStream(DynamicQueryStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
        SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.id
        """

    name = "campaign"
    primary_keys = ["campaign__id"]
    replication_key = None 