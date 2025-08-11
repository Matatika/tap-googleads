"""CampaignCriterionStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignCriterionStream(DynamicQueryStream):
    """Campaign Criterion stream"""

    @property
    def gaql(self):
        return """
        SELECT
          campaign.id,
          campaign_criterion.resource_name,
          campaign_criterion.campaign,
          campaign_criterion.age_range.type,
          campaign_criterion.mobile_application.name,
          campaign_criterion.negative,
          campaign_criterion.youtube_channel.channel_id,
          campaign_criterion.youtube_video.video_id
        FROM campaign_criterion
        """

    name = "campaign_criterion"
    primary_keys = ["campaign__id", "campaignCriterion__resourceName"]