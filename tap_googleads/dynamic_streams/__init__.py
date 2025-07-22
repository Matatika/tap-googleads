"""Dynamic streams package for Google Ads tap."""

from tap_googleads.dynamic_streams.ad_group_ad import AdGroupAdStream
from tap_googleads.dynamic_streams.ad_group_ad_label import AdGroupAdLabelStream
from tap_googleads.dynamic_streams.ad_group_criterion import AdGroupCriterionStream
from tap_googleads.dynamic_streams.ad_group_label import AdGroupLabelStream
from tap_googleads.dynamic_streams.ad_groups import AdGroupsStream
from tap_googleads.dynamic_streams.ad_groups_performance import AdGroupsPerformance
from tap_googleads.dynamic_streams.ad_listing_group_criterion import AdListingGroupCriterionStream
from tap_googleads.dynamic_streams.audience import AudienceStream
from tap_googleads.dynamic_streams.campaign_budget import CampaignBudgetStream
from tap_googleads.dynamic_streams.campaign_criterion import CampaignCriterionStream
from tap_googleads.dynamic_streams.campaign_label import CampaignLabelStream
from tap_googleads.dynamic_streams.campaign_performance_by_age_range_and_device import (
    CampaignPerformanceByAgeRangeAndDevice,
)
from tap_googleads.dynamic_streams.campaign_performance_by_gender_and_device import (
    CampaignPerformanceByGenderAndDevice,
)
from tap_googleads.dynamic_streams.campaign_performance_by_location import (
    CampaignPerformanceByLocation,
)
from tap_googleads.dynamic_streams.campaigns import CampaignsStream
from tap_googleads.dynamic_streams.click_view_report import ClickViewReportStream
from tap_googleads.dynamic_streams.geo_performance import GeoPerformance
from tap_googleads.dynamic_streams.geotargets import GeotargetsStream
from tap_googleads.dynamic_streams.campaign_performance import CampaignPerformance
from tap_googleads.dynamic_streams.user_interest import UserInterestStream
from tap_googleads.dynamic_streams.customer_label import CustomerLabelStream
from tap_googleads.dynamic_streams.customer import CustomerStream
from tap_googleads.dynamic_streams.label import LabelStream

__all__ = [
    "AdGroupAdStream",
    "AdGroupAdLabelStream",
    "AdGroupCriterionStream",
    "AdGroupLabelStream",
    "AdGroupsPerformance",
    "AdGroupsStream",
    "AdListingGroupCriterionStream",
    "AudienceStream",
    "CampaignBudgetStream",
    "CampaignCriterionStream",
    "CampaignLabelStream",
    "CampaignPerformance",
    "CampaignPerformanceByAgeRangeAndDevice",
    "CampaignPerformanceByGenderAndDevice",
    "CampaignPerformanceByLocation",
    "CampaignsStream",
    "ClickViewReportStream",
    "GeoPerformance",
    "GeotargetsStream",
    "UserInterestStream",
    "CustomerLabelStream",
    "CustomerStream",
    "LabelStream",
]
