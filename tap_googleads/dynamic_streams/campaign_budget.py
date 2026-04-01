"""CampaignBudgetStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignBudgetStream(DynamicQueryStream):
    """Campaign Budget stream"""

    @property
    def gaql(self):
        return f"""
        SELECT
          customer.resource_name,
          customer.id,
          campaign.resource_name,
          campaign.id,
          campaign_budget.aligned_bidding_strategy_id,
          campaign_budget.amount_micros,
          campaign_budget.delivery_method,
          campaign_budget.explicitly_shared,
          campaign_budget.has_recommended_budget,
          campaign_budget.id,
          campaign_budget.name,
          campaign_budget.period,
          campaign_budget.recommended_budget_amount_micros,
          campaign_budget.recommended_budget_estimated_change_weekly_clicks,
          campaign_budget.recommended_budget_estimated_change_weekly_cost_micros,
          campaign_budget.recommended_budget_estimated_change_weekly_interactions,
          campaign_budget.recommended_budget_estimated_change_weekly_views,
          campaign_budget.reference_count,
          campaign_budget.resource_name,
          campaign_budget.status,
          campaign_budget.total_amount_micros,
          campaign_budget.type,
          segments.date,
          segments.budget_campaign_association_status.campaign,
          segments.budget_campaign_association_status.status,
          metrics.all_conversions,
          metrics.all_conversions_from_interactions_rate,
          metrics.all_conversions_value,
          metrics.average_cost,
          metrics.average_cpc,
          metrics.average_cpe,
          metrics.average_cpm,
          metrics.trueview_average_cpv,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_from_interactions_rate,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.cost_per_all_conversions,
          metrics.cost_per_conversion,
          metrics.cross_device_conversions,
          metrics.ctr,
          metrics.engagement_rate,
          metrics.engagements,
          metrics.impressions,
          metrics.interaction_event_types,
          metrics.interaction_rate,
          metrics.interactions,
          metrics.value_per_all_conversions,
          metrics.value_per_conversion,
          metrics.video_trueview_view_rate,
          metrics.video_trueview_views,
          metrics.view_through_conversions
        FROM campaign_budget
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    name = "campaign_budget"
    primary_keys = ["customer__id", "campaign__id", "campaignBudget__id"]