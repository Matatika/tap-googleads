"""AdGroupsStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupsStream(DynamicQueryStream):
    """Define custom stream."""

    def _get_gaql(self):
        return """
       SELECT 
       ad_group.url_custom_parameters, 
       ad_group.type, 
       ad_group.tracking_url_template, 
       ad_group.targeting_setting.target_restrictions,
       ad_group.target_roas,
       ad_group.target_cpm_micros,
       ad_group.status,
       ad_group.target_cpa_micros,
       ad_group.resource_name,
       ad_group.percent_cpc_bid_micros,
       ad_group.name,
       ad_group.labels,
       ad_group.id,
       ad_group.final_url_suffix,
       ad_group.excluded_parent_asset_field_types,
       ad_group.effective_target_roas_source,
       ad_group.effective_target_roas,
       ad_group.effective_target_cpa_source,
       ad_group.effective_target_cpa_micros,
       ad_group.display_custom_bid_dimension,
       ad_group.cpv_bid_micros,
       ad_group.cpm_bid_micros,
       ad_group.cpc_bid_micros,
       ad_group.campaign,
       ad_group.base_ad_group,
       ad_group.ad_rotation_mode,
       campaign.resource_name,
       campaign.id,
       campaign.name,
       segments.date
       FROM ad_group 
       """

    name = "adgroups"
    primary_keys = ["adGroup__id", "segments__date"]
    replication_key = "segments__date"
    add_date_filter_to_query = True