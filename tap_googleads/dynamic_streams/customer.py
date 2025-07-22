"""Customer stream for Google Ads tap."""
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CustomerStream(DynamicQueryStream):
    """Customer stream"""

    def _get_gaql(self):
        return """
        SELECT
          customer.id,
          customer.auto_tagging_enabled,
          customer.descriptive_name,
          customer.currency_code,
          customer.final_url_suffix,
          customer.manager,
          customer.optimization_score,
          customer.pay_per_conversion_eligibility_failure_reasons,
          customer.test_account,
          customer.time_zone,
          customer.tracking_url_template,
          segments.date
        FROM customer
        """

    name = "customer"
    primary_keys = ["customer__id", "segments__date"]
    replication_key = "segments__date"
    replication_method = "INCREMENTAL"
    add_date_filter_to_query = True