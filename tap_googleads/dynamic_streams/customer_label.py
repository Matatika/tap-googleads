"""CustomerLabelStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CustomerLabelStream(DynamicQueryStream):
    """Customer Label stream"""

    @property
    def gaql(self):
        return """
        SELECT
          customer.id,
          customer_label.resource_name,
          customer_label.customer,
          customer_label.label
        FROM customer_label
        """

    name = "customer_label"
    primary_keys = ["customerLabel__resourceName"]
