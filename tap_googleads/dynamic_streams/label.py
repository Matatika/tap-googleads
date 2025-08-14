"""LabelStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class LabelStream(DynamicQueryStream):
    """Label stream"""

    @property
    def gaql(self):
        return """
        SELECT 
          label.text_label.background_color, 
          label.text_label.description, 
          label.name, 
          label.status, 
          label.id, 
          label.resource_name 
        FROM label 
        """

    name = "label"
    primary_keys = ["label__id"]
    replication_key = None 