"""AudienceStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AudienceStream(DynamicQueryStream):
    """Audience stream"""

    @property
    def gaql(self):
        return """
        SELECT
          customer.id,
          audience.description,
          audience.dimensions,
          audience.exclusion_dimension,
          audience.id,
          audience.name,
          audience.resource_name,
          audience.status
        FROM audience
        """

    name = "audience"
    primary_keys = ["customer__id", "audience__id"]
    replication_key = None 