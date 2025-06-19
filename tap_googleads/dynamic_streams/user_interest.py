"""UserInterestStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class UserInterestStream(DynamicQueryStream):
    """User Interest stream"""

    @property
    def gaql(self):
        return """
        SELECT
          user_interest.availabilities,
          user_interest.launched_to_all,
          user_interest.name,
          user_interest.resource_name,
          user_interest.taxonomy_type,
          user_interest.user_interest_id,
          user_interest.user_interest_parent
        FROM user_interest
        """

    name = "user_interest"
    primary_keys = ["userInterest__userInterestId"]
    replication_key = None 