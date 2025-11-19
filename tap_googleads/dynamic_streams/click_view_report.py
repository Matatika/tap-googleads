"""ClickViewReportStream for Google Ads tap."""

from __future__ import annotations

import datetime
from functools import cached_property
from http import HTTPStatus

from singer_sdk import typing as th

from tap_googleads.client import ResumableAPIError
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class ClickViewReportStream(DynamicQueryStream):
    """Stream for click view reports."""

    date: datetime.date

    def __init__(self, *args, **kwargs) -> None:
        self.date = datetime.date.today() - datetime.timedelta(days=1)
        super().__init__(*args, **kwargs)

    @property
    def gaql(self):

        return f"""
        SELECT
          click_view.gclid,
          segments.date,
          segments.ad_network_type,
          customer.id,
          click_view.ad_group_ad,
          ad_group.id,
          ad_group.name,
          campaign.id,
          campaign.name,
          segments.click_type,
          segments.device,
          segments.slot,
          metrics.clicks,
          click_view.keyword,
          click_view.keyword_info.match_type
        FROM click_view
        WHERE segments.date = '{self.date.isoformat()}'
        """

    @cached_property
    def schema(self):
        schema = super().schema
        properties: dict = schema["properties"]
        properties.update(th.Property("date", th.DateType).to_dict())

        return schema

    name = "click_view_report"
    primary_keys = ["clickView__gclid"]
    replication_key = "date"

    def post_process(self, row, context):
        row["date"] = row["segments"].pop("date")

        if row.get("clickView", {}).get("keyword") is None:
            row["clickView"]["keyword"] = "UNSPECIFIED"
            row["clickView"]["keywordInfo"] = {"matchType": "UNSPECIFIED"}

        return super().post_process(row, context)

    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.

        """
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        return params

    def request_records(self, context):

        ninety_days_ago = datetime.date.today() - datetime.timedelta(days=90)
        start_value = datetime.date.fromisoformat(self.get_starting_replication_key_value(context))#context.get("bookmarks", {}).get(self.name, {}).get(self.replication_key)#self.get_starting_replication_key_value(context)
        if start_value < ninety_days_ago:
            start_date = ninety_days_ago
        else:
            start_date = start_value

        end_date = datetime.date.fromisoformat(self.config["end_date"])

        delta = end_date - start_date
        dates = (start_date + datetime.timedelta(days=i) for i in range(delta.days))

        for self.date in dates:
            self.logger.info(f"Requesting records for date: {self.date} | customer_id: {context.get('customer_id')}")
            records = super().request_records(context)

            if not records:
                self._increment_stream_state(
                    {"date": self.date.isoformat()}, context=self.context
                )
            yield from records
