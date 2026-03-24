from __future__ import annotations

import itertools

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CustomQueryStream(DynamicQueryStream):
    """Define custom stream."""

    date_filter_mode = "range"

    def __init__(self, *args, **kwargs) -> None:
        """
        Initializes an instance of the class, allowing customization through various
        positional and keyword arguments provided during instantiation.

        Args:
            tap: Singer Tap this stream belongs to.
            *args: Variable length positional arguments.
            **kwargs: Arbitrary keyword arguments for customization.

        Returns:
            None
        """
        self._custom_query = kwargs.pop("custom_query")
        self._gaql = self._custom_query["query"]
        self.name = self._custom_query["name"]
        self.date_filter_mode = self._custom_query.get(
            "date_filter_mode",
            "range" if self._custom_query.get("add_date_filter_to_query", True) else "none",
        )
        super().__init__(*args, **kwargs)

        self.replication_key = self._custom_query.get("replication_key")
        self._primary_keys = self._custom_query.get("primary_keys", [])
        self._replication_method = self._custom_query.get("replication_method")

    @property
    def gaql(self):
        return self._gaql

    def request_records(self, context):
        if self.date_filter_mode != "single_day":
            yield from super().request_records(context)
            return

        for request_date in self.iter_request_dates(context):
            self.request_date = request_date.isoformat()
            self.logger.info(
                "Requesting records for date: %s | customer_id: %s",
                self.request_date,
                context.get("customer_id"),
            )
            records = super().request_records(context)
            record = next(records, None)

            if not record:
                if self.replication_key:
                    self._increment_stream_state(
                        {self.replication_key: self.request_date},
                        context=self.context,
                    )
                continue

            yield from itertools.chain([record], records)
