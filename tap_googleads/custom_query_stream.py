from __future__ import annotations

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CustomQueryStream(DynamicQueryStream):
    """Define custom stream."""

    records_jsonpath = "$[*].results[*]"
    add_date_filter_to_query = True

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
        self._gaql = self._custom_query['query']
        self.name = self._custom_query['name']
        self.add_date_filter_to_query = self._custom_query['add_date_filter_to_query']      
        super().__init__(*args, **kwargs)
        
        self.replication_key = self._custom_query.get("replication_key")
        self._primary_keys = self._custom_query.get("primary_keys", [])
        self._replication_method = self._custom_query.get("replication_method")

    @property
    def gaql(self):
        return self._gaql

    @gaql.setter
    def gaql(self, value):
        self._gaql = value
