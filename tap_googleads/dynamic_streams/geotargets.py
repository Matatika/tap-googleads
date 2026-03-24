"""GeotargetsStream for Google Ads tap."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, Iterable

from tap_googleads.dynamic_query_stream import DynamicQueryStream

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class GeotargetsStream(DynamicQueryStream):
    """Geotargets, worldwide, constant across all customers"""

    gaql = """
    SELECT 
        geo_target_constant.resource_name,
        geo_target_constant.canonical_name,
        geo_target_constant.country_code,
        geo_target_constant.id,
        geo_target_constant.name,
        geo_target_constant.status,
        geo_target_constant.target_type,
        geo_target_constant.parent_geo_target
    FROM geo_target_constant
    """
    name = "geo_target_constant"
    primary_keys = ["geoTargetConstant__id"]

    @cached_property
    def schema(self) -> dict:
        schema = super().schema
        schema["properties"]["parent_geo_target_id"] = {"type": ["integer", "null"]}
        return schema

    def post_process(self, row, context=None) -> dict | None:
        flattened_row = super().post_process(row, context)

        parent_geo_target = flattened_row.get("geoTargetConstant__parentGeoTarget")
        if isinstance(parent_geo_target, str) and "/" in parent_geo_target:
            flattened_row["parent_geo_target_id"] = int(parent_geo_target.rsplit("/", 1)[-1])
        else:
            flattened_row["parent_geo_target_id"] = None

        return flattened_row

    def get_records(self, context: Context) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from super().get_records(context)
        self.selected = False  # sync once only 
