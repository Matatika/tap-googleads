from __future__ import annotations

from functools import cached_property
from typing import Any, Dict, List

import humps
import requests
import sqlparse
from singer_sdk.helpers._flattening import flatten_record

from tap_googleads.streams import ReportsStream

DATE_TYPES = ("segments.date", "segments.month", "segments.quarter", "segments.week")


class CustomQueryStream(ReportsStream):
    """Define custom stream."""

    records_jsonpath = "$.results[*]"
    primary_keys = []
    replication_key = None

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
        """
        self.custom_query = kwargs.pop("custom_query")
        self._query = self.custom_query["query"]
        self.name = self.custom_query["name"]
        super().__init__(*args, **kwargs)

    @property
    def gaql(self):
        return self._query

    @cached_property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        local_json_schema = {
            "type": "object",
            "properties": {},
            "additionalProperties": True,
        }

        google_datatype_mapping = {
            "STRING": "string",
            "MESSAGE": "string",
            "BOOLEAN": "boolean",
            "DATE": "string",
            "ENUM": "string",
            "INT64": "integer",
            "INT32": "integer",
            "DOUBLE": "number",
        }
        try:
            query_object = sqlparse.parse(self.custom_query["query"])[0]
        except ValueError:
            message = f"The custom GAQL query {self.custom_query['name']} failed. Validate your GAQL query with the Google Ads query validator. https://developers.google.com/google-ads/api/fields/v19/query_validator"
            raise Exception(message)

        fields = []
        has_where_clause = False
        for token in query_object.tokens:
            if isinstance(token, sqlparse.sql.IdentifierList):
                fields = [field.strip() for field in token.value.split(",")]
            if isinstance(token, sqlparse.sql.Where):
                has_where_clause = True

        date_filter = (
            f"segments.date >= {self.start_date} and segments.date <= {self.end_date}"
        )
        if "segments.date" not in fields:
            fields.append("segments.date")
        if has_where_clause:
            self._query = self._query + f" AND {date_filter}"
        else:
            self._query = self._query + f" WHERE {date_filter}"

        google_schema = self.get_fields_metadata(fields)

        for field in fields:
            node = google_schema[field]
            google_data_type = node.get("dataType", "")
            field_value = {
                "type": [
                    google_datatype_mapping.get(google_data_type, "string"),
                    "null",
                ]
            }

            if google_data_type == "DATE" and field in DATE_TYPES:
                field_value["format"] = "date"

            if google_data_type == "ENUM":
                field_value = {
                    "type": "string",
                    "enum": list(node.get("enumValues", [])),
                }

            if node.get("isRepeated", False):
                field_value = {"type": ["null", "array"], "items": field_value}

            # GAQL fields look like metrics.cost_micros and response looks like
            # {'metrics': {'costMicros': 1000000}} which gets converted to metrics__costMicros
            field_name = "__".join([humps.camelize(i) for i in field.split(".")])
            local_json_schema["properties"][field_name] = field_value
        # This is always present in the response
        local_json_schema["properties"]["customer_id"] = {"type": ["string", "null"]}
        return local_json_schema

    def _cast_value(self, key: str, value: Any) -> Any:
        # Some values, notably campaign__id, are returned as strings but the field
        # data type from the API is integer. This function casts the value to the correct type.
        if key in self.schema["properties"]:
            if self.schema["properties"][key]["type"][0] == "integer":
                return int(value)
        return value

    def post_process(  # noqa: PLR6301
        self,
        row,
        context=None,
    ) -> dict | None:
        flattened_row = flatten_record(
            record=row,
            flattened_schema=self.schema,
            max_level=2,
        )

        for key, value in flattened_row.items():
            flattened_row[key] = self._cast_value(key, value)

        return flattened_row

    def get_fields_metadata(self, fields: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get field metadata for custom query columns.

        Issue Google API request to get detailed information on data type for custom query columns.
        Uses direct REST API calls.

        Args:
            fields: List of columns for user defined query.

        Returns:
            dict: Field metadata for custom query columns.
        """
        base_url = f"{self.url_base}/googleAdsFields:search"

        fields_sql = ",".join([f"'{field}'" for field in fields])
        query = f"""
        SELECT
          name,
          data_type,
          enum_values,
          is_repeated
        WHERE name in ({fields_sql})
        """

        payload = {"query": query, "pageSize": len(fields)}

        self.authenticator.update_access_token()
        headers = {
            "Authorization": f"Bearer {self.authenticator.access_token}",
            "Content-Type": "application/json",
            "developer-token": self.config["developer_token"],
        }
        response = requests.post(base_url, json=payload, headers=headers)
        response.raise_for_status()

        response_data = response.json()
        return {item.get("name"): item for item in response_data.get("results", [])}
