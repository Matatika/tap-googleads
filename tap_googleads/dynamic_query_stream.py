from __future__ import annotations

import fnmatch
from functools import cached_property
from typing import Any, Dict, List

import humps
import ijson
import requests
import sqlparse
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.helpers._flattening import flatten_record
from singer_sdk.pagination import SinglePagePaginator

from tap_googleads.streams import ReportsStream

DATE_TYPES = ("segments.date", "segments.month", "segments.quarter", "segments.week")


class DynamicQueryStream(ReportsStream):
    """Define dynamic query stream class."""

    add_date_filter_to_query = False

    def get_new_paginator(self):
        return SinglePagePaginator()

    def _request(self, prepared_request, context):
        response = self.requests_session.send(
            prepared_request,
            stream=True,
            timeout=self.timeout,
            allow_redirects=self.allow_redirects,
        )

        self.validate_response(response)
        self.logger.debug("Streaming response received successfully.")
        return response

    def parse_response(self, response):
        with response:
            response.raw.decode_content = True

            for batch in ijson.items(response.raw, "item"):
                yield from batch.get("results", [])

    @staticmethod
    def add_date_filter(fields, has_where_clause, query):
        """Add segments.date to the field list for schema generation."""
        if "segments.date" not in fields:
            fields.append("segments.date")

    def _cast_value(self, key: str, value: Any) -> Any:
        # Some values, notably campaign__id, are returned as strings, but the field
        # data type from the API is integer. This function casts the value to the correct type.
        if key in self.schema["properties"]:
            if self.schema["properties"][key]["type"][0] == "integer":
                return int(value)
        return value

    def _get_gaql(self):
        """Return the base GAQL query. Override this in subclasses."""
        raise NotImplementedError

    @property
    def gaql(self):
        """Return the GAQL query."""
        if not hasattr(self, "_gaql"):
            self._gaql = self._get_gaql()
        return self._gaql

    @gaql.setter
    def gaql(self, value):
        """Set the GAQL query."""
        self._gaql = value

    def _apply_date_filter_to_query(self, context):
        """Apply date filter to the query at request time."""
        if hasattr(self, "_date_filter_applied") and self._date_filter_applied:
            return

        start_date = self.get_starting_replication_key_value(context)
        if not start_date:
            start_date = self.start_date
        else:
            start_date = f"'{start_date}'"
        query = self.gaql
        if "WHERE" in query.upper():
            self.gaql = (
                query.rstrip()
                + f" AND segments.date >= {start_date} AND segments.date <= {self.end_date} ORDER BY segments.date ASC"
            )
        else:
            self.gaql = (
                query.rstrip()
                + f" WHERE segments.date >= {start_date} AND segments.date <= {self.end_date} ORDER BY segments.date ASC"
            )

        self._date_filter_applied = True

    def get_fields_metadata(self, fields: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get field metadata for gaql query columns.

        Issue Google API request to get detailed information on data type for gaql query columns.
        Uses direct REST API calls.

        Args:
            fields: List of columns for user defined query.

        Returns:
            dict: Field metadata for gaql query columns.
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
        headers = {
            "Content-Type": "application/json",
            "developer-token": self.config["developer_token"],
        }

        response = requests.post(
            base_url,
            json=payload,
            headers=headers,
            auth=self.authenticator,
        )

        if not response.ok:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

        response_data = response.json()
        return {item.get("name"): item for item in response_data.get("results", [])}

    @cached_property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the JSON schema for the stream.
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
            query_object = sqlparse.parse(self.gaql)[0]
        except ValueError:
            message = f"The GAQL query {self.name} failed. Validate your GAQL query with the Google Ads query validator. https://developers.google.com/google-ads/api/fields/v20/query_validator"
            raise ValueError(message)

        fields = []
        has_where_clause = False
        for token in query_object.tokens:
            if isinstance(token, sqlparse.sql.IdentifierList):
                fields = [field.strip() for field in token.value.split(",")]
            if isinstance(token, sqlparse.sql.Where):
                has_where_clause = True

        if self.add_date_filter_to_query:
            self.add_date_filter(fields, has_where_clause, query_object)

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

            # some fields are returned under a single JSON object for some reason -
            # update schema to reflect this
            if any(
                fnmatch.fnmatch(field, p)
                for p in [
                    "ad_group_ad.ad.*_ad.*",
                    "segments.keyword.info.*",
                    "ad_group_criterion.webpage.sample.*",
                ]
            ):
                field = field.rsplit(".", 1)[0]
                field_value = {"type": ["string", "null"]}

            # GAQL fields look like metrics.cost_micros and response looks like
            # {'metrics': {'costMicros': 1000000}} which gets converted to metrics__costMicros
            field_name = "__".join([humps.camelize(i) for i in field.split(".")])
            local_json_schema["properties"][field_name] = field_value

        # these are injected from context
        local_json_schema["properties"]["customer_id"] = {"type": ["string", "null"]}
        local_json_schema["properties"]["parent_customer_id"] = {"type": ["string", "null"]}

        return local_json_schema

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

    def prepare_request(self, context, next_page_token):
        """Prepare a request object for the API call."""
        if self.add_date_filter_to_query:
            self._apply_date_filter_to_query(context)

        return super().prepare_request(context, next_page_token)

