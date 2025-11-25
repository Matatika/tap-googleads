"""Stream type classes for tap-googleads."""

from __future__ import annotations

from collections import defaultdict
from enum import Enum
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_googleads.client import GoogleAdsStream, ResumableAPIError

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record

SCHEMAS_DIR = Path(__file__).parent / "./schemas"


class AccessibleCustomers(GoogleAdsStream):
    """Accessible Customers."""

    rest_method = "GET"
    path = "/customers:listAccessibleCustomers"
    name = "accessible_customers"
    primary_keys = ["resourceNames"]
    schema = th.PropertiesList(
        th.Property("resourceNames", th.ArrayType(th.StringType)),
    ).to_dict()

    def generate_child_contexts(
            self,
            record: Record,
            context: Context | None,
    ) -> Iterable[Context | None]:
        """
        Generates child contexts from a given record and parent context.

        This method takes an input record and an optional parent context to produce
        one or more child contexts. The operation is aimed at further processing or
        analysis by organizing information into separate contextual pieces.

        Arguments:
            record: A Record instance representing the data to process.
            context: An optional Context instance representing the parent context.

        Yields:
            Context or None: Each generated child context derived from the input
            record and parent context.
        """
        for customer in record.get("resourceNames", []):
            customer_id = customer.split("/")[1]
            yield {"customer_id": customer_id}


class SkippedReason(Enum):
    """Reasons why a customer might be skipped"""

    NOT_IN_CONFIG = "Not specified in customer_id(s) config"
    MANAGER_ACCOUNT = "Manager account(s)"
    NOT_ENABLED = "Not enabled"

    def __str__(self):
        return self.value


# noinspection SqlNoDataSourceInspection
class CustomerHierarchyStream(GoogleAdsStream):
    """
    Customer Hierarchy.

    Inspiration from Google here
    https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy.

    This stream is strictly to be the Parent Stream, to let all Child Streams
    know when to query the down stream apps
    """

    records_jsonpath = "$.results[*]"
    name = "customer_hierarchy"
    primary_keys = ["customerClient__id"]
    parent_stream_type = AccessibleCustomers
    schema = th.PropertiesList(
        th.Property("customer_id", th.StringType),
        th.Property("parent_customer_id", th.StringType),
        th.Property(
            "customerClient",
            th.ObjectType(
                th.Property("resourceName", th.StringType),
                th.Property("clientCustomer", th.StringType),
                th.Property("level", th.StringType),
                th.Property("status", th.StringType),
                th.Property("timeZone", th.StringType),
                th.Property("manager", th.BooleanType),
                th.Property("descriptiveName", th.StringType),
                th.Property("currencyCode", th.StringType),
                th.Property("id", th.StringType),
            ),
        ),
    ).to_dict()

    seen_customer_ids = set()
    skipped_customer_ids = defaultdict(list)

    @property
    def gaql(self):
        return """
               SELECT customer_client.client_customer,
                      customer_client.level,
                      customer_client.status,
                      customer_client.manager,
                      customer_client.descriptive_name,
                      customer_client.currency_code,
                      customer_client.time_zone,
                      customer_client.id
               FROM customer_client
               """

    def get_records(self, context):
        yield from super().get_records(context)

        if self.skipped_customer_ids:
            self.logger.info("Some customers were skipped")
            for reason, customer_ids in self.skipped_customer_ids.items():
                self.logger.info("%s (%d): %s", reason, len(customer_ids), customer_ids)

        self.skipped_customer_ids.clear()

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            msg = self.response_error_message(response)
            raise ResumableAPIError(msg, response)

    def post_process(self, row, context=None):
        row = super().post_process(row, context)
        customer = row["customerClient"]
        customer_id = customer["id"]

        # sync only customers we haven't seen
        if customer_id in self.seen_customer_ids:
            return None

        self.seen_customer_ids.add(customer_id)

        if self.customer_ids and customer["id"] not in self.customer_ids:
            self.skipped_customer_ids[SkippedReason.NOT_IN_CONFIG].append(
                customer["id"]
            )
            return None

        return row

    def generate_child_contexts(
            self,
            record: Record,
            context: Context | None,
    ) -> Iterable[Context | None]:
        customer = record["customerClient"]
        customer_id = customer["id"]

        # sync only customers we haven't seen
        if customer_id in self.seen_customer_ids:
            return

        self.seen_customer_ids.add(customer_id)

        if customer["manager"]:
            self.skipped_customer_ids[SkippedReason.MANAGER_ACCOUNT].append(customer_id)
            return

        if customer["status"] != "ENABLED":
            self.skipped_customer_ids[SkippedReason.NOT_ENABLED].append(customer_id)
            return

        customer_context = {"customer_id": customer_id}

        # Add parent manager account id if this is a child
        if customer_id != context["customer_id"]:
            customer_context["parent_customer_id"] = context["customer_id"]

        yield customer_context


class ReportsStream(GoogleAdsStream):
    """Base class for all report streams."""
    parent_stream_type = CustomerHierarchyStream
