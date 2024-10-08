"""Utilities used in this module"""

from singer_sdk._singerlib import Catalog
from singer_sdk.helpers._catalog import (
    deselect_all_streams,
    set_catalog_stream_selected,
)

from tap_googleads.tap import TapGoogleAds

accessible_customer_return_data = {
    "resourceNames": ["customers/1234512345", "customers/5432154321"]
}

SINGER_MESSAGES = []


def accumulate_singer_messages(_, message):
    """function to collect singer library write_message in tests"""
    SINGER_MESSAGES.append(message)


def set_up_tap_with_custom_catalog(mock_config, stream_list):
    tap = TapGoogleAds(config=mock_config)
    # Run discovery
    tap.run_discovery()
    # Get catalog from tap
    catalog = Catalog.from_dict(tap.catalog_dict)
    # Reset and re-initialize with an input catalog
    deselect_all_streams(catalog=catalog)
    for stream in stream_list:
        set_catalog_stream_selected(
            catalog=catalog,
            stream_name=stream,
            selected=True,
        )
    # Initialise tap with new catalog
    return TapGoogleAds(config=mock_config, catalog=catalog.to_dict())
