import json
import re

import responses

from tap_googleads.tests.utils import set_up_tap_with_custom_catalog

CONFIG = {
    "oauth_credentials": {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh_token",
    },
    "developer_token": "test_developer_token",
    "start_date": "2025-01-01",
    "custom_queries": [
        {"name": "campaign_history", "query": "SELECT campaign.id FROM campaign"},
        {"name": "campaign_history", "query": "SELECT ad_group.id FROM ad_group"},
    ],
}

ROWS = {
    "customer_client": {
        "customerClient": {"id": "1", "manager": False, "status": "ENABLED"}
    },
    "campaign": {"campaign": {"id": "9"}},
    "ad_group": {"adGroup": {"id": "7"}},
}


def fields_metadata(request):
    names = re.findall(r"'([^']+)'", json.loads(request.body)["query"])
    results = [{"name": name, "dataType": "STRING"} for name in names]
    return 200, {}, json.dumps({"results": results})


def search(request):
    resource = re.search(r"FROM (\w+)", json.loads(request.body)["query"])[1]
    return 200, {}, json.dumps({"results": [ROWS[resource]]})


@responses.activate
def test_last_stream_of_a_name_is_synced(capsys):
    responses.post(
        re.compile(r".*/oauth2/v4/token.*"),
        json={"access_token": "token", "expires_in": 3600},
    )
    responses.get(
        re.compile(r".*/customers:listAccessibleCustomers"),
        json={"resourceNames": ["customers/1"]},
    )
    responses.add_callback(
        responses.POST,
        re.compile(r".*/googleAdsFields:search"),
        callback=fields_metadata,
    )
    responses.add_callback(
        responses.POST,
        re.compile(r".*/googleAds:search"),
        callback=search,
    )

    tap = set_up_tap_with_custom_catalog(CONFIG, ["campaign_history"])
    capsys.readouterr()
    tap.sync_all()

    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    records = [
        message["record"]
        for message in messages
        if message["type"] == "RECORD" and message["stream"] == "campaign_history"
    ]
    assert records == [{"adGroup__id": "7", "customer_id": "1"}]
