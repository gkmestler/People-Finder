"""Verify the app matches the intended workflow (companies → titles + Claude expand → max → preview → enrich)."""

from unittest.mock import MagicMock, patch, call

import pytest

from apollo_client import ApolloClient
from enrichment import run_enrichment, _extract_phone


@pytest.fixture
def flask_client():
    import app as app_module

    app_module.app.config["TESTING"] = True
    with app_module.app.test_client() as c:
        yield c


def test_index_ui_has_target_companies_titles_expand_max_preview_enrich(flask_client):
    """UI exposes the full step flow."""
    r = flask_client.get("/")
    assert r.status_code == 200
    html = r.data.decode()
    assert "Target Companies" in html
    assert "Target Titles" in html
    assert "Expand with Claude" in html
    assert 'id="maxPerCompany"' in html
    assert "Preview" in html
    assert "Enrich All" in html
    assert "/api/expand-titles" in html
    assert "/api/preview" in html
    assert "/api/enrich" in html


def test_enrichment_respects_max_per_company():
    apollo = MagicMock()
    apollo.search_organizations.return_value = [{"id": "org1", "name": "Acme Co"}]
    many = [{"id": f"p{i}", "first_name": "A", "last_name": str(i), "title": "VP"} for i in range(20)]
    apollo.search_all_people.return_value = many
    apollo.bulk_enrich.side_effect = lambda batch, **kw: batch

    out = run_enrichment(
        apollo,
        ["Acme"],
        ["VP"],
        max_per_company=3,
        on_progress=None,
    )

    apollo.search_all_people.assert_called()
    assert out["stats"]["people_found"] == 3
    assert len(out["contacts"]) == 3


@patch("apollo_client.requests.post")
def test_search_people_uses_mixed_people_api_search(mock_post):
    """People search must hit api_search, not mixed_people/search."""
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "total_entries": 12,
        "people": [{"id": "1", "first_name": "A", "title": "CEO", "organization": {"name": "Acme"}}],
    }
    mock_post.return_value = mock_resp

    client = ApolloClient("test-key")
    out = client.search_people(["org-id"], ["CEO", "CFO"], page=2, per_page=10)

    mock_post.assert_called_once()
    url = mock_post.call_args[0][0]
    assert "/mixed_people/api_search" in url
    assert mock_post.call_args[1]["headers"]["X-Api-Key"] == "test-key"
    params = mock_post.call_args[1]["params"]
    assert ("organization_ids[]", "org-id") in params
    assert ("person_titles[]", "CEO") in params
    assert ("person_titles[]", "CFO") in params
    assert ("page", 2) in params
    assert ("per_page", 10) in params
    assert out["total"] == 12
    assert out["people"][0]["organization_name"] == "Acme"


@patch("apollo_client.requests.post")
def test_bulk_enrich_sends_reveal_phone_and_webhook(mock_post):
    """When reveal_phone=True, bulk_match must include query params."""
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "matches": [{"id": "p1", "first_name": "A", "last_name": "B", "title": "CEO", "email": "a@b.com"}],
    }
    mock_post.return_value = mock_resp

    client = ApolloClient("test-key")
    result = client.bulk_enrich(
        [{"id": "p1", "first_name": "A"}],
        reveal_phone=True,
        webhook_url="https://example.com/webhook/phone/abc123",
    )

    mock_post.assert_called_once()
    kw = mock_post.call_args[1]
    assert kw["params"]["reveal_phone_number"] == "true"
    assert "example.com" in kw["params"]["webhook_url"]
    assert len(result) == 1
    assert result[0]["_person_id"] == "p1"
    assert result[0]["phone_number"] == ""


@patch("apollo_client.requests.post")
def test_bulk_enrich_no_phone_params_by_default(mock_post):
    """Without reveal_phone, no phone query params should be sent."""
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {"matches": []}
    mock_post.return_value = mock_resp

    client = ApolloClient("test-key")
    client.bulk_enrich([{"id": "p1"}])

    kw = mock_post.call_args[1]
    assert kw.get("params") is None


def test_extract_phone_from_phone_numbers_array():
    data = {"person": {"id": "1", "phone_numbers": [{"sanitized_number": "+15551234567"}]}}
    assert _extract_phone(data) == "+15551234567"


def test_extract_phone_from_direct_field():
    data = {"person": {"id": "1", "sanitized_phone": "+15559876543"}}
    assert _extract_phone(data) == "+15559876543"


def test_extract_phone_empty_when_no_phone():
    data = {"person": {"id": "1", "first_name": "Bob"}}
    assert _extract_phone(data) == ""
