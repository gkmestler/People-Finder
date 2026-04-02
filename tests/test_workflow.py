"""Verify the app matches the intended workflow (companies → titles + Claude expand → min/max → preview → enrich)."""

from unittest.mock import MagicMock, patch

import pytest

from apollo_client import ApolloClient
from enrichment import run_enrichment, preview_people_fields


@pytest.fixture
def flask_client():
    import app as app_module

    app_module.app.config["TESTING"] = True
    with app_module.app.test_client() as c:
        yield c


def test_index_ui_has_target_companies_titles_expand_min_max_preview_enrich(flask_client):
    """UI exposes the full step flow the product describes."""
    r = flask_client.get("/")
    assert r.status_code == 200
    html = r.data.decode()
    assert "Target Companies" in html
    assert "Target Titles" in html
    assert "Expand with Claude" in html
    assert 'id="minPerCompany"' in html
    assert 'id="maxPerCompany"' in html
    assert "Preview" in html
    assert "Enrich All" in html
    assert "/api/expand-titles" in html
    assert "/api/preview" in html
    assert "/api/enrich" in html


def test_preview_payload_includes_min_max_from_frontend_contract():
    """Document: JS sends min_per_company and max_per_company with preview (see templates/index.html)."""
    # Static contract check without calling Apollo
    from pathlib import Path

    tpl = Path(__file__).resolve().parent.parent / "templates" / "index.html"
    text = tpl.read_text(encoding="utf-8")
    assert "min_per_company: getMinPerCompany()" in text
    assert "max_per_company: getMaxPerCompany()" in text


def test_enrichment_respects_max_per_company():
    apollo = MagicMock()
    apollo.search_organizations.return_value = [{"id": "org1", "name": "Acme Co"}]
    many = [{"id": f"p{i}", "first_name": "A", "last_name": str(i), "title": "VP"} for i in range(20)]
    apollo.search_all_people.return_value = many
    apollo.bulk_enrich.side_effect = lambda batch: batch

    out = run_enrichment(
        apollo,
        ["Acme"],
        ["VP"],
        min_per_company=1,
        max_per_company=3,
        on_progress=None,
    )

    apollo.search_all_people.assert_called()
    assert out["stats"]["people_found"] == 3
    assert len(out["contacts"]) == 3


def test_api_preview_respects_min_per_company(flask_client):
    import app as app_module

    apollo = MagicMock()
    apollo.search_organizations.return_value = [{"id": "1", "name": "Acme"}]
    apollo.search_people.return_value = {"total": 3, "people": []}

    with patch.object(app_module, "get_apollo_client", return_value=apollo):
        r = flask_client.post(
            "/api/preview",
            json={
                "companies": ["Acme"],
                "titles": ["VP"],
                "min_per_company": 5,
                "max_per_company": 50,
            },
        )
    assert r.status_code == 200
    data = r.get_json()
    assert data["total_people"] == 0
    assert data["estimated_credits"] == 0
    row = data["results"][0]
    assert row["skipped_below_min"] is True
    assert row["people_count"] == 0


def test_preview_people_fields_matches_enrich_cap_and_min():
    assert preview_people_fields(100, 1, 50) == {
        "people_count_raw": 100,
        "people_count_capped": 50,
        "people_count": 50,
        "skipped_below_min": False,
    }
    assert preview_people_fields(3, 5, 50) == {
        "people_count_raw": 3,
        "people_count_capped": 3,
        "people_count": 0,
        "skipped_below_min": True,
    }
    assert preview_people_fields(8, 8, 50) == {
        "people_count_raw": 8,
        "people_count_capped": 8,
        "people_count": 8,
        "skipped_below_min": False,
    }


def test_enrichment_skips_company_when_below_min():
    apollo = MagicMock()
    apollo.search_organizations.return_value = [{"id": "org1", "name": "Acme Co"}]
    apollo.search_all_people.return_value = [{"id": "p1", "first_name": "A", "last_name": "1", "title": "VP"}]
    apollo.bulk_enrich.side_effect = lambda batch: batch

    out = run_enrichment(
        apollo,
        ["Acme"],
        ["VP"],
        min_per_company=5,
        max_per_company=50,
        on_progress=None,
    )

    assert out["stats"]["people_found"] == 0
    assert out["contacts"] == []
    apollo.bulk_enrich.assert_not_called()


@patch("apollo_client.requests.post")
def test_search_people_uses_mixed_people_api_search(mock_post):
    """People search must hit api_search (API key), not mixed_people/search (often API_INACCESSIBLE)."""
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
