"""Apollo API client for org search, people search, and enrichment."""

from __future__ import annotations

import time
import requests

API_BASE = "https://api.apollo.io"


class ApolloClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Accept": "application/json",
            "X-Api-Key": api_key,
        }

    def _post(self, endpoint: str, payload: dict) -> dict:
        """POST to Apollo REST API using API key auth."""
        payload["api_key"] = self.api_key
        resp = requests.post(f"{API_BASE}{endpoint}", json=payload, headers=self.headers, timeout=30)
        if not resp.ok:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text[:200]
            raise Exception(f"{resp.status_code} {endpoint}: {detail}")
        return resp.json()

    def _people_api_search(
        self,
        organization_ids: list[str],
        titles: list[str],
        page: int,
        per_page: int,
    ) -> dict:
        """People API Search (master API key). Query params per Apollo docs — not mixed_people/search."""
        url = f"{API_BASE}/api/v1/mixed_people/api_search"
        params: list[tuple[str, str | int]] = []
        for oid in organization_ids:
            if oid:
                params.append(("organization_ids[]", str(oid)))
        for t in titles:
            if t and str(t).strip():
                params.append(("person_titles[]", str(t).strip()))
        params.append(("page", int(page)))
        safe_per = max(1, min(int(per_page), 100))
        params.append(("per_page", safe_per))

        resp = requests.post(url, headers=self.headers, params=params, timeout=45)
        if not resp.ok:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text[:200]
            raise Exception(f"{resp.status_code} /api/v1/mixed_people/api_search: {detail}")
        return resp.json()

    @staticmethod
    def _normalize_api_search_person(p: dict) -> dict:
        org = p.get("organization") or {}
        last = p.get("last_name") or p.get("last_name_obfuscated") or ""
        return {
            "id": p.get("id"),
            "first_name": p.get("first_name") or "",
            "last_name": last,
            "title": (p.get("title") or "") or "",
            "organization_name": org.get("name", ""),
            "linkedin_url": p.get("linkedin_url"),
        }

    def search_organizations(self, company_name: str) -> list[dict]:
        """Search Apollo for an organization by name. Returns list of org matches."""
        data = self._post("/api/v1/mixed_companies/search", {
            "q_organization_name": company_name,
            "page": 1,
            "per_page": 5,
        })
        orgs = data.get("organizations", []) or data.get("accounts", []) or []
        return [
            {
                "id": org.get("id"),
                "name": org.get("name"),
                "domain": org.get("primary_domain") or org.get("domain"),
                "website": org.get("website_url"),
                "industry": org.get("industry"),
                "employees": org.get("estimated_num_employees"),
                "city": org.get("city"),
                "state": org.get("state"),
            }
            for org in orgs
        ]

    def search_people(
        self,
        organization_ids: list[str],
        titles: list[str],
        page: int = 1,
        per_page: int = 25,
    ) -> dict:
        """Search people at orgs by title via People API Search (mixed_people/api_search)."""
        per = max(1, min(int(per_page), 100))
        data = self._people_api_search(organization_ids, titles, page, per)
        people_raw = data.get("people", []) or []
        total = int(data.get("total_entries", 0) or 0)
        total_pages = max(1, (total + per - 1) // per) if total else 1

        return {
            "people": [self._normalize_api_search_person(p) for p in people_raw],
            "total": total,
            "total_pages": total_pages,
            "page": page,
        }

    def search_all_people(
        self,
        organization_ids: list[str],
        titles: list[str],
        max_pages: int = 5,
    ) -> list[dict]:
        """Search all pages of people results. Returns deduplicated list."""
        all_people = []
        seen_ids = set()

        for page in range(1, max_pages + 1):
            result = self.search_people(organization_ids, titles, page=page, per_page=100)
            for person in result["people"]:
                pid = person.get("id")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_people.append(person)

            if page >= result["total_pages"]:
                break
            time.sleep(0.5)

        return all_people

    def bulk_enrich(self, people: list[dict]) -> list[dict]:
        """Enrich up to 10 people. Returns enriched data with key fields only."""
        details = []
        for p in people[:10]:
            entry = {}
            if p.get("id"):
                entry["id"] = p["id"]
            if p.get("first_name"):
                entry["first_name"] = p["first_name"]
            if p.get("organization_name"):
                entry["organization_name"] = p["organization_name"]
            if p.get("linkedin_url"):
                entry["linkedin_url"] = p["linkedin_url"]
            details.append(entry)

        data = self._post("/api/v1/people/bulk_match", {"details": details})
        matches = data.get("matches", [])

        enriched = []
        for m in matches:
            if not m:
                continue
            enriched.append({
                "first_name": m.get("first_name", ""),
                "last_name": m.get("last_name", ""),
                "title": m.get("title", ""),
                "email": m.get("email"),
                "email_status": m.get("email_status", ""),
                "linkedin_url": m.get("linkedin_url", ""),
                "organization_name": (m.get("organization") or {}).get("name", ""),
            })

        return enriched

    def enrich_all(self, people: list[dict], delay: float = 1.0) -> list[dict]:
        """Enrich a list of people in batches of 10. Returns all enriched results."""
        all_enriched = []
        for i in range(0, len(people), 10):
            batch = people[i : i + 10]
            enriched = self.bulk_enrich(batch)
            all_enriched.extend(enriched)
            if i + 10 < len(people):
                time.sleep(delay)
        return all_enriched
