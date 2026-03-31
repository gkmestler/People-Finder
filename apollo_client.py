"""Apollo API client for org search, people search, and enrichment."""

import time
import requests

API_BASE = "https://api.apollo.io"
MCP_BASE = "https://mcp.apollo.io"


class ApolloClient:
    def __init__(self, api_key: str, oauth_token: str = None):
        self.api_key = api_key
        self.oauth_token = oauth_token
        self.headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
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

    def _post_oauth(self, endpoint: str, payload: dict) -> dict:
        """POST to Apollo API using OAuth token auth."""
        if not self.oauth_token:
            raise Exception("OAuth token required for people search. Please connect your Apollo account.")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.oauth_token}",
        }
        # Try API base first, fall back to MCP base
        resp = requests.post(f"{API_BASE}{endpoint}", json=payload, headers=headers, timeout=30)
        if resp.status_code == 403:
            # Try MCP base as proxy
            resp = requests.post(f"{MCP_BASE}{endpoint}", json=payload, headers=headers, timeout=30)
        if not resp.ok:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text[:200]
            raise Exception(f"{resp.status_code} {endpoint}: {detail}")
        return resp.json()

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
        """Search for people at specific orgs with matching titles. Uses OAuth."""
        payload = {
            "organization_ids": organization_ids,
            "person_titles": titles,
            "page": page,
            "per_page": per_page,
        }
        data = self._post_oauth("/api/v1/mixed_people/search", payload)
        people = data.get("people", [])
        pagination = data.get("pagination", {})
        return {
            "people": [
                {
                    "id": p.get("id"),
                    "first_name": p.get("first_name"),
                    "last_name": p.get("last_name"),
                    "title": p.get("title"),
                    "organization_name": (p.get("organization") or {}).get("name", ""),
                    "linkedin_url": p.get("linkedin_url"),
                }
                for p in people
            ],
            "total": pagination.get("total_entries", 0),
            "total_pages": pagination.get("total_pages", 1),
            "page": pagination.get("page", page),
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
                if person["id"] not in seen_ids:
                    seen_ids.add(person["id"])
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
