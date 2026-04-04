"""Core enrichment orchestration: ties Apollo search, enrichment, and output together."""

from __future__ import annotations

import logging
import time
from apollo_client import ApolloClient
from phone_store import phone_store

logger = logging.getLogger(__name__)


def _extract_phone(phone_data: dict) -> str:
    """Extract the best phone number from webhook phone data."""
    # Try phone_numbers array first
    phone_numbers = phone_data.get("phone_numbers") or []
    if phone_numbers:
        for pn in phone_numbers:
            num = pn.get("sanitized_number") or pn.get("raw_number") or ""
            if num:
                return num
    # Try direct fields
    for field in ("sanitized_phone", "phone", "corporate_phone", "mobile_phone"):
        val = phone_data.get(field)
        if val:
            return val
    # Try organization phone
    org = phone_data.get("organization") or {}
    org_phone = org.get("phone")
    if org_phone:
        return org_phone
    primary = org.get("primary_phone") or {}
    if primary.get("number"):
        return primary["number"]
    return ""


def run_enrichment(
    apollo: ApolloClient,
    companies: list[str],
    titles: list[str],
    max_per_company: int = 50,
    include_phone: bool = False,
    webhook_base_url: str | None = None,
    on_progress=None,
) -> dict:
    """Run the full enrichment pipeline.

    Args:
        apollo: ApolloClient instance
        companies: list of company names to search
        titles: list of expanded job titles to search for
        max_per_company: maximum people to keep per company
        include_phone: whether to reveal phone numbers
        webhook_base_url: public base URL for phone webhooks (e.g. https://myapp.up.railway.app/)
        on_progress: optional callback(step, message, pct) for progress updates

    Returns:
        dict with:
          - contacts: list of enriched contact dicts
          - no_results: list of company names with no Apollo results
          - org_map: dict of company -> org info
          - credits_used: total enrichment credits consumed
          - stats: summary stats
    """

    def progress(step, msg, pct=None):
        if on_progress:
            on_progress(step, msg, pct)

    contacts = []
    no_results = []
    org_map = {}
    credits_used = 0

    total_companies = len(companies)

    # --- Step 1: Search for organizations ---
    progress("org_search", f"Searching Apollo for {total_companies} companies...", 0)

    for i, company in enumerate(companies):
        pct = int((i / total_companies) * 30)
        progress("org_search", f"Searching for: {company}", pct)

        try:
            orgs = apollo.search_organizations(company)
        except Exception as e:
            progress("org_search", f"Error searching {company}: {e}", pct)
            no_results.append(company)
            continue

        if not orgs:
            no_results.append(company)
            progress("org_search", f"No results for: {company}", pct)
            continue

        # Use the first/best match
        best_org = orgs[0]
        org_map[company] = best_org
        progress("org_search", f"Found: {best_org['name']} (ID: {best_org['id']})", pct)
        time.sleep(0.3)

    progress("org_search", f"Found {len(org_map)} orgs, {len(no_results)} with no results", 30)

    # --- Step 2: Search for people at each org ---
    progress("people_search", "Searching for matching people...", 30)

    all_people = []  # list of dicts with org context

    for i, (company, org_info) in enumerate(org_map.items()):
        pct = 30 + int((i / max(len(org_map), 1)) * 30)
        progress("people_search", f"Searching people at: {org_info['name']}", pct)

        try:
            # Only fetch enough pages to cover max_per_company
            needed_pages = max((max_per_company + 99) // 100, 1)
            people = apollo.search_all_people(
                organization_ids=[org_info["id"]],
                titles=titles,
                max_pages=min(needed_pages, 5),
            )
        except Exception as e:
            progress("people_search", f"Error searching people at {company}: {e}", pct)
            continue

        people = people[:max_per_company]

        for p in people:
            p["_company_input"] = company
            p["_org_name"] = org_info["name"]
        all_people.extend(people)

        progress("people_search", f"Found {len(people)} people at {org_info['name']}", pct)
        time.sleep(0.3)

    total_people = len(all_people)
    progress("people_search", f"Found {total_people} total people to enrich", 60)

    if total_people == 0:
        return {
            "contacts": [],
            "no_results": no_results,
            "org_map": org_map,
            "credits_used": 0,
            "stats": {"companies_searched": total_companies, "orgs_found": len(org_map), "people_found": 0, "people_enriched": 0},
        }

    # --- Step 3: Enrich in batches ---
    progress("enrichment", f"Enriching {total_people} contacts ({total_people} credits)...", 60)

    for i in range(0, total_people, 10):
        batch = all_people[i : i + 10]
        batch_num = (i // 10) + 1
        total_batches = (total_people + 9) // 10
        pct = 60 + int((i / total_people) * 25)

        progress("enrichment", f"Enriching batch {batch_num}/{total_batches}...", pct)

        try:
            enriched = apollo.bulk_enrich(batch)
            credits_used += len(enriched)

            # Tag each enriched contact with the company input name
            for j, e in enumerate(enriched):
                if j < len(batch):
                    e["_company_input"] = batch[j].get("_company_input", "")
                    # Use org name from enrichment if available, otherwise from search
                    if not e.get("organization_name"):
                        e["organization_name"] = batch[j].get("_org_name", "")
            contacts.extend(enriched)

        except Exception as e:
            progress("enrichment", f"Error in batch {batch_num}: {e}", pct)
            # Add unenriched entries as fallback
            for p in batch:
                contacts.append({
                    "_person_id": p.get("id", ""),
                    "first_name": p.get("first_name", ""),
                    "last_name": p.get("last_name", "(enrichment failed)"),
                    "title": p.get("title", ""),
                    "email": None,
                    "email_status": "error",
                    "linkedin_url": p.get("linkedin_url", ""),
                    "organization_name": p.get("_org_name", ""),
                    "_company_input": p.get("_company_input", ""),
                    "phone_number": "",
                })

        if i + 10 < total_people:
            time.sleep(1.0)

    # --- Step 4: Phone number reveal (async via webhook) ---
    if include_phone and webhook_base_url:
        person_ids = [c["_person_id"] for c in contacts if c.get("_person_id")]

        if person_ids:
            progress("phone_reveal", f"Requesting phone numbers for {len(person_ids)} contacts...", 88)

            job = phone_store.create_job(person_ids, timeout=60.0)
            webhook_url = f"{webhook_base_url.rstrip('/')}/api/webhook/phone/{job.job_id}"
            logger.info(f"Phone reveal webhook URL: {webhook_url}")

            # Fire off phone reveal requests one at a time
            for i, pid in enumerate(person_ids):
                try:
                    apollo.reveal_phone(pid, webhook_url)
                except Exception as e:
                    logger.warning(f"Phone reveal failed for {pid}: {e}")
                if i < len(person_ids) - 1:
                    time.sleep(0.3)

            pct_start = 90
            progress("phone_reveal", f"Waiting for phone data (up to 60s)...", pct_start)

            # Wait for webhooks to arrive
            phone_results = job.wait()
            phone_store.remove_job(job.job_id)

            logger.info(f"Phone reveal: got {len(phone_results)}/{len(person_ids)} results")

            # Merge phone data into contacts
            for contact in contacts:
                pid = contact.get("_person_id")
                if pid and pid in phone_results:
                    contact["phone_number"] = _extract_phone(phone_results[pid])

            progress("phone_reveal", f"Got phone numbers for {len(phone_results)} contacts", 95)

    progress("done", f"Enriched {len(contacts)} contacts using {credits_used} credits", 100)

    return {
        "contacts": contacts,
        "no_results": no_results,
        "org_map": {k: v for k, v in org_map.items()},
        "credits_used": credits_used,
        "stats": {
            "companies_searched": total_companies,
            "orgs_found": len(org_map),
            "people_found": total_people,
            "people_enriched": len(contacts),
        },
    }
