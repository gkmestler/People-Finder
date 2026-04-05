"""Core enrichment orchestration: ties Apollo search, enrichment, and output together."""

from __future__ import annotations

import logging
import time
from apollo_client import ApolloClient
from phone_store import phone_store

logger = logging.getLogger(__name__)


def _extract_phone(webhook_data: dict) -> str:
    """Extract the best phone number from a webhook payload."""
    person = webhook_data.get("person") or webhook_data

    phone_numbers = person.get("phone_numbers") or []
    if phone_numbers:
        for pn in phone_numbers:
            if isinstance(pn, dict):
                num = pn.get("sanitized_number") or pn.get("raw_number") or pn.get("number") or ""
            else:
                num = str(pn)
            if num:
                return num

    for field in ("sanitized_phone", "phone", "corporate_phone", "mobile_phone",
                  "direct_phone", "personal_phone", "home_phone", "work_phone"):
        val = person.get(field)
        if val and isinstance(val, str):
            return val

    org = person.get("organization") or {}
    for field in ("phone", "corporate_phone", "sanitized_phone"):
        val = org.get(field)
        if val and isinstance(val, str):
            return val
    primary = org.get("primary_phone") or {}
    if isinstance(primary, dict) and primary.get("number"):
        return primary["number"]

    def _find_phone_recursive(d):
        if isinstance(d, dict):
            for k, v in d.items():
                if "phone" in k.lower() and isinstance(v, str) and len(v) >= 7:
                    return v
                if isinstance(v, (dict, list)):
                    result = _find_phone_recursive(v)
                    if result:
                        return result
        elif isinstance(d, list):
            for item in d:
                result = _find_phone_recursive(item)
                if result:
                    return result
        return None

    return _find_phone_recursive(webhook_data) or ""


def preview_people_fields(
    total_matches: int,
    min_per_company: int,
    max_per_company: int,
) -> dict:
    """How many people preview/enrich will count for one org (same rules as run_enrichment)."""
    raw = max(0, int(total_matches))
    capped = min(raw, max_per_company)
    if capped < min_per_company:
        return {
            "people_count_raw": raw,
            "people_count_capped": capped,
            "people_count": 0,
            "skipped_below_min": True,
        }
    return {
        "people_count_raw": raw,
        "people_count_capped": capped,
        "people_count": capped,
        "skipped_below_min": False,
    }


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

    When include_phone=True and webhook_base_url is set, bulk_match is called
    with reveal_phone_number=true + webhook_url.  After each batch we wait for
    Apollo to POST phone data to our webhook, then merge it into the contacts.
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

        best_org = orgs[0]
        org_map[company] = best_org
        progress("org_search", f"Found: {best_org['name']} (ID: {best_org['id']})", pct)
        time.sleep(0.3)

    progress("org_search", f"Found {len(org_map)} orgs, {len(no_results)} with no results", 30)

    # --- Step 2: Search for people at each org ---
    progress("people_search", "Searching for matching people...", 30)

    all_people = []

    for i, (company, org_info) in enumerate(org_map.items()):
        pct = 30 + int((i / max(len(org_map), 1)) * 30)
        progress("people_search", f"Searching people at: {org_info['name']}", pct)

        try:
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

    # --- Step 3: Enrich in batches (with optional phone reveal) ---
    want_phone = include_phone and bool(webhook_base_url)
    phone_label = " + phone reveal" if want_phone else ""
    progress("enrichment", f"Enriching {total_people} contacts{phone_label}...", 60)

    for i in range(0, total_people, 10):
        batch = all_people[i : i + 10]
        batch_num = (i // 10) + 1
        total_batches = (total_people + 9) // 10
        pct = 60 + int((i / total_people) * 35)

        progress("enrichment", f"Enriching batch {batch_num}/{total_batches}...", pct)

        # Collect person IDs for this batch so we can match webhook results
        batch_person_ids = [p.get("id") for p in batch if p.get("id")]
        phone_job = None
        batch_webhook_url = None

        if want_phone and batch_person_ids:
            phone_job = phone_store.create_job(batch_person_ids, timeout=90.0)
            batch_webhook_url = f"{webhook_base_url.rstrip('/')}/api/webhook/phone/{phone_job.job_id}"
            logger.info(f"Batch {batch_num}: phone webhook → {batch_webhook_url}")

        try:
            enriched = apollo.bulk_enrich(
                batch,
                reveal_phone=want_phone,
                webhook_url=batch_webhook_url,
            )
            credits_used += len(enriched)

            for j, e in enumerate(enriched):
                if j < len(batch):
                    e["_company_input"] = batch[j].get("_company_input", "")
                    if not e.get("organization_name"):
                        e["organization_name"] = batch[j].get("_org_name", "")
            contacts.extend(enriched)

        except Exception as e:
            progress("enrichment", f"Error in batch {batch_num}: {e}", pct)
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

        # Wait for phone webhooks for this batch
        if phone_job:
            progress("enrichment", f"Batch {batch_num}/{total_batches}: waiting for phone data...", pct)
            phone_results = phone_job.wait()
            phone_store.remove_job(phone_job.job_id)

            merged = 0
            for contact in contacts:
                pid = contact.get("_person_id")
                if pid and pid in phone_results and not contact.get("phone_number"):
                    contact["phone_number"] = _extract_phone(phone_results[pid])
                    merged += 1
            logger.info(f"Batch {batch_num}: merged {merged}/{len(phone_results)} phone numbers")

        if i + 10 < total_people:
            time.sleep(1.0)

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
