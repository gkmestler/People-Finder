"""Apollo Contact Enricher - Flask web app (Vercel-compatible)."""

import os
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, redirect, session

from dotenv import load_dotenv

from apollo_client import ApolloClient
from claude_client import expand_titles
from enrichment import run_enrichment
from excel_builder import build_spreadsheet
from phone_store import phone_store
import oauth

load_dotenv()

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logger.info("Creating Flask app...")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(32).hex())

logger.info("Flask app created successfully")


def get_apollo_client():
    """Create an ApolloClient (People API Search uses master API key only)."""
    api_key = (os.getenv("APOLLO_API_KEY") or "").strip()
    if not api_key:
        raise Exception("APOLLO_API_KEY not set")
    return ApolloClient(api_key)


def apollo_api_configured() -> bool:
    return bool((os.getenv("APOLLO_API_KEY") or "").strip())


def get_webhook_base_url() -> str:
    """Get the public base URL for webhooks."""
    # Allow override via env var (useful if Railway URL differs from request.host_url)
    override = (os.getenv("WEBHOOK_BASE_URL") or "").strip()
    if override:
        return override
    return request.host_url


@app.route("/health")
def health():
    return "ok", 200


@app.route("/")
def index():
    logger.info("Serving index page")
    return render_template("index.html", apollo_connected=apollo_api_configured())


# --- OAuth routes ---

@app.route("/auth/login")
def auth_login():
    """Redirect to Apollo OAuth authorization."""
    url, state, code_verifier = oauth.generate_auth_url()
    session["oauth_state"] = state
    session["oauth_code_verifier"] = code_verifier
    return redirect(url)


@app.route("/auth/callback")
def auth_callback():
    """Handle OAuth callback from Apollo."""
    error = request.args.get("error")
    if error:
        return f"OAuth error: {error} - {request.args.get('error_description', '')}", 400

    code = request.args.get("code")
    state = request.args.get("state")

    if not code:
        return "Missing authorization code", 400

    expected_state = session.get("oauth_state")
    if state != expected_state:
        return "Invalid state parameter", 400

    code_verifier = session.get("oauth_code_verifier")
    try:
        oauth.exchange_code(code, code_verifier)
    except Exception as e:
        return f"Token exchange failed: {e}", 500

    return redirect("/")


@app.route("/auth/logout")
def auth_logout():
    """Clear Apollo OAuth tokens (e.g. after rotating APOLLO_API_KEY)."""
    oauth.clear_tokens()
    session.clear()
    return redirect("/")


@app.route("/auth/status")
def auth_status():
    """Whether Apollo API key is set (People API Search / enrichment)."""
    return jsonify({"connected": apollo_api_configured()})


# --- Webhook routes ---

@app.route("/api/webhook/phone", methods=["POST"])
def webhook_phone():
    """Receive phone number data from Apollo's async phone reveal."""
    job_id = request.args.get("job_id")
    if not job_id:
        return jsonify({"error": "Missing job_id"}), 400

    job = phone_store.get_job(job_id)
    if not job:
        logger.warning(f"Phone webhook received for unknown job: {job_id}")
        return jsonify({"status": "ignored"}), 200

    data = request.json or {}
    person = data.get("person") or data
    person_id = person.get("id", "")

    if person_id:
        job.record_phone(person_id, person)
        logger.info(f"Phone webhook: recorded phone for person {person_id} (job {job_id})")
    else:
        logger.warning(f"Phone webhook: no person_id in payload for job {job_id}")

    return jsonify({"status": "ok"}), 200


# --- API routes ---

@app.route("/api/expand-titles", methods=["POST"])
def api_expand_titles():
    """Expand user titles using Claude API."""
    data = request.json
    titles = data.get("titles", [])
    if not titles:
        return jsonify({"error": "No titles provided"}), 400

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not set"}), 500

    try:
        result = expand_titles(api_key, titles)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/preview", methods=["POST"])
def api_preview():
    """Preview: search orgs and count people before enriching (no credits used)."""
    data = request.json
    companies = data.get("companies", [])
    titles = data.get("titles", [])

    if not companies or not titles:
        return jsonify({"error": "Companies and titles are required"}), 400

    try:
        apollo = get_apollo_client()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    results = []
    no_results = []

    for company in companies:
        try:
            orgs = apollo.search_organizations(company)
            if not orgs:
                no_results.append(company)
                continue

            org = orgs[0]
            people = apollo.search_people(
                organization_ids=[org["id"]],
                titles=titles,
                page=1,
                per_page=1,
            )
            results.append({
                "company_input": company,
                "org_name": org["name"],
                "org_id": org["id"],
                "domain": org.get("domain"),
                "people_count": people["total"],
            })
        except Exception as e:
            results.append({
                "company_input": company,
                "error": str(e),
            })

    try:
        max_per = int(data.get("max_per_company", 50) or 50)
    except (TypeError, ValueError):
        max_per = 50
    max_per = max(1, max_per)

    for r in results:
        if "people_count" in r:
            raw = r["people_count"]
            capped = min(raw, max_per)
            r["people_count_raw"] = raw
            r["people_count"] = capped

    total_people = sum(r.get("people_count", 0) for r in results)

    return jsonify({
        "results": results,
        "no_results": no_results,
        "total_people": total_people,
        "estimated_credits": total_people,
    })


@app.route("/api/enrich", methods=["POST"])
def api_enrich():
    """Run enrichment synchronously and return the Excel file directly."""
    data = request.json
    companies = data.get("companies", [])
    titles = data.get("titles", [])

    if not companies or not titles:
        return jsonify({"error": "Companies and titles are required"}), 400

    try:
        apollo = get_apollo_client()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    include_phone = bool(data.get("include_phone", False))
    webhook_base_url = get_webhook_base_url() if include_phone else None

    try:
        result = run_enrichment(
            apollo, companies, titles,
            max_per_company=data.get("max_per_company", 50),
            include_phone=include_phone,
            webhook_base_url=webhook_base_url,
        )

        # Build spreadsheet in memory
        buf = build_spreadsheet(result["contacts"], result["no_results"], include_phone=include_phone)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"apollo_contacts_{timestamp}.xlsx"

        return send_file(
            buf,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("\n  Apollo Contact Enricher")
    print("  http://localhost:5001\n")
    app.run(debug=True, port=5001, threaded=True)
