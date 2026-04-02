"""Apollo Contact Enricher - Flask web app."""

import os
import json
import uuid
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, redirect, session

from dotenv import load_dotenv

from apollo_client import ApolloClient
from claude_client import expand_titles
from enrichment import run_enrichment, preview_people_fields
from excel_builder import build_spreadsheet
import oauth

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(32).hex())
app.config["OUTPUT_DIR"] = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(app.config["OUTPUT_DIR"], exist_ok=True)

# In-memory job tracking
jobs = {}


def get_apollo_client():
    """Create an ApolloClient (People API Search uses master API key only)."""
    api_key = (os.getenv("APOLLO_API_KEY") or "").strip()
    if not api_key:
        raise Exception("APOLLO_API_KEY not set")
    return ApolloClient(api_key)


def apollo_api_configured() -> bool:
    return bool((os.getenv("APOLLO_API_KEY") or "").strip())


@app.route("/")
def index():
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
        min_per = int(data.get("min_per_company", 1) or 1)
    except (TypeError, ValueError):
        min_per = 1
    try:
        max_per = int(data.get("max_per_company", 50) or 50)
    except (TypeError, ValueError):
        max_per = 50
    min_per = max(1, min_per)
    max_per = max(1, max_per)

    for r in results:
        if "people_count" in r:
            fields = preview_people_fields(r["people_count"], min_per, max_per)
            r.update(fields)

    total_people = sum(r.get("people_count", 0) for r in results)

    return jsonify({
        "results": results,
        "no_results": no_results,
        "total_people": total_people,
        "estimated_credits": total_people,
    })


@app.route("/api/enrich", methods=["POST"])
def api_enrich():
    """Start the full enrichment job. Returns a job ID for polling."""
    data = request.json
    companies = data.get("companies", [])
    titles = data.get("titles", [])

    if not companies or not titles:
        return jsonify({"error": "Companies and titles are required"}), 400

    try:
        apollo = get_apollo_client()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "status": "running",
        "progress": 0,
        "step": "starting",
        "message": "Initializing...",
        "result": None,
        "error": None,
        "file_path": None,
    }

    def run_job():
        try:
            def on_progress(step, msg, pct):
                jobs[job_id]["step"] = step
                jobs[job_id]["message"] = msg
                if pct is not None:
                    jobs[job_id]["progress"] = pct

            result = run_enrichment(
                apollo, companies, titles,
                min_per_company=data.get("min_per_company", 1),
                max_per_company=data.get("max_per_company", 50),
                on_progress=on_progress,
            )

            # Build spreadsheet
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"apollo_contacts_{timestamp}.xlsx"
            filepath = os.path.join(app.config["OUTPUT_DIR"], filename)
            build_spreadsheet(result["contacts"], result["no_results"], filepath)

            jobs[job_id]["status"] = "done"
            jobs[job_id]["progress"] = 100
            jobs[job_id]["message"] = f"Done! {result['stats']['people_enriched']} contacts enriched."
            jobs[job_id]["result"] = result["stats"]
            jobs[job_id]["file_path"] = filepath
            jobs[job_id]["filename"] = filename

        except Exception as e:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)
            jobs[job_id]["message"] = f"Error: {e}"

    thread = threading.Thread(target=run_job, daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/api/job/<job_id>")
def api_job_status(job_id):
    """Poll job status."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/api/download/<job_id>")
def api_download(job_id):
    """Download the result spreadsheet."""
    job = jobs.get(job_id)
    if not job or not job.get("file_path"):
        return jsonify({"error": "File not ready"}), 404
    return send_file(job["file_path"], as_attachment=True, download_name=job.get("filename", "contacts.xlsx"))


if __name__ == "__main__":
    print("\n  Apollo Contact Enricher")
    print("  http://localhost:5001\n")
    app.run(debug=True, port=5001)
