"""Apollo OAuth 2.0 flow via mcp.apollo.io."""

from __future__ import annotations

import os
import json
import hashlib
import base64
import secrets
import requests

TOKEN_FILE = os.path.join(os.path.dirname(__file__), ".apollo_oauth_token.json")
CLIENT_FILE = os.path.join(os.path.dirname(__file__), ".apollo_oauth_client.json")

MCP_BASE = "https://mcp.apollo.io"
REGISTRATION_URL = f"{MCP_BASE}/api/v1/oauth/applications/register_oauth_client"
AUTHORIZE_URL = f"{MCP_BASE}/mcp/oauth_metadata/redirect_to_authorize"
TOKEN_URL = f"{MCP_BASE}/api/v1/oauth/token"

REDIRECT_URI = "http://localhost:5001/auth/callback"
SCOPES = "mixed_people_api_search"


def _register_client() -> dict:
    """Dynamically register an OAuth client."""
    if os.path.exists(CLIENT_FILE):
        with open(CLIENT_FILE) as f:
            return json.load(f)

    resp = requests.post(REGISTRATION_URL, json={
        "client_name": "Apollo Enricher Local",
        "redirect_uris": [REDIRECT_URI],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "scope": SCOPES,
    }, timeout=15)
    resp.raise_for_status()
    client = resp.json()

    with open(CLIENT_FILE, "w") as f:
        json.dump(client, f)
    return client


def get_client():
    """Get or register the OAuth client."""
    return _register_client()


def generate_auth_url() -> tuple[str, str, str]:
    """Generate authorization URL with PKCE. Returns (url, state, code_verifier)."""
    client = get_client()
    state = secrets.token_urlsafe(32)
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = (
        hashlib.sha256(code_verifier.encode())
        .digest()
    )
    code_challenge_b64 = base64.urlsafe_b64encode(code_challenge).rstrip(b"=").decode()

    params = {
        "response_type": "code",
        "client_id": client["client_id"],
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
        "code_challenge": code_challenge_b64,
        "code_challenge_method": "S256",
    }
    qs = "&".join(f"{k}={requests.utils.quote(str(v))}" for k, v in params.items())
    url = f"{AUTHORIZE_URL}?{qs}"
    return url, state, code_verifier


def exchange_code(code: str, code_verifier: str) -> dict:
    """Exchange authorization code for tokens."""
    client = get_client()
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": client["client_id"],
        "code_verifier": code_verifier,
    }, timeout=15)
    resp.raise_for_status()
    tokens = resp.json()

    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f)
    return tokens


def refresh_access_token() -> dict | None:
    """Refresh the access token using the refresh token."""
    tokens = load_tokens()
    if not tokens or not tokens.get("refresh_token"):
        return None

    client = get_client()
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "client_id": client["client_id"],
    }, timeout=15)

    if not resp.ok:
        return None

    new_tokens = resp.json()
    # Keep refresh token if not returned
    if "refresh_token" not in new_tokens and tokens.get("refresh_token"):
        new_tokens["refresh_token"] = tokens["refresh_token"]

    with open(TOKEN_FILE, "w") as f:
        json.dump(new_tokens, f)
    return new_tokens


def load_tokens() -> dict | None:
    """Load stored tokens."""
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE) as f:
        return json.load(f)


def get_access_token() -> str | None:
    """Get a valid access token, refreshing if needed."""
    tokens = load_tokens()
    if not tokens:
        return None
    return tokens.get("access_token")


def is_authenticated() -> bool:
    """Check if we have stored tokens."""
    return load_tokens() is not None
