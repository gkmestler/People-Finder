"""Apollo OAuth 2.0 flow via mcp.apollo.io.

Tokens are stored in-memory (compatible with serverless / Vercel).
On serverless platforms, tokens won't persist across cold starts —
users may need to re-authenticate after idle periods.
"""

from __future__ import annotations

import os
import hashlib
import base64
import secrets
import time
import requests

MCP_BASE = "https://mcp.apollo.io"
REGISTRATION_URL = f"{MCP_BASE}/api/v1/oauth/applications/register_oauth_client"
AUTHORIZE_URL = f"{MCP_BASE}/mcp/oauth_metadata/redirect_to_authorize"
TOKEN_URL = f"{MCP_BASE}/api/v1/oauth/token"

SCOPES = "mixed_people_api_search"

# Refresh this many seconds before Apollo's expires_in to avoid edge 401s
_EXPIRY_SKEW_SEC = 120

# In-memory storage (works on both local and serverless)
_tokens: dict | None = None
_client: dict | None = None


def _get_redirect_uri() -> str:
    """Return the OAuth redirect URI, configurable via env for deployment."""
    return os.getenv("OAUTH_REDIRECT_URI", "http://localhost:5001/auth/callback")


def _api_key_fingerprint(api_key: str | None) -> str | None:
    if not api_key or not str(api_key).strip():
        return None
    return hashlib.sha256(str(api_key).strip().encode()).hexdigest()


def _attach_expiry(tokens: dict) -> dict:
    """Persist expires_at (unix time) from expires_in for proactive refresh."""
    try:
        expires_in = int(tokens.get("expires_in", 3600))
    except (TypeError, ValueError):
        expires_in = 3600
    tokens["expires_at"] = time.time() + max(60, expires_in) - _EXPIRY_SKEW_SEC
    return tokens


def _register_client() -> dict:
    """Dynamically register an OAuth client."""
    global _client
    if _client:
        return _client

    redirect_uri = _get_redirect_uri()
    resp = requests.post(REGISTRATION_URL, json={
        "client_name": "Apollo Enricher",
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "scope": SCOPES,
    }, timeout=15)
    resp.raise_for_status()
    _client = resp.json()
    return _client


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

    redirect_uri = _get_redirect_uri()
    params = {
        "response_type": "code",
        "client_id": client["client_id"],
        "redirect_uri": redirect_uri,
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
    global _tokens
    client = get_client()
    redirect_uri = _get_redirect_uri()
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client["client_id"],
        "code_verifier": code_verifier,
    }, timeout=15)
    resp.raise_for_status()
    tokens = _attach_expiry(resp.json())
    fp = _api_key_fingerprint(os.getenv("APOLLO_API_KEY"))
    if fp:
        tokens["api_key_fp"] = fp

    _tokens = tokens
    return tokens


def refresh_access_token() -> dict | None:
    """Refresh the access token using the refresh token."""
    global _tokens
    tokens = _tokens
    if not tokens or not tokens.get("refresh_token"):
        return None
    if not tokens_match_current_api_key(tokens):
        return None

    client = get_client()
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "client_id": client["client_id"],
    }, timeout=15)

    if not resp.ok:
        return None

    new_tokens = _attach_expiry(resp.json())
    if "refresh_token" not in new_tokens and tokens.get("refresh_token"):
        new_tokens["refresh_token"] = tokens["refresh_token"]
    if tokens.get("api_key_fp"):
        new_tokens["api_key_fp"] = tokens["api_key_fp"]

    _tokens = new_tokens
    return new_tokens


def get_access_token() -> str | None:
    """Get a valid access token, refreshing if expired or near expiry."""
    global _tokens
    tokens = _tokens
    if not tokens:
        return None
    if not tokens_match_current_api_key(tokens):
        return None
    exp = tokens.get("expires_at")
    if exp is not None:
        try:
            if time.time() >= float(exp):
                refreshed = refresh_access_token()
                if refreshed:
                    tokens = _tokens
        except (TypeError, ValueError):
            pass
    return tokens.get("access_token") if tokens else None


def tokens_match_current_api_key(tokens: dict) -> bool:
    """OAuth tokens are tied to the master API key in use; key change requires reconnect."""
    want = _api_key_fingerprint(os.getenv("APOLLO_API_KEY"))
    if not want:
        return False
    got = tokens.get("api_key_fp")
    if not got:
        return False
    return got == want


def is_authenticated() -> bool:
    """True if we have tokens issued for the current APOLLO_API_KEY."""
    tokens = _tokens
    if not tokens:
        return False
    if not tokens.get("access_token") and not tokens.get("refresh_token"):
        return False
    return tokens_match_current_api_key(tokens)


def clear_tokens() -> None:
    """Remove stored OAuth tokens (e.g. disconnect)."""
    global _tokens
    _tokens = None
