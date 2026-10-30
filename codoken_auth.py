"""
Codoken client authentication helper (shared by worker.py / validator.py /
developer.py).

Two modes, selected per client with the same env var the broker honors:

  CODOKEN_AUTH=shared_secret   (default)  Devnet: the role's shared-secret
                                          token is passed as ?token=<secret>.
  CODOKEN_AUTH=firebase            Production: signs in with Firebase Auth
                                          via the REST API and presents the
                                          Firebase ID token as
                                          "Authorization: Bearer <id_token>"
                                          in the WebSocket upgrade headers; a
                                          background task re-sends a fresh
                                          token before expiry (the broker
                                          re-verifies it,
                                          action=refresh_token).

                                          Sign-in method is chosen by the
                                          credentials present:
                                          - $CODOKEN_EMAIL + $CODOKEN_PASSWORD
                                            (or persisted store) -> email/password
                                          - neither set -> anonymous (useful for
                                            the online-check test script only)

The Firebase web app config is resolved from:
  1. $FIREBASE_WEB_CONFIG -> path to a JSON file
  2. ./firebase-web-config.json
  3. $FIREBASE_API_KEY (+ optional $FIREBASE_APP_ID / $FIREBASE_PROJECT_ID)

The config file is the snippet the Firebase console prints for a web app:
  { "apiKey": "...", "appId": "...", "projectId": "codoken-68831" }

Identity stability: the refresh token is persisted to a local file
($CODOKEN_AUTH_STORE, default ./.codoken_auth.json — gitignored) so a
restarted client reuses the same Firebase user (same UID, same credits and
EMA) instead of minting a brand-new anonymous identity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Callable, Dict, Optional, Tuple

import httpx

log = logging.getLogger("codoken-auth")

# Must stay in sync with AUTH_TOKENS in broker.py (devnet mode only).
FALLBACK_TOKENS: Dict[str, str] = {
    "developer": "developer-secret-token-789",
    "worker":    "worker-secret-token-123",
    "validator": "validator-secret-token-456",
}

BROKER_BASE = os.environ.get("CODOKEN_BROKER_WS", "wss://broker.codoken.dev")
REFRESH_INTERVAL_S = 50 * 60  # ID tokens live ~1h; refresh well before expiry

FIREBASE_SIGNIN_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signUp"
    "?key={api_key}"
)
FIREBASE_SIGNIN_EMAIL_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
    "?key={api_key}"
)
FIREBASE_SIGNUP_EMAIL_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signUp"
    "?key={api_key}"
)
FIREBASE_RESET_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode"
    "?key={api_key}"
)
FIREBASE_REFRESH_URL = (
    "https://securetoken.googleapis.com/v1/token"
    "?key={api_key}"
)


class CodokenAuthError(RuntimeError):
    pass


def _load_web_config() -> Optional[Dict[str, str]]:
    """Resolve the Firebase web app config; None when not configured."""
    path = os.environ.get("FIREBASE_WEB_CONFIG") or "firebase-web-config.json"
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                cfg = json.load(fh)
        except Exception as exc:  # noqa: BLE001
            log.warning("could not parse %s: %s", path, exc)
            return None
        if cfg.get("apiKey"):
            return cfg
        log.warning("%s has no apiKey; ignoring", path)
    api_key = os.environ.get("FIREBASE_API_KEY")
    if api_key:
        return {
            "apiKey": api_key,
            "appId": os.environ.get("FIREBASE_APP_ID", ""),
            "projectId": os.environ.get("FIREBASE_PROJECT_ID", ""),
        }
    return None


class CodokenAuth:
    """Builds the connection credentials for one client role."""

    def __init__(self, role: str, broker_base: Optional[str] = None,
                 auth_store: Optional[str] = None,
                 email: Optional[str] = None,
                 password: Optional[str] = None) -> None:
        if role not in FALLBACK_TOKENS:
            raise CodokenAuthError(f"unknown role {role!r}")
        self.role = role
        self.base = (broker_base or BROKER_BASE).rstrip("/")
        self.mode = os.environ.get("CODOKEN_AUTH", "shared_secret").lower()
        self._auth_store = auth_store  # per-instance override
        # Email/password: explicit args win, then environment, then None (anonymous fallback)
        self._email: Optional[str] = email or os.environ.get("CODOKEN_EMAIL") or None
        self._password: Optional[str] = password or os.environ.get("CODOKEN_PASSWORD") or None
        self._cfg: Optional[Dict[str, str]] = None
        self._token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._uid: Optional[str] = None
        if self.mode == "firebase":
            self._cfg = _load_web_config()
            if self._cfg is None:
                log.warning(
                    "[CodokenAuth:%s] CODOKEN_AUTH=firebase but no web config "
                    "found ($FIREBASE_WEB_CONFIG / firebase-web-config.json / "
                    "FIREBASE_API_KEY); connection will fail auth", role,
                )

    def base_uri(self) -> str:
        return f"{self.base}/ws/{self.role}"

    async def connect_config(self) -> Tuple[str, Optional[Dict[str, str]]]:
        """(uri, extra-headers) for websockets.connect()."""
        if self.mode != "firebase":
            return self.base_uri() + f"?token={FALLBACK_TOKENS[self.role]}", None
        await self._ensure_token()
        log.info("[CodokenAuth:%s] firebase identity ready (uid=%s…)",
                 self.role, (self._uid or "?")[:12])
        return self.base_uri(), {"Authorization": f"Bearer {self._token}"}

    # ------------------------------------------------------------------
    # Firebase REST auth (no SDK required)
    # ------------------------------------------------------------------
    async def _ensure_token(self) -> None:
        """Sign in or restore a persisted session to get a valid ID token."""
        if self._cfg is None:
            raise CodokenAuthError(
                "CODOKEN_AUTH=firebase requires a web config: create "
                "firebase-web-config.json (Firebase console -> project -> "
                "Project settings -> Your apps) and enable the Email/Password "
                "sign-in method in Authentication."
            )
        api_key = self._cfg["apiKey"]
        persisted = self._load_persisted()
        if persisted and persisted.get("refresh_token"):
            try:
                await self._refresh_id_token(api_key, persisted["refresh_token"])
                log.info("[CodokenAuth:%s] restored persisted identity (uid=%s…)",
                         self.role, (self._uid or "?")[:12])
                self._persist()
                return
            except Exception as exc:  # noqa: BLE001
                log.warning("[CodokenAuth:%s] session restore failed (%s); "
                            "signing in fresh", self.role, exc)

        if self._email and self._password:
            await self._sign_in_email(api_key, self._email, self._password)
        else:
            raise CodokenAuthError(
                "CODOKEN_AUTH=firebase but no credentials found. "
                "Run the setup wizard first: python codoken_setup.py"
            )
        self._persist()

    async def _sign_in_anonymous(self, api_key: str) -> None:
        """Anonymous sign-in via Firebase Auth REST API."""
        url = FIREBASE_SIGNIN_URL.format(api_key=api_key)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json={"returnSecureToken": True})
        if r.status_code != 200:
            raise CodokenAuthError(
                f"Firebase anonymous sign-in failed ({r.status_code}): {r.text[:200]}"
            )
        data = r.json()
        self._token = data["idToken"]
        self._refresh_token = data["refreshToken"]
        self._uid = data.get("localId")

    async def _sign_in_email(self, api_key: str, email: str, password: str) -> None:
        """Email/password sign-in via Firebase Auth REST API."""
        url = FIREBASE_SIGNIN_EMAIL_URL.format(api_key=api_key)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json={
                "email": email,
                "password": password,
                "returnSecureToken": True,
            })
        if r.status_code != 200:
            raise CodokenAuthError(
                f"Firebase email sign-in failed ({r.status_code}): {r.text[:200]}"
            )
        data = r.json()
        self._token = data["idToken"]
        self._refresh_token = data["refreshToken"]
        self._uid = data.get("localId")
        self._email = data.get("email", email)

    @staticmethod
    async def send_signin_link(api_key: str, email: str, continue_url: str) -> None:
        """Send a passwordless email sign-in link via Firebase Auth REST API."""
        url = FIREBASE_RESET_URL.format(api_key=api_key)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json={
                "requestType": "EMAIL_SIGNIN",
                "email": email,
                "continueUrl": continue_url,
                "canHandleCodeInApp": False,
            })
        if r.status_code != 200:
            err = r.json().get("error", {})
            raise CodokenAuthError(
                f"Sign-in link failed: {err.get('message', r.text[:200])}"
            )

    @staticmethod
    async def send_password_reset(api_key: str, email: str) -> None:
        """Send a password-reset email via Firebase Auth REST API.
        Raises CodokenAuthError if the request fails."""
        url = FIREBASE_RESET_URL.format(api_key=api_key)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json={
                "requestType": "PASSWORD_RESET",
                "email": email,
            })
        if r.status_code != 200:
            err = r.json().get("error", {})
            raise CodokenAuthError(
                f"Password reset failed: {err.get('message', r.text[:200])}"
            )

    @staticmethod
    async def sign_up(api_key: str, email: str, password: str) -> dict:
        """Create a new Firebase account. Returns {uid, idToken, refreshToken}.
        Raises CodokenAuthError on failure (e.g. email already in use)."""
        url = FIREBASE_SIGNUP_EMAIL_URL.format(api_key=api_key)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json={
                "email": email,
                "password": password,
                "returnSecureToken": True,
            })
        if r.status_code != 200:
            err = r.json().get("error", {})
            raise CodokenAuthError(
                f"Firebase sign-up failed: {err.get('message', r.text[:200])}"
            )
        data = r.json()
        return {
            "uid": data.get("localId"),
            "idToken": data["idToken"],
            "refreshToken": data["refreshToken"],
            "email": data.get("email", email),
        }

    async def _refresh_id_token(self, api_key: str, refresh_token: str) -> None:
        """Exchange a refresh token for a fresh ID token."""
        url = FIREBASE_REFRESH_URL.format(api_key=api_key)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            })
        if r.status_code != 200:
            raise CodokenAuthError(
                f"Firebase token refresh failed ({r.status_code}): {r.text[:200]}"
            )
        data = r.json()
        self._token = data["id_token"]
        self._refresh_token = data["refresh_token"]
        self._uid = data.get("user_id")

    # ------------------------------------------------------------------
    # Local identity persistence (stable UID across restarts)
    # ------------------------------------------------------------------
    def _store_path(self) -> str:
        base = self._auth_store or os.environ.get("CODOKEN_AUTH_STORE") or ".codoken_auth.json"
        if not os.path.isabs(base):
            base = os.path.join(os.getcwd(), base)
        return base if base.endswith(".json") else os.path.join(base, f"auth-{self.role}.json")

    def _persist(self) -> None:
        try:
            path = self._store_path()
            try:
                with open(path, encoding="utf-8") as fh:
                    store = json.load(fh)
            except Exception:  # noqa: BLE001
                store = {}
            store[self.role] = {
                "uid": self._uid,
                "id_token": self._token,
                "refresh_token": self._refresh_token,
                "saved_at": time.time(),
            }
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(store, fh)
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
        except Exception as exc:  # noqa: BLE001
            log.warning("[CodokenAuth:%s] could not persist session: %s", self.role, exc)

    def _load_persisted(self) -> Optional[Dict[str, Any]]:
        try:
            with open(self._store_path(), encoding="utf-8") as fh:
                store = json.load(fh)
            return store.get(self.role)
        except Exception:  # noqa: BLE001
            return None

    # ------------------------------------------------------------------
    # Interactive re-authentication (browser flow, no setup wizard needed)
    # ------------------------------------------------------------------
    @staticmethod
    async def reauth(auth_store: Optional[str] = None) -> None:
        """Open a browser sign-in, persist the result, and return.

        Call this when _ensure_token raises CodokenAuthError due to missing
        credentials. Works exactly like the setup wizard auth step but without
        the rest of the wizard. After returning, re-create the CodokenAuth
        instance and retry connect_config().
        """
        import importlib
        import json as _json
        import pathlib
        import secrets
        import threading
        from http.server import BaseHTTPRequestHandler, HTTPServer
        import webbrowser

        CLI_AUTH_URL = "https://codoken.io/cli-auth.html"
        _AUTH_TIMEOUT = 120

        def _free_port() -> int:
            import socket
            with socket.socket() as s:
                s.bind(("127.0.0.1", 0))
                return s.getsockname()[1]

        port  = _free_port()
        nonce = secrets.token_urlsafe(16)
        url   = f"{CLI_AUTH_URL}?port={port}&nonce={nonce}"

        result: dict = {}
        event = threading.Event()

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass

            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    data   = _json.loads(self.rfile.read(length))
                except Exception:
                    self.send_response(400); self.end_headers(); return
                if data.get("nonce") != nonce:
                    self.send_response(403); self.end_headers(); return
                result.update(data)
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "https://codoken.io")
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":true}')
                event.set()

            def do_OPTIONS(self):
                self.send_response(204)
                self.send_header("Access-Control-Allow-Origin", "https://codoken.io")
                self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

        server = HTTPServer(("127.0.0.1", port), _Handler)
        server.timeout = 1

        def _serve():
            while not event.is_set():
                server.handle_request()
            server.server_close()

        threading.Thread(target=_serve, daemon=True).start()

        opened = webbrowser.open(url)
        if opened:
            print(f"\n  Opening browser → {CLI_AUTH_URL}")
            print("  Sign in with Google or email, then return here.")
        else:
            print(f"\n  Could not open browser automatically.")
            print(f"  Open this URL manually:\n\n    {url}\n")

        print(f"  Waiting for sign-in (timeout {_AUTH_TIMEOUT}s)…", end="", flush=True)
        waited = 0
        while not event.is_set() and waited < _AUTH_TIMEOUT:
            await asyncio.sleep(1)
            waited += 1
            if waited % 10 == 0:
                print(".", end="", flush=True)
        print("")

        if not result:
            print(
                f"\n  Timed out. Open this URL and sign in, then paste the token:\n\n"
                f"    {CLI_AUTH_URL}\n"
            )
            id_token = input("  Paste ID token: ").strip()
            if not id_token:
                raise CodokenAuthError("No token provided — re-authentication aborted.")
            result = {"idToken": id_token, "refreshToken": "", "uid": "", "email": ""}

        store_path = pathlib.Path(
            auth_store
            or os.environ.get("CODOKEN_AUTH_STORE")
            or (pathlib.Path.home() / ".codoken" / "auth.json")
        )
        store_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            existing = _json.loads(store_path.read_text()) if store_path.exists() else {}
        except Exception:
            existing = {}
        role_entry = {
            "uid":           result.get("uid", ""),
            "id_token":      result.get("idToken", ""),
            "refresh_token": result.get("refreshToken", ""),
            "email":         result.get("email", ""),
            "saved_at":      time.time(),
        }
        for role in ("worker", "validator", "developer"):
            existing[role] = role_entry
        store_path.write_text(_json.dumps(existing, indent=2))
        email_label = result.get("email") or (result.get("uid") or "")[:12] + "…"
        print(f"  Signed in as {email_label}  ✓\n")

    # ------------------------------------------------------------------
    # Periodic re-auth (keeps long-lived sessions verifiable)
    # ------------------------------------------------------------------
    async def start_refresh(self, send: Callable[[str], Any]) -> Optional[asyncio.Task]:
        if self.mode != "firebase" or self._cfg is None:
            return None

        api_key = self._cfg["apiKey"]

        async def _loop() -> None:
            while True:
                try:
                    await asyncio.sleep(REFRESH_INTERVAL_S)
                    if self._refresh_token:
                        await self._refresh_id_token(api_key, self._refresh_token)
                        self._persist()
                    if self._token:
                        await send(json.dumps({
                            "action": "refresh_token",
                            "token": self._token,
                        }))
                except asyncio.CancelledError:
                    return
                except Exception as exc:  # noqa: BLE001
                    log.warning("[CodokenAuth:%s] token refresh failed: %s", self.role, exc)

        self._refresh_task = asyncio.create_task(_loop())
        return self._refresh_task

    def stop(self) -> None:
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            self._refresh_task = None
