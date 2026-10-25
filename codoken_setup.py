"""
Codoken setup — onboarding wizard and stack doctor.

Usage:
    python codoken_setup.py                   interactive onboarding wizard
    python codoken_setup.py --defaults        non-interactive: write current effective values
    python codoken_setup.py check             doctor: packages, broker, auth, LLM, container engine
    python codoken_setup.py print-env         shell export lines for the effective values (CI / .zshrc)
    python codoken_setup.py service install   install a launchd (macOS) or systemd (Linux) service
    python codoken_setup.py service uninstall remove the service

The wizard:
  1. Asks which role(s) to run: worker, validator, or both
  2. Signs up or signs in with email + password (Firebase)
  3. Detects the LLM backend (worker path)
  4. Detects Docker/Podman and pulls runtime images (validator path)
  5. Writes ~/.codoken/config.json
  6. Verifies the connection to the broker
  7. Optionally installs a background service
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

import click

import codoken_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
BROKER_DEFAULT = "wss://broker.codoken.dev"
USER_CONFIG = Path.home() / ".codoken" / "config.json"

RUNTIME_IMAGES = [
    "python:3.11-slim",
    "node:18-slim",                   # javascript (typescript builds on top)
    "gcc:latest",                     # cpp
    "golang:alpine",                  # go
    "rust:latest",                    # rust
    "php:8.2-cli",                    # php
    "ruby:3.2-slim",                  # ruby
    "eclipse-temurin:21-jdk-alpine",  # java
]

# Images built locally from a base (not pulled from a registry).
# The validator builds these automatically on first start.
RUNTIME_BUILD_IMAGES = {
    "codoken-ts:latest": {
        "base": "node:18-slim",
        "description": "TypeScript (ts-node pre-installed)",
    },
}


def _http_base(broker_base: str) -> str:
    base = broker_base.rstrip("/")
    if base.startswith("wss://"):
        return "https://" + base[len("wss://"):]
    if base.startswith("ws://"):
        return "http://" + base[len("ws://"):]
    return base


def _web_config_api_key() -> Optional[str]:
    import codoken_auth
    cfg = codoken_auth._load_web_config()
    return cfg["apiKey"] if cfg else None


CLI_AUTH_URL = "https://codoken.io/cli-auth.html"
_AUTH_TIMEOUT = 120  # seconds to wait for browser callback


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


async def _browser_auth() -> dict:
    """
    Open cli-auth.html in the browser, spin up a localhost HTTP server to receive
    the Firebase token, return {"idToken", "refreshToken", "uid", "email"}.
    Falls back to manual token paste if the browser can't be opened or times out.
    """
    port  = _free_port()
    nonce = secrets.token_urlsafe(16)
    url   = f"{CLI_AUTH_URL}?port={port}&nonce={nonce}"

    result: dict = {}
    event = threading.Event()

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *_): pass  # silence request logs

        def do_POST(self):
            try:
                length = int(self.headers.get('Content-Length', 0))
                body   = self.rfile.read(length)
                data   = json.loads(body)
            except Exception:
                self.send_response(400); self.end_headers()
                return
            if data.get('nonce') != nonce:
                self.send_response(403); self.end_headers()
                return
            result.update(data)
            # CORS: the page is on codoken.io, server is localhost
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', 'https://codoken.io')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
            event.set()

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header('Access-Control-Allow-Origin', 'https://codoken.io')
            self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()

    server = HTTPServer(('127.0.0.1', port), _Handler)
    server.timeout = 1  # unblock serve_forever polling

    def _serve():
        while not event.is_set():
            server.handle_request()
        server.server_close()

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()

    opened = webbrowser.open(url)
    if opened:
        click.echo(f"  Opening browser → {CLI_AUTH_URL}")
        click.echo("  Sign in with Google or email, then return here.")
    else:
        click.echo(f"  Could not open browser automatically.")
        click.echo(f"  Open this URL manually:\n\n    {url}\n")

    # Wait up to _AUTH_TIMEOUT seconds
    click.echo(f"  Waiting for sign-in (timeout {_AUTH_TIMEOUT}s)…", nl=False)
    waited = 0
    while not event.is_set() and waited < _AUTH_TIMEOUT:
        await asyncio.sleep(1)
        waited += 1
        if waited % 10 == 0:
            click.echo(".", nl=False)
    click.echo("")

    if not result:
        # Timeout — offer manual paste fallback
        click.echo(
            "\n  Timed out waiting for browser.\n"
            f"  Open this URL and sign in, then paste the token shown on the page:\n\n"
            f"    {CLI_AUTH_URL}\n"
        )
        id_token = click.prompt("  Paste ID token").strip()
        if not id_token:
            click.echo("[ERROR] No token provided.")
            sys.exit(1)
        result = {"idToken": id_token, "refreshToken": "", "uid": "", "email": ""}

    return result


# ---------------------------------------------------------------------------
# Wizard
# ---------------------------------------------------------------------------
async def wizard(defaults: bool) -> None:
    click.echo("\n=== Codoken Setup Wizard ===")
    click.echo("This wizard configures your Codoken node in about 2 minutes.")
    click.echo("You can re-run it at any time to change settings.\n")

    # -- Role --
    if defaults:
        role_input = codoken_config.effective("role") or "worker,validator"
    else:
        click.echo("Roles:")
        click.echo("  worker    — your machine runs a local LLM (Ollama, LM Studio, llama.cpp)")
        click.echo("              and generates code for tasks submitted by developers.")
        click.echo("  validator — your machine runs sandboxed containers (Docker/Podman)")
        click.echo("              and independently verifies generated code.")
        click.echo("  both      — run both roles on this machine (recommended if you have")
        click.echo("              both an LLM and a container engine installed).\n")
        role_input = click.prompt(
            "Role", default="both",
        ).strip().lower()
        if role_input == "both":
            role_input = "worker,validator"

    roles = {r.strip() for r in role_input.split(",") if r.strip()}

    # -- Email / password --
    api_key = _web_config_api_key()
    if not api_key:
        click.echo("\nfirebase-web-config.json not found — trying to fetch it from the broker...")
        import httpx as _httpx
        try:
            async with _httpx.AsyncClient(timeout=8) as http:
                r = await http.get(f"{_http_base(BROKER_DEFAULT)}/client-config")
                data = r.json()
            if data.get("apiKey"):
                dest = Path("firebase-web-config.json")
                dest.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
                click.echo(f"  Downloaded to {dest.resolve()}  ✓")
                api_key = data["apiKey"]
            else:
                raise ValueError(data.get("error", "no apiKey in response"))
        except Exception as exc:
            click.echo(
                f"  [ERROR] Could not fetch config from broker: {exc}\n"
                "  firebase-web-config.json is required before setup can proceed.\n"
                "  Get it from whoever invited you to Codoken, or from:\n"
                "  Firebase console -> Project settings -> Your apps -> SDK setup -> Config\n"
                "  Place firebase-web-config.json in this directory and re-run setup."
            )
            sys.exit(1)

    click.echo("\n-- Account --")
    click.echo("Sign in with your Codoken account (Google, email link, or email + password).")
    click.echo("Your credits and identity are tied to this account.\n")

    from codoken_auth import CodokenAuth, CodokenAuthError

    token_data = await _browser_auth()

    # Persist the token to ~/.codoken/auth.json
    USER_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    auth_path = USER_CONFIG.parent / "auth.json"
    auth_record = {
        "mode":          "firebase",
        "token":         token_data.get("idToken", ""),
        "refresh_token": token_data.get("refreshToken", ""),
        "uid":           token_data.get("uid", ""),
        "email":         token_data.get("email", ""),
    }
    with open(auth_path, "w", encoding="utf-8") as fh:
        json.dump(auth_record, fh, indent=2)
        fh.write("\n")
    try:
        os.chmod(auth_path, 0o600)
    except OSError:
        pass

    uid_preview = (token_data.get("uid") or "")[:12]
    email_label = token_data.get("email") or token_data.get("uid") or "unknown"
    click.echo(f"  Signed in as {email_label}  (uid {uid_preview}…)  ✓")

    # -- Worker: detect LLM backend --
    doc_model = None
    if "worker" in roles:
        click.echo("\n-- Worker: LLM backend --")
        click.echo("The worker needs a locally running LLM to generate code.")
        click.echo("Supported: Ollama (port 11434), LM Studio (port 1234), llama.cpp (port 8080).")
        import httpx
        ENDPOINTS = {
            "Ollama":    ("http://localhost:11434", "/api/tags"),
            "LM Studio": ("http://localhost:1234",  "/v1/models"),
            "llama.cpp": ("http://localhost:8080",  "/health"),
        }
        found_llm = []
        async with httpx.AsyncClient(timeout=2) as http:
            for name, (base, path) in ENDPOINTS.items():
                try:
                    r = await http.get(base + path)
                    if r.status_code < 500:
                        found_llm.append(name)
                except Exception:
                    pass
        if found_llm:
            click.echo(f"  Found: {', '.join(found_llm)}  ✓")
            # Try to auto-detect the first available model from the first backend
            detected_model = None
            first_base = list(ENDPOINTS.values())[list(ENDPOINTS.keys()).index(found_llm[0])]
            try:
                async with httpx.AsyncClient(timeout=2) as http:
                    name = found_llm[0]
                    base, _ = first_base
                    if name == "Ollama":
                        r = await http.get(f"{base}/api/tags")
                        models = r.json().get("models", [])
                        if models:
                            detected_model = models[0].get("name") or models[0].get("model")
                    else:
                        r = await http.get(f"{base}/v1/models")
                        data = r.json().get("data", [])
                        if data:
                            detected_model = data[0].get("id")
            except Exception:
                pass
            if detected_model:
                click.echo(f"  Detected model: {detected_model}")
                confirmed_model = click.prompt(
                    "  Model to use (Enter to confirm, or type a different name)",
                    default=detected_model,
                )
            else:
                click.echo("  Could not auto-detect model name.")
                confirmed_model = click.prompt(
                    "  Enter the model name your LLM backend is serving",
                    default="",
                )
            if confirmed_model.strip():
                doc_model = confirmed_model.strip()
            else:
                doc_model = None
        else:
            doc_model = None
            click.echo(
                "  [WARN] No local LLM detected — none of the above ports responded.\n"
                "  The worker will keep retrying every 5 s once started, so you can\n"
                "  install the LLM after setup and it will connect automatically.\n\n"
                "  Recommended options:\n"
                "    Ollama      https://ollama.ai          (free, CLI-first)\n"
                "    LM Studio   https://lmstudio.ai        (free, GUI, easy model download)\n"
                "    llama.cpp   https://github.com/ggerganov/llama.cpp  (advanced)\n\n"
                "  After installing, load any GGUF model and make sure the server is\n"
                "  running before starting the worker."
            )

    # -- Validator: detect container engine + pull images --
    if "validator" in roles:
        click.echo("\n-- Validator: container engine --")
        click.echo("The validator runs submitted code inside isolated containers.")
        click.echo("It needs Docker or Podman installed and running.")
        engine = None
        for cmd in ("podman", "docker"):
            if shutil.which(cmd):
                try:
                    subprocess.run([cmd, "info"], capture_output=True, check=True, timeout=5)
                    engine = cmd
                    click.echo(f"  Found: {cmd}  ✓")
                    break
                except Exception:
                    click.echo(f"  {cmd} is installed but the daemon is not running.")
                    # On Mac, offer to start podman machine automatically
                    if cmd == "podman" and sys.platform == "darwin":
                        if click.confirm("  Start Podman machine now?", default=True):
                            click.echo("  Starting Podman machine (this may take a minute)...")
                            try:
                                subprocess.run(["podman", "machine", "start"], check=True, timeout=120)
                                subprocess.run([cmd, "info"], capture_output=True, check=True, timeout=10)
                                engine = cmd
                                click.echo(f"  Podman machine started  ✓")
                            except Exception as e:
                                click.echo(f"  Could not start Podman machine: {e}\n" + _engine_start_hint(cmd))
                    else:
                        click.echo(_engine_start_hint(cmd))
        if not engine:
            if not shutil.which("podman") and not shutil.which("docker"):
                click.echo(
                    "  [WARN] No container engine found.\n"
                    + _engine_install_hint()
                )
            click.echo(
                "  The validator will fail to run tasks until a container engine is\n"
                "  running. Start it and then re-run:  python codoken_setup.py check"
            )
        else:
            click.echo(
                f"  Note (Windows): Docker Desktop must have your Temp directory\n"
                f"  drive listed under Settings -> Resources -> File Sharing,\n"
                f"  otherwise sandbox volume mounts will fail."
            ) if sys.platform == "win32" else None
            pull = click.confirm(
                f"\n  Pull the {len(RUNTIME_IMAGES)} required runtime images now?\n"
                f"  (~2 GB total, one-time download — tasks run faster after this)",
                default=True,
            )
            if pull:
                click.echo("  Pulling images (this may take several minutes on a slow connection)...")
                for image in RUNTIME_IMAGES:
                    click.echo(f"    {image} ...", nl=False)
                    try:
                        subprocess.run([engine, "pull", image], check=True, timeout=300,
                                       capture_output=True)
                        click.echo(" done")
                    except Exception as e:
                        click.echo(f" FAILED ({e})")
                        click.echo(f"    Retry manually: {engine} pull {image}")
                # Build custom images
                for tag, spec in RUNTIME_BUILD_IMAGES.items():
                    click.echo(f"    {tag} (building from {spec['base']}) ...", nl=False)
                    dockerfile = f"FROM {spec['base']}\nRUN npm install -g ts-node typescript @types/node --silent\n"
                    try:
                        import tempfile as _tmp, os as _os
                        with _tmp.TemporaryDirectory() as dctx:
                            df_path = _os.path.join(dctx, "Dockerfile")
                            with open(df_path, "w") as df:
                                df.write(dockerfile)
                            subprocess.run(
                                [engine, "build", "-t", tag, dctx],
                                check=True, timeout=300, capture_output=True,
                            )
                        click.echo(" done")
                    except Exception as e:
                        click.echo(f" FAILED ({e})")
                        click.echo(f"    The validator will retry building it on first start.")
            else:
                click.echo(
                    "  Skipped. Images will be pulled on demand when the first task\n"
                    "  arrives, which will make that task slow. Pull them later with:\n"
                    + "".join(f"    {engine} pull {img}\n" for img in RUNTIME_IMAGES)
                )

    # -- Write config --
    click.echo("\nWriting configuration...")
    USER_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "broker": BROKER_DEFAULT,
        "auth": "firebase",
        "role": role_input,
        "firebase_web_config": str(
            Path(os.environ.get("FIREBASE_WEB_CONFIG", "firebase-web-config.json")).resolve()
        ),
        "auth_store": str(USER_CONFIG.parent / "auth.json"),
        "worker_parallel": "2",
        "worker_model": doc_model or "",
    }
    with open(USER_CONFIG, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")
    try:
        os.chmod(USER_CONFIG, 0o600)
    except OSError:
        pass
    click.echo(f"  Saved {USER_CONFIG}")
    click.echo("  To change settings later, edit that file or re-run this wizard.")

    # -- Verify broker connection --
    click.echo("\nVerifying broker connection...")
    import httpx
    try:
        async with httpx.AsyncClient(timeout=8) as http:
            r = await http.get(f"{_http_base(BROKER_DEFAULT)}/health")
            body = r.json()
        if body.get("status") == "ok":
            click.echo(f"  Broker OK — spec {body.get('spec_version')}, "
                       f"auth={body.get('auth_mode')}  ✓")
        else:
            click.echo(f"  [WARN] Broker returned unexpected status: {body}")
    except Exception as exc:
        click.echo(
            f"  [WARN] Could not reach broker: {exc}\n"
            "  Your node will keep retrying. Check your internet connection\n"
            f"  or verify the broker is up:  curl {_http_base(BROKER_DEFAULT)}/health"
        )

    # -- Service install prompt --
    click.echo("")
    click.echo("Background service: if installed, your node starts automatically")
    click.echo("on login and restarts itself if it crashes.")
    service_installed = False
    if click.confirm("Install as a background service? (recommended)", default=True):
        await _service_install(roles, doc)
        service_installed = True
    else:
        click.echo("  Skipped. You can install it later with:  python codoken_setup.py service install")

    # -- Test broker connection + trigger welcome bonus --
    auth_mode = os.environ.get("CODOKEN_AUTH", "shared_secret").lower()
    if auth_mode == "firebase":
        click.echo("")
        click.echo("  Testing broker connection…", nl=False)
        try:
            balance = asyncio.run(_test_broker_connection())
            click.echo(f" connected!  Balance: {balance:.2f} CU")
        except Exception as e:
            click.echo(f" skipped ({e})")

    # -- Congratulations --
    click.echo("")
    click.echo("=" * 60)
    click.echo("")
    click.echo("  Congratulations — your node is ready!")
    click.echo("")
    click.echo("  Welcome to the Codoken local agent coding community.")
    click.echo("  Your machine is now part of a peer-to-peer network of")
    click.echo("  developers and contributors running local AI to generate")
    click.echo("  and verify real code, earning credits for every task.")
    click.echo("")
    if service_installed:
        click.echo("  Your node will start automatically on login.")
        click.echo("  Sit back — credits will accumulate while you work.")
    else:
        click.echo("  Start your node manually when you're ready:")
        if sys.platform == "win32":
            set_env = "set CODOKEN_AUTH=firebase &&"
        else:
            set_env = "CODOKEN_AUTH=firebase"
        if "worker" in roles:
            click.echo(f"    {set_env} python worker.py")
        if "validator" in roles:
            click.echo(f"    {set_env} python validator.py")
    click.echo("")
    click.echo("  Track your earnings:   https://codoken.io/dashboard.html")
    click.echo("  Useful commands:")
    click.echo("    python codoken_setup.py check            — verify everything")
    click.echo("    python codoken_setup.py service install  — set up auto-start")
    click.echo("    python codoken_setup.py                  — re-run this wizard")
    click.echo("")
    click.echo("=" * 60)
    click.echo("")


async def _test_broker_connection() -> float:
    """Connect to the broker as developer, receive the welcome message, return balance."""
    import websockets as _ws
    from codoken_auth import CodokenAuth, CodokenAuthError
    auth = CodokenAuth("developer")
    uri, headers = await auth.connect_config()
    async with _ws.connect(uri, additional_headers=headers, open_timeout=10) as ws:
        import asyncio as _asyncio
        raw = await _asyncio.wait_for(ws.recv(), timeout=10)
        msg = json.loads(raw)
        if msg.get("type") == "welcome":
            return float(msg.get("balance", 0.0))
    return 0.0


def codoken_auth_load_web_config():
    import codoken_auth
    return codoken_auth._load_web_config()


def _engine_install_hint() -> str:
    if sys.platform == "darwin":
        return (
            "  macOS — recommended: Podman Desktop (no licence fee)\n"
            "    brew install podman && podman machine init && podman machine start\n"
            "    or download Podman Desktop: https://podman-desktop.io\n"
            "  Alternative: Docker Desktop  https://www.docker.com/products/docker-desktop"
        )
    elif sys.platform == "win32":
        return (
            "  Windows — install one of:\n"
            "    Docker Desktop  https://www.docker.com/products/docker-desktop\n"
            "    Podman Desktop  https://podman-desktop.io\n"
            "  Both require WSL2. Enable it first:\n"
            "    wsl --install   (run in an admin PowerShell, then reboot)"
        )
    else:
        return (
            "  Linux — install Podman (rootless, no daemon needed):\n"
            "    Ubuntu/Debian: sudo apt install podman\n"
            "    Fedora/RHEL:   sudo dnf install podman\n"
            "  Or Docker:       sudo apt install docker.io && sudo usermod -aG docker $USER\n"
            "  (log out and back in after adding yourself to the docker group)"
        )


def _engine_start_hint(cmd: str) -> str:
    if cmd == "podman":
        if sys.platform == "darwin":
            return "    Start it: podman machine start\n"
        elif sys.platform.startswith("linux"):
            return "    It should start automatically; try: systemctl --user start podman.socket\n"
    elif cmd == "docker":
        if sys.platform == "darwin":
            return "    Start Docker Desktop from your Applications folder.\n"
        elif sys.platform == "win32":
            return "    Start Docker Desktop from the Start menu.\n"
        else:
            return "    sudo systemctl start docker\n"
    return ""


# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------
def _repo_python() -> str:
    return str(Path(sys.executable).resolve())


def _repo_dir() -> str:
    return str(Path(__file__).parent.resolve())


async def _service_install(roles: set[str], config: dict) -> None:
    if sys.platform == "darwin":
        await _launchd_install(roles, config)
    elif sys.platform.startswith("linux"):
        await _systemd_install(roles, config)
    elif sys.platform == "win32":
        click.echo(
            "  Windows auto-start is not yet automated.\n"
            "  Option 1 — Task Scheduler (built-in):\n"
            "    1. Open Task Scheduler -> Create Basic Task\n"
            "    2. Trigger: 'When I log on'\n"
            f"    3. Action: Start program -> {_repo_python()}\n"
            f"       Arguments: worker.py  (or validator.py)\n"
            f"       Start in:  {_repo_dir()}\n"
            "    4. Set CODOKEN_AUTH=firebase in the task's environment\n\n"
            "  Option 2 — NSSM (https://nssm.cc):\n"
            f"    nssm install CodokenWorker {_repo_python()} worker.py\n"
            f"    nssm set CodokenWorker AppDirectory {_repo_dir()}\n"
            "    nssm set CodokenWorker AppEnvironmentExtra CODOKEN_AUTH=firebase\n"
            "    nssm start CodokenWorker"
        )
    else:
        click.echo("  Auto-start service not supported on this platform. "
                   "Run worker.py / validator.py manually.")


def _launchd_plist(label: str, script: str, log_path: str, env: dict) -> str:
    env_xml = "\n".join(
        f"        <key>{k}</key>\n        <string>{v}</string>"
        for k, v in env.items()
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{_repo_python()}</string>
        <string>{script}</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
{env_xml}
    </dict>
    <key>WorkingDirectory</key>
    <string>{_repo_dir()}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{log_path}</string>
    <key>StandardErrorPath</key>
    <string>{log_path}</string>
</dict>
</plist>
"""


async def _launchd_install(roles: set[str], config: dict) -> None:
    agents_dir = Path.home() / "Library" / "LaunchAgents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path.home() / ".codoken" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    env = {
        "CODOKEN_AUTH": "firebase",
        "FIREBASE_WEB_CONFIG": config.get("firebase_web_config", ""),
        "CODOKEN_AUTH_STORE": config.get("auth_store", ""),
    }
    installed = []
    for role in ("worker", "validator"):
        if role not in roles:
            continue
        label = f"dev.codoken.{role}"
        script = str(Path(_repo_dir()) / f"{role}.py")
        log_path = str(log_dir / f"{role}.log")
        plist_path = agents_dir / f"{label}.plist"
        plist_path.write_text(_launchd_plist(label, script, log_path, env))
        try:
            subprocess.run(["launchctl", "unload", str(plist_path)],
                           capture_output=True)
            subprocess.run(["launchctl", "load", str(plist_path)], check=True)
            click.echo(f"  Installed and started: {label}")
            click.echo(f"  Logs: {log_path}")
            installed.append(label)
        except Exception as exc:
            click.echo(f"  [WARN] launchctl load failed: {exc}")
    if installed:
        click.echo("  Services will auto-start on next login.")


def _systemd_unit(role: str, script: str, env: dict) -> str:
    env_lines = "\n".join(f"Environment={k}={v}" for k, v in env.items())
    return f"""[Unit]
Description=Codoken {role.capitalize()} node
After=network.target

[Service]
Type=simple
WorkingDirectory={_repo_dir()}
ExecStart={_repo_python()} {script}
{env_lines}
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
"""


async def _systemd_install(roles: set[str], config: dict) -> None:
    systemd_dir = Path.home() / ".config" / "systemd" / "user"
    systemd_dir.mkdir(parents=True, exist_ok=True)
    env = {
        "CODOKEN_AUTH": "firebase",
        "FIREBASE_WEB_CONFIG": config.get("firebase_web_config", ""),
        "CODOKEN_AUTH_STORE": config.get("auth_store", ""),
    }
    for role in ("worker", "validator"):
        if role not in roles:
            continue
        script = str(Path(_repo_dir()) / f"{role}.py")
        unit_path = systemd_dir / f"codoken-{role}.service"
        unit_path.write_text(_systemd_unit(role, script, env))

    # Single daemon-reload after writing all units.
    try:
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    except Exception as exc:
        click.echo(f"  [WARN] daemon-reload failed: {exc}")

    for role in ("worker", "validator"):
        if role not in roles:
            continue
        svc = f"codoken-{role}.service"
        unit_path = systemd_dir / svc
        try:
            subprocess.run(["systemctl", "--user", "enable", "--now", svc], check=True)
            click.echo(f"  Installed and started: {svc}")
        except subprocess.CalledProcessError as exc:
            err = str(exc).lower()
            click.echo(f"  [WARN] systemctl failed: {exc}\n"
                       f"  Unit file written to {unit_path}")
            if "bus" in err or "dbus" in err or "connect" in err:
                click.echo(
                    "  TIP: On headless/SSH servers, run:\n"
                    f"       loginctl enable-linger {os.environ.get('USER', '$USER')}\n"
                    "  This allows user services to persist without an active login session.\n"
                    "  Then retry:  systemctl --user enable --now " + svc
                )
        except Exception as exc:
            click.echo(f"  [WARN] systemctl failed: {exc}\n"
                       f"  Unit file written to {unit_path}")


async def _service_uninstall() -> None:
    if sys.platform == "darwin":
        agents_dir = Path.home() / "Library" / "LaunchAgents"
        for role in ("worker", "validator"):
            label = f"dev.codoken.{role}"
            plist = agents_dir / f"{label}.plist"
            if plist.exists():
                subprocess.run(["launchctl", "unload", str(plist)], capture_output=True)
                plist.unlink()
                click.echo(f"  Removed {label}")
    elif sys.platform.startswith("linux"):
        for role in ("worker", "validator"):
            svc = f"codoken-{role}.service"
            subprocess.run(["systemctl", "--user", "disable", "--now", svc],
                           capture_output=True)
            unit = Path.home() / ".config" / "systemd" / "user" / svc
            if unit.exists():
                unit.unlink()
                click.echo(f"  Removed {svc}")
        subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)


# ---------------------------------------------------------------------------
# Doctor
# ---------------------------------------------------------------------------
class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.warnings: list[str] = []

    def _line(self, tag: str, name: str, detail: str = "") -> None:
        line = f"  [{tag}] {name}"
        if detail:
            line += f"  -> {detail}"
        print(line)

    def ok(self, name: str, detail: str = "") -> None:
        self._line("PASS", name, detail)

    def warn(self, name: str, detail: str = "", record: bool = True) -> None:
        self._line("WARN", name, detail)
        if record:
            self.warnings.append(name)

    def fail(self, name: str, detail: str = "") -> None:
        self._line("FAIL", name, detail)
        self.failures.append(name)


async def _probe_packages(rep: Report) -> None:
    core = {
        "websockets": "pip install websockets",
        "httpx":      "pip install httpx",
        "click":      "pip install click",
        "yaml":       "pip install PyYAML",
        "docker":     "pip install docker",
    }
    missing = [m for m in core if importlib.util.find_spec(m) is None]
    if missing:
        rep.fail("core packages",
                 ", ".join(missing) + " — " + "; ".join(core[m] for m in missing))
    else:
        rep.ok("core packages")


async def _probe_broker(rep: Report, broker_base: str, auth: str) -> dict | None:
    import httpx

    try:
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get(f"{_http_base(broker_base)}/health")
            body = r.json()
    except Exception as exc:  # noqa: BLE001
        rep.fail("broker reachable",
                 f"{broker_base} ({exc}) — is it running?")
        return None
    if body.get("status") != "ok":
        rep.fail("broker reachable", f"status={body.get('status')}")
        return None
    detail = (f"spec {body.get('spec_version')}, auth={body.get('auth_mode')}, "
              f"persistence={body.get('persistence')}, "
              f"{body.get('tracked_tasks', 0)} tracked task(s)")
    if body.get("auth_mode") != auth:
        rep.ok("broker reachable", detail)
        rep.warn("auth mode matches broker",
                 f"config says {auth!r}, broker runs {body.get('auth_mode')!r}")
    else:
        rep.ok("broker reachable", detail)
    return body


async def _probe_auth(rep: Report, broker_base: str) -> None:
    from codoken_auth import CodokenAuth, CodokenAuthError

    api_key = _web_config_api_key()
    if api_key is None:
        rep.fail("firebase web config",
                 "create firebase-web-config.json or set $FIREBASE_WEB_CONFIG")
        return
    rep.ok("firebase web config")

    # Use a throwaway store so the doctor never touches the real persisted identity.
    tmp_store = os.path.join(tempfile.mkdtemp(prefix="codoken-doctor-"), "auth.json")
    email = os.environ.get("CODOKEN_EMAIL")
    password = os.environ.get("CODOKEN_PASSWORD")
    instance = CodokenAuth("developer", broker_base=broker_base,
                           auth_store=tmp_store, email=email, password=password)
    try:
        uri, headers = await asyncio.wait_for(instance.connect_config(), 20)
        method = "email" if email else "anonymous"
        rep.ok(f"firebase sign-in ({method})", f"uid={(instance._uid or '?')[:12]}…")
    except Exception as exc:  # noqa: BLE001
        rep.fail("firebase sign-in", str(exc)[:120])
    finally:
        instance.stop()


async def _probe_llm(rep: Report) -> None:
    import httpx

    ENDPOINTS = {
        "ollama":    ("http://localhost:11434", "/api/tags"),
        "lmstudio":  ("http://localhost:1234",  "/v1/models"),
        "llamacpp":  ("http://localhost:8080",  "/health"),
    }
    found = []
    async with httpx.AsyncClient(timeout=1.5) as http:
        for name, (base, path) in ENDPOINTS.items():
            try:
                r = await http.get(base + path)
                if r.status_code < 500:
                    found.append(name)
            except Exception:
                continue
    if found:
        rep.ok("local LLM backend", ", ".join(found))
    else:
        rep.warn("local LLM backend",
                 "none of ollama :11434 / LM Studio :1234 / llama.cpp :8080 reachable")


def _probe_engine(rep: Report) -> None:
    for cmd in ("podman", "docker"):
        if not shutil.which(cmd):
            continue
        # Try subprocess first (works for both podman and docker regardless of socket path)
        try:
            subprocess.run([cmd, "info"], capture_output=True, check=True, timeout=5)
            rep.ok("container engine", f"{cmd} daemon responds")
            return
        except Exception:
            pass
        # Fallback: Docker SDK (picks up DOCKER_HOST / custom socket configs)
        try:
            import docker
            client = docker.from_env()
            client.ping()
            rep.ok("container engine", f"{cmd} daemon responds")
            return
        except Exception as exc:  # noqa: BLE001
            rep.warn("container engine",
                     f"{cmd} found but daemon not running ({exc!r})")
            return
    rep.warn("container engine",
             "neither podman nor docker on PATH — validator cannot run tests")


async def doctor(roles: str, no_network: bool) -> int:
    role_set = {r.strip() for r in roles.split(",") if r.strip()}
    if not role_set:
        role_set = set(codoken_config.ROLES)

    broker_base = codoken_config.effective("broker") or BROKER_DEFAULT
    auth = (codoken_config.effective("auth") or "shared_secret").lower()
    file = codoken_config.config_file()

    print("== Codoken setup check ==")
    print(f"  config file : {file if file else 'none (built-in defaults / environment)'}")
    print(f"  broker      : {broker_base}")
    print(f"  auth        : {auth}\n")

    rep = Report()
    print("environment")
    rep.ok(f"python {sys.version.split()[0]}")
    await _probe_packages(rep)

    if no_network:
        rep.warn("broker / auth probes", "skipped (--no-network)")
    else:
        print("broker")
        await _probe_broker(rep, broker_base, auth)
        if auth == "firebase":
            print("auth")
            await _probe_auth(rep, broker_base)

    print("node capabilities")
    if "worker" in role_set:
        await _probe_llm(rep)
    if "validator" in role_set:
        _probe_engine(rep)

    print()
    if rep.failures:
        print(f"SETUP CHECK: {len(rep.failures)} failure(s), {len(rep.warnings)} warning(s)")
        for name in rep.failures:
            print(f"  - {name}")
        return 1
    if rep.warnings:
        print(f"SETUP CHECK: all hard checks passed, {len(rep.warnings)} warning(s)")
        return 0
    print("SETUP CHECK: all checks passed")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def print_env() -> None:
    is_win = sys.platform == "win32"
    for key in codoken_config.KEYS:
        value = codoken_config.effective(key)
        if not value:
            continue
        env_name = codoken_config.KEYS[key][0]
        if is_win:
            print(f'set {env_name}={value}')
        else:
            print(f'export {env_name}="{value}"')


@click.group(invoke_without_command=True,
             context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--defaults", is_flag=True,
              help="non-interactive: write the current effective values")
@click.pass_context
def main(ctx: click.Context, defaults: bool) -> None:
    """Codoken setup: onboarding wizard, stack doctor, env printer, service manager."""
    if ctx.invoked_subcommand is None:
        asyncio.run(wizard(defaults))


@main.command()
@click.option("--role", default="worker,validator,developer",
              help="comma list from: broker,worker,validator,developer")
@click.option("--no-network", is_flag=True,
              help="skip broker + live auth probes")
def check(role: str, no_network: bool) -> None:
    """Doctor: packages, broker health, live auth, LLM endpoints, container engine."""
    sys.exit(asyncio.run(doctor(role, no_network)))


@main.command("print-env")
def print_env_cmd() -> None:
    """Print shell export lines for every effective preference."""
    print_env()


@main.command("reset-password")
@click.argument("email", required=False)
def reset_password_cmd(email: str | None) -> None:
    """Send a password-reset email to your registered address."""
    asyncio.run(_reset_password(email))


async def _reset_password(email: str | None) -> None:
    from codoken_auth import CodokenAuth, CodokenAuthError

    api_key = _web_config_api_key()
    if not api_key:
        click.echo(
            "[ERROR] firebase-web-config.json not found.\n"
            "Run the setup wizard first:  python codoken_setup.py"
        )
        sys.exit(1)

    if not email:
        email = click.prompt("Email address").strip()

    click.echo(f"Sending password reset email to {email}...")
    try:
        await CodokenAuth.send_password_reset(api_key, email)
        click.echo("  Done. Check your inbox (and spam folder) for a reset link.")
        click.echo("  After resetting, re-run:  python codoken_setup.py")
    except CodokenAuthError as exc:
        err = str(exc)
        if "EMAIL_NOT_FOUND" in err:
            click.echo(f"  [ERROR] No account found for {email}.")
        else:
            click.echo(f"  [ERROR] {exc}")
        sys.exit(1)


@main.group()
def service() -> None:
    """Manage the background service (launchd on macOS, systemd on Linux)."""


@service.command("install")
def service_install() -> None:
    """Install and start the Codoken node service."""
    cfg = codoken_config.load_file()
    role_input = cfg.get("role", "worker,validator")
    roles = {r.strip() for r in role_input.split(",") if r.strip()}
    asyncio.run(_service_install(roles, cfg))


@service.command("uninstall")
def service_uninstall() -> None:
    """Stop and remove the Codoken node service."""
    asyncio.run(_service_uninstall())


@main.command("logs")
@click.argument("role", required=False, default=None,
                metavar="[worker|validator|developer]")
@click.option("--lines", "-n", default=50, show_default=True,
              help="Number of lines to show (0 = follow live)")
def logs_cmd(role: str | None, lines: int) -> None:
    """Tail Codoken node logs from ~/.codoken/logs/."""
    log_dir = Path.home() / ".codoken" / "logs"
    if not log_dir.exists():
        click.echo("No logs found. Have you installed and started a node service?")
        click.echo(f"  Expected log directory: {log_dir}")
        return

    if role:
        files = [log_dir / f"{role}.log"]
    else:
        files = sorted(log_dir.glob("*.log"))

    if not files or not any(f.exists() for f in files):
        click.echo(f"No log files found in {log_dir}")
        return

    if lines == 0:
        # Follow mode — tail -f all matching files
        cmd = ["tail", "-f"] + [str(f) for f in files if f.exists()]
        try:
            subprocess.run(cmd)
        except KeyboardInterrupt:
            pass
    else:
        for f in files:
            if not f.exists():
                continue
            if len(files) > 1:
                click.echo(f"\n=== {f.name} ===")
            try:
                result = subprocess.run(
                    ["tail", "-n", str(lines), str(f)],
                    capture_output=True, text=True
                )
                click.echo(result.stdout, nl=False)
            except Exception as exc:
                click.echo(f"[WARN] Could not read {f}: {exc}")


if __name__ == '__main__':
    main()
