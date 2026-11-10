import asyncio
import hashlib
import json
import math
import os
import socket
import sys
import subprocess
import tempfile
from pathlib import Path
from typing import Optional
import docker
import websockets
from websockets.exceptions import ConnectionClosed as WsConnectionClosed, InvalidStatus

import codoken_config

# Layered preferences (.codoken/config.json -> env). Must run before
# codoken_auth is imported: it captures $CODOKEN_BROKER_WS at import time.
codoken_config.apply_to_env("validator")

from codoken_auth import CodokenAuth, CodokenAuthError, CodokenNetworkError  # noqa: E402

# ==========================================
# 1. Zero-Config Environment Auto-Discovery
# ==========================================
def auto_configure_docker_host():
    """Zero-config engine detection (spec 3.4).

    Probes for a running container engine. An explicit $DOCKER_HOST always
    wins. On each platform tries Podman then Docker in order of preference.
    Falls back to docker.from_env() (which tries /var/run/docker.sock) when
    nothing is found.
    """
    if os.environ.get("DOCKER_HOST"):
        return

    def _set(path: str) -> None:
        uri = path if path.startswith(("unix://", "npipe://")) else f"unix://{path}"
        os.environ["DOCKER_HOST"] = uri
        print(f"[Validator] Auto-configured DOCKER_HOST -> {uri}")

    def _sock_exists(path: str) -> bool:
        return bool(path) and os.path.exists(path)

    if sys.platform == "darwin":
        # 1. Podman machine socket
        try:
            result = subprocess.run(
                ["podman", "machine", "inspect", "--format",
                 "{{.ConnectionInfo.PodmanSocket.Path}}"],
                capture_output=True, text=True, timeout=5,
            )
            sock = result.stdout.strip()
            if sock and _sock_exists(sock):
                _set(sock)
                return
        except (subprocess.SubprocessError, FileNotFoundError, subprocess.TimeoutExpired):
            pass
        # 2. Docker Desktop socket
        docker_sock = "/var/run/docker.sock"
        if _sock_exists(docker_sock):
            _set(docker_sock)

    elif sys.platform.startswith("linux"):
        # 1. Podman rootless — $XDG_RUNTIME_DIR/podman/podman.sock
        xdg = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
        candidates = [
            os.path.join(xdg, "podman", "podman.sock"),
            os.path.expanduser("~/.local/share/containers/podman/machine/podman.sock"),
        ]
        for sock in candidates:
            if _sock_exists(sock):
                _set(sock)
                return
        # 2. Podman machine socket (less common on Linux)
        try:
            result = subprocess.run(
                ["podman", "machine", "inspect", "--format",
                 "{{.ConnectionInfo.PodmanSocket.Path}}"],
                capture_output=True, text=True, timeout=5,
            )
            sock = result.stdout.strip()
            if sock and _sock_exists(sock):
                _set(sock)
                return
        except (subprocess.SubprocessError, FileNotFoundError, subprocess.TimeoutExpired):
            pass
        # 3. Docker — fall through to docker.from_env() -> /var/run/docker.sock

    elif sys.platform == "win32":
        # 1. Podman Desktop named pipe
        podman_pipe = "npipe:////./pipe/podman-machine-default"
        # 2. Docker Desktop named pipe
        docker_pipe = "npipe:////./pipe/docker_engine"
        for pipe in (podman_pipe, docker_pipe):
            try:
                import docker as _docker
                _docker.DockerClient(base_url=pipe).ping()
                _set(pipe)
                return
            except Exception:
                pass

# ==========================================
# 2. Configuration & Runtimes
# ==========================================
# Broker URI + credentials come from codoken_auth (CODOKEN_AUTH /
# CODOKEN_BROKER_WS env vars; devnet default is ?token=<role secret>).
ROLE = "validator"
VERSION = "2.0.0"

RECONNECT_DELAY = 5
RECONNECT_MAX   = 300

PAUSE_FILE = os.path.join(os.path.expanduser("~"), ".codoken", "paused")


def _backoff(failures: int) -> float:
    return min(RECONNECT_DELAY * (2 ** failures), RECONNECT_MAX)

# Polyglot runtime map (spec 3.4). `files` is the file layout written into
# the read-only /app mount: one file = solution + harness concatenated
# (script-style languages); two files = Go, where a single file may not
# carry two `package` clauses, so solution and harness are sibling files.
RUNTIME_CONFIG = {
    "python": {
        "image": "python:3.11-slim@sha256:cce12316475d7d56d605f76e1d811d0dc4e7227f2d4afd29951521264f504ff8",
        "files": ("solution.py",),
        "command": "python solution.py"
    },
    "cpp": {
        "image": "gcc:latest@sha256:f07e6385ea2bba4ee5582cadc50e5d3ce60cf758f0a3c9168ba0615475f1e459",
        "files": ("solution.cpp",),
        "command": "g++ solution.cpp -o /tmp/solution && /tmp/solution"
    },
    "javascript": {
        "image": "node:18-slim@sha256:f9ab18e354e6855ae56ef2b290dd225c1e51a564f87584b9bd21dd651838830e",
        "files": ("solution.js",),
        "command": "node solution.js"
    },
    "typescript": {
        "image": "codoken-ts:1.0",
        "files": ("solution.ts",),
        "command": "ts-node solution.ts",
        "build": {
            "base": "node:18-slim",
            "run": "npm install -g ts-node@10.9.2 typescript@5.3.3 @types/node@18 --silent",
        },
    },
    "rust": {
        "image": "rust:latest@sha256:931c2c0531a8b143e044f0f3914dfe802f15bb3d1955f0b657e9359327ac2ffc",
        "files": ("solution.rs",),
        "command": "rustc solution.rs -o /tmp/solution && /tmp/solution",
        "mem_limit": "512m",
        "timeout": 180,
    },
    "golang": {
        "image": "golang:alpine@sha256:d530cf60723abf1ffae0cd16bb546b379ec0550d2f3815e5b16ed69dbc00fba4",
        "files": ("solution.go",),
        "command": "go run solution.go",
        "mem_limit": "512m",
        "timeout": 120,
    },
    "php": {
        "image": "php:8.2-cli@sha256:2d72ff4b0cfe82e22f01f15223bc4ab4c825d318cb2e3275554cd32299a5ea50",
        "files": ("solution.php",),
        "command": "php solution.php"
    },
    "ruby": {
        "image": "ruby:3.2-slim@sha256:84184c9e2c368885a1d0c93ad1953c33d81081058d274b87b4aa6f3e209e5d16",
        "files": ("solution.rb",),
        "command": "ruby solution.rb"
    },
    "java": {
        "image": "eclipse-temurin:21-jdk-alpine@sha256:fd52084c35bbac78890d95d57707d31ffc9d7a3324199e89c06be2a9dc71bfa0",
        "files": ("Solution.java", "Test.java"),
        "command": "javac -d /tmp Solution.java Test.java && java -ea -cp /tmp Test"
    },
    "gdscript": {
        "image": "barichello/godot-ci:4.3@sha256:8d3a9fc683fcaa5b7d8b1c90fa94318402ebb00edeb5f6e2df4bac46665b98ff",
        "files": ("solution.gd",),
        "command": "godot --headless --script solution.gd",
        "mem_limit": "512m",
        "timeout": 90,
    },
    "gdscript3": {
        "image": "barichello/godot-ci:3.6.2@sha256:026e1f652dd1d718073bb5c32372b3a5d73e7ad7bfe13b9052423604fca6918f",
        "files": ("solution.gd",),
        "command": "godot --video-driver Dummy --script solution.gd",
        "mem_limit": "512m",
        "timeout": 90,
    },
    "sqlite": {
        "image": "python:3.12-slim@sha256:4840d514c583a4d3d8cad3940e68dee6f0f6f3635c12fcd0a3d74c21a5b93a54",
        "files": ("solution.py",),
        "command": "python solution.py",
    },
    "postgresql": {
        "image": "codoken-pg:1.0",
        "files": ("solution.py",),
        "command": "python3 solution.py",
        "mem_limit": "512m",
        "timeout": 60,
        "build": {
            "base": "postgres:16-alpine@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139",
            "run": "apk add --no-cache python3",
        },
    },
}

LANGUAGE_ALIASES = {
    "go":       "golang",
    "js":       "javascript",
    "ts":       "typescript",
    "rb":       "ruby",
    "gd":       "gdscript",
    "godot":    "gdscript",
    "gd3":      "gdscript3",
    "godot3":   "gdscript3",
    "gdscript4": "gdscript",
    "godot4":   "gdscript",
    "sql":      "sqlite",
    "postgres": "postgresql",
    "psql":     "postgresql",
}

# Spec 5.2: verdicts carry (status, stdout, exit_code). The broker hashes
# the carried stdout and confronts the validators' outputs; a run that spews
# megabytes of noise is not a useful report, so the carried output is capped.
MAX_STDOUT_CHARS = 8192


def compute_tier(completion_tokens: Optional[int], thresholds: Optional[dict]) -> str:
    """Derive complexity tier from worker token count and per-worker thresholds.

    During bootstrap (thresholds is None) always returns 'standard'.
    thresholds: {"simple": float, "complex": float} — both derived from the
    worker's Welford distribution by the broker before sending task_review.
    """
    if thresholds is None or completion_tokens is None:
        return "standard"
    if completion_tokens < thresholds["simple"]:
        return "simple"
    if completion_tokens > thresholds["complex"]:
        return "complex"
    return "standard"


def _docker_volume_path(path: str) -> str:
    """Return a path Docker/Podman can use as a bind-mount source.

    On Windows, Docker Desktop (WSL2 backend) expects paths in the form
    /run/desktop/mnt/host/<drive>/<rest>, e.g.:
        C:\\Users\\foo\\AppData\\Local\\Temp\\xyz
        -> /run/desktop/mnt/host/c/Users/foo/AppData/Local/Temp/xyz
    Paths that are already Unix-style are returned unchanged.
    """
    if sys.platform != "win32":
        return path
    p = Path(path).resolve()
    drive = p.drive.rstrip(":").lower()           # "C:" -> "c"
    rest = p.as_posix().lstrip("/")               # strip leading slash after posix conversion
    # Remove the "c/" prefix that as_posix() adds on Windows ("C:/foo" -> "c/foo")
    if rest.startswith(f"{drive}/"):
        rest = rest[len(drive) + 1:]
    return f"/run/desktop/mnt/host/{drive}/{rest}"


def hash_stdout(stdout: str) -> str:
    """sha256 of the exact stdout string a verdict carries (spec 5.2 hash)."""
    return hashlib.sha256(stdout.encode("utf-8")).hexdigest()

# ==========================================
# 3. Image Auto-Caching
# ==========================================
def ensure_images_cached(client):
    for key, config in RUNTIME_CONFIG.items():
        image_name = config["image"]
        build_spec = config.get("build")
        try:
            client.images.get(image_name)
            print(f"[Validator] Image cached: {image_name}")
        except docker.errors.ImageNotFound:
            if build_spec:
                print(f"[Validator] Building custom image {image_name} "
                      f"(from {build_spec['base']})...")
                try:
                    dockerfile = (
                        f"FROM {build_spec['base']}\n"
                        f"RUN {build_spec['run']}\n"
                    )
                    import io
                    client.images.build(
                        fileobj=io.BytesIO(dockerfile.encode()),
                        tag=image_name,
                        rm=True,
                    )
                    print(f"[Validator] Built {image_name}")
                except Exception as e:
                    print(f"[Validator] WARNING: Failed to build {image_name}: {e}")
            else:
                print(f"[Validator] Pulling missing image: {image_name}...")
                try:
                    client.images.pull(image_name)
                except Exception as e:
                    print(f"[Validator] WARNING: Failed to pull {image_name}: {e}")

# ==========================================
# 4. Sandboxed Execution Engine
# ==========================================

def _sqlite_harness(generated_code: str, test_code: str) -> str:
    import base64
    gen_b64 = base64.b64encode(generated_code.encode()).decode()
    tst_b64 = base64.b64encode(test_code.encode()).decode()
    return (
        "import sqlite3, sys, base64\n"
        "conn = sqlite3.connect(':memory:')\n"
        "conn.row_factory = sqlite3.Row\n"
        "cur = conn.cursor()\n"
        f"cur.executescript(base64.b64decode({gen_b64!r}).decode())\n"
        f"stmts = [s.strip() for s in base64.b64decode({tst_b64!r}).decode().split(';') if s.strip()]\n"
        "failed = False\n"
        "for stmt in stmts:\n"
        "    cur.execute(stmt)\n"
        "    for row in cur.fetchall():\n"
        "        keys = row.keys()\n"
        "        ok = row['ok'] if 'ok' in keys else 1\n"
        "        if ok not in (1, True, '1', 't'):\n"
        "            got = row['got'] if 'got' in keys else '?'\n"
        "            exp = row['expected'] if 'expected' in keys else '?'\n"
        "            print(f'FAIL: got={got!r} expected={exp!r}')\n"
        "            failed = True\n"
        "        else:\n"
        "            print('PASS')\n"
        "conn.close()\n"
        "if failed: sys.exit(1)\n"
        "print('ALL TESTS PASSED')\n"
    )


def _postgresql_harness(generated_code: str, test_code: str) -> str:
    import base64
    gen_b64 = base64.b64encode(generated_code.encode()).decode()
    tst_b64 = base64.b64encode(test_code.encode()).decode()
    return (
        "import subprocess, sys, base64, csv, io, time, os\n"
        f"gen = base64.b64decode({gen_b64!r}).decode()\n"
        f"tst = base64.b64decode({tst_b64!r}).decode()\n"
        "os.makedirs('/tmp/pgdata', exist_ok=True)\n"
        "r = subprocess.run(['initdb','-D','/tmp/pgdata','-U','postgres',\n"
        "    '--auth=trust','--no-locale','--encoding=UTF8'], capture_output=True, text=True)\n"
        "if r.returncode != 0: print(r.stderr); sys.exit(1)\n"
        "subprocess.run(['pg_ctl','-D','/tmp/pgdata','-l','/tmp/pg.log',\n"
        "    '-o','-k /tmp','start'], capture_output=True)\n"
        "for _ in range(20):\n"
        "    if subprocess.run(['pg_isready','-h','/tmp','-U','postgres'],\n"
        "        capture_output=True).returncode == 0: break\n"
        "    time.sleep(0.5)\n"
        "PSQL = ['psql','-h','/tmp','-U','postgres']\n"
        "with open('/tmp/setup.sql','w') as f: f.write(gen)\n"
        "r = subprocess.run(PSQL + ['-f','/tmp/setup.sql'], capture_output=True, text=True)\n"
        "if r.returncode != 0:\n"
        "    print(r.stderr)\n"
        "    subprocess.run(['pg_ctl','-D','/tmp/pgdata','stop','-m','fast'], capture_output=True)\n"
        "    sys.exit(1)\n"
        "stmts = [s.strip() for s in tst.split(';') if s.strip()]\n"
        "failed = False\n"
        "for stmt in stmts:\n"
        "    r = subprocess.run(PSQL + ['--csv','-c',stmt+';'], capture_output=True, text=True)\n"
        "    if r.returncode != 0:\n"
        "        print(f'ERROR: {r.stderr.strip()}')\n"
        "        failed = True\n"
        "        continue\n"
        "    for row in csv.DictReader(io.StringIO(r.stdout)):\n"
        "        ok = row.get('ok','')\n"
        "        if ok != 't':\n"
        "            print(f'FAIL: got={row.get(\"got\",\"?\")!r} expected={row.get(\"expected\",\"?\")!r}')\n"
        "            failed = True\n"
        "        else:\n"
        "            print('PASS')\n"
        "subprocess.run(['pg_ctl','-D','/tmp/pgdata','stop','-m','fast'], capture_output=True)\n"
        "if failed: sys.exit(1)\n"
        "print('ALL TESTS PASSED')\n"
    )


def run_sandboxed_test(client, language: str, generated_code: str, test_code: str) -> tuple[str, str, str, int | None]:
    """Run the generated code against the test harness in a sandboxed
    container. Returns (status, details, stdout, exit_code) where `stdout`
    is the capped raw run output the verdict reports (spec 5.2)."""
    import re as _re
    _raw = generated_code.strip()
    fence_match = _re.search(r"```[a-zA-Z0-9+# ]*\r?\n(.*?)```", _raw, _re.DOTALL)
    if fence_match:
        _extracted = fence_match.group(1).strip()
        generated_code = _extracted if _extracted else _raw
    else:
        generated_code = _raw

    key = LANGUAGE_ALIASES.get(language.lower(), language.lower())
    config = RUNTIME_CONFIG.get(key)
    if not config:
        return "rejected", f"Unsupported language runtime: {language}", "", None

    if not generated_code.strip():
        return "rejected", "Worker failed to generate code (empty implementation).", "", None

    with tempfile.TemporaryDirectory() as temp_dir:
        files = config["files"]
        if len(files) == 1:
            if key == "golang":
                full_code = (
                    "package main\n\nimport (\n\t\"fmt\"\n\t\"strings\"\n)\n\n"
                    f"{generated_code}\n\n"
                    f"func main() {{\n{test_code}\n}}\n"
                )
            elif key == "rust":
                full_code = (
                    f"{generated_code}\n\n"
                    f"fn main() {{\n{test_code}\n}}\n"
                )
            elif key == "php":
                # Strip leading <?php from test_code if present; validator adds its own
                test_stripped = test_code.lstrip()
                if test_stripped.startswith("<?php"):
                    test_stripped = test_stripped[5:].lstrip("\n")
                full_code = f"<?php\n{generated_code}\n{test_stripped}\n"
            elif key in ("gdscript", "gdscript3"):
                # Godot requires consistent indentation across the file.
                # The LLM typically outputs spaces; we normalise generated_code
                # to spaces (4-space tabs) then build the harness with spaces too.
                def _tabs_to_spaces(src, n=4):
                    return "\n".join(
                        line.replace("\t", " " * n) if line.startswith("\t") else line
                        for line in src.splitlines()
                    )
                gen_normalised = _tabs_to_spaces(generated_code)
                full_code = (
                    "extends SceneTree\n\n"
                    f"{gen_normalised}\n\n"
                    "func _init():\n"
                    "    var _failed := false\n"
                    "    var _errors := []\n"
                )
                for line in test_code.splitlines():
                    stripped = line.strip()
                    if stripped.startswith("assert "):
                        expr = stripped[len("assert "):]
                        full_code += (
                            f"    if not ({expr}):\n"
                            f'        _errors.append("FAIL: assert {expr.replace(chr(34), chr(39))}")\n'
                            f"        _failed = true\n"
                        )
                    elif stripped:
                        full_code += f"    {stripped}\n"
                full_code += (
                    "    if _failed:\n"
                    "        for e in _errors: print(e)\n"
                    "        quit(1)\n"
                    "    else:\n"
                    '        print("All assertions passed.")\n'
                    "        quit(0)\n"
                )
            elif key == "sqlite":
                full_code = _sqlite_harness(generated_code, test_code)
            elif key == "postgresql":
                full_code = _postgresql_harness(generated_code, test_code)
            else:
                full_code = f"{generated_code}\n\n{test_code}" if test_code else generated_code
            contents = {files[0]: full_code}
        else:
            contents = {files[0]: generated_code, files[1]: test_code}
        for name, content in contents.items():
            with open(os.path.join(temp_dir, name), "w") as f:
                f.write(content)

        try:
            container = client.containers.run(
                image=config["image"],
                command=f"sh -c '{config['command']}'",
                volumes={_docker_volume_path(temp_dir): {"bind": "/app", "mode": "ro"}},
                working_dir="/app",
                network_disabled=True,
                mem_limit=config.get("mem_limit", "256m"),
                cpu_quota=50000,
                remove=False,
                stderr=True,
                stdout=True,
                detach=True,
                cap_drop=["ALL"],
                security_opt=["no-new-privileges:true"],
                read_only=True,
                tmpfs={"/tmp": "size=128m,mode=1777"},
                pids_limit=64,
            )
            result = container.wait(timeout=config.get("timeout", 120))
            exit_code = result.get("StatusCode", 0) if isinstance(result, dict) else 0
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", "replace")
            container.remove()
            if exit_code != 0:
                return "rejected", (
                    f"Execution failed (Exit Code {exit_code}):\n{logs[-2048:]}"
                ), logs[:MAX_STDOUT_CHARS], exit_code
            return "approved", logs[-2048:], logs[:MAX_STDOUT_CHARS], exit_code

        except Exception as ex:
            # Catch timeout (requests.exceptions.ReadTimeout) and all other errors.
            # Best-effort cleanup — container may or may not still exist.
            try:
                container.kill()
                container.remove()
            except Exception:
                pass
            msg = str(ex)
            if "Read timed out" in msg or "ReadTimeout" in type(ex).__name__:
                limit = config.get("timeout", 120)
                return "rejected", f"Execution timed out after {limit}s", "", None
            return "rejected", f"Sandbox execution error: {msg}", "", None

# ==========================================
# 5. WebSocket Client & Task Loop
# ==========================================
async def validator_client():
    # Retry until the container engine socket is available (e.g. launchd may
    # start this service before Podman machine is fully up).
    client = None
    while client is None:
        auto_configure_docker_host()
        try:
            candidate = docker.from_env()
            candidate.ping()
            client = candidate
        except Exception as e:
            print(f"[Validator] Waiting for container engine: {e}. Retrying in 10s...")
            os.environ.pop("DOCKER_HOST", None)  # force re-probe on next attempt
            await asyncio.sleep(10)

    ensure_images_cached(client)

    auth_store = os.environ.get("CODOKEN_AUTH_STORE", str(Path.home() / ".codoken" / "auth.json"))
    auth = CodokenAuth(ROLE, auth_store=auth_store)
    _reauthed = False
    consecutive_failures = 0
    while True:
        # Pause file check — user ran `codoken pause`
        if os.path.exists(PAUSE_FILE):
            try:
                import json as _j, datetime as _dt
                data = _j.loads(open(PAUSE_FILE).read())
                resume_at = data.get("resume_at")
                if resume_at and _dt.datetime.now() >= _dt.datetime.fromisoformat(resume_at):
                    os.unlink(PAUSE_FILE)
                    print("[Validator] Pause expired — resuming.")
                else:
                    print("[Validator] Paused (run `codoken resume` to restart). Sleeping 60s...")
                    await asyncio.sleep(60)
                    continue
            except Exception:
                print("[Validator] Paused. Sleeping 60s...")
                await asyncio.sleep(60)
                continue

        try:
            uri, extra_headers = await auth.connect_config()
            print(f"[Validator] Connecting to Broker at {uri}...")
            async with websockets.connect(uri, additional_headers=extra_headers) as websocket:
                await websocket.send(json.dumps({
                    "action": "register_validator",
                    "version": VERSION,
                }))
                print("[Validator] Successfully connected and listening for task reviews...")
                consecutive_failures = 0  # reset backoff on successful connect

                # Keep the Firebase ID token fresh for the life of the session
                # (no-op in shared_secret mode).
                await auth.start_refresh(websocket.send)

                async for message_str in websocket:
                    message = json.loads(message_str)
                    print(f"[Validator Debug] Received raw message type: {message.get('type')}")

                    if message.get("type") == "error" and message.get("code") == "version_too_old":
                        print(f"[Validator] {message.get('message')}")
                        sys.exit(1)

                    if message.get("type") == "task_review":
                        task_id = message.get("task_id")
                        manifest = message.get("manifest", {})
                        generated_code = message.get("generated_code", "")
                        completion_tokens = message.get("completion_tokens")
                        tier_thresholds = message.get("tier_thresholds")

                        # Correctly extract nested environment language & test code
                        env_config = manifest.get("environment", {})
                        language = env_config.get("language", "python")

                        validator_payload = manifest.get("validator_payload", {})
                        test_code = validator_payload.get("test_code", "")

                        print(f"[Validator] Reviewing task [{task_id}] for language [{language}]...")

                        status, details, stdout, exit_code = run_sandboxed_test(
                            client, language, generated_code, test_code)

                        tier = compute_tier(completion_tokens, tier_thresholds)

                        print(f"[Validator] Task [{task_id}] verdict: [{status.upper()}] tier={tier} (exit_code={exit_code}, {len(stdout)}B output)")
                        print(f"[Validator] Details:\n{details}")

                        verdict_payload = {
                            "action": "validate_task",
                            "task_id": task_id,
                            "status": status,
                            "details": details,
                            "exit_code": exit_code,
                            # spec 5.2: (status, stdout, exit_code) + the hash
                            # the broker uses for output confrontation
                            "stdout": stdout,
                            "stdout_hash": hash_stdout(stdout),
                            "tier": tier,
                        }
                        await websocket.send(json.dumps(verdict_payload))

        except WsConnectionClosed:
            consecutive_failures += 1
            print(f"[Validator] Connection lost. Reconnecting in {_backoff(consecutive_failures):.0f}s...")
        except (socket.gaierror, OSError, CodokenNetworkError) as e:
            consecutive_failures += 1
            delay = _backoff(consecutive_failures)
            print(f"[Validator] Broker unreachable ({type(e).__name__}). Retrying in {delay:.0f}s...")
        except InvalidStatus as e:
            if e.response.status_code in (401, 403):
                if _reauthed:
                    print("[Validator] Authentication failed after reauth. Exiting.")
                    sys.exit(1)
                print("[Validator] Authentication rejected by broker. Launching re-authentication...")
                try:
                    await CodokenAuth.reauth(auth_store=auth_store)
                    _reauthed = True
                except CodokenNetworkError as reauth_net_exc:
                    consecutive_failures += 1
                    print(f"[Validator] Re-authentication skipped ({reauth_net_exc}). Retrying in {_backoff(consecutive_failures):.0f}s...")
                except Exception as reauth_exc:
                    print(f"[Validator] Re-authentication failed: {reauth_exc}. Exiting.")
                    sys.exit(1)
                consecutive_failures = 0
                continue
            else:
                consecutive_failures += 1
                print(f"[Validator] Broker returned HTTP {e.response.status_code}. Retrying in {_backoff(consecutive_failures):.0f}s...")
        except CodokenAuthError as e:
            if _reauthed:
                print(f"[Validator] Authentication failed after reauth: {e}. Exiting.")
                sys.exit(1)
            print(f"[Validator] Authentication failed: {e}. Launching re-authentication...")
            try:
                await CodokenAuth.reauth(auth_store=auth_store)
                _reauthed = True
            except CodokenNetworkError as reauth_net_exc:
                consecutive_failures += 1
                print(f"[Validator] Re-authentication skipped ({reauth_net_exc}). Retrying in {_backoff(consecutive_failures):.0f}s...")
            except Exception as reauth_exc:
                print(f"[Validator] Re-authentication failed: {reauth_exc}. Exiting.")
                sys.exit(1)
            consecutive_failures = 0
            continue
        except Exception as e:
            consecutive_failures += 1
            print(f"[Validator] Unexpected error: {e}. Reconnecting in {_backoff(consecutive_failures):.0f}s...")
        finally:
            auth.stop()

        await asyncio.sleep(_backoff(consecutive_failures))

if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in ("--version", "-V"):
        print(VERSION)
        sys.exit(0)
    asyncio.run(validator_client())
