import asyncio
import hashlib
import json
import math
import os
import sys
import subprocess
import tempfile
from pathlib import Path
from typing import Optional
import docker
import websockets

import codoken_config

# Layered preferences (.codoken/config.json -> env). Must run before
# codoken_auth is imported: it captures $CODOKEN_BROKER_WS at import time.
codoken_config.apply_to_env("validator")

from codoken_auth import CodokenAuth, CodokenAuthError  # noqa: E402

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
VERSION = "1.1.0"

# Polyglot runtime map (spec 3.4). `files` is the file layout written into
# the read-only /app mount: one file = solution + harness concatenated
# (script-style languages); two files = Go, where a single file may not
# carry two `package` clauses, so solution and harness are sibling files.
RUNTIME_CONFIG = {
    "python": {
        "image": "python:3.11-slim",
        "files": ("solution.py",),
        "command": "python solution.py"
    },
    "cpp": {
        "image": "gcc:latest",
        "files": ("solution.cpp",),
        "command": "g++ solution.cpp -o solution && ./solution"
    },
    "javascript": {
        "image": "node:18-slim",
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
        "image": "rust:latest",
        "files": ("solution.rs",),
        "command": "rustc solution.rs -o solution && ./solution"
    },
    "golang": {
        "image": "golang:alpine",
        "files": ("solution.go", "test.go"),
        "command": "go run solution.go test.go"
    },
    "php": {
        "image": "php:8.2-cli",
        "files": ("solution.php",),
        "command": "php solution.php"
    },
    "ruby": {
        "image": "ruby:3.2-slim",
        "files": ("solution.rb",),
        "command": "ruby solution.rb"
    },
    "java": {
        "image": "eclipse-temurin:21-jdk-alpine",
        "files": ("Solution.java", "Test.java"),
        # Compile both files together, run Test which calls Solution methods.
        "command": "javac Solution.java Test.java && java -ea -cp . Test"
    },
}

LANGUAGE_ALIASES = {
    "go":  "golang",
    "js":  "javascript",
    "ts":  "typescript",
    "rb":  "ruby",
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
                mem_limit="256m",
                cpu_quota=50000,
                remove=False,
                stderr=True,
                stdout=True,
                detach=True,
            )
            result = container.wait()
            exit_code = result.get("StatusCode", 0) if isinstance(result, dict) else 0
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", "replace")
            container.remove()
            if exit_code != 0:
                return "rejected", (
                    f"Execution failed (Exit Code {exit_code}):\n{logs[-2048:]}"
                ), logs[:MAX_STDOUT_CHARS], exit_code
            return "approved", logs[-2048:], logs[:MAX_STDOUT_CHARS], exit_code

        except docker.errors.ContainerError as ce:
            logs = ce.container.logs(stdout=True, stderr=True).decode("utf-8", "replace")
            ce.container.remove()
            return "rejected", (
                f"Execution failed (Exit Code {ce.exit_status}):\n{logs[-2048:]}"
            ), logs[:MAX_STDOUT_CHARS], ce.exit_status
        except Exception as ex:
            return "rejected", f"Sandbox execution error: {str(ex)}", "", None

def _check_auth_ready() -> None:
    """Exit with a clear message if firebase auth is configured but no credentials exist."""
    auth_mode = os.environ.get("CODOKEN_AUTH", "shared_secret").lower()
    if auth_mode != "firebase":
        return
    auth_store = os.environ.get("CODOKEN_AUTH_STORE", str(Path.home() / ".codoken" / "auth.json"))
    has_store = os.path.exists(auth_store)
    has_env = bool(os.environ.get("CODOKEN_EMAIL") and os.environ.get("CODOKEN_PASSWORD"))
    if not has_store and not has_env:
        print(
            "[Validator] CODOKEN_AUTH=firebase but no credentials found.\n"
            "  Run the setup wizard first:  python setup.py\n"
            "  Or set CODOKEN_EMAIL and CODOKEN_PASSWORD environment variables."
        )
        sys.exit(1)


# ==========================================
# 5. WebSocket Client & Task Loop
# ==========================================
async def validator_client():
    _check_auth_ready()

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
    while True:
        auth = CodokenAuth(ROLE, auth_store=auth_store)
        try:
            uri, extra_headers = await auth.connect_config()
            print(f"[Validator] Connecting to Broker at {uri}...")
            async with websockets.connect(uri, additional_headers=extra_headers) as websocket:
                await websocket.send(json.dumps({
                    "action": "register_validator",
                    "version": VERSION,
                }))
                print("[Validator] Successfully connected and listening for task reviews...")

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

        except websockets.exceptions.ConnectionClosed:
            print("[Validator] Connection lost. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except CodokenAuthError as e:
            print(f"[Validator] Authentication failed: {e}. Exiting.")
            sys.exit(1)
        except Exception as e:
            print(f"[Validator] Unexpected error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        finally:
            auth.stop()

if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in ("--version", "-V"):
        print(VERSION)
        sys.exit(0)
    asyncio.run(validator_client())
