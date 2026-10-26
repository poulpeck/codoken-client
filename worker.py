import asyncio
import json
import logging
import os
import socket
import sys
import time
from typing import Optional
import httpx
import websockets

import codoken_config

# Layered preferences (.codoken/config.json -> env). Must run before
# codoken_auth is imported: it captures $CODOKEN_BROKER_WS at import time.
codoken_config.apply_to_env("worker")

from codoken_auth import CodokenAuth, CodokenAuthError  # noqa: E402

# Configure logging for clean output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# Broker URI + credentials come from codoken_auth (CODOKEN_AUTH /
# CODOKEN_BROKER_WS env vars; devnet default is ?token=<role secret>).
ROLE = "worker"
VERSION = "1.1.0"

# Model name: $CODOKEN_WORKER_MODEL > ~/.codoken/config.json > auto-detect from backend
WORKER_MODEL: Optional[str] = os.environ.get("CODOKEN_WORKER_MODEL") or None

# Known local endpoints to probe
ENDPOINTS = {
    "ollama": {"base": "http://localhost:11434", "test": "/api/tags", "type": "ollama"},
    "lmstudio": {"base": "http://localhost:1234", "test": "/v1/models", "type": "openai"},
    "llamacpp": {"base": "http://localhost:8080", "test": "/health", "type": "openai"}
}

# Low reasoning configuration parameters (low temperature for swift, direct task execution)
LOW_REASONING_CONFIG = {
    "temperature": 0.1,
    "top_p": 0.9,
    "max_tokens": 2048
}

SYSTEM_GUARDRAIL_PROMPT = """
You are a pure-function code synthesis node for Codoken. 
Your sole purpose is to generate stateless, side-effect-free, atomic code functions.
STRICT RULES:
1. Do not use file systems (fstream), networking (sockets), concurrency (threads), or OS system calls.
2. If requested, output ONLY: ERROR: NON_COMPLIANT_TASK
"""

RECONNECT_DELAY = 5

# Spec 3.3: how many prompt completions to run concurrently. A local LLM
# (Ollama / LM Studio / llama.cpp) usually serves 2 comfortably; the worker
# declares the same number to the broker on registration, so dispatch never
# oversubscribes. Set CODOKEN_WORKER_PARALLEL=1 for a single-slot worker.
MAX_PARALLEL = max(1, int(os.environ.get("CODOKEN_WORKER_PARALLEL", "2")))

# When the session ends, give in-flight generations this long to finish and
# flush their completion reports; beyond that we bail and the broker's
# dispatch watchdog re-queues anything we could not deliver.
DRAIN_TIMEOUT_S = 10.0

async def detect_local_llm() -> dict:
    """Probes local endpoints to find an active LLM backend."""
    async with httpx.AsyncClient(timeout=2.0) as client:
        for name, info in ENDPOINTS.items():
            try:
                url = f"{info['base']}{info['test']}"
                response = await client.get(url)
                if response.status_code == 200:
                    logging.info(f"Auto-detected active LLM backend: [{name.upper()}] at {info['base']}")
                    backend = {"provider": name, "base_url": info["base"], "api_type": info["type"]}
                    # Auto-detect first available model if none configured
                    if not WORKER_MODEL:
                        backend["model"] = await _detect_first_model(client, info)
                    else:
                        backend["model"] = WORKER_MODEL
                    return backend
            except Exception:
                continue
    return None


async def _detect_first_model(client: httpx.AsyncClient, info: dict) -> Optional[str]:
    """Return the first model name reported by the backend, or None."""
    try:
        if info["type"] == "ollama":
            r = await client.get(f"{info['base']}/api/tags", timeout=2.0)
            if r.status_code == 200:
                models = r.json().get("models", [])
                if models:
                    name = models[0].get("name") or models[0].get("model")
                    if name:
                        logging.info(f"Auto-selected Ollama model: {name}")
                        return name
        elif info["type"] == "openai":
            r = await client.get(f"{info['base']}/v1/models", timeout=2.0)
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    name = data[0].get("id")
                    if name:
                        logging.info(f"Auto-selected model: {name}")
                        return name
    except Exception:
        pass
    return None

async def execute_local_llm(backend: dict, prompt: str) -> tuple[str, dict]:
    """Routes the prompt to the auto-detected backend using appropriate payload structures."""
    provider = backend["provider"]
    base_url = backend["base_url"]
    api_type = backend["api_type"]
    
    logging.info(f"Executing LLM via auto-detected [{provider.upper()}] with low reasoning settings...")
    
    async with httpx.AsyncClient() as client:
        try:
            if api_type == "ollama":
                model = backend.get("model") or "llama3"
                payload = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": SYSTEM_GUARDRAIL_PROMPT},
                        {"role": "user", "content": prompt}
                    ],
                    "stream": False,
                    "options": {
                        "temperature": LOW_REASONING_CONFIG["temperature"],
                        "top_p": LOW_REASONING_CONFIG["top_p"]
                    }
                }
                response = await client.post(f"{base_url}/api/chat", json=payload, timeout=120.0)
                if response.status_code == 200:
                    data = response.json()
                    content = data.get("message", {}).get("content", "").strip()
                    prompt_tokens = data.get("prompt_eval_count", 0) or 0
                    completion_tokens = data.get("eval_count", 0) or 0
                    total_duration_ns = data.get("total_duration", 0) or 0
                    total_latency_ms = round(total_duration_ns / 1_000_000, 2)
                    tps = round(completion_tokens / (total_duration_ns / 1_000_000_000), 2) if total_duration_ns > 0 else 0.0
                    telemetry = {
                        "model_used": data.get("model", payload["model"]),
                        "inference_engine": "ollama",
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": completion_tokens,
                        "total_latency_ms": total_latency_ms,
                        "tokens_per_second": tps
                    }
                    return content, telemetry
                else:
                    return f"Ollama Error: Status {response.status_code}", {}

            elif api_type == "openai":
                model = backend.get("model") or "local-model"
                payload = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": SYSTEM_GUARDRAIL_PROMPT},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": LOW_REASONING_CONFIG["temperature"],
                    "max_tokens": LOW_REASONING_CONFIG["max_tokens"],
                    "stream": False
                }
                start = time.monotonic()
                response = await client.post(f"{base_url}/v1/chat/completions", json=payload, timeout=120.0)
                if response.status_code == 200:
                    data = response.json()
                    content = data["choices"][0]["message"]["content"].strip()
                    usage = data.get("usage", {}) or {}
                    prompt_tokens = usage.get("prompt_tokens", 0) or 0
                    completion_tokens = usage.get("completion_tokens", 0) or 0
                    total_latency_ms = round((time.monotonic() - start) * 1000, 2)
                    tps = round(completion_tokens / (total_latency_ms / 1000), 2) if total_latency_ms > 0 else 0.0
                    telemetry = {
                        "model_used": data.get("model", payload["model"]),
                        "inference_engine": provider,
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": completion_tokens,
                        "total_latency_ms": total_latency_ms,
                        "tokens_per_second": tps
                    }
                    return content, telemetry
                else:
                    return f"{provider.capitalize()} Error: Status {response.status_code}", {}

        except Exception as e:
            error_msg = f"Failed to communicate with local LLM ({provider}): {str(e)}"
            logging.error(error_msg)
            return error_msg, {}

async def worker_session(websocket, auth: CodokenAuth, backend: dict) -> None:
    """Run one broker session: register, then service the task pool until the
    socket closes.

    ``websocket`` is duck-typed (needs ``send`` + async iteration) so the
    session is testable against an in-memory double.

    Task assignments are handled by a bounded pool of concurrent tasks
    (spec 3.3 multi-task parallelism). Every outbound frame — completion
    reports and token refreshes alike — funnels through a single outbox
    writer, so concurrent completions can never interleave.
    """
    outbox: asyncio.Queue[str | None] = asyncio.Queue(maxsize=1024)
    in_flight: set[asyncio.Task] = set()
    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    async def send(payload: str) -> None:
        await outbox.put(payload)

    async def writer() -> None:
        while True:
            payload = await outbox.get()
            try:
                if payload is not None:  # None is the shutdown sentinel
                    await websocket.send(payload)
            except Exception:  # noqa: BLE001 - socket gone; drop the rest
                pass
            finally:
                outbox.task_done()
            if payload is None:
                return

    async def process(message: dict) -> None:
        async with semaphore:
            task_id = message.get("task_id")
            try:
                manifest = message.get("manifest") or {}
                prompt = (manifest.get("worker_payload") or {}).get("prompt", "")
                language = (manifest.get("environment") or {}).get("language", "?")

                logging.info(f"Received task assignment [{task_id}] for language: {language}")

                # Execute local LLM generation
                generated_code, telemetry = await execute_local_llm(backend, prompt)

                # Extract code from LLM output: if there's a fenced code block,
                # take only its contents; otherwise use the full output stripped.
                import re as _re
                _fence_match = _re.search(
                    r"```[a-zA-Z0-9+#]*\n(.*?)```", generated_code.strip(), _re.DOTALL
                )
                if _fence_match:
                    generated_code = _fence_match.group(1).strip()
                else:
                    generated_code = generated_code.strip()

                # Check for self-refusal or LLM communication failure
                LLM_ERROR_PREFIXES = ("ERROR:", "Failed to communicate", "Ollama Error:", "Lmstudio Error:", "Llamacpp Error:")
                if any(generated_code.startswith(p) for p in LLM_ERROR_PREFIXES) or not telemetry:
                    status = "rejected_by_worker"
                    if generated_code.startswith("ERROR:"):
                        logging.warning(f"Task [{task_id}] rejected by worker guardrails.")
                    else:
                        logging.error(f"Task [{task_id}] rejected: LLM call failed — {generated_code[:120]}")
                else:
                    status = "success"
                    logging.info(f"Successfully generated code for task [{task_id}].")

                # Send completed result back to the broker
                await send(json.dumps({
                    "action": "task_completed",
                    "task_id": task_id,
                    "status": status,
                    "result": generated_code,
                    "telemetry": telemetry,
                }))
                logging.info(f"Sent completion report for task [{task_id}] to broker.")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - never crash the pool
                logging.error(f"Task [{task_id}] failed: {exc}")

    writer_task = asyncio.create_task(writer())
    try:
        # Keep the Firebase ID token fresh for the life of the session
        # (no-op in shared_secret mode); refreshed tokens share the outbox
        # with completions so outbound frames stay serialized.
        await auth.start_refresh(send)

        # Register as a worker node, declaring the pool's capacity so the
        # broker's dispatch never oversubscribes it.
        await send(json.dumps({
            "action": "register_worker",
            "version": VERSION,
            "capabilities": ["python", "cpp", "javascript", "typescript",
                             "rust", "golang", "php", "ruby", "java"],
            "max_concurrent": MAX_PARALLEL,
        }))
        logging.info(
            f"Registered worker capabilities with broker (max_concurrent={MAX_PARALLEL}). "
            "Listening for tasks..."
        )

        # Listen for dispatches; each assignment joins the pool.
        async for message_str in websocket:
            try:
                message = json.loads(message_str)
            except json.JSONDecodeError:
                logging.error("Error: Failed to decode received JSON message.")
                continue
            if message.get("type") == "error" and message.get("code") == "version_too_old":
                logging.error(message.get("message"))
                sys.exit(1)
            if message.get("type") == "task_assignment":
                job = asyncio.create_task(process(message))
                in_flight.add(job)
                job.add_done_callback(in_flight.discard)

    finally:
        # Graceful drain: let in-flight generations finish, then flush the
        # outbox (queued completion reports) through the writer before it
        # exits on the sentinel. Anything still pending is abandoned — the
        # broker's dispatch watchdog re-queues those tasks.
        if in_flight:
            await asyncio.wait(in_flight, timeout=DRAIN_TIMEOUT_S)
            for job in list(in_flight):
                job.cancel()
        try:
            await asyncio.wait_for(outbox.put(None), timeout=DRAIN_TIMEOUT_S)
        except (asyncio.TimeoutError, Exception):  # noqa: BLE001 - writer gone
            pass
        joiner = asyncio.create_task(outbox.join())
        try:
            await asyncio.wait_for(joiner, timeout=DRAIN_TIMEOUT_S)
        except (asyncio.TimeoutError, Exception):  # noqa: BLE001 - best effort
            pass
        finally:
            joiner.cancel()
        writer_task.cancel()
        try:
            await writer_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass


def _check_auth_ready() -> None:
    """Exit with a clear message if firebase auth is configured but no credentials exist."""
    auth_mode = os.environ.get("CODOKEN_AUTH", "shared_secret").lower()
    if auth_mode != "firebase":
        return
    auth_store = os.environ.get("CODOKEN_AUTH_STORE", ".codoken_auth.json")
    has_store = os.path.exists(auth_store)
    has_env = bool(os.environ.get("CODOKEN_EMAIL") and os.environ.get("CODOKEN_PASSWORD"))
    if not has_store and not has_env:
        logging.error(
            "CODOKEN_AUTH=firebase but no credentials found.\n"
            "  Run the setup wizard first:  python setup.py\n"
            "  Or set CODOKEN_EMAIL and CODOKEN_PASSWORD environment variables."
        )
        sys.exit(1)


async def run_worker():
    _check_auth_ready()
    while True:
        # Auto-detect active local LLM before connecting to the broker
        active_backend = await detect_local_llm()
        if not active_backend:
            logging.warning("No local LLM backend detected (Ollama, LM Studio, or llama.cpp). Retrying search in 5s...")
            await asyncio.sleep(5)
            continue

        auth = CodokenAuth(ROLE)
        try:
            uri, extra_headers = await auth.connect_config()
            logging.info(f"Connecting Worker to Codoken broker ({uri})...")
            async with websockets.connect(uri, additional_headers=extra_headers) as websocket:
                logging.info("Successfully connected to the broker!")
                await worker_session(websocket, auth, active_backend)
        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"Connection lost with broker ({e}). Reconnecting in {RECONNECT_DELAY}s...")
        except socket.gaierror:
            logging.error(f"Network or DNS issue. Reconnecting in {RECONNECT_DELAY}s...")
        except CodokenAuthError as e:
            logging.error(f"Authentication failed: {e}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Unexpected error: {e}. Reconnecting in {RECONNECT_DELAY}s...")
        finally:
            auth.stop()

        await asyncio.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logging.info("Worker stopped by user.")