import asyncio
import json
import logging
import os
import socket
import sys
import time
from typing import Any, Dict, Optional
import httpx
import websockets
from websockets.exceptions import ConnectionClosed as WsConnectionClosed, InvalidStatus

import codoken_config

codoken_config.apply_to_env("trusted_worker")

from codoken_auth import CodokenAuth, CodokenAuthError, CodokenNetworkError  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

ROLE = "trusted_worker"
VERSION = "1.4.0"

RECONNECT_DELAY = 5
RECONNECT_MAX   = 300

# LLM config — overridable via env or ~/.codoken/config.json (trusted_worker section)
LLM_URL:     str = os.environ.get("CODOKEN_TRUSTED_LLM_URL",   "http://localhost:11434/v1")
LLM_MODEL:   str = os.environ.get("CODOKEN_TRUSTED_LLM_MODEL", "")
LLM_LABEL:   str = os.environ.get("CODOKEN_TRUSTED_LLM_LABEL", "")
LLM_TIMEOUT: float = float(os.environ.get("CODOKEN_TRUSTED_LLM_TIMEOUT", "300"))

PAUSE_FILE = os.path.join(os.path.expanduser("~"), ".codoken", "paused")


def _backoff(failures: int) -> float:
    return min(RECONNECT_DELAY * (2 ** failures), RECONNECT_MAX)


def _load_config_overrides() -> None:
    """Apply ~/.codoken/config.json[trusted_worker] keys to module globals."""
    global LLM_URL, LLM_MODEL, LLM_LABEL
    cfg_path = os.path.join(os.path.expanduser("~"), ".codoken", "config.json")
    try:
        with open(cfg_path) as f:
            cfg = json.load(f)
        tw = cfg.get("trusted_worker", {})
        if tw.get("llm_url") and not os.environ.get("CODOKEN_TRUSTED_LLM_URL"):
            LLM_URL = tw["llm_url"]
        if tw.get("model") and not os.environ.get("CODOKEN_TRUSTED_LLM_MODEL"):
            LLM_MODEL = tw["model"]
        if tw.get("label") and not os.environ.get("CODOKEN_TRUSTED_LLM_LABEL"):
            LLM_LABEL = tw["label"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass


async def _auto_detect_model(base_url: str) -> str:
    """Try to discover the first available model from the endpoint."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(f"{base_url}/models")
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    return data[0].get("id", "")
    except Exception:
        pass
    return ""


async def _call_llm(prompt: Optional[str], messages_json: Optional[str] = None,
                   temperature: Optional[float] = None, max_tokens: Optional[int] = None) -> tuple[str, int]:
    """Call the OpenAI-compatible endpoint; return (content, total_tokens).

    If messages_json is provided it takes precedence over prompt — this is the
    path used by the HTTP proxy so the full conversation history is preserved.
    """
    model = LLM_MODEL or await _auto_detect_model(LLM_URL)
    if not model:
        raise RuntimeError(f"No model configured and auto-detection failed for {LLM_URL}")

    if messages_json:
        try:
            messages = json.loads(messages_json)
        except Exception:
            messages = [{"role": "user", "content": messages_json}]
    elif prompt:
        messages = [{"role": "user", "content": prompt}]
    else:
        raise RuntimeError("Neither prompt nor messages_json provided")

    payload: Dict[str, Any] = {"model": model, "messages": messages, "stream": False}
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens

    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        r = await client.post(f"{LLM_URL}/chat/completions", json=payload)
        r.raise_for_status()
        data = r.json()

    content = data["choices"][0]["message"]["content"].strip()
    usage = data.get("usage") or {}
    total_tokens = usage.get("total_tokens") or (
        (usage.get("prompt_tokens") or 0) + (usage.get("completion_tokens") or 0)
    )
    return content, total_tokens


async def trusted_worker_session(websocket, auth: CodokenAuth) -> None:
    outbox: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=512)

    async def send(payload: str) -> None:
        await outbox.put(payload)

    async def writer() -> None:
        while True:
            payload = await outbox.get()
            try:
                if payload is not None:
                    await websocket.send(payload)
            except Exception:
                pass
            finally:
                outbox.task_done()
            if payload is None:
                return

    writer_task = asyncio.create_task(writer())

    try:
        await auth.start_refresh(send)

        await send(json.dumps({
            "action":         "register_trusted_worker",
            "version":        VERSION,
            "model_label":    LLM_LABEL or LLM_MODEL or "unknown",
            "endpoint_label": LLM_URL,
        }))
        logging.info("Registered as trusted worker. Waiting for tasks...")

        async for message_str in websocket:
            try:
                message = json.loads(message_str)
            except json.JSONDecodeError:
                logging.error("Failed to decode message.")
                continue

            msg_type = message.get("type")
            action   = message.get("action")

            if msg_type == "error" and message.get("code") == "version_too_old":
                logging.error(message.get("message"))
                sys.exit(1)

            elif action == "trusted_task_dispatch" or message.get("type") == "trusted_task_dispatch":
                task_id       = message.get("task_id", "?")
                prompt        = message.get("prompt") or None
                messages_json = message.get("messages_json") or None
                temperature   = message.get("temperature")
                max_tokens    = message.get("max_tokens")
                logging.info(f"[trusted] task {task_id} received — calling LLM...")
                try:
                    content, total_tokens = await _call_llm(
                        prompt, messages_json=messages_json,
                        temperature=temperature, max_tokens=max_tokens,
                    )
                    logging.info(f"[trusted] task {task_id} done — {total_tokens} tokens")
                    await send(json.dumps({
                        "action":       "trusted_task_completed",
                        "task_id":      task_id,
                        "result":       content,
                        "total_tokens": total_tokens,
                    }))
                except Exception as exc:
                    logging.error(f"[trusted] task {task_id} failed: {exc}")
                    await send(json.dumps({
                        "action":  "trusted_task_failed",
                        "task_id": task_id,
                        "error":   str(exc),
                    }))

    finally:
        auth.stop()
        await asyncio.wait_for(outbox.put(None), timeout=5.0)
        try:
            await asyncio.wait_for(outbox.join(), timeout=5.0)
        except asyncio.TimeoutError:
            pass
        writer_task.cancel()
        try:
            await writer_task
        except (asyncio.CancelledError, Exception):
            pass


async def run_trusted_worker():
    _load_config_overrides()

    if not LLM_MODEL and not LLM_URL:
        logging.error("No LLM configured. Set CODOKEN_TRUSTED_LLM_URL and CODOKEN_TRUSTED_LLM_MODEL or run codoken_setup.py.")
        sys.exit(1)

    logging.info(f"Trusted worker starting — endpoint: {LLM_URL}, model: {LLM_MODEL or '(auto-detect)'}, label: {LLM_LABEL or '(none)'}")

    _reauthed = False
    consecutive_failures = 0
    auth = CodokenAuth(ROLE)

    while True:
        if os.path.exists(PAUSE_FILE):
            logging.info("Paused. Sleeping 60s...")
            await asyncio.sleep(60)
            continue

        try:
            uri, extra_headers = await auth.connect_config()
            logging.info(f"Connecting trusted worker to {uri}...")
            async with websockets.connect(uri, additional_headers=extra_headers) as websocket:
                logging.info("Connected to broker.")
                consecutive_failures = 0
                await trusted_worker_session(websocket, auth)
        except WsConnectionClosed as e:
            consecutive_failures += 1
            logging.warning(f"Connection lost ({e}). Reconnecting in {_backoff(consecutive_failures):.0f}s...")
        except (socket.gaierror, OSError, CodokenNetworkError) as e:
            consecutive_failures += 1
            delay = _backoff(consecutive_failures)
            logging.warning(f"Broker unreachable ({type(e).__name__}). Retrying in {delay:.0f}s...")
        except InvalidStatus as e:
            if e.response.status_code in (401, 403):
                if _reauthed:
                    logging.error("Authentication failed after reauth. Exiting.")
                    sys.exit(1)
                logging.warning("Authentication rejected. Launching re-authentication...")
                try:
                    await CodokenAuth.reauth()
                    _reauthed = True
                except Exception as reauth_exc:
                    logging.error(f"Re-authentication failed: {reauth_exc}. Exiting.")
                    sys.exit(1)
                consecutive_failures = 0
                continue
            else:
                consecutive_failures += 1
                logging.error(f"Broker returned HTTP {e.response.status_code}. Retrying in {_backoff(consecutive_failures):.0f}s...")
        except CodokenAuthError as e:
            if _reauthed:
                logging.error(f"Authentication failed after reauth: {e}. Exiting.")
                sys.exit(1)
            logging.warning(f"Authentication failed: {e}. Launching re-authentication...")
            try:
                await CodokenAuth.reauth()
                _reauthed = True
            except Exception as reauth_exc:
                logging.error(f"Re-authentication failed: {reauth_exc}. Exiting.")
                sys.exit(1)
            consecutive_failures = 0
            continue
        except Exception as e:
            consecutive_failures += 1
            logging.error(f"Unexpected error: {e}. Reconnecting in {_backoff(consecutive_failures):.0f}s...")

        await asyncio.sleep(_backoff(consecutive_failures))


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in ("--version", "-V"):
        print(VERSION)
        sys.exit(0)
    try:
        asyncio.run(run_trusted_worker())
    except KeyboardInterrupt:
        logging.info("Trusted worker stopped by user.")
