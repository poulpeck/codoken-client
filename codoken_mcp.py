"""
Codoken MCP Server — exposes Codoken as tools for AI agents (Claude Code,
Claude Desktop, Cursor, etc.) via the Model Context Protocol stdio transport.

Usage (add to your MCP client config):
    {
      "mcpServers": {
        "codoken": {
          "command": "python",
          "args": ["/path/to/codoken_mcp.py"],
          "env": {
            "CODOKEN_AUTH": "firebase",
            "CODOKEN_AUTH_STORE": "/path/to/.codoken_auth_developer.json"
          }
        }
      }
    }

Or run directly for testing:
    CODOKEN_AUTH=firebase python codoken_mcp.py
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, Optional

from mcp.server import MCPServer

import codoken_config
codoken_config.apply_to_env("developer")

from codoken_auth import CodokenAuth, CodokenAuthError  # noqa: E402

import websockets
from websockets.exceptions import ConnectionClosed

# ---------------------------------------------------------------------------
# Supported languages (canonical list lives in codoken_config)
# ---------------------------------------------------------------------------
from codoken_config import SUPPORTED_LANGUAGES

# ---------------------------------------------------------------------------
# Broker connection helpers
# ---------------------------------------------------------------------------

async def _connect(broker: Optional[str] = None):
    auth_store = os.environ.get("CODOKEN_AUTH_STORE", str(Path.home() / ".codoken" / "auth.json"))
    auth = CodokenAuth("developer", broker_base=broker, auth_store=auth_store)
    uri, headers = await auth.connect_config()
    ws = await websockets.connect(uri, additional_headers=headers)
    await auth.start_refresh(ws.send)
    return ws, auth


async def _send_then_await(ws, action: str, want_type: str,
                            extra: Optional[Dict[str, Any]] = None,
                            timeout: float = 60.0) -> Dict[str, Any]:
    await ws.send(json.dumps({"action": action, **(extra or {})}))
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"broker never answered {action!r}")
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        except ConnectionClosed as exc:
            raise RuntimeError(f"broker closed the connection: {exc}") from exc
        msg = json.loads(raw)
        if msg.get("type") == want_type:
            return msg


async def _submit_and_wait(task_id: str, manifest: Dict[str, Any],
                            broker: Optional[str], timeout: float = 120.0) -> Dict[str, Any]:
    ws, auth = await _connect(broker)
    try:
        await ws.send(json.dumps({
            "action": "submit_manifest",
            "task_id": task_id,
            "manifest": manifest,
        }))
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"status": "timeout", "task_id": task_id}
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except (asyncio.TimeoutError, ConnectionClosed):
                return {"status": "connection_closed", "task_id": task_id}
            msg = json.loads(raw)
            if msg.get("type") == "task_result" and msg.get("task_id") == task_id:
                return msg
    finally:
        auth.stop()
        try:
            await ws.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------
mcp = MCPServer("codoken")
_broker = os.environ.get("CODOKEN_BROKER_WS")


@mcp.tool()
async def submit_atom(
    task_id: str,
    language: str,
    prompt: str,
    test_code: str,
    timeout: float = 120.0,
) -> str:
    """Submit a single-atom coding task to the Codoken network.

    A worker LLM generates code; a validator committee runs it against the
    test harness and returns APPROVED or REJECTED with the generated code.
    Use this to generate and verify a single function or class with known
    inputs/outputs.

    Args:
        task_id: Unique identifier for this task (e.g. 'task_factorial').
        language: Programming language — one of: python, javascript,
            typescript, golang, rust, cpp, php, ruby, java.
        prompt: Instruction to the worker LLM. Be precise: specify function
            name, signature, return type, and edge cases. Return only the
            function/class definition, no main block.
        test_code: Test harness appended to the solution. Must exit 0 on
            success, non-zero on failure. Use assert statements or raise
            exceptions for failures.
        timeout: Max seconds to wait for a verdict (default 120).
    """
    if language not in SUPPORTED_LANGUAGES:
        return f"Unsupported language '{language}'. Supported: {', '.join(SUPPORTED_LANGUAGES)}"

    manifest = {
        "version": "1.0",
        "environment": {"language": language},
        "worker_payload": {"prompt": prompt},
        "validator_payload": {"test_code": test_code},
    }

    try:
        result = await _submit_and_wait(task_id, manifest, _broker, timeout)
    except CodokenAuthError as exc:
        return f"Auth error: {exc}"
    except TimeoutError as exc:
        return f"Timeout: {exc}"
    except Exception as exc:
        return f"Error: {exc}"

    status = result.get("status", "unknown")
    code = result.get("code", "")
    credits = result.get("credits") or {}

    if status == "approved":
        return (
            f"APPROVED — task {task_id}\n"
            f"Credits: developer {credits.get('developer', 0):+.2f}, "
            f"worker {credits.get('worker', 0):+.2f}\n\n"
            f"Generated code:\n```{language}\n{code}\n```"
        )
    elif status == "rejected":
        return (
            f"REJECTED — task {task_id}\n"
            f"Reason: {result.get('details', 'validation failed')}\n"
            f"Credits: developer {credits.get('developer', 0):+.2f}"
        )
    else:
        return f"Task {task_id} ended with status: {status}\n{json.dumps(result, indent=2)}"


@mcp.tool()
async def submit_manifest(
    task_id: str,
    language: str,
    atoms: list,
    timeout: float = 300.0,
) -> str:
    """Submit a multi-atom manifest to the Codoken network.

    Each atom is independently dispatched to a worker and validated in
    parallel. The overall task is APPROVED only if every atom is approved.
    Use this to generate and verify several related functions in one shot.

    Args:
        task_id: Unique identifier for the overall task (e.g. 'task_string_utils').
        language: Default programming language for all atoms — one of: python,
            javascript, typescript, golang, rust, cpp, php, ruby, java.
            Individual atoms can override this with their own 'language' key.
        atoms: List of atom dicts. Each must have:
            - 'id' (str): unique atom identifier within this task
            - 'prompt' (str): instruction to the worker LLM
            - 'test_code' (str): test harness that exits 0 on success
            Optionally: 'language' (str) to override the top-level language.
        timeout: Max seconds to wait for all atoms to resolve (default 300).
    """
    if not atoms:
        return "Error: 'atoms' must be a non-empty list"
    if len(atoms) > 16:
        return "Error: too many atoms; max is 16"

    for i, atom in enumerate(atoms):
        if not isinstance(atom, dict):
            return f"Error: atoms[{i}] must be a dict"
        if not atom.get("id"):
            return f"Error: atoms[{i}] is missing 'id'"
        if not atom.get("prompt"):
            return f"Error: atoms[{i}] is missing 'prompt'"
        if not atom.get("test_code"):
            return f"Error: atoms[{i}] is missing 'test_code'"
        lang = atom.get("language", language)
        if lang not in SUPPORTED_LANGUAGES:
            return f"Error: atoms[{i}] language '{lang}' not supported. Supported: {', '.join(SUPPORTED_LANGUAGES)}"

    manifest_atoms = []
    for atom in atoms:
        entry: Dict[str, Any] = {
            "id": atom["id"],
            "worker_payload": {"prompt": atom["prompt"]},
            "validator_payload": {"test_code": atom["test_code"]},
        }
        if "language" in atom:
            entry["environment"] = {"language": atom["language"]}
        manifest_atoms.append(entry)

    manifest = {
        "version": "1.0",
        "environment": {"language": language},
        "atoms": manifest_atoms,
    }

    try:
        result = await _submit_and_wait(task_id, manifest, _broker, timeout)
    except CodokenAuthError as exc:
        return f"Auth error: {exc}"
    except TimeoutError as exc:
        return f"Timeout: {exc}"
    except Exception as exc:
        return f"Error: {exc}"

    status = result.get("status", "unknown")
    credits = result.get("credits") or {}
    consensus = result.get("consensus") or {}
    atom_verdicts = consensus.get("atoms") or []
    codes: Dict[str, str] = result.get("code") or {}

    if status not in ("approved", "rejected"):
        return f"Task {task_id} ended with status: {status}\n{json.dumps(result, indent=2)}"

    lines = [
        f"{'APPROVED' if status == 'approved' else 'REJECTED'} — task {task_id}",
        f"Credits: developer {credits.get('developer', 0):+.2f}",
        "",
        f"Atoms ({len(atom_verdicts)}):",
    ]
    for av in atom_verdicts:
        aid = av.get("atom_id", "?")
        decision = av.get("decision", "unknown")
        lines.append(f"  [{aid}] {decision}")
        if decision == "APPROVED" and aid in codes:
            lang = next((a.get("language", language) for a in atoms if a["id"] == aid), language)
            lines.append(f"  ```{lang}")
            lines.append(codes[aid])
            lines.append("  ```")
        elif decision != "APPROVED":
            lines.append(f"  Reason: {av.get('reason', 'validation failed')}")

    return "\n".join(lines)


@mcp.tool()
async def get_status(task_id: str) -> str:
    """Check the current status of a submitted Codoken task.

    Args:
        task_id: Task ID to check.
    """
    try:
        ws, auth = await _connect(_broker)
        try:
            msg = await _send_then_await(ws, "get_status", "task_status",
                                          extra={"task_id": task_id})
        finally:
            auth.stop()
            try:
                await ws.close()
            except Exception:
                pass
    except CodokenAuthError as exc:
        return f"Auth error: {exc}"
    except Exception as exc:
        return f"Error: {exc}"

    if msg.get("status") == "not_found":
        return f"Task {task_id}: not found"

    verdict = f" [{msg['decision']}]" if msg.get("resolved") else ""
    text = (
        f"Task {task_id}: {msg.get('status')}{verdict}\n"
        f"Round: {msg.get('round', 0)}, Votes: {msg.get('votes', 0)}"
    )
    if msg.get("worker"):
        text += f"\nWorker: {msg['worker']}"
    return text


@mcp.tool()
async def get_result(task_id: str) -> str:
    """Fetch the approved generated code for a resolved Codoken task.

    Returns the code string if APPROVED, or an error if not yet resolved
    or REJECTED.

    Args:
        task_id: Task ID to fetch.
    """
    try:
        ws, auth = await _connect(_broker)
        try:
            msg = await _send_then_await(ws, "get_status", "task_status",
                                          extra={"task_id": task_id})
        finally:
            auth.stop()
            try:
                await ws.close()
            except Exception:
                pass
    except CodokenAuthError as exc:
        return f"Auth error: {exc}"
    except Exception as exc:
        return f"Error: {exc}"

    if msg.get("status") == "not_found":
        return f"Task {task_id}: not found"
    if not msg.get("resolved"):
        return f"Task {task_id} is not yet resolved (status: {msg.get('status')})"
    if msg.get("decision") != "APPROVED":
        return f"Task {task_id} was {msg.get('decision')} — no code available"

    return msg.get("code") or ""


@mcp.tool()
async def get_balance() -> str:
    """Get the current Codoken credit balance for this account."""
    try:
        ws, auth = await _connect(_broker)
        try:
            msg = await _send_then_await(ws, "get_balance", "balance")
        finally:
            auth.stop()
            try:
                await ws.close()
            except Exception:
                pass
    except CodokenAuthError as exc:
        return f"Auth error: {exc}"
    except Exception as exc:
        return f"Error: {exc}"

    return f"Balance: {msg.get('balance', 0):,.2f} CU (uid: {msg.get('client_id')})"


@mcp.tool()
async def list_languages() -> str:
    """List all programming languages supported by the Codoken validator network."""
    return "Supported languages:\n" + "\n".join(f"- {lang}" for lang in SUPPORTED_LANGUAGES)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="stdio")
