"""
Codoken Developer CLI (spec 3.2: CLI & Agent Harness).

Submits local task manifests (YAML or JSON) to the broker and stream-logs
the pipeline in real time — dispatch, committee review, final verdict,
credit settlement — so it doubles as a drop-in for autonomous coding
agents:

    python developer.py submit manifests/gcd.yaml
    python developer.py status task_gcd
    python developer.py balance

Manifest files are YAML (plain JSON works too — it's a YAML subset). A
document is either the manifest itself, or a wire-style envelope with an
optional top-level task_id:

    task_id: task_gcd            # optional; defaults to "task_<file-stem>"
    manifest:
      environment: { language: cpp, runtime_version: cpp17 }
      worker_payload: { prompt: "..." }
      validator_payload: { test_code: "..." }

Exit codes (submit): 0 = APPROVED, 1 = REJECTED, 2 = error or timeout.
Exit codes (status):  0 = in flight or APPROVED, 1 = REJECTED, 2 = error.

Authentication follows codoken_auth: CODOKEN_AUTH=shared_secret (devnet)
or firebase (production anonymous identity, stable across runs).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import click
import websockets
import yaml
from websockets.exceptions import ConnectionClosed

VERSION = "1.1.0"

import codoken_config

# Layered preferences (.codoken/config.json -> env). Must run before
# codoken_auth is imported: it captures $CODOKEN_BROKER_WS at import time.
codoken_config.apply_to_env("developer")

from codoken_auth import CodokenAuth, CodokenAuthError  # noqa: E402


# ---------------------------------------------------------------------------
# Manifest loading (spec 3.2: "parse local task manifests (yaml/json)")
# ---------------------------------------------------------------------------
def load_manifest(path: str) -> Tuple[str, Dict[str, Any]]:
    """Parse a manifest file -> (task_id, manifest dict).

    Accepts YAML or JSON (yaml.safe_load covers both). The document may be
    a bare manifest mapping, or an envelope {"task_id": ..., "manifest": {...}}.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise click.ClickException(f"invalid YAML in {path}: {exc}") from exc
    except OSError as exc:
        raise click.ClickException(f"cannot read {path}: {exc}") from exc

    if not isinstance(doc, dict):
        raise click.ClickException(f"{path}: manifest must be a mapping")

    manifest = doc.get("manifest") if isinstance(doc.get("manifest"), dict) else doc
    if not manifest:
        raise click.ClickException(f"{path}: manifest is empty")
    if not isinstance(manifest, dict):
        raise click.ClickException(f"{path}: 'manifest' must be a mapping")

    # Multi-atom workloads (spec §2): an 'atoms' list, each a full or
    # partial atom (per-atom values override the top-level ones). Without it
    # the manifest is the legacy single-atom workload.
    atoms_raw = manifest.get("atoms")
    if atoms_raw is None:
        atoms = [manifest]
    else:
        if not isinstance(atoms_raw, list) or not atoms_raw:
            raise click.ClickException(f"{path}: 'atoms' must be a non-empty list")
        for i, atom in enumerate(atoms_raw):
            if not isinstance(atom, dict):
                raise click.ClickException(f"{path}: atoms[{i}] must be a mapping")
        seen: Dict[str, int] = {}
        for i, atom in enumerate(atoms_raw):
            aid = str(atom.get("id") or f"atom-{i + 1}")
            n = seen.get(aid, 0) + 1
            seen[aid] = n
            atom["id"] = aid if n == 1 else f"{aid}-{n}"
        atoms = list(atoms_raw)

    for i, atom in enumerate(atoms):
        label = str(atom.get("id") or f"atom-{i + 1}")
        for key in ("environment", "worker_payload", "validator_payload"):
            value = atom.get(key, manifest.get(key))
            if not isinstance(value, dict):
                value = {}
            if key == "environment" and not value.get("language"):
                raise click.ClickException(
                    f"{path}: atom '{label}': environment.language is required")
            if key == "worker_payload" and not value.get("prompt"):
                raise click.ClickException(
                    f"{path}: atom '{label}': worker_payload.prompt is required")
            if key == "validator_payload" and not value.get("test_code"):
                raise click.ClickException(
                    f"{path}: atom '{label}': validator_payload.test_code is required")

    task_id = str(doc.get("task_id") or f"task_{Path(path).stem}")
    return task_id, manifest


# ---------------------------------------------------------------------------
# Connection + streaming core
# ---------------------------------------------------------------------------
def _log(line: str = "", json_mode: bool = False) -> None:
    if not json_mode:
        print(line, flush=True)


def _emit_json(data: Any) -> None:
    print(json.dumps(data, indent=2), flush=True)


def _ts() -> str:
    return time.strftime("%H:%M:%S")


async def _connect(broker: Optional[str]):
    """Auth + open the developer socket. Returns (websocket, CodokenAuth)."""
    auth_store = os.environ.get("CODOKEN_AUTH_STORE", str(Path.home() / ".codoken" / "auth.json"))
    auth = CodokenAuth("developer", broker_base=broker, auth_store=auth_store)
    try:
        uri, headers = await auth.connect_config()
    except CodokenAuthError as exc:
        raise click.ClickException(f"authentication failed: {exc}") from exc
    _log(f"connecting to {uri} ...")
    try:
        websocket = await websockets.connect(uri, additional_headers=headers)
    except OSError as exc:
        auth.stop()
        raise click.ClickException(f"cannot reach broker at {uri}: {exc}") from exc
    await auth.start_refresh(websocket.send)  # no-op in shared_secret mode
    return websocket, auth


async def _send_then_await(
    websocket, action: str, want_type: str, extra: Optional[Dict[str, Any]] = None,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    """Send one action and wait for its reply, skipping unrelated messages."""
    payload = {"action": action, **(extra or {})}
    await websocket.send(json.dumps(payload))
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise click.ClickException(f"broker never answered {action!r}")
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
        except ConnectionClosed as exc:
            raise click.ClickException(f"broker closed the connection: {exc}") from exc
        msg = json.loads(raw)
        if msg.get("type") == want_type:
            return msg
        # welcome / strays: skip


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
@click.group()
@click.version_option(VERSION, "--version", "-V", message="%(version)s")
def cli() -> None:
    """Codoken developer client: submit manifests, track tasks, check credits."""


@cli.command()
@click.argument("manifest", type=click.Path(exists=True))
@click.option("--task-id", default=None, help="override the manifest's task_id")
@click.option("--broker", envvar="CODOKEN_BROKER_WS", default=None,
              help="broker base URL (default $CODOKEN_BROKER_WS or wss://broker.codoken.dev)")
@click.option("--out", type=click.Path(dir_okay=False), default=None,
              help="write approved generated code to this path")
@click.option("--timeout", type=float, default=0,
              help="abort the stream after N seconds (0 = no limit)")
@click.option("--json", "json_mode", is_flag=True, default=False,
              help="emit a single JSON object instead of human-readable output")
def submit(manifest: str, task_id: Optional[str], broker: Optional[str],
           out: Optional[str], timeout: float, json_mode: bool) -> None:
    """Submit a manifest file and stream the pipeline until the final verdict.

    Exits 0 when the committee approves, 1 when it rejects, 2 on error/timeout.
    """
    file_task_id, manifest_doc = load_manifest(manifest)
    task_id = task_id or file_task_id

    exit_code = asyncio.run(_submit(task_id, manifest_doc, broker, out, timeout, json_mode))
    sys.exit(exit_code)


async def _submit(task_id: str, manifest: Dict[str, Any],
                  broker: Optional[str], out: Optional[str],
                  timeout: float, json_mode: bool = False) -> int:
    websocket, auth = await _connect(broker)
    try:
        await websocket.send(json.dumps({
            "action": "submit_manifest",
            "task_id": task_id,
            "manifest": manifest,
        }))
        if not json_mode:
            _log(f"submitted {task_id}")

        deadline = time.monotonic() + timeout if timeout > 0 else None
        final_msg: Optional[Dict[str, Any]] = None
        while True:
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if json_mode:
                        _emit_json({"task_id": task_id, "status": "timeout"})
                    else:
                        _log(f"timed out — the task is still on the broker; check: "
                             f"python developer.py status {task_id}")
                    return 2
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            except (asyncio.TimeoutError, ConnectionClosed):
                if json_mode:
                    _emit_json({"task_id": task_id, "status": "connection_closed"})
                else:
                    _log("connection closed before a verdict; check: "
                         f"python developer.py status {task_id}")
                return 2
            msg = json.loads(raw)
            if json_mode:
                if msg.get("type") == "task_result" and msg.get("task_id") == task_id:
                    _emit_json(msg)
                    return 0 if msg.get("status") == "approved" else 1
            else:
                code = _render(msg, task_id, out)
                if code is not None:
                    return code
    finally:
        auth.stop()
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass


def _render(msg: Dict[str, Any], task_id: str,
            out: Optional[str] = None) -> Optional[int]:
    """Log one broker message. Returns an exit code only for a final verdict."""
    mtype = msg.get("type")
    if mtype == "welcome":
        uid = f", uid {msg['uid'][:12]}…" if msg.get("uid") else ""
        _log(f"connected as {msg.get('client_id')} ({msg.get('balance'):,.2f} credits{uid})")
    elif mtype == "token_refresh":
        pass  # keep the stream clean
    elif mtype in ("task_update", "task_result"):
        if msg.get("task_id") != task_id:
            return None  # not ours (broker scoping is per-developer, be safe)
        if mtype == "task_update":
            status = str(msg.get("status"))
            _log(f"  [{_ts()}] {status:<12} {msg.get('details', '')}")
            return 2 if status == "error" else None
        return _render_result(msg, out)
    return None


def _render_result(msg: Dict[str, Any], out: Optional[str] = None) -> int:
    status = msg.get("status")
    credits = msg.get("credits") or {}
    consensus = msg.get("consensus") or {}
    _log(f"  [{_ts()}] {status or '?':<12} {msg.get('details', '')}")
    if msg.get("worker"):
        _log(f"    generated by {msg['worker']}")
    atoms = consensus.get("atoms")
    if atoms:
        # Composite (multi-atom) verdict: one line per atom. Credits may map
        # a per-client amount for the workers that served different atoms.
        for a in atoms:
            line = (f"    atom {a.get('atom_id')}: {a.get('decision') or '?'}"
                    f" — {a.get('reason', '')}")
            if a.get("worker"):
                line += f" (by {a['worker']})"
            _log(line)
        parts = [f"developer {credits.get('developer', 0):+.2f}"]
        worker = credits.get("worker")
        if isinstance(worker, dict):
            parts.append("workers "
                         + ", ".join(f"{k} {v:+.2f}" for k, v in worker.items()))
        elif worker:
            parts.append(f"worker {worker:+.2f}")
        if credits.get("validators"):
            parts.append("validators " + ", ".join(
                f"{k} {v:+.2f}" for k, v in credits["validators"].items()))
        _log(f"    credits — " + ", ".join(parts))
        code = msg.get("code")
        if status == "approved" and isinstance(code, dict) and code:
            for aid, text in code.items():
                if out:
                    base = Path(out)
                    dest = base.with_name(f"{base.stem}-{aid}{base.suffix}")
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text(text, encoding="utf-8")
                    _log(f"    atom {aid} code written to {dest}")
                else:
                    _log(f"    atom {aid} code:")
                    _log("\n".join(f"    {line}" for line in str(text).splitlines()))
        return 0 if status == "approved" else 1
    rounds = consensus.get("rounds") or []
    for rnd in rounds:
        votes = ", ".join(
            f"{v.get('validator')}:{v.get('status')}({v.get('exit_code')})"
            + (f" — {v['details']}" if v.get('details') else "")
            for v in rnd.get("votes") or []
        )
        line = f"    round {rnd.get('round')}: {votes or 'no votes'}"
        if rnd.get("stdout_match") is False:
            line += (f"  [output confrontation FAILED: "
                     f"{rnd.get('distinct_outputs')} distinct outputs]")
        _log(line)
    if consensus.get("outliers"):
        _log(f"    outliers (unpaid): {', '.join(consensus['outliers'])}")
    _log(f"    credits — developer {credits.get('developer', 0):+.2f}"
         + (f", worker {credits['worker']:+.2f}" if credits.get("worker") else "")
         + (", validators "
            + ", ".join(f"{k} {v:+.2f}" for k, v in credits.get("validators", {}).items())
            if credits.get("validators") else ""))

    code = msg.get("code")
    if status == "approved":
        if code and out:
            Path(out).parent.mkdir(parents=True, exist_ok=True)
            Path(out).write_text(code, encoding="utf-8")
            _log(f"    generated code written to {out}")
        elif code:
            _log("    generated code:")
            _log("\n".join(f"    {line}" for line in code.splitlines()))
        return 0
    return 1


@cli.command(name="status")
@click.argument("task_id")
@click.option("--broker", envvar="CODOKEN_BROKER_WS", default=None,
              help="broker base URL (default $CODOKEN_BROKER_WS or wss://broker.codoken.dev)")
@click.option("--json", "json_mode", is_flag=True, default=False,
              help="emit a single JSON object instead of human-readable output")
def status_cmd(task_id: str, broker: Optional[str], json_mode: bool) -> None:
    """One-shot: show where a task is in the pipeline."""
    try:
        code = asyncio.run(_status(task_id, broker, json_mode))
    except click.ClickException as exc:
        sys.exit(f"error: {exc.message}")
    sys.exit(code)


async def _status(task_id: str, broker: Optional[str], json_mode: bool = False) -> int:
    websocket, auth = await _connect(broker)
    try:
        msg = await _send_then_await(websocket, "get_status", "task_status",
                                     extra={"task_id": task_id})
    finally:
        auth.stop()
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass
    if json_mode:
        _emit_json(msg)
        return 2 if msg.get("status") == "not_found" else (
            0 if msg.get("decision") == "APPROVED" else 1 if msg.get("resolved") else 0
        )
    state = msg.get("status")
    if state == "not_found":
        _log(f"{task_id}: not found (no such task on this broker)")
        return 2
    verdict = f"  [resolved: {msg['decision']}]" if msg.get("resolved") else ""
    if msg.get("atoms"):
        _log(f"{task_id}: {state}{verdict}  {len(msg['atoms'])} atom(s)")
        for a in msg["atoms"]:
            atom_verdict = f"  [{a['decision']}]" if a.get("decision") else ""
            lang = f" ({a['language']})" if a.get("language") else ""
            _log(f"  atom {a.get('id')}: {a.get('state')}{atom_verdict}{lang}")
    else:
        _log(f"{task_id}: {state}{verdict}  round {msg.get('round', 0)}"
             f", {msg.get('votes', 0)} vote(s){', worker ' + msg['worker'] if msg.get('worker') else ''}")
    if msg.get("details"):
        _log(f"  {msg['details']}")
    if msg.get("resolved"):
        return 0 if msg.get("decision") == "APPROVED" else 1
    return 0


@cli.command()
@click.option("--broker", envvar="CODOKEN_BROKER_WS", default=None,
              help="broker base URL (default $CODOKEN_BROKER_WS or wss://broker.codoken.dev)")
@click.option("--json", "json_mode", is_flag=True, default=False,
              help="emit a single JSON object instead of human-readable output")
def balance(broker: Optional[str], json_mode: bool) -> None:
    """One-shot: show this client's credit balance."""
    try:
        msg = asyncio.run(_balance(broker))
    except click.ClickException as exc:
        sys.exit(f"error: {exc.message}")
    if json_mode:
        _emit_json({"client_id": msg.get("client_id"), "balance": msg.get("balance")})
    else:
        _log(f"{msg.get('client_id')}: balance {msg.get('balance'):,.2f} credits")
    sys.exit(0)


async def _balance(broker: Optional[str]) -> Dict[str, Any]:
    websocket, auth = await _connect(broker)
    try:
        return await _send_then_await(websocket, "get_balance", "balance")
    finally:
        auth.stop()
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001 - already closed is fine
            pass


@cli.command()
@click.option("--broker", envvar="CODOKEN_BROKER_WS", default=None,
              help="broker base URL (default $CODOKEN_BROKER_WS or wss://broker.codoken.dev)")
def info(broker: Optional[str]) -> None:
    """Show your account, credit balance, network status, and recent tasks."""
    try:
        asyncio.run(_info(broker))
    except click.ClickException as exc:
        sys.exit(f"error: {exc.message}")
    sys.exit(0)


async def _info(broker: Optional[str]) -> None:
    import httpx

    broker_base = broker or codoken_config.effective("broker") or "wss://broker.codoken.dev"

    def http_base(b: str) -> str:
        b = b.rstrip("/")
        if b.startswith("wss://"):
            return "https://" + b[len("wss://"):]
        if b.startswith("ws://"):
            return "http://" + b[len("ws://"):]
        return b

    # Broker snapshot (no auth needed)
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            health = (await http.get(f"{http_base(broker_base)}/health")).json()
            root = (await http.get(f"{http_base(broker_base)}/")).json()
    except Exception as exc:
        _log(f"[WARN] Could not reach broker: {exc}")
        health = {}
        root = {}

    _log("=== Codoken Network ===")
    status = health.get("status", "unreachable")
    _log(f"  Broker       : {broker_base}  [{status}]")
    if status == "ok":
        _log(f"  Workers      : {health.get('active_workers', 0)} connected  "
             f"({health.get('busy_task_slots', 0)} busy)")
        _log(f"  Validators   : {health.get('active_validators', 0)} connected")
        _log(f"  Tasks tracked: {health.get('tracked_tasks', 0)}")

    # Account (requires auth)
    _log("\n=== Your Account ===")
    websocket, auth = await _connect(broker)
    try:
        bal_msg = await _send_then_await(websocket, "get_balance", "balance", timeout=10)
        my_uid = bal_msg.get("client_id") or auth._uid or ""
        _log(f"  UID          : {my_uid}")
        _log(f"  Balance      : {bal_msg.get('balance', 0):,.2f} credits")

        # Recent tasks from broker root snapshot — filtered to this user
        tasks = root.get("tasks", {})
        my_tasks = [
            (tid, t) for tid, t in tasks.items()
            if t.get("developer") == my_uid
        ]
        if my_tasks:
            _log(f"\n=== Recent Tasks (last {min(5, len(my_tasks))}) ===")
            for tid, t in list(my_tasks)[-5:]:
                decision = t.get("decision") or t.get("status") or "in progress"
                _log(f"  {tid:<30} {decision}")
        else:
            _log("\n  No tasks found for your account on this broker.")
    finally:
        auth.stop()
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass


@cli.command(name="result")
@click.argument("task_id")
@click.option("--broker", envvar="CODOKEN_BROKER_WS", default=None,
              help="broker base URL")
@click.option("--out", type=click.Path(dir_okay=False), default=None,
              help="write code to this file instead of stdout")
@click.option("--json", "json_mode", is_flag=True, default=False,
              help="emit a single JSON object")
def result_cmd(task_id: str, broker: Optional[str], out: Optional[str],
               json_mode: bool) -> None:
    """Fetch the approved generated code for a resolved task."""
    try:
        code = asyncio.run(_result(task_id, broker, out, json_mode))
    except click.ClickException as exc:
        sys.exit(f"error: {exc.message}")
    sys.exit(code)


async def _result(task_id: str, broker: Optional[str], out: Optional[str],
                  json_mode: bool) -> int:
    websocket, auth = await _connect(broker)
    try:
        msg = await _send_then_await(websocket, "get_status", "task_status",
                                     extra={"task_id": task_id})
    finally:
        auth.stop()
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass

    if msg.get("status") == "not_found":
        if json_mode:
            _emit_json({"task_id": task_id, "error": "not_found"})
        else:
            _log(f"{task_id}: not found")
        return 2

    if not msg.get("resolved"):
        if json_mode:
            _emit_json({"task_id": task_id, "status": msg.get("status"), "resolved": False})
        else:
            _log(f"{task_id}: not yet resolved (status: {msg.get('status')})")
        return 2

    if msg.get("decision") != "APPROVED":
        if json_mode:
            _emit_json({"task_id": task_id, "decision": msg.get("decision")})
        else:
            _log(f"{task_id}: {msg.get('decision')} — no code to retrieve")
        return 1

    code = msg.get("code") or ""
    if json_mode:
        _emit_json({"task_id": task_id, "decision": "APPROVED", "code": code})
    elif out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        Path(out).write_text(code, encoding="utf-8")
        _log(f"code written to {out}")
    else:
        _log(code)
    return 0


@cli.command(name="validate")
@click.argument("manifest", type=click.Path(exists=True))
@click.option("--json", "json_mode", is_flag=True, default=False,
              help="emit a single JSON object")
def validate_cmd(manifest: str, json_mode: bool) -> None:
    """Pre-flight check a manifest file without submitting it."""
    try:
        task_id, manifest_doc = load_manifest(manifest)
    except click.ClickException as exc:
        if json_mode:
            _emit_json({"valid": False, "error": exc.message})
        else:
            _log(f"INVALID: {exc.message}")
        sys.exit(1)

    atoms = manifest_doc.get("atoms")
    atom_count = len(atoms) if isinstance(atoms, list) else 1
    language = (manifest_doc.get("environment") or {}).get("language", "?")
    if isinstance(atoms, list):
        languages = list({
            (a.get("environment") or manifest_doc.get("environment") or {}).get("language", "?")
            for a in atoms
        })
    else:
        languages = [language]

    if json_mode:
        _emit_json({
            "valid": True,
            "task_id": task_id,
            "atoms": atom_count,
            "languages": languages,
        })
    else:
        _log(f"OK  {manifest}")
        _log(f"    task_id  : {task_id}")
        _log(f"    atoms    : {atom_count}")
        _log(f"    languages: {', '.join(languages)}")
    sys.exit(0)


if __name__ == "__main__":
    cli()
