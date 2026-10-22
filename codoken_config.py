"""
Codoken layered configuration — one place for every component's preferences.

Resolution order per key (first non-empty value wins):

  1. environment variable      always wins (CI, one-off overrides)
  2. .codoken/config.json      written by `python setup.py`
  3. built-in default

The preferences file is located by:

  * $CODOKEN_CONFIG pointing directly at a file, or
  * walking UP from the working directory looking for .codoken/config.json
    (git-style: it works from any subdirectory of the checkout).

$CODOKEN_CONFIG=off (also "none"/"0"/empty) disables the file lookup
entirely. The test suites (test_offline / test_cli / run_local_tests)
export this so a personal preference file can never leak into a test run.

How it integrates: the client components call apply_to_env(role) at
import time, which copies the file's values for that role into the
environment with setdefault-style semantics (an already-exported
variable is never touched). All existing os.environ reads keep working
unchanged, so setups that only use environment variables behave exactly
as before.

Example file (see codoken.config.example.json):
    {
      "broker": "ws://127.0.0.1:9000",
      "auth": "shared_secret",
      "persist": "off",
      "worker_parallel": "2"
    }
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

CONFIG_DIR = ".codoken"
CONFIG_FILE = "config.json"

ROLES = ("broker", "worker", "validator", "developer")

# key -> (environment variable, built-in default or None, roles that use it)
KEYS: Dict[str, tuple] = {
    "broker": ("CODOKEN_BROKER_WS", "wss://broker.codoken.dev",
               frozenset({"worker", "validator", "developer"})),
    "auth": ("CODOKEN_AUTH", "shared_secret",
             frozenset({"broker", "worker", "validator", "developer"})),
    "persist": ("CODOKEN_PERSIST", "off", frozenset({"broker"})),
    "firebase_credentials": ("FIREBASE_CREDENTIALS", "firebase-credentials.json",
                             frozenset({"broker"})),
    "firebase_web_config": ("FIREBASE_WEB_CONFIG", "firebase-web-config.json",
                            frozenset({"worker", "validator", "developer"})),
    "auth_store": ("CODOKEN_AUTH_STORE", ".codoken_auth.json",
                   frozenset({"worker", "validator", "developer"})),
    "worker_parallel": ("CODOKEN_WORKER_PARALLEL", "2", frozenset({"worker"})),
    "worker_model": ("CODOKEN_WORKER_MODEL", None, frozenset({"worker"})),
    "docker_host": ("DOCKER_HOST", None, frozenset({"validator"})),
}

_OFF = ("", "off", "none", "0")


def config_file() -> Optional[Path]:
    """Path of the preferences file in effect, or None.

    Resolution order:
      1. $CODOKEN_CONFIG (explicit path, or 'off'/'none'/'0' to disable)
      2. ~/.codoken/config.json  (user-level install; written by the setup wizard)
      3. Walk up from CWD looking for .codoken/config.json  (repo-local devnet)
    """
    raw = os.environ.get("CODOKEN_CONFIG")
    if raw is not None and raw.strip().lower() in _OFF:
        return None
    if raw and raw.strip():
        path = Path(raw.strip()).expanduser()
        return path if path.is_file() else None
    user_cfg = Path.home() / CONFIG_DIR / CONFIG_FILE
    if user_cfg.is_file():
        return user_cfg
    here = Path.cwd().resolve()
    for candidate in (here, *here.parents):
        path = candidate / CONFIG_DIR / CONFIG_FILE
        if path.is_file():
            return path
    return None


def load_file(path: Optional[Path] = None) -> Dict[str, Any]:
    """The preferences file as a dict; {} when absent or unreadable."""
    file = path if path is not None else config_file()
    if file is None:
        return {}
    try:
        with open(file, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:  # noqa: BLE001 - corrupt file: fall back to defaults
        return {}
    return data if isinstance(data, dict) else {}


def file_value(key: str, data: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """The file's value for `key` as a string; None when absent/empty."""
    if key not in KEYS:
        raise KeyError(f"unknown config key {key!r} (known: {sorted(KEYS)})")
    if data is None:
        data = load_file()
    value = data.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None  # an empty value means "unset" (e.g. docker_host auto)


def effective(key: str) -> Optional[str]:
    """Full resolution for one key: environment > file > built-in default."""
    env_name, default, _ = KEYS[key]
    value = os.environ.get(env_name)
    if value is not None and value.strip():
        return value
    value = file_value(key)
    if value is not None:
        return value
    return default


def keys_for_role(role: str) -> list[str]:
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r} (expected one of {ROLES})")
    return [key for key, (_, _, roles) in KEYS.items() if role in roles]


def apply_to_env(role: str) -> list[str]:
    """Push the preferences file's values for `role` into the environment.

    setdefault semantics: an already-exported (non-empty) variable always wins,
    so CI/automation is unaffected. Import-safe: a silent no-op when there is
    no preferences file. Returns the environment names that were applied.
    """
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r} (expected one of {ROLES})")
    data = load_file()
    if not data:
        return []
    applied = []
    for key in keys_for_role(role):
        value = file_value(key, data)
        if value is None:
            continue
        env_name = KEYS[key][0]
        if not os.environ.get(env_name, "").strip():
            os.environ[env_name] = value
            applied.append(env_name)
    return applied
