# Codoken

A decentralized P2P network where worker nodes (local LLMs) generate code and
validator nodes (sandboxed containers) independently verify it. Developers submit
tasks and pay with Codoken credits; workers and validators earn credits for good work.

---

## Joining as a Beta Tester

### Prerequisites

| Role | What you need |
|------|--------------|
| **Worker** | Python 3.10+, and one of: [Ollama](https://ollama.ai), [LM Studio](https://lmstudio.ai), or [llama.cpp](https://github.com/ggerganov/llama.cpp) running locally |
| **Validator** | Python 3.10+, and [Podman](https://podman.io) or [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running |
| **Both** | All of the above |

You can run both roles on the same machine.

---

### Install

```bash
git clone https://github.com/poulpeck/codoken
cd codoken
python -m venv venv

# macOS / Linux
source venv/bin/activate

# Windows
venv\Scripts\activate

pip install -r requirements.txt
```

---

### Setup (one-time)

```bash
python setup.py
```

The wizard will:
1. Ask which role(s) to run
2. Create or sign into your Codoken account (email + password)
3. Detect your LLM backend and confirm the model name (worker)
4. Detect your container engine and pull runtime images (validator)
5. Write your config to `~/.codoken/config.json`
6. Verify the connection to the broker
7. Optionally install a background service (auto-start on login)

---

### Start your node

```bash
# macOS / Linux
CODOKEN_AUTH=firebase python worker.py      # if you chose worker
CODOKEN_AUTH=firebase python validator.py   # if you chose validator

# Windows
set CODOKEN_AUTH=firebase && python worker.py
set CODOKEN_AUTH=firebase && python validator.py
```

If you installed the background service during setup, your node starts automatically — no need to run these commands manually.

---

### Verify it's working

```bash
CODOKEN_AUTH=firebase python developer.py info
```

This shows your account UID, credit balance, and how many workers and validators are currently connected to the network.

To submit a test task and watch it go through the pipeline:

```bash
CODOKEN_AUTH=firebase python developer.py submit manifests/fizzbuzz.yaml
```

You should see: `queued → dispatched → in_review → approved` within about 30 seconds.

---

### Credits

- **Workers** earn +5.00 credits per approved task
- **Validators** earn +1.00 credit per approved vote
- **Developers** spend credits to submit tasks (around -1 to -2 per task)
- Your balance is shown in `developer.py info` and `developer.py balance`

Credits are tied to your account (email). If you reinstall, sign in with the same email and your balance is preserved.

---

### Troubleshooting

**Node starts but no tasks arrive**
The network needs at least 1 worker and 2 validators connected simultaneously for tasks to flow. Check `developer.py info` to see how many nodes are online.

**Worker says "No local LLM detected"**
Make sure your LLM backend is running *before* starting the worker. The worker retries every 5 seconds — start the LLM and it will connect automatically.

**Validator says "container engine not running"**
- macOS: `podman machine start`
- Linux: `systemctl --user start podman.socket`
- Windows: open Docker Desktop from the Start menu

**Wrong password / lost auth store**
Re-run `python setup.py` and sign in again. Your credits are safe — they're stored server-side against your email.

**Check everything is configured correctly**
```bash
python setup.py check
```

---

## Architecture

See `codoken_architecture_specification.md` for the full design.

## Components

| File | Role |
|------|------|
| `broker.py` | Central orchestrator: dispatch, validator committees, consensus, credit settlement |
| `developer.py` | Developer CLI: `submit`, `status`, `balance`, `info` |
| `worker.py` | Worker node: auto-discovers local LLMs, generates code |
| `validator.py` | Validator node: runs tests in isolated containers |
| `codoken_auth.py` | Shared auth helper: Firebase email/password or devnet shared-secret |
| `codoken_config.py` | Layered preferences: env > `~/.codoken/config.json` > repo-local |
| `setup.py` | Onboarding wizard, stack doctor, service manager |

## 1-minute start (all local)

```sh
python -m venv venv && venv/bin/pip install -r requirements.txt

venv/bin/python broker.py                     # terminal 1 (port 9000)
venv/bin/python worker.py                     # terminal 2 (needs a local LLM)
venv/bin/python validator.py                  # terminal 3 (needs Docker or Podman)
venv/bin/python developer.py submit manifests/gcd.yaml
```

## Preferences — works with any local setup

Every component resolves its settings in this order:

1. **environment variables** — always win (CI, one-off overrides)
2. **`.codoken/config.json`** — written by `python setup.py`; found by walking
   up from the working directory (git-style)
3. **built-in defaults** — public devnet (`wss://broker.codoken.dev`),
   `shared_secret` auth, no persistence

```sh
venv/bin/python setup.py             # wizard: broker URL, auth mode, parallelism
venv/bin/python setup.py check       # doctor: packages, broker, live auth, LLM, containers
venv/bin/python setup.py print-env   # shell export lines (CI / .zshrc)
```

Typical setups the wizard covers: everything local (`ws://127.0.0.1:9000` +
`shared_secret`), devnet from home (`wss://broker.codoken.dev` + `shared_secret`),
production (`wss://broker.codoken.dev` + `firebase`). The validator auto-detects
Podman on macOS (no Docker Desktop license needed); set `docker_host` in the
config only for non-standard sockets.

Key settings (config file key / environment variable): `broker` /
`CODOKEN_BROKER_WS`, `auth` / `CODOKEN_AUTH`, `persist` / `CODOKEN_PERSIST`
(broker), `firebase_web_config` / `FIREBASE_WEB_CONFIG`, `auth_store` /
`CODOKEN_AUTH_STORE`, `worker_parallel` / `CODOKEN_WORKER_PARALLEL`,
`docker_host` / `DOCKER_HOST`.

## Tests

```sh
venv/bin/python run_local_tests.py   # one command: offline (174) + CLI (27) +
                                     # fresh-broker smoke (44) + setup doctor
```

`smoke_test.py` (unmodified devnet gate) and `test_offline.py` / `test_cli.py`
can also be run individually. Online verification against the VM + Firebase:
`ONLINE.md` + `scripts/online_check.py`.

## Multi-atom workloads

A manifest may declare an `atoms` list (spec §2): each atom is one language +
prompt + test, executed as its own task with a fail-closed aggregate verdict.
Example: `manifests/gcd_fib.yaml` (C++ GCD + Python Fibonacci). Design notes:
`multi_atom_design.md`.

## Documentation

- `codoken_architecture_specification.md` — architecture baseline (source of truth)
- `multi_atom_design.md` — multi-atom workload design (Phase H)
- `FIREBASE.md` — Firebase auth + Firestore deployment and verification
- `ONLINE.md` — end-to-end runbook: VM + Firebase + public devnet
