# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

FreezeFrame is an IBM Z Datathon 2026 entry (track: "AI Secured: Innovation Without Exposure"). Banks stream their own payments, encrypted, into a shared enclave. Inside it, account numbers become keyed pseudonyms. IBM's Graph Feature Preprocessor (GFP, part of Snap ML) mines laundering patterns, a booster scores each payment, and a cost rule decides release, hold, or freeze. Each decision goes only to the bank that owns the payment.

The design doc is `freezeframe.pdf`, the plan is `PLAN.md`, and `todo.md` lists what still needs LinuxONE, Secure Execution, or people.

## Environment

Snap ML has no macOS arm64 wheel. On Apple Silicon, everything runs in an **x86_64 venv under Rosetta**:

```bash
./scripts/setup_mac.sh                       # builds .venv + .venv-omp (x86 libomp), patches .so paths
arch -x86_64 .venv/bin/python -m pytest      # every python command needs the arch prefix
```

On Linux (x86_64 or s390x), use `pip install -e '.[dev,xgb]'` and plain `python`.

`xgboost` is pinned to `<3`, because Snap ML 1.17 can't import XGBoost 3.x JSON ("no objective defined"). After any reinstall on macOS, rerun `setup_mac.sh` so the libomp paths get patched again.

## Commands

```bash
PY="arch -x86_64 .venv/bin/python"
$PY -m freezeframe.ingest HI-Small                         # CSV -> data/processed/HI-Small.parquet
$PY -m freezeframe.experiment HI-Small --tier full         # alone vs together, 3 seeds (~1 h)
$PY -m freezeframe.experiment HI-Small --tier hot --together-only   # model for the live hot path
./scripts/run_experiments.sh HI-Small                      # both of the above
$PY -m bench.latency --speeds 1 100 1000 --seconds 60      # streaming latency -> results/
$PY -m demo.app                                            # dashboard on http://localhost:8000
$PY -m freezeframe.enclave.service                         # HTTP enclave on :8443
$PY -m pytest tests/test_enclave.py::test_hop_two_is_held  # single test
FF_KAFKA=localhost:9092 $PY -m pytest tests/test_kafka.py  # needs a running broker
```

The demo, service, and benchmark need `artifacts/models/<dataset>/{hot,full}.pkl`, which the experiment writes. The demo also uses `artifacts/predictions/` for its "alone vs together" columns.

## Architecture

- **Boundary.** `ingest.py` runs on the bank side, where raw account strings exist. Everything under `enclave/`, plus `features.py`, `model.py`, and `decide.py`, works on pseudonyms (`pseudonym.py`, HMAC-SHA256 truncated to 52 bits so the IDs stay exact as float64). `Engine._frame` drops raw accounts immediately after hashing. The only raw strings kept in enclave memory are receiving accounts in `Engine.routing`, used to address freeze notices. Tests assert that no raw account reaches features, queued frames, metrics, or model files.
- **One feature path for batch and streaming.** `features.GraphFeatures.update` (GFP `transform`) runs once per timestamp group, both in the experiment and in the live engine. That gives three guarantees: no look-ahead, training features identical to live features, and a test that checks it (`tests/test_features.py`). Three GFP behaviours to know about:
  - `fit_transform` inserts every edge twice. Never use it.
  - Inside one `transform` call, every edge sees the whole batch, including later edges. So batches are always cut at timestamp boundaries.
  - The `*_tw` windows aren't hard look-back bounds. The enclave primes from **full** history (`demo_setup.PRIME_SECONDS = None`).
- **Hub splitting.** Accounts with more than `HUB_DEGREE` edges in the train split get a fresh vertex per edge (`features.edge_matrix`), plus is-hub flags. Without this, GFP runtime on HI-Small blows up: two accounts have more than 100k edges each. With it, the full feature set takes about 90 s for 5M rows.
- **Two tiers.** The hot tier (`config.GFP_HOT`, no scatter-gather) scores every payment in `enclave/engine.py`. The warm tier (`enclave/warm.py`, `config.GFP_FULL`) re-scores recent payments every few seconds and escalates ones the hot path released. Both feed one watchlist: when a payment is held or frozen, its receiving account is watched, so the next hop from that account is held (`reason: upstream_flag`).
- **Experiment.** `experiment.py` splits by time 60/20/20. Banks are dealt at random to 5 members. A member's view is every payment its banks send or receive; a payment is owned by the sending bank's member. Alone and together are scored on the same owned test rows, and an assertion enforces it. The pooled run caches features to `artifacts/features/`. Delete the cache after changing any GFP parameter in `config.py`.
- **Transport.** `enclave/stream.py` defines `InProcessBus` and `KafkaBus` (same interface), `BankClient` (seals payments to the enclave key), and `HotConsumer` (micro-batches, then seals decisions to each member's key). `enclave/service.py` is the HTTP alternative. `enclave/crypto.py` uses libsodium sealed boxes.

## Conventions

- Don't add a `Co-Authored-By` trailer to commits.
- Report results only from files under `results/`. Label any number that wasn't measured on LinuxONE with the machine it came from. Label the enclave "simulated" unless attestation says otherwise.
