# FreezeFrame — what is left, and how to do it

Everything in `PLAN.md` that could be built and checked on the dev machine (M2 MacBook Air,
8 GB RAM, ~8 GB free disk, macOS, no LinuxONE access) is built. This file lists what could
not be finished there, why, and the plan for each. Items are ordered by how much they matter
for the submission.

Legend: **[blocked]** needs hardware/access/accounts we don't have here ·
**[human]** needs a person · **[build]** code work that can be done anywhere.

---

## 1. Run on LinuxONE Community Cloud — [blocked: needs the VM]

The submission requires a working demo on LinuxONE. Nothing has run on s390x yet.

Plan (`deploy/linuxone.md` has the commands):
1. Request the VM (RHEL 9 / Ubuntu 22.04, s390x); record cores, RAM, disk.
2. `pip install -e '.[dev,xgb]'` and run `pytest`. Snap ML 1.17.2 has an s390x wheel. If
   XGBoost <3 has no s390x wheel, build from source, or train on x86 and copy `artifacts/models/`.
   The bundles hold XGBoost JSON and re-import it into Snap ML on load, so models are portable.
3. Run `scripts/run_experiments.sh HI-Small` on the VM and compare its F1 with the laptop run.
   They should match to about 1e-3; the features are deterministic.
4. Run `python -m bench.latency` and `python -m demo.app` there; tunnel port 8000 for the demo.
5. Commit `results/` from the VM run. Its files are named by machine (`latency_*_s390x.json`).

Note: on the dev laptop (x86_64 Snap ML under Rosetta), GFP crashed (SIGSEGV) once and
hung twice in about 40 runs. These were not reproducible on rerun. Watch for this on s390x;
if it shows up on native Linux, report it to IBM with the edge list that triggers it.

Done when the tests pass on s390x, the experiment and latency JSON from the VM are committed, and the demo has been recorded from the VM.

## 2. AI accelerator latency (IBM Z Integrated Accelerator for AI) — [blocked: needs z16/z17]

Scoring goes through Snap ML `import_model(..., tree_format='auto')`, which picks
`zdnn_tensors` (the accelerator) when present. `Scorer.tree_format`, the benchmark JSON
(`scoring_tree_format`) and the dashboard badge all report which one was used. Here it
is `compress_trees` (CPU).

Plan:
1. On the VM: `grep -w nnpa /proc/cpuinfo`. If it's absent, report CPU-only numbers and say so on the slide (PDF fallback).
2. If present, run the benchmark twice:
   `FF_TREE_FORMAT=compress_trees python -m bench.latency ...` and
   `FF_TREE_FORMAT=zdnn_tensors python -m bench.latency ...`. Compare the `score` stage
   p50/p99 and the maximum rate within the 50 ms budget.
3. Add a micro-benchmark of the booster alone, at batch sizes 1, 64, 1024, and 8192. The accelerator's advantage depends on batch size.

## 3. Secure Execution enclave + attestation — [blocked: needs Secure Execution / HPVS]

Today the "enclave" is an isolated process: the service, stream consumer and engine.
`attestation_status()` reports `simulated` unless `/sys/firmware/uv/prot_virt_guest` is 1.
Payments are sealed to the enclave's key, decisions are sealed to each member's key, and
pseudonymisation happens before any processing. All of that is built and tested
(`tests/test_enclave.py`).

Plan for production:
1. Build `deploy/Dockerfile` for s390x, sign it, and push it to a registry.
2. Write an IBM Confidential Computing Container Runtime (HPVS) contract:
   - `workload`: the image and its port.
   - `env`: encrypted to the HPVS key, carrying the pseudonym-key seed and the member public keys.
3. At boot, the enclave generates its transport keypair. It publishes the public key along with the attestation record (the image measurement).
4. Banks verify the attestation (the expected image hash) before sealing payments to that key. Add `BankClient.verify_attestation()`.
5. Move the pseudonym key from `artifacts/enclave_pseudonym.key` (simulated sealed storage) into the contract-provided secret.

If Community Cloud lacks Secure Execution: the label stays "simulated", and the slide shows this design (PDF fallback).

## 4. Kafka / IBM Event Streams transport — [build, partly verified]

`KafkaBus` (`freezeframe/enclave/stream.py`) implements the same interface as
`InProcessBus`: one partition on `payments.in` and one topic per member. See the "Local
Kafka check" section of `README.md` for what was verified locally.

Plan:
1. Run single-node Kafka (KRaft) on the LinuxONE VM. It's a JVM, so it runs on s390x.
   `bench.latency --bus kafka` compares its latency with the in-process bus.
2. Add per-member ACLs (SASL users per bank) so member *m* can only read `decisions.m` and
   `notices.m`, and can only write `payments.in`.
3. Replace polling with a consumer thread feeding an `asyncio.Queue`, to cut idle latency.
4. For IBM Event Streams, the only change is the bootstrap address plus SASL settings.

## 5. HI-Medium scale test (32M transactions) — [blocked: disk and RAM here]

HI-Medium is about 3 GB of CSV. Its features would be 32M × 83 float32 ≈ 10.6 GB of RAM;
this machine has 8 GB of RAM and about 8 GB of free disk.

Plan:
1. On a larger machine or the VM, download HI-Medium and run `freezeframe.ingest HI-Medium`.
2. Make `build_features` write to a `np.memmap` instead of an in-RAM array. `GraphFeatures.stream` already works per timestamp, so only the output buffer changes.
3. Train XGBoost with its external-memory `DMatrix`, or keep all positives plus a 20% sample of negatives and reweight.
4. Report GFP throughput (edges/s) and peak RSS, for the scalability criterion.

## 6. Graph state snapshot / restore — [build]

Snap ML's GraphFeaturePreprocessor exposes no export or import of its graph. Today a
restarted enclave re-primes from history, which takes about 70–90 s for 4M edges per tier.

Plan:
1. Make the enclave write an append-only *pseudonymous* edge log: txn id, src, dst, t, amount. No raw accounts, so it is safe to persist under the enclave's sealed storage.
2. On restart, replay the log through `Engine.prime` and `WarmTier.prime`.
3. Expose readiness through a `/v1/health` endpoint that reports `priming` or `ready`.

## 7. GFP window semantics — [build: investigation]

`tests/test_features.py::test_priming` shows that priming with 48 h of history (twice the
longest configured 24 h window) still changes features slightly for under 2% of rows: some
vertex statistics and length-constrained cycle counts. That means GFP's `*_tw` windows aren't
hard look-back bounds. The enclave therefore primes from full history (exact, and tested).

Plan:
1. Ask IBM, or read Snap ML's GFP source/docs, for how `time_window` evicts edges. It may evict lazily, or cycles may anchor on the earliest edge.
2. If there is a hard bound (for example, 2× the window), prime with that bound to cut restart time.

## 8. Member authentication on the HTTP API — [build]

The service checks that a submitted payment's sending bank belongs to the claimed member,
and seals every response to that member's *registered* key. An impostor therefore can't read
results, and can't get decisions on another member's payments. However, it could still inject
payments that claim to be from member *m*.

Plan: mTLS with a certificate per member (CN = member id), or Ed25519-signed submissions
(PyNaCl `SigningKey`), with the verify key registered at onboarding. Reject unsigned batches.

## 9. Model quality follow-ups — [build]

- The pooled model reaches the published GFP + XGBoost F1 (see `results/`). Hyperparameters
  are hand-set (`freezeframe/model.py`), with no search yet. Plan: a small Optuna or grid search on the tune split (depth, learning rate, scale_pos_weight, number of rounds), then rerun the alone and together arms with the chosen configuration.
- The tune split does two jobs: early stopping, and calibration plus thresholds. Plan: carve the last 10% of train out for early stopping, so tune is used only for calibration and thresholds.
- The hold rule (p × amount > $100 review cost) holds about 1.8% of test payments (17,962 legitimate). That is more than the 0.5% review budget the recall metric assumes. Plan: set the review cost so that holds on the tune split equal the review budget (the cost then becomes an output, not an input), or cap holds per bank per day by expected loss. Then report dollars stopped under that budget, which would actually separate alone from together. Today the figure is 99.7% in every arm.
- The Snap ML booster (`--model snap`) is wired in but not benchmarked against XGBoost on HI-Small.
- LI-Small (the harder check in the PDF): see `results/` for whether it ran. If it didn't, run `scripts/run_experiments.sh LI-Small`.

## 10. Docker image build — [blocked: Docker daemon not running here]

`deploy/Dockerfile` is written but not built. Plan: `docker build` on the VM (natively s390x)
or with `docker buildx --platform linux/s390x,linux/amd64`; then run `pytest` inside the image.

## 11. Submission items — [human]

- [ ] Check the Datathon rules on what may be prepared before kickoff (PDF: "check the official rules"). The repo is private until then.
- [ ] Make the repo public when allowed: `gh repo edit adipkg/freezeframe --visibility public --accept-visibility-change-consequences`
- [ ] Pitch deck. The numbers come from `results/HI-Small/summary_full_xgb.md` and `latency_*.json`.
- [ ] Record the 3-minute demo: `python -m demo.app`, then press Play. Record it from the LinuxONE VM if possible.
- [ ] Short project description (the PDF abstract) on the submission form.
- [ ] Rehearse.
