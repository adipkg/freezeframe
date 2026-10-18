# FreezeFrame implementation plan

This plan is taken from `freezeframe.pdf`. Each phase lists what it produces and how to tell it is done.

## Data (downloaded to `data/`, gitignored)

| File | Rows | Use |
|---|---|---|
| `HI-Small_Trans.csv` | 5,078,345 | Main experiment |
| `HI-Small_Patterns.txt` | 370 laundering attempts | Per-pattern breakdown and demo chains |
| `LI-Small_Trans.csv` | 6,924,049 | Harder check |
| `LI-Small_Patterns.txt` | 117 attempts | Per-pattern breakdown |

Source: Kaggle `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`. The download works without a Kaggle login:
`curl -L -o data/<file> https://www.kaggle.com/api/v1/datasets/download/ealtman2019/ibm-transactions-for-anti-money-laundering-aml/<file>`
HI-Medium (32M rows, about 3 GB) is not downloaded. It is the scale test, and this disk has only about 9 GB free.

## Repo layout

```
freezeframe/
  ingest.py        # CSV → typed parquet; accounts keyed as bank+account; currency → USD
  pseudonym.py     # HMAC-SHA256(enclave_key, bank||account) → int64 id; key lives only in the enclave
  consortium.py    # random bank → 5 members (seeded); member "view" = txns its banks send OR receive
  features.py      # Snap ML GraphFeaturePreprocessor wrapper: fit on train, partial_fit/transform streaming
  model.py         # Snap ML BoostingMachineClassifier; XGBoost → ONNX/PMML import fallback
  calibrate.py     # isotonic/Platt on tuning split
  decide.py        # hold if p*amount > review_cost; freeze if p >= t95 (95% precision on tune split)
  enclave/
    service.py     # FastAPI: POST /submit (encrypted batch from a bank) → decisions for that bank only
    crypto.py      # per-bank keypair; batches encrypted to the enclave public key
    stream.py      # hot path: consume payments topic → micro-batch → features → score → decide → reply topic
    warm.py        # warm path: full-pattern GFP every few seconds; re-scores and escalates downstream hops
    state.py       # in-memory sliding-window graph state, edge caps, eviction, snapshot/restore
  experiment.py    # alone vs together, 3 seeds, writes results/*.json
  metrics.py       # minority F1, recall@top-0.5%, laundered $ stopped, per-pattern recall
demo/
  replay.py        # streams test-split txns through 3 bank clients → enclave
  loadgen.py       # replays the test split at 1x / 100x / 1000x real time for latency runs
bench/
  latency.py       # per-stage p50/p95/p99 and the sustained throughput at which p99 stays in budget
  dashboard/       # web UI: chain lights up across banks, hold at hop 2, alone-vs-together chart
tests/
deploy/linuxone.md # Community Cloud setup, accelerator/Secure Execution checks
```

## Phases

**1. Ingest and split (PDF hours 0–8)**
- Parse timestamps and form account keys as `bank+account`. Account IDs repeat across banks, so the bank must be part of the key.
- Convert amounts to USD with a fixed rate table built from the data's own paired amount columns.
- Split by time into 60/20/20 for train, tune, and test.
- Parse `*_Patterns.txt` into an attempt id and pattern type for each laundering transaction. This feeds the per-pattern breakdown.
- Done when the split row counts are printed and the laundering rate is about 1 in 981 in every split.

**2. Pseudonymization (hours 2–10)**
- Use HMAC with a key that only the enclave has. Raw account strings are dropped right after hashing.
- Done when a test asserts that no raw account string shows up in features, logs, or saved models.

**3. Graph features (hours 3–8)**
- Use GFP with the paper's settings: fan, degree, scatter-gather, temporal cycle, length-constrained cycle, and vertex stats on amount.
- Fit on train, then stream tune and test through `partial_fit` in time order so there's no look-ahead.
- Add the raw transaction features: amount, currencies, payment format, and time of day.
- Done when features exist for all three splits, along with a runtime and memory figure.

**4. Model and calibration (hours 4–14)**
- Train a Snap ML booster on the pooled data and compare its F1 to the published 63.23.
- If it trails, train XGBoost and import it into Snap ML via ONNX.
- Calibrate on the tune split, then set the review-cost and 95%-precision freeze thresholds.

**5. Alone vs together (hours 8–17)**
- Assign banks to five members using seeds 0, 1, and 2.
- **Alone:** for each member, run GFP and the booster on its view only, trained, tuned, and tested within that view.
- **Together:** run the pooled pipeline on pseudonymized data, then score each member's transactions with the pooled model.
- Score both on the same test transactions, per member: F1, recall at a top-0.5% review budget, and dollars stopped.
- Report the per-pattern breakdown and the spread across seeds.

**6. Enclave service (hours 2–10)**
- Banks encrypt batches to the enclave's public key, using libsodium sealed boxes.
- The service decrypts, pseudonymizes, updates GFP, scores, decides, and returns decisions to the submitting bank only.
- Done when a test checks that bank A's response contains no transaction ids belonging to bank B.

**7. Demo (hours 4–21)**
- Pick a real cross-bank chain from `HI-Small_Patterns.txt` that crosses at least three members.
- Replay it: the victim's payment comes in, the chain lights up across the banks, and the hold fires at the second hop. End on the alone-vs-together chart.

**8. LinuxONE (hours 0–3 and 14–16)**
- Install `snapml` (s390x wheel) on Community Cloud and run the pipeline there.
- Check for the AI accelerator: `lscpu` or `/proc/cpuinfo` showing the `nnpa` facility, and the snapml `use_gpu`/accelerator flags. Measure latency.
- Check for Secure Execution: `/sys/firmware/uv/prot_virt_guest` = 1. If it isn't available, label the enclave "simulated".

**9. Streaming path (hours 8–16; builds on phase 6, feeds phases 7 and 8)**
See "Streaming design" below. Done when all of the following hold:
- `bench/latency.py` reports p99 decision latency on LinuxONE.
- The streaming-vs-batch feature test passes.
- The demo runs on the live stream, not a pre-scored file.

## Streaming design (realtime)

**Goal.** Each payment gets a release, hold, or freeze decision before the money can move to the next hop. That means milliseconds per payment. The batch experiment (phase 5) still produces the alone-vs-together numbers. The streaming path shows those decisions can be made live.

**Two tiers.** The benchmark shows some graph patterns are cheap and others aren't, so the work is split:

| Tier | Runs | Features | Model | Job |
|---|---|---|---|---|
| Hot | Every payment, in micro-batches of 10–50 ms | Raw transaction features, vertex stats, temporal cycle, length-limited cycle, plus fan/degree with edge caps | Hot booster, trained on hot features only | Release / hold / freeze decision inside the latency budget |
| Warm | Every 2–5 s over the sliding window | All patterns, including scatter-gather and uncapped fan/degree | Full booster | Re-score recent payments. If a released payment now looks like laundering, send a hold or freeze to the bank that owns the *next hop* |

The warm tier fits mule chains well. If it misses hop 1, it can still stop the money at hop 2, which is the demo's story. Report both tiers' F1 so the cost of the hot tier's reduced features is visible.

**Message flow.**
```
bank client ──encrypt──▶ topic payments.in  (one partition, time-ordered)
                               │
                         enclave/stream.py
   decrypt → pseudonymize → GFP partial_fit (micro-batch) → hot booster → calibrate → decide
                               │
                  topic decisions.<bank>   (each bank reads only its own topic)
                               ▲
                         enclave/warm.py ── escalations to the next hop's bank
```
- **Transport:** single-node Apache Kafka (KRaft) on the LinuxONE VM; it's JVM-based, so it runs on s390x. If Kafka is too heavy for the VM, use an in-process asyncio queue behind the same interface. Production on Z would use IBM Event Streams or IBM MQ.
- **Ordering:** GFP needs payments in time order, so use one partition, consumed by one enclave consumer. Payments up to a few seconds late are reordered in a small buffer; anything later is scored without being added to the graph, and counted.
- **Isolation:** each bank has its own decisions topic, with ACLs if Kafka is used. The phase 6 test, that bank A never sees bank B's ids, runs against the topics too.

**Graph state** (`state.py`)
- Keep a sliding window equal to the longest feature window, evicting older edges.
- Cap edges per account (`max_no_edges`) so a hub account can't stall a micro-batch. The 1M-row benchmark shows the slowdown comes from these hubs.
- Snapshot the state periodically so a restart doesn't lose the window.

**Latency budget** (targets, to be checked on LinuxONE)

| Stage | Target p99 |
|---|---|
| Decrypt + pseudonymize | 2 ms |
| Micro-batch wait | ≤ 25 ms |
| GFP partial_fit + transform (hot features) | 15 ms |
| Booster score (AI accelerator, or CPU) | 2 ms |
| Calibrate + decide + publish | 3 ms |
| **Ingest → decision** | **< 50 ms** |

- Measure each stage with timestamps on the message, not by estimating.
- Report p50, p95, and p99 at 1x, 100x, and 1000x real time. HI-Small averages about 6 payments/s at 1x, so 1000x is about 6,000/s.
- The headline number is the highest throughput at which p99 stays under 50 ms. Report it for the AI accelerator and for CPU.
- **Context for the slide:** from `*_Patterns.txt`, compute the median time between consecutive hops in a laundering chain. That shows how much time a 50 ms decision leaves before the next hop.

**Correctness checks**
- **Streaming equals batch:** on a 100k-row slice, features from `partial_fit` in micro-batches match `fit_transform` on the same rows (within float tolerance). Otherwise the streaming numbers can't be compared with the phase 5 experiment.
- **No look-ahead:** a payment's features use only edges with timestamp ≤ its own.
- **Model swap:** models are retrained offline and hot-swapped by atomic file rename, and every decision logs its model version.

**Demo hooks**
- `loadgen.py` sends background traffic from the test split.
- The victim's payment from a real cross-bank chain in `HI-Small_Patterns.txt` is injected live.
- The dashboard subscribes to the decision topics and shows live p99 latency and throughput next to the chain.

## Local dev notes

- Snap ML has no macOS arm64 wheel. It runs here through the x86_64 wheel under Rosetta, but the wheel's `.so` files hard-code `/usr/local/opt/libomp`. Install x86 `llvm-openmp` from conda-forge (`CONDA_SUBDIR=osx-64`) and point the `.so` files at it with `install_name_tool`. `DYLD_LIBRARY_PATH` doesn't help because `arch` strips it. Docker `linux/amd64` is the cleaner alternative.
- GFP benchmark (M2, 8 GB, Rosetta, all patterns on): 500k rows → 220 features in 13.6 s, peak about 1 GB. The full 5M rows didn't finish in 30 min, so the runtime grows much faster than the row count.
- Per-group timings on 1M rows (24 h graph window): vertex_stats 3.6 s, temp-cycle 2.9 s, lc-cycle 2.9 s, scatter-gather 49 s. Fan and degree took about 1.5 s at 300k rows but more than 200 s at 1M. The likely cause is hub accounts with a very high degree. Try `max_no_edges` and shorter `fan_tw`/`degree_tw` first. `degree` also crashed once (SIGSEGV) at 100k under Rosetta, so don't trust Rosetta for timings. Benchmark on LinuxONE or linux/amd64 instead.
