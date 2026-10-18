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
  experiment.py    # alone vs together, 3 seeds, writes results/*.json
  metrics.py       # minority F1, recall@top-0.5%, laundered $ stopped, per-pattern recall
demo/
  replay.py        # streams test-split txns through 3 bank clients → enclave
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

## Local dev notes

- Snap ML has no macOS arm64 wheel. It runs here through the x86_64 wheel under Rosetta, but the wheel's `.so` files hard-code `/usr/local/opt/libomp`. Install x86 `llvm-openmp` from conda-forge (`CONDA_SUBDIR=osx-64`) and point the `.so` files at it with `install_name_tool`. `DYLD_LIBRARY_PATH` doesn't help because `arch` strips it. Docker `linux/amd64` is the cleaner alternative.
- GFP benchmark (M2, 8 GB, Rosetta, all patterns on): 500k rows → 220 features in 13.6 s, peak about 1 GB.
