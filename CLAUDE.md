# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

There is no code yet. `freezeframe.pdf` is the design document and `PLAN.md` is the implementation plan built from it. When build, test, or run commands are added, record them here. Don't guess at them.

Data lives in `data/` (gitignored): the HI-Small and LI-Small `*_Trans.csv` and `*_Patterns.txt` files from Kaggle `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`. They download without a Kaggle login; the curl command is in `PLAN.md`.

Snap ML has no macOS arm64 wheel. To run it locally, use the x86_64 wheel under Rosetta with a patched libomp path, or Docker `linux/amd64`. `PLAN.md` § "Local dev notes" has the details. The real target is the s390x wheel on LinuxONE.

## What FreezeFrame is

FreezeFrame is an IBM Z Datathon 2026 entry (track: "AI Secured: Innovation Without Exposure"). It finds money-mule chains that cross banks and holds a scam payment before the money moves on. No bank ever sees another bank's customers. It targets IBM LinuxONE (s390x), and the demo must run on **LinuxONE Community Cloud**.

## Architecture (the pipeline inside the shared enclave)

Each bank sends only its own transactions, encrypted to the enclave. Inside, the steps run in this order:

1. **Pseudonymize.** Account numbers become keyed pseudonyms *before any processing*. The key never leaves the enclave. Features, logs, and models must never hold a real account number.
2. **Mine graph patterns.** IBM's Graph Feature Preprocessor (part of Snap ML) adds fan-in, fan-out, cycle, and scatter-gather features. It updates incrementally as transactions stream in.
3. **Score.** A Snap ML gradient-boosted model scores each payment. It runs on the on-chip AI accelerator (z16+) when one is present, otherwise on CPU. If Snap ML's booster trails XGBoost, train XGBoost and import it into Snap ML through ONNX or PMML.
4. **Decide.** Plain code works on *calibrated* scores:
   - **Hold for review** when probability × amount is greater than the cost of a review.
   - **Request a freeze** when probability clears a threshold. The threshold is tuned on the tuning split for 95% precision.
   - Otherwise **release**.

**The enclave boundary is the core invariant.** Only two things leave the enclave: decisions about a bank's *own* transactions, and consortium-level metrics. No bank receives another bank's transactions or features. In production the enclave is IBM Confidential Computing Container Runtime / Secure Execution. If the Community Cloud instance lacks Secure Execution, the enclave runs as an isolated service and must be labelled "simulated".

## The core experiment ("privacy dividend")

- Data: IBM synthetic AMLworld (Kaggle "IBM Transactions for Anti Money Laundering"). The main dataset is HI-Small (5M transactions, 515K accounts, 10 days, 1 in 981 laundering). LI-Small is the harder check. HI-Medium is the scale test.
- **Split by time:** train on the first 60%, tune thresholds on the next 20%, test on the last 20%.
- Assign the dataset's banks at random to **5 consortium members**. A member's view is every transaction its banks send *or* receive.
- **Alone:** each member runs the preprocessor and booster on its own view.
- **Together:** the enclave runs the same pipeline over all members' pseudonymized data. Each score goes back to the owning bank.
- Report per member and overall: minority-class F1, recall at a top-0.5% review budget, and laundered dollars stopped. Repeat over 3 random seeds and report the spread.
- Target for the pooled run is about 63% F1 on HI-Small (published GFP+XGBoost baseline is 63.23, GFP+LightGBM 62.86).

## Reporting honesty rules (from the risk plan)

- With no AI accelerator, report CPU latency and say so.
- With no Secure Execution, label the enclave as simulated.
- If the VM can't hold the full dataset, use a subset and state which subset.
- If pooling adds little, report that honestly, with a per-pattern breakdown.

## Deliverables

The deliverables are a working demo on LinuxONE Community Cloud, a public repo, a short description, and a pitch deck. The demo runs about 3 minutes: a victim's payment enters, the mule chain lights up across three banks, the payment is held at the second hop, and then one chart shows caught-alone versus caught-together. The plan sets feature freeze at hour 18 of 24.
