"""Demo dashboard: a real cross-bank mule chain from the test split, replayed live through
sealed bank clients -> enclave hot path (+ warm tier), with background traffic around it.

The page is the demo operator's view: it merges what each member bank sees on its *own*
decision topic (each bank client unseals only its own messages), plus consortium-level
metrics from the enclave. "Alone" decisions come from the experiment's per-member models.

Run:  arch -x86_64 .venv/bin/python -m demo.app   then open http://localhost:8000
"""
import asyncio
import json
import os
import time
from pathlib import Path

import numpy as np
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse

from freezeframe import config
from freezeframe.consortium import assign_members
from freezeframe.demo_setup import build_engine, chain_candidates, to_payment
from freezeframe.experiment import enclave_key
from freezeframe.pseudonym import Pseudonymizer
from freezeframe.enclave.service import attestation_status
from freezeframe.enclave.stream import (BankClient, HotConsumer, InProcessBus,
                                        decisions_topic, notices_topic)
from freezeframe.ingest import load

DATASET = os.environ.get("FF_DATASET", "HI-Small")
TIER, MODEL = "full", os.environ.get("FF_MODEL", "xgb")
LEAD_SECONDS = 30 * 60        # history ends this long before hop 1
LIVE_LEAD = 6 * 60            # real traffic replayed live before each hop (data seconds)
LIVE_SPEED = float(os.environ.get("FF_LIVE_SPEED", 40))   # x real time while live
STATIC = Path(__file__).parent / "static"

app = FastAPI(title="FreezeFrame demo")
S: dict = {"subscribers": set(), "playing": False}


def _predictions(kind):
    path = config.ARTIFACTS_DIR / "predictions" / DATASET / f"{kind}_{TIER}_{MODEL}.npz"
    return dict(np.load(path)) if path.exists() else None


def pick_chain(raw, bank_member):
    """Prefer a chain the pooled model catches at its first hop and a lone bank misses."""
    together = _predictions("together")
    alone_path = config.ARTIFACTS_DIR / "predictions" / DATASET / f"alone_{TIER}_{MODEL}_seed0.npz"
    alone = dict(np.load(alone_path)) if alone_path.exists() else None
    cands = chain_candidates(raw, bank_member, max_span_hours=96)
    if not cands:
        raise RuntimeError("no demo chain found")

    def decisions(arr, ids):
        if arr is None:
            return [None] * len(ids)
        pos = {int(t): i for i, t in enumerate(arr["txn_id"])}
        return [int(arr["decision"][pos[i]]) if i in pos else None for i in ids]

    best = None
    for c in cands[:300]:
        ids = raw.index[raw.attempt == c["attempt"]].tolist()
        tog, alo = decisions(together, ids), decisions(alone, ids)
        caught = sum(1 for d in tog if d) / len(ids)
        missed_alone = sum(1 for d in alo if d == 0) / len(ids)
        score = (bool(tog[0]), caught + missed_alone, c["members"])
        if best is None or score > best[0]:
            best = (score, c, ids, tog, alo)
    _, c, ids, tog, alo = best
    return c, ids, tog, alo


@app.on_event("startup")
async def startup():
    raw = load(DATASET)
    bm = assign_members(raw, 0)
    cand, ids, tog, alo = pick_chain(raw, bm)
    chain = raw.loc[ids].sort_values("ts")
    t_live = int(chain.t.iloc[0]) - LEAD_SECONDS
    engine, keys, warm, ctx = await asyncio.to_thread(
        build_engine, DATASET, 0, 8, 2.0, True, t_live, warm_process=True)
    await asyncio.to_thread(warm.start)
    bus = InProcessBus()
    members = sorted(set(bm.values()))
    banks = {m: BankClient(m, bus, keys.public_b64, linger_ms=5.0) for m in members}
    consumer = HotConsumer(engine, bus, keys, {m: b.keys.public_b64 for m, b in banks.items()})
    S.update(engine=engine, warm=warm, keys=keys, bus=bus, banks=banks, consumer=consumer,
             ctx=ctx, chain=chain, cand=cand, bm=bm, tog=tog, alo=alo,
             chain_refs={str(t): i for i, t in enumerate(chain.txn_id)})
    asyncio.create_task(consumer.run())
    for b in banks.values():
        asyncio.create_task(b.run_flusher(asyncio.Event()))
    for m in members:
        asyncio.create_task(bank_listener(m))
    asyncio.create_task(stats_pump())


def broadcast(ev: dict):
    for q in list(S["subscribers"]):
        q.put_nowait(ev)


async def bank_listener(m):
    """Bank m reads only its own topics and reports what it sees to the operator view."""
    bus, bank = S["bus"], S["banks"][m]
    while True:
        got = False
        msg = bus.get_nowait(decisions_topic(m))
        if msg is not None:
            got = True
            for d in bank.open(msg)["decisions"]:
                hop = S["chain_refs"].get(d["ref"])
                if hop is not None:
                    broadcast({"type": "hop", "hop": hop, "member": m, **d})
        note = bus.get_nowait(notices_topic(m))
        if note is not None:
            got = True
            for n in bank.open(note):
                chain_accts = set(S["chain"].to_acct)
                if n["to_acct"] in chain_accts:
                    broadcast({"type": "notice", "member": m, "kind": n["kind"],
                               "to_acct": n["to_acct"][-4:], "amount_usd": n["amount_usd"]})
        if not got:
            await asyncio.sleep(0.02)


async def stats_pump():
    last_n, last_t = 0, time.time()
    while True:
        await asyncio.sleep(0.5)
        eng, cons = S["engine"], S["consumer"]
        now = time.time()
        e2e = np.array(cons.e2e_ms[-5000:]) if cons.e2e_ms else np.array([0.0])
        n = eng.stats.payments
        broadcast({"type": "stats", "rate": (n - last_n) / (now - last_t), "payments": n,
                   "p50": float(np.percentile(e2e, 50)), "p99": float(np.percentile(e2e, 99)),
                   "actions": eng.stats.actions, "upstream_holds": eng.stats.upstream_holds,
                   "escalations": eng.stats.escalations, "playing": S["playing"]})
        last_n, last_t = n, now


async def _replay(rows, speed):
    """Send bank-side rows through the sealed bank clients at ``speed``x real time."""
    if len(rows) == 0:
        return
    bm = S["bm"]
    rng = np.random.default_rng(int(rows.t.iloc[0]))
    t_data = rows.t.to_numpy().astype(float) + rng.uniform(0, 59, len(rows))
    is_chain = rows.index.isin(S["chain"].index)
    t_data[is_chain] = rows.t.to_numpy()[is_chain] + 30   # hops sit mid-minute, in data order
    order = np.argsort(t_data, kind="stable")
    rows, t_data = rows.iloc[order], t_data[order] - t_data[order][0]
    owners = rows.from_bank.astype(str).map(bm).to_numpy()
    t0 = time.time()
    for i, row in enumerate(rows.itertuples(index=False)):
        due = t0 + t_data[i] / speed
        wait = due - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        await S["banks"][int(owners[i])].send(to_payment(row), sent_at=due)
    for b in S["banks"].values():
        await b.flush()


def _fast_forward(rows):
    """Load rows into the enclave graph without scoring them (the gap between two hops)."""
    if len(rows):
        S["engine"].prime(Pseudonymizer(enclave_key()).apply(rows))


async def play():
    """For each hop: replay LIVE_LEAD of real traffic before it, live, at LIVE_SPEED. The hours
    between hops are fast-forwarded into the graph (not scored, not shown) and labelled so."""
    S["playing"] = True
    chain, live = S["chain"], S["ctx"]["live"]
    hop_t = sorted(set(chain.t.tolist()))
    cursor = int(live.t.iloc[0])
    for k, t_hop in enumerate(hop_t):
        start = max(cursor, t_hop - LIVE_LEAD)
        gap = live[(live.t >= cursor) & (live.t < start)]
        if len(gap):
            broadcast({"type": "skip", "payments": len(gap), "hours": (start - cursor) / 3600})
            await asyncio.to_thread(_fast_forward, gap)
            await asyncio.sleep(0.3)   # let the consumer drain before going live again
        end = t_hop + 60
        broadcast({"type": "live", "hop_time": str(chain[chain.t == t_hop].ts.iloc[0]),
                   "speed": LIVE_SPEED})
        await _replay(live[(live.t >= start) & (live.t < end)], LIVE_SPEED)
        cursor = end
        await asyncio.sleep(1.0)
    await asyncio.sleep(3)
    S["warm"].run_once()
    S["playing"] = False
    broadcast({"type": "done"})


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/chain")
def api_chain():
    chain, bm = S["chain"], S["bm"]
    acct_tag = {}
    def tag(b, a):
        return acct_tag.setdefault((b, a), f"#{len(acct_tag) + 1}")
    hops = []
    for i, r in enumerate(chain.itertuples()):
        hops.append({"hop": i, "ts": str(r.ts), "from_member": bm[str(r.from_bank)],
                     "to_member": bm[str(r.to_bank)], "from": tag(str(r.from_bank), r.from_acct),
                     "to": tag(str(r.to_bank), r.to_acct), "amount_usd": round(float(r.amount_usd), 2),
                     "currency": str(r.cur_paid), "fmt": str(r.fmt),
                     "together": S["tog"][i], "alone": S["alo"][i]})
    return {"pattern": S["cand"]["pattern"], "span_hours": S["cand"]["span_hours"], "hops": hops,
            "attestation": attestation_status(), "tree_format": S["engine"].scorer.tree_format}


@app.get("/api/results")
def api_results():
    path = config.RESULTS_DIR / DATASET / f"experiment_{TIER}_{MODEL}.json"
    if not path.exists():
        return {}
    r = json.loads(path.read_text())
    seeds = r.get("seeds", {})
    s0 = seeds.get("0") or (next(iter(seeds.values())) if seeds else None)
    pct = lambda d: d["laundered_usd_stopped"] / max(d["laundered_usd"], 1)
    return {
        "together_f1": r["together_overall"]["f1"],
        "together_usd": pct(r["together_overall"]),
        "alone_f1_by_seed": {k: v["alone_overall"]["f1"] for k, v in seeds.items()},
        "alone_usd_by_seed": {k: pct(v["alone_overall"]) for k, v in seeds.items()},
        "members": [] if not s0 else [{"member": m["member"], "alone": m["alone"]["f1"],
                                       "together": m["together"]["f1"],
                                       "positives": m["alone"]["positives"]} for m in s0["members"]],
    }


@app.post("/api/play")
async def api_play():
    if not S["playing"]:
        asyncio.create_task(play())
    return {"ok": True}


@app.get("/api/stream")
async def api_stream():
    q: asyncio.Queue = asyncio.Queue()
    S["subscribers"].add(q)

    async def gen():
        try:
            while True:
                ev = await q.get()
                yield f"data: {json.dumps(ev, default=float)}\n\n"
        finally:
            S["subscribers"].discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", 8000)))
