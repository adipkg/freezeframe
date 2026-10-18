"""HTTP face of the enclave (request/response alternative to the stream).

  GET  /v1/enclave              public key + attestation status
  POST /v1/members/{m}/key      register a member's public key (onboarding)
  POST /v1/submit               {"member", "sealed"} -> {"sealed"} decisions for that member only
  GET  /v1/notices/{m}          {"sealed"} freeze requests for m's own accounts
  GET  /v1/metrics              consortium-level counts and latency; no transaction data

Run:  arch -x86_64 .venv/bin/python -m freezeframe.enclave.service
"""
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .crypto import KeyPair, seal


class Submission(BaseModel):
    member: int
    sealed: str


class MemberKey(BaseModel):
    public_key: str


def attestation_status() -> str:
    """'secure-execution' inside an IBM Secure Execution guest, otherwise 'simulated'."""
    p = Path("/sys/firmware/uv/prot_virt_guest")
    try:
        return "secure-execution" if p.read_text().strip() == "1" else "simulated"
    except OSError:
        return "simulated"


def create_app(engine, keys: KeyPair, warm=None) -> FastAPI:
    app = FastAPI(title="FreezeFrame enclave")
    member_keys: dict[int, str] = {}
    app.state.engine, app.state.member_keys = engine, member_keys

    @app.get("/v1/enclave")
    def enclave():
        return {"public_key": keys.public_b64, "attestation": attestation_status(),
                "model": engine.model_version}

    @app.post("/v1/members/{m}/key")
    def register(m: int, body: MemberKey):
        member_keys[m] = body.public_key
        return {"ok": True}

    @app.post("/v1/submit")
    def submit(body: Submission):
        if body.member not in member_keys:
            raise HTTPException(403, "unknown member")
        try:
            batch = keys.open(body.sealed)
        except Exception:
            raise HTTPException(400, "cannot open sealed batch")
        payments = batch["payments"] if isinstance(batch, dict) else batch
        out, rejected = engine.process([(body.member, p) for p in payments])
        return {"sealed": seal({"decisions": out.get(body.member, []),
                                "rejected": rejected.get(body.member, [])},
                               member_keys[body.member])}

    @app.get("/v1/notices/{m}")
    def notices(m: int):
        if m not in member_keys:
            raise HTTPException(403, "unknown member")
        return {"sealed": seal(engine.drain_notices(m), member_keys[m])}

    @app.get("/v1/metrics")
    def metrics():
        out = engine.stats.summary()
        if warm is not None:
            out["warm"] = warm.summary()
        return out

    return app


def main():
    import uvicorn
    from ..demo_setup import build_engine
    engine, keys, warm, _ = build_engine(os.environ.get("FF_DATASET", "HI-Small"))
    warm.start()
    uvicorn.run(create_app(engine, keys, warm), host="0.0.0.0", port=int(os.environ.get("PORT", 8443)))


if __name__ == "__main__":
    main()
