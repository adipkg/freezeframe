"""Streaming hot path: banks publish sealed payments, the enclave consumes micro-batches and
publishes sealed decisions on one topic per member.

Topics
  payments.in        {"member", "sealed"}   sealed to the enclave key; the sealed body is
                     {"payments": [...], "sent_at": [...]}: a bank gateway batches for a few ms
  decisions.<m>      {"sealed", "sent_at"}             sealed to member m's key
  notices.<m>        {"sealed"}                        freeze requests / escalations for m

``InProcessBus`` is the default transport (asyncio queues). ``KafkaBus`` speaks the same
interface over Apache Kafka / IBM Event Streams.
"""
import asyncio
import json
import time

from .crypto import KeyPair, seal

PAYMENTS = "payments.in"


def decisions_topic(m: int) -> str:
    return f"decisions.{m}"


def notices_topic(m: int) -> str:
    return f"notices.{m}"


class InProcessBus:
    def __init__(self):
        self.queues: dict[str, asyncio.Queue] = {}

    def _q(self, topic):
        return self.queues.setdefault(topic, asyncio.Queue())

    async def publish(self, topic: str, msg: dict):
        self._q(topic).put_nowait(msg)

    async def get(self, topic: str, timeout: float | None = None):
        if timeout is None:
            return await self._q(topic).get()
        return await asyncio.wait_for(self._q(topic).get(), timeout)

    def get_nowait(self, topic: str):
        try:
            return self._q(topic).get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def close(self):
        pass


class KafkaBus:
    """Same interface over Kafka (kafka-python). One partition on payments.in keeps time order."""

    def __init__(self, bootstrap: str = "localhost:9092", group: str = "freezeframe-enclave",
                 prefix: str = ""):
        from kafka import KafkaConsumer, KafkaProducer
        self.prefix = prefix
        self._producer = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=1,
                                       value_serializer=lambda v: json.dumps(v).encode())
        self._consumers = {}
        self._mk = lambda topic: KafkaConsumer(
            prefix + topic, bootstrap_servers=bootstrap, group_id=f"{group}-{topic}",
            auto_offset_reset="earliest", enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b))
        self._buf: dict[str, list] = {}

    def _consumer(self, topic):
        if topic not in self._consumers:
            self._consumers[topic] = self._mk(topic)
        return self._consumers[topic]

    async def publish(self, topic, msg):
        self._producer.send(self.prefix + topic, msg)

    def _poll(self, topic, timeout_ms):
        buf = self._buf.setdefault(topic, [])
        if not buf:
            for recs in self._consumer(topic).poll(timeout_ms=timeout_ms, max_records=5000).values():
                buf.extend(r.value for r in recs)
        return buf.pop(0) if buf else None

    async def get(self, topic, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self._producer.flush()
            msg = await asyncio.to_thread(self._poll, topic, 50)
            if msg is not None:
                return msg
            if deadline and time.monotonic() > deadline:
                raise asyncio.TimeoutError

    def get_nowait(self, topic):
        buf = self._buf.get(topic)
        return buf.pop(0) if buf else None

    async def close(self):
        self._producer.flush()
        self._producer.close()
        for c in self._consumers.values():
            c.close()


class BankClient:
    """A consortium member: seals its own payments to the enclave, reads only its own topics.

    With ``linger_ms`` > 0 the client batches payments for that long before sealing (one
    public-key operation per batch instead of per payment); latency is still measured from each
    payment's own send time, so the linger is counted against the budget.
    """

    def __init__(self, member: int, bus, enclave_public_b64: str, linger_ms: float = 0.0):
        self.member = member
        self.keys = KeyPair()
        self.bus = bus
        self.enclave_pub = enclave_public_b64
        self.linger = linger_ms / 1e3
        self._buf: list = []
        self._first = 0.0

    async def send(self, payment: dict, sent_at: float | None = None):
        sent_at = time.time() if sent_at is None else sent_at
        if self.linger <= 0:
            await self._publish([payment], [sent_at])
            return
        if not self._buf:
            self._first = time.monotonic()
        self._buf.append((payment, sent_at))
        if time.monotonic() - self._first >= self.linger:
            await self.flush()

    async def flush(self):
        if self._buf:
            buf, self._buf = self._buf, []
            await self._publish([p for p, _ in buf], [t for _, t in buf])

    async def run_flusher(self, stop: asyncio.Event):
        while not stop.is_set():
            await asyncio.sleep(self.linger or 0.005)
            if self._buf and time.monotonic() - self._first >= self.linger:
                await self.flush()
        await self.flush()

    async def _publish(self, payments, sent_at):
        await self.bus.publish(PAYMENTS, {"member": self.member, "sealed": seal(
            {"payments": payments, "sent_at": sent_at}, self.enclave_pub)})

    def open(self, msg: dict):
        return self.keys.open(msg["sealed"])


class HotConsumer:
    """Drains payments.in into micro-batches and runs them through the engine."""

    def __init__(self, engine, bus, enclave_keys: KeyPair, member_keys: dict[int, str],
                 linger_ms: float = 2.0, max_batch: int = 500):
        self.engine, self.bus, self.keys = engine, bus, enclave_keys
        self.member_keys = member_keys
        self.linger = linger_ms / 1e3
        self.max_batch = max_batch
        self.e2e_ms: list[float] = []
        self.batch_sizes: list[int] = []
        self._stop = asyncio.Event()

    def stop(self):
        self._stop.set()

    async def _collect(self):
        try:
            first = await self.bus.get(PAYMENTS, timeout=0.2)
        except asyncio.TimeoutError:
            return []
        batch = [first]
        deadline = time.monotonic() + self.linger
        while len(batch) < self.max_batch:   # messages; each may carry several payments
            msg = self.bus.get_nowait(PAYMENTS)
            if msg is None:
                if time.monotonic() >= deadline:
                    break
                await asyncio.sleep(0)
                continue
            batch.append(msg)
        return batch

    async def run(self):
        while not self._stop.is_set():
            batch = await self._collect()
            if not batch:
                continue
            t0 = time.perf_counter()
            items, sent = [], []
            for m in batch:
                body = self.keys.open(m["sealed"])
                items.extend((int(m["member"]), p) for p in body["payments"])
                sent.extend(body["sent_at"])
            self.engine.stats.add_stage("decrypt", (time.perf_counter() - t0) * 1e3)
            out, rejected = self.engine.process(items)
            t_pub = time.perf_counter()
            for member, decisions in out.items():
                await self.bus.publish(decisions_topic(member), {
                    "sealed": seal({"decisions": decisions, "rejected": rejected.get(member, [])},
                                   self.member_keys[member])})
            for member in self.member_keys:   # receivers may have no payments in this batch
                notes = self.engine.drain_notices(member)
                if notes:
                    await self.bus.publish(notices_topic(member),
                                           {"sealed": seal(notes, self.member_keys[member])})
            now = time.time()
            self.engine.stats.add_stage("publish", (time.perf_counter() - t_pub) * 1e3)
            self.e2e_ms.extend((now - ts) * 1e3 for ts in sent)
            self.batch_sizes.append(len(items))
            await asyncio.sleep(0)
