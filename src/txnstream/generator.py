"""Synthetic transaction generator for an Indian retail bank's card/UPI feed.

Produces mostly normal traffic plus labelled fraud scenarios so the streaming
job has something meaningful to detect. All data is fake.

    python -m txnstream.generator --sink stdout --count 20
    python -m txnstream.generator --sink kafka --bootstrap localhost:9092 --rate 50
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator

CITIES = ["Hyderabad", "Bengaluru", "Mumbai", "Chennai", "Delhi", "Pune", "Kolkata", "Vijayawada"]
MCCS = {"5411": "grocery", "5812": "restaurants", "5732": "electronics",
        "4111": "transit", "5999": "misc_retail", "6011": "atm"}
SCENARIOS = ("VELOCITY", "HIGH_VALUE", "MULTI_CITY", "ATM_BURST", "CARD_TESTING")


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TransactionGenerator:
    def __init__(self, seed: int | None = None, n_cards: int = 500, fraud_rate: float = 0.02):
        self.rng = random.Random(seed)
        self.cards = [f"CARD{idx:06d}" for idx in range(n_cards)]
        self.home_city = {c: self.rng.choice(CITIES) for c in self.cards}
        self.fraud_rate = fraud_rate

    def _txn(self, card: str, ts: datetime, *, amount: float, channel: str,
             city: str | None = None, mcc: str | None = None, label: str = "NORMAL") -> dict:
        mcc = mcc or ("6011" if channel == "ATM" else self.rng.choice([m for m in MCCS if m != "6011"]))
        return {
            "txn_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "card_id": card,
            "customer_id": "CUST" + card[4:],
            "amount": round(amount, 2),
            "currency": "INR",
            "merchant_id": f"M{self.rng.randint(1, 9999):05d}",
            "mcc": mcc,
            "channel": channel,
            "city": city or self.home_city[card],
            "is_international": False,
            "event_time": _iso(ts),
            # Ground truth for evaluating the rules. The job ignores it.
            "scenario": label,
        }

    def normal(self, ts: datetime) -> dict:
        card = self.rng.choice(self.cards)
        channel = self.rng.choices(["UPI", "POS", "ECOM", "ATM"], weights=[55, 25, 15, 5])[0]
        amount = self.rng.lognormvariate(6.5, 1.0)  # median ~ INR 665
        if channel == "ATM":
            amount = self.rng.choice([500, 1000, 2000, 5000, 10000])
        if channel == "ECOM":
            amount = max(amount, 149.0)  # keep normal ECOM above the probe threshold
        return self._txn(card, ts, amount=min(amount, 150_000), channel=channel)

    def scenario(self, name: str, ts: datetime) -> list[dict]:
        card = self.rng.choice(self.cards)
        step = lambda i: ts + timedelta(seconds=40 * i)  # noqa: E731 - all inside 10 min
        if name == "VELOCITY":
            return [self._txn(card, step(i), amount=self.rng.uniform(300, 2000), channel="UPI", label=name)
                    for i in range(7)]
        if name == "HIGH_VALUE":
            return [self._txn(card, step(i), amount=self.rng.uniform(95_000, 140_000), channel="POS",
                              mcc="5732", label=name) for i in range(2)]
        if name == "MULTI_CITY":
            cities = self.rng.sample(CITIES, 3)
            return [self._txn(card, step(i), amount=self.rng.uniform(500, 4000), channel="POS",
                              city=c, label=name) for i, c in enumerate(cities)]
        if name == "ATM_BURST":
            return [self._txn(card, step(i), amount=10_000, channel="ATM", label=name) for i in range(4)]
        if name == "CARD_TESTING":
            return [self._txn(card, step(i), amount=self.rng.uniform(1, 20), channel="ECOM",
                              mcc="5999", label=name) for i in range(4)]
        raise ValueError(f"unknown scenario {name}")

    def stream(self, start: datetime | None = None, spacing_seconds: float = 1.0) -> Iterator[dict]:
        ts = start or datetime.now(timezone.utc)
        while True:
            if self.rng.random() < self.fraud_rate:
                yield from self.scenario(self.rng.choice(SCENARIOS), ts)
            else:
                yield self.normal(ts)
            ts += timedelta(seconds=spacing_seconds)


def _kafka_producer(bootstrap: str):
    from kafka import KafkaProducer  # imported lazily so tests don't need a broker

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
        linger_ms=20,
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sink", choices=["stdout", "kafka"], default="stdout")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="card-transactions")
    p.add_argument("--count", type=int, default=0, help="stop after N events (0 = run forever)")
    p.add_argument("--rate", type=float, default=20.0, help="events per second (wall clock)")
    p.add_argument("--fraud-rate", type=float, default=0.02)
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args(argv)

    gen = TransactionGenerator(seed=args.seed, fraud_rate=args.fraud_rate)
    producer = _kafka_producer(args.bootstrap) if args.sink == "kafka" else None
    sent = 0
    try:
        for txn in gen.stream(spacing_seconds=1.0 / args.rate if args.sink == "kafka" else 1.0):
            if producer:
                # keyed by card so one card's events stay ordered within a partition
                producer.send(args.topic, key=txn["card_id"], value=txn)
                time.sleep(1.0 / args.rate)
            else:
                sys.stdout.write(json.dumps(txn) + "\n")
            sent += 1
            if args.count and sent >= args.count:
                break
    finally:
        if producer:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
