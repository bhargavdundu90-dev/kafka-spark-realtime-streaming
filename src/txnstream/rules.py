"""Pure-Python reference implementation of the fraud-signal aggregation.

The Spark job expresses the same logic with DataFrame columns. Tests run both
over identical input and assert they agree ("rule parity"), which lets the
business rules be unit-tested in milliseconds without a JVM.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from .config import DEFAULT_THRESHOLDS, SIGNAL_ORDER, FraudThresholds
from .schema import parse_event_time

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class WindowStats:
    card_id: str
    window_start: datetime
    window_end: datetime
    txn_count: int
    total_amount: float
    distinct_cities: int
    atm_count: int
    small_ecom_count: int


@dataclass(frozen=True)
class Signal:
    stats: WindowStats
    signals: tuple[str, ...]
    risk_score: int
    is_flagged: bool


def window_starts(ts: datetime, window: timedelta, slide: timedelta) -> list[datetime]:
    """Sliding windows [start, start+window) containing ts, aligned to epoch
    exactly like Spark's window(col, window, slide)."""
    offset = (ts - EPOCH) % slide
    last = ts - offset
    starts = []
    start = last
    while start > ts - window:
        starts.append(start)
        start -= slide
    return sorted(starts)


def aggregate(
    txns: Iterable[dict], th: FraudThresholds = DEFAULT_THRESHOLDS
) -> list[WindowStats]:
    window = timedelta(minutes=th.window_minutes)
    slide = timedelta(minutes=th.slide_minutes)
    buckets: dict[tuple[str, datetime], list[dict]] = defaultdict(list)
    for t in txns:
        ts = parse_event_time(t["event_time"])
        for start in window_starts(ts, window, slide):
            buckets[(t["card_id"], start)].append(t)

    out = []
    for (card_id, start), rows in sorted(buckets.items()):
        out.append(
            WindowStats(
                card_id=card_id,
                window_start=start,
                window_end=start + window,
                txn_count=len(rows),
                total_amount=round(sum(float(r["amount"]) for r in rows), 2),
                distinct_cities=len({r.get("city") for r in rows if r.get("city")}),
                atm_count=sum(1 for r in rows if r["channel"] == "ATM"),
                small_ecom_count=sum(
                    1
                    for r in rows
                    if r["channel"] == "ECOM" and float(r["amount"]) < th.small_ecom_amount
                ),
            )
        )
    return out


def evaluate(stats: WindowStats, th: FraudThresholds = DEFAULT_THRESHOLDS) -> Signal:
    hits = {
        "VELOCITY": stats.txn_count >= th.velocity_count,
        "HIGH_VALUE": stats.total_amount >= th.high_value_total,
        "MULTI_CITY": stats.distinct_cities >= th.multi_city_count,
        "ATM_BURST": stats.atm_count >= th.atm_burst_count,
        "CARD_TESTING": stats.small_ecom_count >= th.card_testing_count,
    }
    signals = tuple(name for name in SIGNAL_ORDER if hits[name])
    score = sum(th.weights[name] for name in signals)
    return Signal(stats, signals, score, score >= th.flag_score)


def score_transactions(
    txns: Iterable[dict], th: FraudThresholds = DEFAULT_THRESHOLDS
) -> list[Signal]:
    return [evaluate(s, th) for s in aggregate(txns, th)]
