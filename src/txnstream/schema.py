"""Transaction contract shared by the generator, validator and Spark job."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from .config import CHANNELS

# name -> python type accepted in the JSON payload
FIELDS: dict[str, type | tuple[type, ...]] = {
    "txn_id": str,
    "card_id": str,
    "customer_id": str,
    "amount": (int, float),
    "currency": str,
    "merchant_id": str,
    "mcc": str,
    "channel": str,
    "city": str,
    "is_international": bool,
    "event_time": str,  # ISO-8601, UTC, e.g. 2026-10-28T10:15:00Z
}

REQUIRED = ("txn_id", "card_id", "amount", "channel", "event_time")


def parse_event_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate(record: dict[str, Any]) -> list[str]:
    """Return a list of human-readable problems; empty list means valid.

    Mirrors the filter used in the Spark job so bad records land in the
    quarantine table instead of silently disappearing.
    """
    errors: list[str] = []
    for name in REQUIRED:
        if record.get(name) in (None, ""):
            errors.append(f"missing:{name}")
    for name, expected in FIELDS.items():
        value = record.get(name)
        if value is None:
            continue
        # bool is a subclass of int; don't let True pass as an amount
        if name == "amount" and isinstance(value, bool):
            errors.append("type:amount")
        elif not isinstance(value, expected):
            errors.append(f"type:{name}")
    amount = record.get("amount")
    if isinstance(amount, (int, float)) and not isinstance(amount, bool) and amount <= 0:
        errors.append("range:amount")
    channel = record.get("channel")
    if isinstance(channel, str) and channel and channel not in CHANNELS:
        errors.append("domain:channel")
    ts = record.get("event_time")
    if isinstance(ts, str) and ts:
        try:
            parse_event_time(ts)
        except ValueError:
            errors.append("format:event_time")
    return errors
