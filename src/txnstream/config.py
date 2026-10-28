"""Thresholds and runtime settings.

The fraud thresholds live in exactly one place so that the PySpark job and the
pure-Python reference implementation (used in tests) can never drift apart.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

CHANNELS = ("POS", "ECOM", "UPI", "ATM")


@dataclass(frozen=True)
class FraudThresholds:
    """Per-card, per-window rules. Amounts are in INR."""

    window_minutes: int = 10
    slide_minutes: int = 5
    velocity_count: int = 5          # >= N transactions in one window
    high_value_total: float = 200_000.0  # >= total spend in one window
    multi_city_count: int = 3        # >= distinct cities in one window
    atm_burst_count: int = 3         # >= ATM withdrawals in one window
    small_ecom_amount: float = 100.0  # an ECOM txn below this is a "probe"
    card_testing_count: int = 3      # >= probe transactions in one window
    flag_score: int = 50             # risk_score at/above this => is_flagged
    weights: dict = field(
        default_factory=lambda: {
            "VELOCITY": 30,
            "HIGH_VALUE": 40,
            "MULTI_CITY": 35,
            "ATM_BURST": 25,
            "CARD_TESTING": 30,
        }
    )


# Order matters: it is the order signals appear in the output array.
SIGNAL_ORDER = ("VELOCITY", "HIGH_VALUE", "MULTI_CITY", "ATM_BURST", "CARD_TESTING")

DEFAULT_THRESHOLDS = FraudThresholds()


@dataclass(frozen=True)
class JobSettings:
    kafka_bootstrap: str
    topic: str
    output_path: str
    checkpoint_path: str
    trigger_seconds: int
    watermark: str
    starting_offsets: str
    msk_iam: bool = False

    @classmethod
    def from_env(cls) -> "JobSettings":
        return cls(
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            topic=os.getenv("KAFKA_TOPIC", "card-transactions"),
            output_path=os.getenv("OUTPUT_PATH", "./lake").rstrip("/"),
            checkpoint_path=os.getenv("CHECKPOINT_PATH", "./checkpoints").rstrip("/"),
            trigger_seconds=int(os.getenv("TRIGGER_SECONDS", "30")),
            watermark=os.getenv("WATERMARK", "15 minutes"),
            starting_offsets=os.getenv("STARTING_OFFSETS", "latest"),
            msk_iam=os.getenv("MSK_IAM", "false").lower() == "true",
        )
