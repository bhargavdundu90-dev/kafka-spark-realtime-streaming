from datetime import datetime, timedelta, timezone

from txnstream.config import FraudThresholds
from txnstream.rules import aggregate, score_transactions, window_starts

UTC = timezone.utc


def test_sliding_windows_align_to_epoch_like_spark():
    ts = datetime(2026, 10, 28, 10, 7, 30, tzinfo=UTC)
    starts = window_starts(ts, timedelta(minutes=10), timedelta(minutes=5))
    assert starts == [datetime(2026, 10, 28, 10, 0, tzinfo=UTC), datetime(2026, 10, 28, 10, 5, tzinfo=UTC)]


def test_event_on_boundary_belongs_to_window_starting_there():
    ts = datetime(2026, 10, 28, 10, 10, tzinfo=UTC)
    starts = window_starts(ts, timedelta(minutes=10), timedelta(minutes=5))
    # [10:05, 10:15) and [10:10, 10:20); not [10:00, 10:10) since end is exclusive
    assert starts == [datetime(2026, 10, 28, 10, 5, tzinfo=UTC), datetime(2026, 10, 28, 10, 10, tzinfo=UTC)]


def test_normal_customer_is_not_flagged(txn):
    txns = [txn(ts="2026-10-28T10:01:00Z", amount=450), txn(ts="2026-10-28T10:03:00Z", amount=1200)]
    assert all(not s.is_flagged and s.signals == () for s in score_transactions(txns))


def test_velocity_signal(txn):
    txns = [txn(ts=f"2026-10-28T10:0{i}:00Z") for i in range(5)]
    hit = [s for s in score_transactions(txns) if "VELOCITY" in s.signals]
    assert hit and hit[0].stats.txn_count == 5


def test_high_value_plus_multi_city_is_flagged(txn):
    txns = [
        txn(ts="2026-10-28T10:01:00Z", amount=120_000, channel="POS", city="Mumbai"),
        txn(ts="2026-10-28T10:02:00Z", amount=90_000, channel="POS", city="Delhi"),
        txn(ts="2026-10-28T10:03:00Z", amount=1_000, channel="POS", city="Chennai"),
    ]
    best = max(score_transactions(txns), key=lambda s: s.risk_score)
    assert best.signals == ("HIGH_VALUE", "MULTI_CITY")
    assert best.risk_score == 75 and best.is_flagged


def test_card_testing_counts_only_small_ecom(txn):
    txns = [txn(ts=f"2026-10-28T10:0{i}:00Z", amount=5, channel="ECOM") for i in range(3)]
    txns.append(txn(ts="2026-10-28T10:04:00Z", amount=5, channel="UPI"))  # not ECOM
    stats = aggregate(txns)[0]
    assert stats.small_ecom_count == 3


def test_single_rule_below_flag_score_is_signal_not_flag(txn):
    txns = [txn(ts=f"2026-10-28T10:0{i}:00Z", amount=10_000, channel="ATM") for i in range(3)]
    s = max(score_transactions(txns), key=lambda s: s.risk_score)
    assert s.signals == ("ATM_BURST",) and s.risk_score == 25 and not s.is_flagged


def test_thresholds_are_configurable(txn):
    strict = FraudThresholds(velocity_count=2)
    txns = [txn(ts="2026-10-28T10:01:00Z"), txn(ts="2026-10-28T10:02:00Z")]
    assert any("VELOCITY" in s.signals for s in score_transactions(txns, strict))


def test_cards_are_isolated(txn):
    txns = [txn(card=f"CARD00000{i}", ts=f"2026-10-28T10:0{i}:00Z") for i in range(5)]
    assert not any("VELOCITY" in s.signals for s in score_transactions(txns))
