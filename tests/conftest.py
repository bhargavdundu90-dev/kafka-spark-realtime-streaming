import pytest


def make_txn(card="CARD000001", ts="2026-10-28T10:01:00Z", amount=500.0, channel="UPI",
             city="Hyderabad", txn_id=None, **extra):
    t = {
        "txn_id": txn_id or f"{card}-{ts}-{amount}-{channel}-{city}",
        "card_id": card,
        "customer_id": "CUST" + card[4:],
        "amount": amount,
        "currency": "INR",
        "merchant_id": "M00001",
        "mcc": "5411",
        "channel": channel,
        "city": city,
        "is_international": False,
        "event_time": ts,
    }
    t.update(extra)
    return t


@pytest.fixture
def txn():
    return make_txn
