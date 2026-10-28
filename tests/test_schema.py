from txnstream.schema import validate


def test_valid_record_has_no_errors(txn):
    assert validate(txn()) == []


def test_missing_required_fields_are_reported(txn):
    rec = txn()
    del rec["txn_id"]
    rec["card_id"] = ""
    errors = validate(rec)
    assert "missing:txn_id" in errors
    assert "missing:card_id" in errors


def test_non_positive_amount_rejected(txn):
    assert "range:amount" in validate(txn(amount=0))
    assert "range:amount" in validate(txn(amount=-10.5))


def test_bool_is_not_an_amount(txn):
    assert "type:amount" in validate(txn(amount=True))


def test_unknown_channel_rejected(txn):
    assert validate(txn(channel="NEFT")) == ["domain:channel"]


def test_bad_timestamp_rejected(txn):
    assert "format:event_time" in validate(txn(ts="28/10/2026 10:00"))
