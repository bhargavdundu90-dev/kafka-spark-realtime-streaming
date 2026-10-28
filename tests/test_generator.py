import itertools
import json

from txnstream.generator import SCENARIOS, TransactionGenerator, main
from txnstream.rules import score_transactions
from txnstream.schema import validate


def test_generated_records_satisfy_contract():
    gen = TransactionGenerator(seed=7, fraud_rate=0.2)
    for rec in itertools.islice(gen.stream(), 500):
        assert validate(rec) == [], rec


def test_seed_makes_output_reproducible():
    a = list(itertools.islice(TransactionGenerator(seed=1).stream(), 20))
    b = list(itertools.islice(TransactionGenerator(seed=1).stream(), 20))
    assert [r["txn_id"] for r in a] == [r["txn_id"] for r in b]


def test_every_injected_scenario_is_detected_by_its_rule():
    gen = TransactionGenerator(seed=3)
    from datetime import datetime, timezone

    ts = datetime(2026, 10, 28, 10, 0, 30, tzinfo=timezone.utc)
    for name in SCENARIOS:
        signals = score_transactions(gen.scenario(name, ts))
        assert any(name in s.signals for s in signals), name


def test_cli_stdout(capsys):
    main(["--sink", "stdout", "--count", "3", "--seed", "11"])
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 3 and all(validate(json.loads(l)) == [] for l in lines)
