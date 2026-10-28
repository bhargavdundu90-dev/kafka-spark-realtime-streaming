"""Spark tests. Skipped automatically when pyspark or a JVM is missing, so the
pure-Python suite still runs anywhere; CI installs Java and runs everything."""
import json
import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    import os

    if not os.environ.get("JAVA_HOME"):
        pytest.skip("no JVM available", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from txnstream.generator import SCENARIOS, TransactionGenerator  # noqa: E402
from txnstream.job import build_streams  # noqa: E402
from txnstream.rules import score_transactions  # noqa: E402
from txnstream.transforms import fraud_signals, parse_json, split_valid  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("txnstream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _as_values(spark, records):
    return spark.createDataFrame([(json.dumps(r) if isinstance(r, dict) else r,) for r in records], "value string")


def _sample(seed=5):
    import itertools
    from datetime import datetime, timezone

    gen = TransactionGenerator(seed=seed, n_cards=20, fraud_rate=0.0)
    base = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
    rows = list(itertools.islice(gen.stream(start=base, spacing_seconds=20), 120))
    for i, name in enumerate(SCENARIOS):
        rows += gen.scenario(name, base.replace(minute=5 + 6 * i))
    return rows


def test_quarantine_catches_bad_records(spark, txn):
    good = txn()
    bad = [txn(amount=-1, txn_id="neg"), txn(channel="NEFT", txn_id="ch"), "not json at all"]
    valid, quarantine = split_valid(parse_json(_as_values(spark, [good, *bad])))
    assert valid.count() == 1
    reasons = sorted(r.reject_reason for r in quarantine.collect())
    assert reasons == ["domain:channel", "missing:txn_id", "range:amount"]


def test_spark_rules_match_python_reference(spark):
    """Rule parity: the Spark job and the reference implementation agree on
    every window, every signal and every score."""
    rows = _sample()
    valid, _ = split_valid(parse_json(_as_values(spark, rows)))
    got = {
        (r.card_id, r.window_start.replace(tzinfo=None), tuple(r.signals), r.risk_score, r.txn_count)
        for r in fraud_signals(valid).collect()
    }
    expected = {
        (s.stats.card_id, s.stats.window_start.replace(tzinfo=None), s.signals, s.risk_score, s.stats.txn_count)
        for s in score_transactions(rows)
    }
    assert got == expected
    flagged = {sig for (_, _, sigs, _, _) in got for sig in sigs}
    assert flagged == set(SCENARIOS)


def test_streaming_pipeline_end_to_end(spark, tmp_path):
    """Run the real streaming plan over a file source standing in for Kafka
    (same `value` string column), including watermark + dedupe."""
    rows = _sample(seed=9)
    rows.append(dict(rows[0]))  # duplicate delivery of the same txn_id
    # a late event far beyond the watermark advances event time so windows close
    late = dict(rows[1], txn_id="clock-advance", event_time="2026-10-28T13:00:00Z")
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "batch1.json").write_text("\n".join(json.dumps(r) for r in rows) + "\nbroken{\n")
    (inbox / "batch2.json").write_text(json.dumps(late) + "\n")

    source = spark.readStream.option("maxFilesPerTrigger", 1).text(str(inbox))
    streams = build_streams(source, watermark="15 minutes")
    for name, df in streams.items():
        q = (
            df.writeStream.format("memory").queryName(f"t_{name}").outputMode("append")
            .option("checkpointLocation", str(tmp_path / "chk" / name))
            .trigger(availableNow=True).start()
        )
        q.awaitTermination()
        # availableNow emits finalised windows only once the watermark has moved;
        # a final empty batch is processed by Spark when there is no new data.

    txns = spark.table("t_transactions")
    assert txns.count() == len(rows)  # duplicate dropped, late clock event kept
    assert txns.select("txn_id").distinct().count() == txns.count()
    assert spark.table("t_quarantine").count() == 1

    detected = {s for r in spark.table("t_fraud_signals").collect() for s in r.signals}
    assert detected == set(SCENARIOS)
