-- Flagged cards in the last 24 hours, worst first (partition-pruned via window_date)
SELECT card_id, window_start, signals, risk_score, txn_count, total_amount
FROM txnstream_db.fraud_signals
WHERE window_date >= current_date - INTERVAL '1' DAY
  AND is_flagged
ORDER BY risk_score DESC, window_start DESC
LIMIT 100;

-- Which rules fire most? Useful when tuning thresholds with the fraud-ops team.
SELECT signal, count(*) AS windows
FROM txnstream_db.fraud_signals
CROSS JOIN UNNEST(signals) AS t(signal)
WHERE window_date >= current_date - INTERVAL '7' DAY
GROUP BY signal
ORDER BY windows DESC;
