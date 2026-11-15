-- Singular Audit Test: Fails if ANY journal entry violates double-entry balance (SUM != 0.0000)
SELECT
    entry_id,
    SUM(signed_amount) AS net_imbalance
FROM {{ ref('fact_journal_lines') }}
GROUP BY entry_id
HAVING SUM(signed_amount) <> 0.0000
