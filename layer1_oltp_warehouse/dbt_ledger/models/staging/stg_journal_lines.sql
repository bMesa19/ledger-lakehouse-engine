SELECT
    jl.line_id,
    jl.entry_id,
    je.idempotency_key,
    je.transaction_reference,
    je.entry_status,
    jl.account_id,
    jl.direction,
    jl.amount,
    jl.signed_amount,
    jl.posted_at,
    DATE_TRUNC('day', jl.posted_at)::DATE AS posted_date
FROM {{ source('core_ledger_oltp', 'journal_lines') }} jl
INNER JOIN {{ source('core_ledger_oltp', 'journal_entries') }} je
    ON jl.entry_id = je.entry_id
   AND jl.posted_at = je.posted_at
WHERE je.entry_status = 'POSTED'
