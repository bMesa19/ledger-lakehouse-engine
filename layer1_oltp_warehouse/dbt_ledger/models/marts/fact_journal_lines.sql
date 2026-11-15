{{
    config(
        materialized='incremental',
        unique_key=['line_id', 'posted_at'],
        incremental_strategy='delete+insert'
    )
}}

SELECT
    l.line_id,
    l.entry_id,
    l.idempotency_key,
    l.transaction_reference,
    l.account_id,
    a.account_code,
    a.account_type,
    a.financial_statement,
    l.direction,
    l.amount,
    l.signed_amount,
    l.posted_at,
    l.posted_date
FROM {{ ref('stg_journal_lines') }} l
INNER JOIN {{ ref('dim_accounts') }} a
    ON l.account_id = a.account_id

{% if is_incremental() %}
WHERE l.posted_at > (SELECT COALESCE(MAX(posted_at), '1900-01-01'::timestamptz) FROM {{ this }})
{% endif %}
