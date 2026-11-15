SELECT
    account_id,
    account_code,
    account_name,
    account_type,
    CASE
        WHEN account_type IN ('ASSET', 'LIABILITY', 'EQUITY') THEN 'BALANCE_SHEET'
        ELSE 'INCOME_STATEMENT'
    END AS financial_statement,
    CASE
        WHEN account_type IN ('ASSET', 'EXPENSE') THEN 'DR'
        ELSE 'CR'
    END AS normal_balance_side,
    currency,
    created_at
FROM {{ source('core_ledger_oltp', 'accounts') }}
