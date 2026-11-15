SELECT
    account_id,
    account_code,
    account_name,
    account_type,
    financial_statement,
    normal_balance_side,
    currency,
    (account_code IN ('ACC-00000', 'ACC-00001', 'ACC-00002', 'ACC-00003', 'ACC-00004')) AS is_whale_clearing_account,
    created_at
FROM {{ ref('stg_accounts') }}
