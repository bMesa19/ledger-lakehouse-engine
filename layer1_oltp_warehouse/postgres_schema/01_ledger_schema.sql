-- 01_ledger_schema.sql
-- High-Throughput Double-Entry Financial Ledger (PostgreSQL 16) 
#
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
#
-- 1. Chart of Accounts (Dimension / Reference)
CREATE TABLE accounts (
    account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_code VARCHAR(20) UNIQUE NOT NULL,
    account_name VARCHAR(100) NOT NULL,
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE')),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Journal Entries Header (Enforces Idempotency on Ingestion)
CREATE TABLE journal_entries (
    entry_id UUID NOT NULL DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(64) NOT NULL,
    transaction_reference VARCHAR(100) NOT NULL,
    entry_status VARCHAR(20) NOT NULL DEFAULT 'POSTED' CHECK (entry_status IN ('PENDING', 'POSTED', 'REVERSED')),
    posted_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entry_id, posted_at),
    UNIQUE (idempotency_key, posted_at)
) PARTITION BY RANGE (posted_at);
90 130
-- 3. Journal Lines Fact Table (Append-Only Immutable Ledger Lines)
-- Invariant: For every entry_id, SUM(signed_amount) MUST equal 0.0000
CREATE TABLE journal_lines (
    line_id BIGSERIAL,
    entry_id UUID NOT NULL,
    account_id UUID NOT NULL,
    direction CHAR(2) NOT NULL CHECK (direction IN ('DR', 'CR')),
    amount NUMERIC(18, 4) NOT NULL CHECK (amount > 0),
    signed_amount NUMERIC(18, 4) GENERATED ALWAYS AS (
        CASE WHEN direction = 'DR' THEN amount ELSE -amount END
    ) STORED,
    posted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (line_id, posted_at)
) PARTITION BY RANGE (posted_at);

-- 4. Create Monthly Partitions (2026 Q4)
CREATE TABLE journal_entries_y2026m10 PARTITION OF journal_entries
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

CREATE TABLE journal_entries_y2026m11 PARTITION OF journal_entries
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

CREATE TABLE journal_lines_y2026m10 PARTITION OF journal_lines
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

CREATE TABLE journal_lines_y2026m11 PARTITION OF journal_lines
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

-- 5. Covering Index for Index-Only Scans
CREATE INDEX idx_journal_lines_account_covering
    ON journal_lines (account_id, posted_at)
    INCLUDE (signed_amount);
