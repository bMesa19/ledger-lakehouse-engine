cat << 'EOF' > layer1_oltp_warehouse/ingestion/ingest_ledger.py
import asyncio
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import asyncpg

DB_DSN = "postgresql://ledger_admin:ledger_secret_pass@localhost:5432/core_ledger"
here we are connecting to the docjer yml compoese
TOTAL_ACCOUNTS = 5_000
WHALE_ACCOUNT_COUNT = 5          # 0.1% of accounts hold 80% of volume (Pareto skew for Layer 2)
TOTAL_ENTRIES = 5_000_000        # 5M entries x 2 lines = 10,000,000 ledger rows
BATCH_SIZE = 250_000             # 250K entries (500K lines) per atomic transaction

START_TIME = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
SPAN_SECONDS = 60 * 86400        # Spans Oct 1, 2026 -> Nov 30, 2026 (hits both partitions)


async def seed_chart_of_accounts(conn: asyncpg.Connection) -> tuple[list[uuid.UUID], list[uuid.UUID]]:
    """Seeds 5,000 accounts and returns (whale_ids, standard_ids)."""
    existing = await conn.fetchval("SELECT COUNT(*) FROM accounts")
    if existing >= TOTAL_ACCOUNTS:
        rows = await conn.fetch("SELECT account_id FROM accounts ORDER BY account_code")
        all_ids = [r["account_id"] for r in rows]
        return all_ids[:WHALE_ACCOUNT_COUNT], all_ids[WHALE_ACCOUNT_COUNT:]

    account_types = ["ASSET", "LIABILITY", "EQUITY", "REVENUE", "EXPENSE"]
    records = []
    for i in range(TOTAL_ACCOUNTS):
        acc_id = uuid.uuid4()
        acc_code = f"ACC-{i:05d}"
        acc_name = f"Institutional Clearing #{i}" if i < WHALE_ACCOUNT_COUNT else f"Merchant Account #{i}"
        acc_type = account_types[i % len(account_types)]
        records.append((acc_id, acc_code, acc_name, acc_type, "USD"))

    await conn.copy_records_to_table(
        "accounts",
        records=records,
        columns=["account_id", "account_code", "account_name", "account_type", "currency"],
    )
    all_ids = [r[0] for r in records]
    return all_ids[:WHALE_ACCOUNT_COUNT], all_ids[WHALE_ACCOUNT_COUNT:]


def generate_balanced_batch(
    batch_idx: int,
    batch_size: int,
    whales: list[uuid.UUID],
    standards: list[uuid.UUID],
) -> tuple[list[tuple], list[tuple]]:
    """
    Generates a batch of balanced double-entry transactions.
    Invariant: Every entry produces 1 DR line and 1 CR line with identical Decimal amount.
    """
    entries = []
    lines = []
    base_offset = batch_idx * batch_size

    for i in range(batch_size):
        seq = base_offset + i
        entry_id = uuid.uuid4()
        idempotency_key = f"idem-2026-{seq:010d}"
        tx_ref = f"TXN-{seq:010d}"

        # Distribute timestamps across Oct & Nov 2026 partitions
        posted_at = START_TIME + timedelta(seconds=random.randint(0, SPAN_SECONDS - 1))

        # Exact base-10 decimal amount (zero floating-point drift)
        cents = random.randint(100, 250_000)
        amount = (Decimal(cents) / Decimal(100)).quantize(Decimal("0.0001"))

        # 80% of transactions route through the 5 whale accounts
        dr_account = random.choice(whales) if random.random() < 0.80 else random.choice(standards)
        cr_account = random.choice(standards)
        if cr_account == dr_account:
            cr_account = standards[(i + 1) % len(standards)]

        entries.append((entry_id, idempotency_key, tx_ref, "POSTED", posted_at))
        lines.append((entry_id, dr_account, "DR", amount, posted_at))
        lines.append((entry_id, cr_account, "CR", amount, posted_at))

    return entries, lines


async def ingest_batch(conn: asyncpg.Connection, entries: list[tuple], lines: list[tuple]) -> None:
    """
    Atomic batch load:
    1. Binary COPY headers into an unlogged TEMP staging table.
    2. Deduplicate via ON CONFLICT (idempotency_key, posted_at) DO NOTHING.
    3. Binary COPY immutable ledger lines directly into partitioned journal_lines.
    """
    async with conn.transaction():
        await conn.execute("""
            CREATE TEMP TABLE tmp_entries (
                entry_id UUID,
                idempotency_key VARCHAR(64),
                transaction_reference VARCHAR(100),
                entry_status VARCHAR(20),
                posted_at TIMESTAMPTZ
            ) ON COMMIT DROP;
        """)

        await conn.copy_records_to_table(
            "tmp_entries",
            records=entries,
            columns=["entry_id", "idempotency_key", "transaction_reference", "entry_status", "posted_at"],
        )

        await conn.execute("""
            INSERT INTO journal_entries (entry_id, idempotency_key, transaction_reference, entry_status, posted_at)
            SELECT entry_id, idempotency_key, transaction_reference, entry_status, posted_at
            FROM tmp_entries
            ON CONFLICT (idempotency_key, posted_at) DO NOTHING;
        """)

        await conn.copy_records_to_table(
            "journal_lines",
            records=lines,
            columns=["entry_id", "account_id", "direction", "amount", "posted_at"],
        )


async def main() -> None:
    t0 = time.perf_counter()
    conn = await asyncpg.connect(DB_DSN)
    try:
        whales, standards = await seed_chart_of_accounts(conn)
        print(f"Chart of Accounts ready: {len(whales)} whale accounts, {len(standards)} standard accounts.")

        num_batches = TOTAL_ENTRIES // BATCH_SIZE
        for b in range(num_batches):
            b_start = time.perf_counter()
            entries, lines = generate_balanced_batch(b, BATCH_SIZE, whales, standards)
            await ingest_batch(conn, entries, lines)
            elapsed = time.perf_counter() - b_start
            total_lines = (b + 1) * BATCH_SIZE * 2
            rate = (BATCH_SIZE * 2) / elapsed
            print(f"Batch {b + 1}/{num_batches} | Total Lines: {total_lines:,} | Speed: {rate:,.0f} lines/sec")

        # Critical: Run VACUUM ANALYZE to set Visibility Map bits for Index-Only Scans
        print("Running VACUUM ANALYZE to update planner statistics and Visibility Map...")
        await conn.execute("VACUUM ANALYZE accounts;")
        await conn.execute("VACUUM ANALYZE journal_entries;")
        await conn.execute("VACUUM ANALYZE journal_lines;")

        # Audit Invariant Check: Global sum of signed_amount MUST equal 0.0000
        drift = await conn.fetchval("SELECT SUM(signed_amount) FROM journal_lines;")
        total_time = time.perf_counter() - t0
        print(f"\nCOMPLETE in {total_time:.2f}s | Global Ledger Balance Drift: {drift}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
EOF
