import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal
import asyncpg
#
DB_DSN = "postgresql://ledger_admin:ledger_secret_pass@localhost:5432/core_ledger"


async def setup_test_account(conn: asyncpg.Connection, initial_balance: Decimal) -> tuple[uuid.UUID, uuid.UUID]:
    """Creates a dedicated test asset account and funding equity account with exact initial_balance."""
    cash_id = uuid.uuid4()
    equity_id = uuid.uuid4()
    ts = datetime.now(timezone.utc)

    await conn.execute(
        "INSERT INTO accounts (account_id, account_code, account_name, account_type, currency) VALUES ($1, $2, $3, 'ASSET', 'USD')",
        cash_id, f"TEST-CASH-{str(cash_id)[:8]}", "Concurrency Test Cash"
    )
    await conn.execute(
        "INSERT INTO accounts (account_id, account_code, account_name, account_type, currency) VALUES ($1, $2, $3, 'EQUITY', 'USD')",
        equity_id, f"TEST-EQ-{str(equity_id)[:8]}", "Concurrency Test Equity"
    )

    entry_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO journal_entries (entry_id, idempotency_key, transaction_reference, entry_status, posted_at) VALUES ($1, $2, $3, 'POSTED', $4)",
        entry_id, f"init-{entry_id}", "INIT-FUNDING", ts
    )
    await conn.execute(
        "INSERT INTO journal_lines (entry_id, account_id, direction, amount, posted_at) VALUES ($1, $2, 'DR', $3, $4), ($1, $5, 'CR', $3, $4)",
        entry_id, cash_id, initial_balance, ts, equity_id
    )
    return cash_id, equity_id


async def unsafe_withdraw(pool: asyncpg.Pool, cash_id: uuid.UUID, equity_id: uuid.UUID, amount: Decimal, idx: int) -> bool:
    """ANTI-PATTERN (DDIA Ch. 7 Write Skew / Lost Update): Checks balance under READ COMMITTED without FOR UPDATE."""
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="read_committed"):
            bal = await conn.fetchval("SELECT COALESCE(SUM(signed_amount), 0) FROM journal_lines WHERE account_id = $1", cash_id)
            await asyncio.sleep(0.02)  # Simulate network/business logic latency window
            if bal >= amount:
                entry_id = uuid.uuid4()
                ts = datetime.now(timezone.utc)
                await conn.execute(
                    "INSERT INTO journal_entries (entry_id, idempotency_key, transaction_reference, entry_status, posted_at) VALUES ($1, $2, $3, 'POSTED', $4)",
                    entry_id, f"unsafe-{entry_id}-{idx}", f"UNSAFE-WD-{idx}", ts
                )
                await conn.execute(
                    "INSERT INTO journal_lines (entry_id, account_id, direction, amount, posted_at) VALUES ($1, $2, 'CR', $3, $4), ($1, $5, 'DR', $3, $4)",
                    entry_id, cash_id, amount, ts, equity_id
                )
                return True
            return False


async def safe_pessimistic_withdraw(pool: asyncpg.Pool, cash_id: uuid.UUID, equity_id: uuid.UUID, amount: Decimal, idx: int) -> bool:
    """PRODUCTION FIX: Acquires row-level FOR UPDATE lock on the parent account row before reading ledger balance."""
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="read_committed"):
            # Serialize concurrent mutations on this specific account_id without locking the whole table
            await conn.fetchrow("SELECT account_id FROM accounts WHERE account_id = $1 FOR UPDATE", cash_id)
            bal = await conn.fetchval("SELECT COALESCE(SUM(signed_amount), 0) FROM journal_lines WHERE account_id = $1", cash_id)
            await asyncio.sleep(0.02)
            if bal >= amount:
                entry_id = uuid.uuid4()
                ts = datetime.now(timezone.utc)
                await conn.execute(
                    "INSERT INTO journal_entries (entry_id, idempotency_key, transaction_reference, entry_status, posted_at) VALUES ($1, $2, $3, 'POSTED', $4)",
                    entry_id, f"safe-{entry_id}-{idx}", f"SAFE-WD-{idx}", ts
                )
                await conn.execute(
                    "INSERT INTO journal_lines (entry_id, account_id, direction, amount, posted_at) VALUES ($1, $2, 'CR', $3, $4), ($1, $5, 'DR', $3, $4)",
                    entry_id, cash_id, amount, ts, equity_id
                )
                return True
            return False


async def main() -> None:
    pool = await asyncpg.create_pool(DB_DSN, min_size=10, max_size=25)
    try:
        initial_balance = Decimal("1000.0000")
        withdrawal_amount = Decimal("200.0000")  # Max 5 withdrawals allowed; we fire 15 simultaneously!

        # 1. UNSAFE RACE CONDITION TEST
        async with pool.acquire() as conn:
            u_cash, u_eq = await setup_test_account(conn, initial_balance)
        results_unsafe = await asyncio.gather(*[unsafe_withdraw(pool, u_cash, u_eq, withdrawal_amount, i) for i in range(15)])
        async with pool.acquire() as conn:
            final_unsafe = await conn.fetchval("SELECT SUM(signed_amount) FROM journal_lines WHERE account_id = $1", u_cash)
        print(f"\n[TEST A: Naive READ COMMITTED] Approved: {sum(results_unsafe)}/15 | Final Balance: ${final_unsafe} (OVERDRAFT CORRUPTION!)")

        # 2. SAFE PESSIMISTIC LOCKING TEST (SELECT ... FOR UPDATE)
        async with pool.acquire() as conn:
            s_cash, s_eq = await setup_test_account(conn, initial_balance)
        results_safe = await asyncio.gather(*[safe_pessimistic_withdraw(pool, s_cash, s_eq, withdrawal_amount, i) for i in range(15)])
        async with pool.acquire() as conn:
            final_safe = await conn.fetchval("SELECT SUM(signed_amount) FROM journal_lines WHERE account_id = $1", s_cash)
        print(f"[TEST B: Pessimistic FOR UPDATE] Approved: {sum(results_safe)}/15 | Final Balance: ${final_safe} (INVARIANT PROTECTED!)")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
