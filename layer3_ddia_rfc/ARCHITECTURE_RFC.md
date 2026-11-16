# RFC-001: Hybrid ACID Double-Entry Ledger & Distributed Iceberg Lakehouse Engine

**Status:** Implemented & Benchmarked  
**Author:** Brayan Sepulveda  
**Target Scale:** 10,000,000+ Double-Entry Journal Lines (Pareto 80/20 Clearing Account Skew)

---

## 1. Context & Architectural Problem
Financial ledger systems face two conflicting workload requirements:
1. **OLTP Write Path:** High-concurrency transactional ingestion requiring strict double-entry zero-sum invariants (SUM(signed_amount) = 0.0000), deterministic NUMERIC(18,4) arithmetic, idempotent retry handling, and serializable balance protection against concurrent overdraft races.
2. **OLAP Analytical Path:** Multi-million-row historical aggregations, regulatory time-travel audits, and schema evolution without locking operational writers.

Single-engine architectures fail under load: row-oriented OLTP storage stalls on multi-million-row analytical scans (402.78s initial warehouse build), while columnar lakehouses lack row-level pessimistic locking for sub-second concurrent account mutations.

---

## 2. Three-Layer System Architecture

### Layer 1: Partitioned PostgreSQL 16 OLTP Core & dbt Core Star Schema
* **Storage Layout:** Declarative monthly range partitioning (`PARTITION BY RANGE (posted_at)`) across `journal_entries` and `journal_lines` to keep active B-Tree indexes pinned inside `shared_buffers` RAM.
* **Covering Indexes (`Heap Fetches: 0`):** Composite B-Tree indexes `(account_id, posted_at) INCLUDE (signed_amount)` paired with Visibility Map maintenance (`VACUUM ANALYZE`) aggregate **387,633 rows** for a single skewed whale account in **58.244 ms** with zero heap page lookups.
* **Bulk Ingestion:** Asynchronous Python (`asyncpg`) binary `COPY` streaming into unlogged `TEMP` tables followed by set-based `INSERT ... ON CONFLICT DO NOTHING`, sustaining **105,000+ lines/sec** (peaking at **135,402 lines/sec**).
* **Analytical Warehouse & Audit Gate:** `dbt Core` incremental Kimball Star Schema (`dim_accounts`, `fact_journal_lines`) gated by a custom singular SQL audit test (`assert_zero_sum_entries.sql`) that fails CI/CD if any journal entry deviates from `0.0000`.

### Layer 2: Distributed Compute (PySpark 3.5.5) & Apache Iceberg 1.8.1 Lakehouse
* **Catalog & Storage:** Apache Iceberg REST Catalog (`http://localhost:8181`) managing immutable snapshot trees (`metadata.json` -> manifest lists -> manifest files -> Parquet) with hidden partitioning (`months(posted_at)`).
* **Parallel JDBC Extraction:** 8-way partitioned JDBC extraction (`fetchsize=50000`) ingests all **10,000,000 rows** from PostgreSQL into compressed Iceberg Parquet files in **11.62s**.
* **Pareto Skew Mitigation (FinOps):** Because 5 clearing accounts hold 80% of all volume, naive `SortMergeJoin` suffers straggler reducer bottlenecks (`2.73s`). Combining **BroadcastHashJoin** for `dim_accounts` with **16-way key salting** (`(rand() * 16).cast("int")`) reduces execution time to **1.43s (-47.5% latency)**.
* **Governance & Lifecycle:** Zero-copy schema evolution via immutable field IDs (`ALTER TABLE ... ADD COLUMNS`), point-in-time audit queries via snapshot isolation (`snapshot-id`), and cost-aware bin-pack compaction (`rewrite_data_files`).

### Layer 3: Concurrency & Isolation Invariants (DDIA Chapter 7)
* **Failure Mode:** Under default `READ COMMITTED` snapshot isolation without row locking, 15 concurrent `$200` withdrawals against a `$1,000` balance all read the initial `$1,000` balance simultaneously, approving `15/15` withdrawals and corrupting the ledger to **$-2,000.0000** (Write Skew / Lost Update).
* **Production Guarantee:** Acquiring an explicit row-level mutex (`SELECT account_id FROM accounts WHERE account_id = $1 FOR UPDATE`) on the parent `accounts` row serializes mutations per account without phantom-insert races or table-wide lock contention, approving exactly **5/15** withdrawals and preserving **$0.0000** final balance.
