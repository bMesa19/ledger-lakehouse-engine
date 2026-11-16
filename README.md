# Ledger Lakehouse Engine (`ledger-lakehouse-engine`)

A production-grade, high-throughput **Double-Entry Financial Ledger & Distributed Lakehouse Engine** engineered to process **10,000,000+ double-entry journal lines** under extreme Pareto data skew (80% volume concentrated in 0.1% clearing accounts) while enforcing strict ACID accounting invariants.

## Architecture Overview

    +-----------------------------------------------------------------------------------+
    | LAYER 1: OLTP CORE & ANALYTICAL WAREHOUSE (PostgreSQL 16 + asyncpg + dbt Core)    |
    |  - Declarative Monthly Range Partitioning + Covering Indexes (Heap Fetches: 0)    |
    |  - Binary COPY Ingestion via Unlogged Temp Staging (135,402 lines/sec peak)       |
    |  - Kimball Star Schema (dim_accounts, fact_journal_lines) + Zero-Sum Audit Gate   |
    +-----------------------------------------+-----------------------------------------+
                                              | Parallel JDBC (8 partitions, fetchsize=50k)
                                              v
    +-----------------------------------------------------------------------------------+
    | LAYER 2: DISTRIBUTED LAKEHOUSE (PySpark 3.5.5 + Apache Iceberg 1.8.1 REST)        |
    |  - Hidden Partitioning: months(posted_at) | 10M Rows Ingested in 11.62s           |
    |  - FinOps Skew Optimization: BroadcastHashJoin + 16-Way Salting (-47.5% Latency)  |
    |  - Zero-Copy Schema Evolution, Time-Travel Snapshots & Bin-Pack Compaction        |
    +-----------------------------------------+-----------------------------------------+
                                              | Linearizable Row-Level Mutexes
                                              v
    +-----------------------------------------------------------------------------------+
    | LAYER 3: CONCURRENCY & ARCHITECTURE RFC (DDIA Chapter 7 Isolation Proofs)         |
    |  - Empirical Proof: Naive READ COMMITTED Overdraft (-$2,000) vs FOR UPDATE ($0)   |
    |  - Principal Architecture Decision Record (layer3_ddia_rfc/ARCHITECTURE_RFC.md)   |
    +-----------------------------------------------------------------------------------+

## Verified Production Benchmarks (10,000,000 Rows)

| Subsystem / Benchmark | Metric / Result | Engineering Mechanism |
| :--- | :--- | :--- |
| **PostgreSQL Binary `COPY` Ingestion** | **135,402 lines/sec** (peak) | `asyncpg` binary protocol + unlogged `TEMP` staging + `ON CONFLICT DO NOTHING` |
| **Whale Account Balance Query (387k rows)** | **58.24 ms (`Heap Fetches: 0`)** | Monthly partition pruning + `(account_id, posted_at) INCLUDE (signed_amount)` |
| **dbt Core Zero-Sum Audit Gate** | **0.0000 drift across 5M entries** | Custom singular SQL assertion (`assert_zero_sum_entries.sql`) |
| **Parallel JDBC -> Apache Iceberg Load** | **11.62 seconds (10M rows)** | 8 concurrent JDBC range partitions + Iceberg REST Catalog (`localhost:8181`) |
| **Skewed Aggregation (`80/20` Whale Skew)** | **2.73s -> 1.43s (-47.5%)** | `BroadcastHashJoin` + 16-way random salt bucket pre-aggregation |
| **Concurrency Race Test (15x $200 on $1k)** | **5/15 approved (`$0.0000` bal)** | Row-level `SELECT ... FOR UPDATE` mutex eliminating DDIA Ch. 7 Write Skew |

## Repository Structure

* `docker/docker-compose.yml` — PostgreSQL 16 (`shm_size: 1gb`, `wal_level=logical`) & Apache Iceberg REST Catalog.
* `layer1_oltp_warehouse/` — Partitioned DDL, `asyncpg` binary ingestion generator, covering index `EXPLAIN` proofs, and `dbt_ledger` project.
* `layer2_spark_iceberg/` — PySpark 3.5 + Iceberg 1.8 skew benchmark (`ingest_and_benchmark_skew.py`) and lifecycle operations (`iceberg_lifecycle_ops.py`).
* `layer3_ddia_rfc/` — Principal Architecture RFC (`ARCHITECTURE_RFC.md`) and `asyncio` concurrency stress-test (`test_ddia_isolation.py`).
