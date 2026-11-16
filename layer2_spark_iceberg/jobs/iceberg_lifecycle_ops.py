import glob
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_latest_metadata(table_path: str) -> dict:
    meta_files = sorted(glob.glob(f"{table_path}/metadata/*.metadata.json"))
    with open(meta_files[-1]) as f:
        return json.load(f)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("LedgerLakehouse-Iceberg-LifecycleOps")
        .master("local[*]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ledger_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ledger_catalog.type", "rest")
        .config("spark.sql.catalog.ledger_catalog.uri", "http://localhost:8181")
        .config("spark.sql.catalog.ledger_catalog.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
        .config("spark.sql.defaultCatalog", "ledger_catalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    table_fqn = "ledger_catalog.finance.fact_journal_lines"
    meta_v1 = get_latest_metadata("lakehouse-warehouse/finance/fact_journal_lines")
    snap_v1 = meta_v1["current-snapshot-id"]
    print(f"\n[Snapshot V1] ID: {snap_v1} | Records: {meta_v1['snapshots'][-1]['summary']['total-records']}")

    # 1. ZERO-COPY SCHEMA EVOLUTION: Add audit_batch_tag without rewriting existing Parquet files
    print("\n--- 1. Zero-Copy Schema Evolution (Adding 'audit_batch_tag' column) ---")
    spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMNS (audit_batch_tag STRING)")

    # 2. APPEND NEW SNAPSHOT (V2): Simulating a late-arriving regulatory adjustment entry (DR + CR)
    print("--- 2. Appending Balanced Adjustment Entry (Creating Snapshot V2) ---")
    sample_acc = spark.table("ledger_catalog.finance.dim_accounts").limit(2).collect()
    acc_dr, acc_cr = sample_acc[0]["account_id"], sample_acc[1]["account_id"]

    adj_df = spark.sql(f"""
        SELECT
            CAST(10000001 AS BIGINT) AS line_id,
            uuid() AS entry_id,
            '{acc_dr}' AS account_id,
            'DR' AS direction,
            CAST(50000.0000 AS DECIMAL(18,4)) AS amount,
            CAST(50000.0000 AS DECIMAL(18,4)) AS signed_amount,
            TIMESTAMP '2026-11-15 12:00:00' AS posted_at,
            'REG-ADJ-2026-Q4' AS audit_batch_tag
        UNION ALL
        SELECT
            CAST(10000002 AS BIGINT) AS line_id,
            uuid() AS entry_id,
            '{acc_cr}' AS account_id,
            'CR' AS direction,
            CAST(-50000.0000 AS DECIMAL(18,4)) AS amount,
            CAST(-50000.0000 AS DECIMAL(18,4)) AS signed_amount,
            TIMESTAMP '2026-11-15 12:00:00' AS posted_at,
            'REG-ADJ-2026-Q4' AS audit_batch_tag
    """)
    adj_df.writeTo(table_fqn).append()

    meta_v2 = get_latest_metadata("lakehouse-warehouse/finance/fact_journal_lines")
    snap_v2 = meta_v2["current-snapshot-id"]
    print(f"[Snapshot V2] ID: {snap_v2} | Records: {meta_v2['snapshots'][-1]['summary']['total-records']} | Data Files: {meta_v2['snapshots'][-1]['summary']['total-data-files']}")

    # 3. TIME TRAVEL AUDIT QUERY: Compare Snapshot V1 vs Snapshot V2
    print("\n--- 3. Time Travel Audit Verification ---")
    count_v1 = spark.read.option("snapshot-id", snap_v1).table(table_fqn).count()
    count_v2 = spark.table(table_fqn).count()
    print(f"Time Travel Count @ Snapshot V1 ({snap_v1}): {count_v1:,} rows")
    print(f"Current Count     @ Snapshot V2 ({snap_v2}): {count_v2:,} rows")

    # 4. BIN-PACK COMPACTION: Merge tiny micro-batch files into optimal Parquet files
    print("\n--- 4. Running Iceberg Bin-Pack Compaction (rewrite_data_files) ---")
    spark.sql(f"CALL ledger_catalog.system.rewrite_data_files(table => 'finance.fact_journal_lines')").show(truncate=False)

    meta_v3 = get_latest_metadata("lakehouse-warehouse/finance/fact_journal_lines")
    print(f"[Snapshot V3 Post-Compaction] Data Files: {meta_v3['snapshots'][-1]['summary']['total-data-files']} | Total Records: {meta_v3['snapshots'][-1]['summary']['total-records']}")

    spark.stop()


if __name__ == "__main__":
    main()
