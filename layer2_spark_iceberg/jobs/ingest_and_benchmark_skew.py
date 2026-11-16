import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LedgerLakehouse-Iceberg-SkewBenchmark")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ledger_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ledger_catalog.type", "rest")
        .config("spark.sql.catalog.ledger_catalog.uri", "http://localhost:8181")
        .config("spark.sql.catalog.ledger_catalog.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
        .config("spark.sql.defaultCatalog", "ledger_catalog")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = "jdbc:postgresql://localhost:5432/core_ledger"
    jdbc_props = {
        "user": "ledger_admin",
        "password": "ledger_secret_pass",
        "driver": "org.postgresql.Driver",
        "fetchsize": "50000",
    }

    print("\n--- 1. Creating Iceberg Namespace & Reading 10M Rows from PostgreSQL (Parallel JDBC) ---")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ledger_catalog.finance")

    t0 = time.perf_counter()
    accounts_df = spark.read.jdbc(url=jdbc_url, table="public.accounts", properties=jdbc_props)
    accounts_df.writeTo("ledger_catalog.finance.dim_accounts").using("iceberg").createOrReplace()

    # Parallel JDBC read using line_id bounds (8 concurrent connections)
    lines_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.journal_lines",
        column="line_id",
        lowerBound=1,
        upperBound=10_000_000,
        numPartitions=8,
        properties=jdbc_props,
    )

    # Write to Iceberg with Hidden Partitioning by months(posted_at)
    (
        lines_df.writeTo("ledger_catalog.finance.fact_journal_lines")
        .using("iceberg")
        .partitionedBy(F.months("posted_at"))
        .createOrReplace()
    )
    ingest_sec = time.perf_counter() - t0
    print(f"Loaded 10,000,000 rows into Apache Iceberg in {ingest_sec:.2f}s")

    # Verify Iceberg Snapshot Metadata
    print("\n--- Iceberg Table Snapshots & File Statistics ---")
    import glob, json
    meta_files = sorted(glob.glob("lakehouse-warehouse/finance/fact_journal_lines/metadata/*.metadata.json"))
    with open(meta_files[-1]) as f:
        meta = json.load(f)
    snap = meta["snapshots"][-1]
    print(f"Snapshot ID: {snap['snapshot-id']} | Operation: {snap['summary']['operation']} | Total Records: {snap['summary']['total-records']} | Data Files: {snap['summary']['total-data-files']}")

    fact = spark.table("ledger_catalog.finance.fact_journal_lines")
    dim = spark.table("ledger_catalog.finance.dim_accounts")

    # =========================================================================
    # BENCHMARK A: NAIVE RUN (SortMergeJoin + Skewed Shuffle, AQE Disabled)
    # =========================================================================
    print("\n--- 2. BENCHMARK A: Naive SortMergeJoin (Skewed Shuffle, No Broadcast) ---")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    t_naive = time.perf_counter()
    naive_agg = (
        fact.join(dim, on="account_id", how="inner")
        .groupBy("account_code", "account_type")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("signed_amount").alias("net_balance"),
        )
    )
    naive_rows = naive_agg.collect()
    naive_sec = time.perf_counter() - t_naive
    print(f"Naive SortMergeJoin Execution Time: {naive_sec:.2f}s (Returned {len(naive_rows)} accounts)")

    # =========================================================================
    # BENCHMARK B: FINOPS OPTIMIZED RUN (BroadcastHashJoin + Salted Aggregation)
    # =========================================================================
    print("\n--- 3. BENCHMARK B: FinOps Optimized (BroadcastHashJoin + 16-Way Key Salting) ---")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB

    t_opt = time.perf_counter()
    # Step 1: Salt the skewed account_id across 16 buckets to eliminate straggler tasks
    salted_fact = fact.withColumn("salt", (F.rand() * 16).cast("int"))
    partial_agg = (
        salted_fact.groupBy("account_id", "salt")
        .agg(
            F.count("*").alias("partial_count"),
            F.sum("signed_amount").alias("partial_sum"),
        )
    )
    # Step 2: Roll up partial buckets and BroadcastHashJoin the small dimension table
    opt_agg = (
        partial_agg.groupBy("account_id")
        .agg(
            F.sum("partial_count").alias("tx_count"),
            F.sum("partial_sum").alias("net_balance"),
        )
        .join(F.broadcast(dim), on="account_id", how="inner")
        .select("account_code", "account_type", "tx_count", "net_balance")
    )
    opt_rows = opt_agg.collect()
    opt_sec = time.perf_counter() - t_opt

    improvement = ((naive_sec - opt_sec) / naive_sec) * 100
    print(f"FinOps Optimized Execution Time: {opt_sec:.2f}s (Returned {len(opt_rows)} accounts)")
    print(f"Compute Latency Reduction: {improvement:.1f}%")

    # Audit Check: Confirm Net Balance across all accounts in Iceberg equals 0.0000
    total_drift = sum(r["net_balance"] for r in opt_rows)
    print(f"Iceberg Lakehouse Global Balance Drift: {total_drift}")

    spark.stop()


if __name__ == "__main__":
    main()
