#!/usr/bin/env python3
"""
Cymbal Retail - Cross-Cloud Spark Modernization Pipeline
=========================================================
Refactored from Databricks PySpark notebook for Google Cloud Dataproc Serverless (Runtime 2.3).
Preserves the 7-stage vectorized reconciliation logic, conformed 12-column schema contract,
and commits directly to BigQuery Managed Iceberg table with Dataplex lineage enabled.
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, IntegerType, DoubleType
)

# Schema Constants - Conformed 12-Column Contract
LEDGER_COLUMNS = [
    "business_date", "store_id", "store_name", "city", "item_id", "unit_price_usd",
    "opening_qty", "shelf_qty", "backroom_qty", "intraday_gross_revenue_usd",
    "est_cover_hours_remaining", "reconciliation_status",
]

STATUS_CRITICAL = "CRITICAL BURN SPIKE - STOCKOUT RISK"
STATUS_MONITOR  = "MONITOR VELOCITY"
STATUS_NORMAL   = "RECONCILED NORMAL HEALTH"

SAFETY_STOCK_UNITS   = 3
PEAK_HOUR            = 17
CITY_CANDIDATES      = ["city", "store_city", "locality", "town", "municipality"]
CITY_FALLBACKS       = ["country", "region", "global_region", "market"]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)-7s] [%(name)-24s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("CymbalDataprocReconciliation")


class ContractError(RuntimeError):
    """Raised when ledger schema drifts from contract or row count does not match position grid."""


class DataprocReconciliationPipeline:
    """
    Cymbal Retail Multi-Day Inventory Reconciliation Engine
    ========================================================
    - Reads POS transactions, inventory baseline, and store dimensions from BigQuery / Lakehouse catalog.
    - Evaluates 5-minute intraday depletion curves shaped around 5:00 PM peak rush.
    - Commits conformed 12-column ledger to BigQuery Managed Iceberg table.
    - Exact 1-to-1 parity with Databricks workload with zero proprietary API dependencies.
    """
    def __init__(
        self,
        spark_session: SparkSession,
        catalog: str,
        target_table: str,
        gcs_staging_bucket: str,
        start_date: str = "2025-01-01",
        until_date: str = "2026-09-10",
        horizon_days: int = 1,
        slot_minutes: int = 5,
        critical_cover_hours: float = 6.0,
        monitor_cover_hours: float = 12.0,
        expr_passes: int = 1,
        write_mode: str = "overwrite",
        dry_run: bool = False
    ):
        self.spark = spark_session
        self.catalog = catalog.rstrip("/")
        self.target_table = target_table
        self.gcs_staging_bucket = gcs_staging_bucket.rstrip("/")
        self.start_date = start_date
        self.until_date = until_date
        self.horizon_days = horizon_days
        self.slot_minutes = slot_minutes
        self.critical_cover_hours = critical_cover_hours
        self.monitor_cover_hours = monitor_cover_hours
        self.expr_passes = expr_passes
        self.write_mode = write_mode
        self.dry_run = dry_run

        if 60 % slot_minutes:
            raise ValueError(f"slot_minutes must divide 60, got {slot_minutes}")
        self.slots_per_hour = 60 // slot_minutes
        self.slots_per_day  = 24 * self.slots_per_hour
        self.total_slots    = horizon_days * self.slots_per_day
        self.hours_per_slot = slot_minutes / 60.0
        self.n_positions: Optional[int] = None

    @staticmethod
    def _pick(df: DataFrame, names: List[str]) -> Optional[str]:
        low = {c.lower(): c for c in df.columns}
        return next((low[n] for n in names if n in low), None)

    # 1. READ SOURCES FROM LAKEHOUSE CATALOG / BIGQUERY
    def read_sources(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        log.info("[1/7 READ] Ingesting date range '%s' to '%s' from Catalog: %s",
                 self.start_date, self.until_date, self.catalog)
        pos_where = ""
        if self.start_date and self.until_date:
            pos_where = f"business_date >= '{self.start_date}' AND business_date <= '{self.until_date}'"
        elif self.until_date and self.until_date.lower() != "all":
            pos_where = f"business_date <= '{self.until_date}'"

        def rd(table_name: str, where_clause: str = "") -> DataFrame:
            parts = self.catalog.split(".")
            sql_target = f"`{'`.`'.join(parts)}`.`{table_name}`"

            # 1. Direct BigQuery connector load
            candidates = [
                f"{self.catalog}.{table_name}",
            ]
            if len(parts) == 3:
                # project:dataset.table or project.catalog.namespace
                candidates.append(f"{parts[0]}:{parts[1]}.{parts[2]}.{table_name}")

            for target in candidates:
                try:
                    df = self.spark.read.format("bigquery").load(target)
                    if where_clause:
                        df = df.where(where_clause)
                    log.info("  Loaded table '%s' via BigQuery connector: `%s`", table_name, target)
                    return df
                except Exception:
                    pass

            # 2. spark.table API
            try:
                df = self.spark.table(f"{self.catalog}.{table_name}")
                if where_clause:
                    df = df.where(where_clause)
                log.info("  Loaded table '%s' via spark.table: `%s`", table_name, f"{self.catalog}.{table_name}")
                return df
            except Exception:
                pass

            # 3. BigQuery SQL query fallback
            try:
                query = f"SELECT * FROM {sql_target}"
                if where_clause:
                    query += f" WHERE {where_clause}"
                log.info("  Loading table '%s' via BigQuery SQL query: %s", table_name, query)
                df = (self.spark.read.format("bigquery")
                      .option("query", query)
                      .option("viewsEnabled", "true")
                      .option("materializationDataset", "cymbal_silver")
                      .load())
                log.info("  Loaded table '%s' via BigQuery query option.", table_name)
                return df
            except Exception as e_query:
                log.error("Failed to load table '%s' using all BigQuery methods: %s", table_name, e_query)
                raise e_query

        bronze = rd("bronze_pos_stream_events", pos_where)
        inv    = rd("silver_store_inventory")
        nodes  = rd("store_nodes")
        return bronze, inv, nodes

    # 2. SILVER POS TRANSFORMATION
    def silver_pos(self, bronze: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
        log.info("[2/7 SILVER POS] Processing POS transactions for range: %s to %s", self.start_date, self.until_date)
        have = {c.lower() for c in bronze.columns}
        order = [F.col(c).desc_nulls_last() for c in ("_ingested_at", "_msk_message_id") if c in have] or [F.lit(1)]

        # Handle items JSON string if not already parsed as array
        if "items" in bronze.columns and str(bronze.schema["items"].dataType).lower().startswith("string"):
            item_schema = ArrayType(StructType([
                StructField("line_seq", IntegerType(), True),
                StructField("item_id", StringType(), True),
                StructField("item_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("unit_price_usd", DoubleType(), True),
                StructField("total_item_price", DoubleType(), True),
                StructField("item_discount", DoubleType(), True),
                StructField("item_net_amount", DoubleType(), True)
            ]))
            bronze = bronze.withColumn("items", F.from_json(F.col("items"), item_schema))

        txn = (bronze
               .withColumn("_rk", F.row_number().over(Window.partitionBy("transaction_id").orderBy(*order)))
               .filter(F.col("_rk") == 1)
               .withColumn("event_ts", F.coalesce(F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"), F.to_timestamp("event_timestamp")))
               .withColumn("business_dt", F.coalesce(F.to_date("business_date", "yyyy-MM-dd"), F.to_date(F.col("event_ts"))))
               .filter(F.col("transaction_id").isNotNull() & F.col("store_id").isNotNull()))

        if self.start_date:
            txn = txn.filter(F.col("business_dt") >= F.to_date(F.lit(self.start_date)))
        if self.until_date and self.until_date.lower() != "all":
            txn = txn.filter(F.col("business_dt") <= F.to_date(F.lit(self.until_date)))

        dates_df = txn.select(F.col("business_dt").alias("business_date")).distinct()

        daily_store_revenue = txn.groupBy("business_dt", "store_id").agg(
            F.round(F.sum(F.coalesce(F.col("total_amount_usd").cast("double"), F.lit(0.0))), 2).alias("intraday_gross_revenue_usd")
        ).withColumnRenamed("business_dt", "business_date")

        lines = (txn.select("business_dt", "store_id", F.explode_outer("items").alias("ln"))
                    .select(F.col("business_dt").alias("business_date"),
                            F.col("store_id"),
                            F.col("ln.item_id").alias("item_id"),
                            F.col("ln.quantity").cast("double").alias("qty"))
                    .filter(F.col("item_id").isNotNull()))

        daily_sku_units = (lines.groupBy("business_date", "store_id", "item_id")
                                .agg(F.greatest(F.sum("qty"), F.lit(0.0)).alias("daily_units")))

        return dates_df, daily_store_revenue, daily_sku_units

    # 3. POSITION GRID
    def positions(self, inv: DataFrame, nodes: DataFrame,
                  dates_df: DataFrame, daily_store_revenue: DataFrame,
                  daily_sku_units: DataFrame) -> DataFrame:
        log.info("[3/7 POSITIONS] Building Grid: (dates x store_id x item_id)")
        city_col = self._pick(nodes, CITY_CANDIDATES) or self._pick(nodes, CITY_FALLBACKS)
        city_expr = F.col(city_col) if city_col else F.lit(None)
        dim = (nodes.select(F.col("store_id"),
                            F.col("store_name").cast("string").alias("store_name"),
                            city_expr.cast("string").alias("city"))
                    .dropDuplicates(["store_id"]))

        # Deduplicate base inventory to 1 baseline snapshot per store and item
        inv_baseline = inv.dropDuplicates(["store_id", "item_id"])

        base_inv = (inv_baseline.drop("store_name", "city")
                       .join(F.broadcast(dim), "store_id", "inner")
                       .withColumn("on_hand", (F.coalesce(F.col("shelf_qty"), F.lit(0)) + F.coalesce(F.col("backroom_qty"), F.lit(0))).cast("double")))

        grid = dates_df.crossJoin(F.broadcast(base_inv))

        pos = (grid.join(daily_store_revenue, ["business_date", "store_id"], "left")
                   .join(daily_sku_units, ["business_date", "store_id", "item_id"], "left")
                   .withColumn("intraday_gross_revenue_usd", F.coalesce(F.col("intraday_gross_revenue_usd"), F.lit(0.0)).cast("double"))
                   .withColumn("daily_units", F.coalesce(F.col("daily_units"), F.lit(0.0)).cast("double"))
                   .withColumn("pos_key", F.concat_ws("~", F.col("business_date").cast("string"), F.col("store_id"), F.col("item_id"))))

        self.n_positions = pos.count()
        log.info("  Grid: %s positions across evaluated horizon.", f"{self.n_positions:,}")
        return pos

    # 4. TIME FABRIC EXPANSION
    def fabric(self, pos: DataFrame) -> DataFrame:
        n = self.n_positions * self.total_slots
        log.info("[4/7 FABRIC] %s positions x %s slots = %s calculation rows",
                 f"{self.n_positions:,}", f"{self.total_slots:,}", f"{n:,}")
        slots = self.spark.range(0, self.total_slots).withColumnRenamed("id", "slot")
        return (pos.crossJoin(F.broadcast(slots))
                   .withColumn("slot", F.col("slot").cast("int"))
                   .withColumn("hour", F.pmod((F.col("slot") / F.lit(self.slots_per_hour)).cast("int"), F.lit(24))))

    # 5. DEMAND SHAPING & AUDIT HASH
    def project(self, fab: DataFrame) -> DataFrame:
        log.info("[5/7 PROJECT] Vectorized 5:00 PM Peak Demand Shaping & Cryptographic Auditing")
        phase = (F.col("hour") - F.lit(float(PEAK_HOUR))) * F.lit(3.141592653589793 / 12.0)
        shape = (F.pow(F.cos(phase), F.lit(4.0)) * F.lit(1.85) + F.lit(0.15)) / F.lit(0.84375)

        df = fab.withColumn("slot_demand", F.col("daily_units") * shape / F.lit(float(self.slots_per_day)))
        chain = F.concat_ws("|", F.col("pos_key"), F.col("slot").cast("string"))
        for i in range(self.expr_passes):
            chain = F.sha2(F.concat_ws("#", chain, F.lit(f"s{i}")), 256)

        return df.withColumn("audit_hash", chain)

    # 6. CUMULATIVE DEPLETION WINDOW
    def window(self, df: DataFrame) -> DataFrame:
        log.info("[6/7 WINDOW] Cumulative Depletion Window (Safety Stock <= %d units)", SAFETY_STOCK_UNITS)
        w_pos = (Window.partitionBy("pos_key").orderBy("slot")
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        return (df.withColumn("burned", F.sum("slot_demand").over(w_pos))
                  .withColumn("projected_on_hand", F.greatest(F.lit(0.0), F.col("on_hand") - F.col("burned")))
                  .withColumn("is_out", (F.col("projected_on_hand") <= F.lit(float(SAFETY_STOCK_UNITS))).cast("int")))

    # 7. DAILY GRAIN SYNTHESIS
    def collapse(self, df: DataFrame) -> DataFrame:
        log.info("[7/7 COLLAPSE] Synthesizing Daily Reconciliation Ledger")
        carry = ["business_date", "store_id", "store_name", "city", "item_id", "unit_price_usd",
                 "opening_qty", "shelf_qty", "backroom_qty", "intraday_gross_revenue_usd",
                 "on_hand", "daily_units"]

        return (df.groupBy("pos_key")
                  .agg(*[F.first(c, ignorenulls=True).alias(c) for c in carry],
                       F.min(F.when(F.col("is_out") == 1, F.col("slot"))).alias("out_slot"))
                  .withColumn("business_date", F.to_date(F.col("business_date")))
                  .withColumn("hourly_burn", F.col("daily_units") / F.lit(24.0))
                  .withColumn("est_cover_hours_remaining",
                              F.round(F.coalesce(
                                  F.col("out_slot") * F.lit(self.hours_per_slot),
                                  F.when(F.col("hourly_burn") > 0, F.col("on_hand") / F.col("hourly_burn"))), 1).cast("double"))
                  .withColumn("reconciliation_status",
                              F.when(F.col("est_cover_hours_remaining") <= F.lit(self.critical_cover_hours), F.lit(STATUS_CRITICAL))
                               .when(F.col("est_cover_hours_remaining") <= F.lit(self.monitor_cover_hours), F.lit(STATUS_MONITOR))
                               .otherwise(F.lit(STATUS_NORMAL)))
                  .withColumn("opening_qty", F.col("opening_qty").cast("int"))
                  .withColumn("shelf_qty", F.col("shelf_qty").cast("int"))
                  .withColumn("backroom_qty", F.col("backroom_qty").cast("int"))
                  .withColumn("unit_price_usd", F.col("unit_price_usd").cast("double"))
                  .withColumn("intraday_gross_revenue_usd", F.col("intraday_gross_revenue_usd").cast("double"))
                  .select(*LEDGER_COLUMNS)
                  .orderBy(F.col("business_date").desc(), F.col("est_cover_hours_remaining").asc_nulls_last()))

    # REPORTING & CONTRACT VALIDATION
    def report(self, led: DataFrame) -> int:
        n = led.count()
        if list(led.columns) != LEDGER_COLUMNS:
            raise ContractError(f"Schema drift detected: expected {LEDGER_COLUMNS}, got {list(led.columns)}")
        if self.n_positions is not None and n != self.n_positions:
            raise ContractError(f"Row drift detected: {n:,} rows out vs {self.n_positions:,} positions in")
        log.info("✅ Contract Verified: 12 conformed columns, %s positions in -> %s rows out.", f"{self.n_positions:,}", f"{n:,}")

        print("\n" + "=" * 128)
        print(f"FULL HISTORICAL INVENTORY RECONCILIATION LEDGER ($ USD) [{self.start_date} to {self.until_date}]")
        print(f"Execution Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("=" * 128)

        cols = ["business_date", "store_id", "city", "item_id", "unit_price_usd", "shelf_qty", "est_cover_hours_remaining", "reconciliation_status"]
        led.select(*cols).show(15, truncate=False)

        print("-" * 128)
        print(f"Reconciliation Status Distribution for Full Horizon ({self.start_date} to {self.until_date}):")
        led.groupBy("reconciliation_status").count().orderBy(F.col("count").desc()).show(truncate=False)
        print("=" * 128 + "\n")
        return n

    # WRITE TO BIGQUERY TARGET TABLE
    def write(self, led: DataFrame) -> None:
        if self.dry_run:
            log.info("[DRY-RUN] Dry run enabled. Skipping push to BigQuery target table.")
            return

        log.info("Committing reconciled ledger directly to BigQuery target table: %s (mode=%s)...",
                 self.target_table, self.write_mode)
        gcs_bucket = self.gcs_staging_bucket.replace("gs://", "").strip("/")

        # BigQuery Managed Iceberg tables do not support direct WRITE_TRUNCATE in Spark load jobs.
        # When overwrite mode is requested, truncate via BigQuery DML prior to append.
        actual_mode = self.write_mode
        if self.write_mode.lower() == "overwrite":
            log.info("Target is Managed Iceberg table. Performing truncation via BigQuery DML prior to load...")
            try:
                from google.cloud import bigquery
                client = bigquery.Client()
                tbl = self.target_table.replace(":", ".")
                query = f"DELETE FROM `{tbl}` WHERE TRUE"
                query_job = client.query(query)
                query_job.result()
                log.info("  ✅ Successfully truncated target Iceberg table: %s", self.target_table)
                actual_mode = "append"
            except Exception as te:
                log.warning("  Could not execute truncate DML via BigQuery client: %s. Proceeding...", te)

        try:
            (led.write
                .format("bigquery")
                .option("table", self.target_table)
                .option("temporaryGcsBucket", gcs_bucket)
                .mode(actual_mode)
                .save())
            log.info("  ✅ Committed ledger table to BigQuery Managed Iceberg: %s", self.target_table)
        except Exception as e:
            log.error("❌ Failed write to BigQuery table '%s': %s", self.target_table, e)
            raise e

    # RUN PIPELINE
    def run(self) -> DataFrame:
        t0 = time.time()
        print("=" * 96)
        print(f"🚀 Starting Dataproc Serverless Multi-Day Inventory Reconciliation Pipeline | {self.start_date} -> {self.until_date}")
        print(f"📍 BigQuery Lakehouse Catalog Source : {self.catalog}")
        print(f"☁️ GCS Temporary Staging Bucket      : {self.gcs_staging_bucket}")
        print(f"📋 Target BigQuery Table             : {self.target_table}")
        print(f"⚡ Cover Thresholds                  : Critical <= {self.critical_cover_hours:.1f}h | Monitor <= {self.monitor_cover_hours:.1f}h")
        print("=" * 96)

        bronze, inv, nodes = self.read_sources()
        dates_df, store_revenue, sku_units = self.silver_pos(bronze)
        pos = self.positions(inv, nodes, dates_df, store_revenue, sku_units)
        ledger = self.collapse(self.window(self.project(self.fabric(pos))))

        self.report(ledger)
        self.write(ledger)
        duration = time.time() - t0
        print(f"\n✅ Pipeline completed successfully in {duration:.2f} seconds.")
        return ledger


def parse_args():
    parser = argparse.ArgumentParser(description="Cymbal Retail Dataproc Serverless Inventory Reconciliation")
    parser.add_argument("--catalog", type=str, required=True,
                        help="BigQuery Lakehouse Catalog dataset (e.g. <PROJECT_ID>.cymbal-lakehouse.elevate_data)")
    parser.add_argument("--target-table", type=str, required=True,
                        help="Target BigQuery destination table (e.g. <PROJECT_ID>.cymbal_gold.gold_inventory_reconciliation_ledger)")
    parser.add_argument("--gcs-staging-bucket", type=str, required=True,
                        help="GCS temporary staging bucket (e.g. <PROJECT_ID>-module1-bucket)")
    parser.add_argument("--start-date", type=str, default="2025-01-01",
                        help="Evaluation start date (YYYY-MM-DD)")
    parser.add_argument("--until-date", type=str, default="2026-09-10",
                        help="Evaluation end date (YYYY-MM-DD)")
    parser.add_argument("--write-mode", type=str, default="overwrite",
                        choices=["overwrite", "append", "ignore", "error"],
                        help="BigQuery write mode")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run transformation without committing to BigQuery")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("Cymbal-Inventory-Reconciliation").getOrCreate()

    pipeline = DataprocReconciliationPipeline(
        spark_session=spark,
        catalog=args.catalog,
        target_table=args.target_table,
        gcs_staging_bucket=args.gcs_staging_bucket,
        start_date=args.start_date,
        until_date=args.until_date,
        write_mode=args.write_mode,
        dry_run=args.dry_run
    )
    pipeline.run()


if __name__ == "__main__":
    main()
