"""
Airflow DAG: Cymbal Retail Nightly Inventory Reconciliation
===========================================================
Orchestrates Dataproc Serverless (Lightning Engine 2.3) batch job to reconcile
store inventory positions across 20 flagship stores and 20 SKUs, followed by
data quality verification on the BigQuery Managed Iceberg table.

Schedule: Nightly @ 10:00 PM UTC (0 22 * * *)
Target Gold Table: <PROJECT_ID>.cymbal_gold.gold_inventory_reconciliation_ledger
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# Environment defaults with Airflow Variable overrides
PROJECT_ID = os.environ.get("GCP_PROJECT") or Variable.get("project_id", default_var="praxis-magnet-508004-d7")
REGION = Variable.get("gcp_region", default_var="us-central1")
CATALOG = f"{PROJECT_ID}.cymbal-lakehouse.elevate_data"
TARGET_TABLE = f"{PROJECT_ID}.cymbal_gold.gold_inventory_reconciliation_ledger"
GCS_STAGING_BUCKET = f"{PROJECT_ID}-module1-bucket"
SCRIPT_URI = f"gs://{GCS_STAGING_BUCKET}/code/migrated_inventory_reconciliation_pipeline.py"
SERVICE_ACCOUNT = f"cymbal-sa-data@{PROJECT_ID}.iam.gserviceaccount.com"
SUBNET_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/cymbal-retail-subnet-{REGION}"

default_args = {
    "owner": "cymbal-data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cymbal_nightly_inventory_reconciliation",
    default_args=default_args,
    description="Nightly Dataproc Serverless Lightning Engine Inventory Reconciliation & Quality Check",
    schedule_interval="0 22 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cymbal", "dataproc", "serverless", "lightning-engine", "iceberg", "inventory"],
) as dag:

    # 1. Execute Winning Dataproc Serverless Lightning Engine Batch Job
    run_inventory_reconciliation_lightning = DataprocCreateBatchOperator(
        task_id="run_inventory_reconciliation_lightning",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="recon-nightly-{{ ds_nodash }}-{{ ts_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SCRIPT_URI,
                "args": [
                    f"--catalog={CATALOG}",
                    f"--target-table={TARGET_TABLE}",
                    f"--gcs-staging-bucket={GCS_STAGING_BUCKET}",
                    "--start-date=2025-01-01",
                    "--until-date=2026-09-10",
                    "--write-mode=overwrite",
                ],
            },
            "runtime_config": {
                "version": "2.3",
                "properties": {
                    "dataproc.tier": "premium",
                    "spark.dataproc.engine": "lightningEngine",
                    "spark.dataproc.lineage.enabled": "true",
                },
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "subnetwork_uri": SUBNET_URI,
                }
            },
        },
    )

    # 2. Assert Exact Data Quality & Row Parity in BigQuery Gold Ledger (COUNT = 8,400)
    validate_gold_table_parity = BigQueryCheckOperator(
        task_id="validate_gold_table_parity",
        sql=f"""
            SELECT COUNT(*) = 8400
            FROM `{TARGET_TABLE}`
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    run_inventory_reconciliation_lightning >> validate_gold_table_parity
