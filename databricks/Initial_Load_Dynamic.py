# File: Initial_Load_Dynamic.py
import json
import logging
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Initial_Load_Dynamic")

# 1. Define Widgets
dbutils.widgets.text("table_id", "", "Table ID")
dbutils.widgets.text("source_system_id", "", "Source System ID")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("entity_id", "", "Entity ID")
dbutils.widgets.text("primary_key_columns", "", "Primary Key Columns")
dbutils.widgets.text("is_composite_key", "0", "Is Composite Key")
dbutils.widgets.text("source_path", "", "Source Path (Parquet)")
dbutils.widgets.text("bronze_base_path", "", "Bronze Base Path")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

# 2. Get Widget Values
table_id = dbutils.widgets.get("table_id")
source_system_id = dbutils.widgets.get("source_system_id")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
entity_id = dbutils.widgets.get("entity_id")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
is_composite_key = dbutils.widgets.get("is_composite_key")
source_path = dbutils.widgets.get("source_path")
bronze_base_path = dbutils.widgets.get("bronze_base_path")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

# Construct Target Paths and Names
target_path = f"{bronze_base_path}/{source_system_id}/{schema_name}/{table_name}/delta"
full_table_name = f"bronze.{source_system_id}_{schema_name}_{table_name}"

def update_control_table(status, rows=0, sync_version=None, error_msg=None):
    """Updates the control.table_metadata table using Spark SQL."""
    try:
        escaped_error = error_msg.replace("'", "''") if error_msg else ""

        # Determine SQL values
        sql_sync_version = str(sync_version) if sync_version is not None else "NULL"
        sql_rows = str(rows)

        if status == 'success':
            query = f"""
                UPDATE control.table_metadata
                SET
                    initial_load_completed = 1,
                    last_sync_version = {sql_sync_version},
                    last_sync_timestamp = current_timestamp(),
                    last_load_status = 'success',
                    last_load_rows_processed = {sql_rows},
                    updated_by = 'pipeline',
                    updated_at = current_timestamp()
                WHERE table_id = '{table_id}'
            """
        else:
            query = f"""
                UPDATE control.table_metadata
                SET
                    last_load_status = 'failed',
                    updated_by = 'pipeline',
                    updated_at = current_timestamp()
                    -- Note: Error logging usually goes to a separate log table or column if schema permits,
                    -- assuming standard update here based on requirements.
                WHERE table_id = '{table_id}'
            """

        spark.sql(query)
        logger.info(f"Control table updated for table_id {table_id} with status {status}")

    except Exception as e:
        logger.error(f"Failed to update control table: {str(e)}")
        # Do not raise here, we want to return the main failure details

try:
    logger.info(f"Starting Initial Load for {full_table_name} from {source_path}")

    # 3. Read Source Data
    # Schema inference is automatic with parquet
    df_source = spark.read.format("parquet").load(source_path)

    # 4. Extract Max Sync Version and Row Count
    # We calculate this before transformations to ensure accuracy against source state
    try:
        row_count = df_source.count()
        if row_count > 0:
            # Assuming _sync_version is numeric (BIGINT/LONG)
            max_sync_version_row = df_source.agg(F.max(F.col("_sync_version"))).collect()[0]
            max_sync_version = max_sync_version_row[0]
        else:
            max_sync_version = 0
            logger.warning("Source file is empty.")
    except AnalysisException:
        # Fallback if _sync_version is missing in source, though critical requirement
        logger.warning("_sync_version column not found in source. Defaulting to 0.")
        max_sync_version = 0

    # 5. Transformations
    # Drop temp columns if they exist in source to avoid ambiguity
    cols_to_drop = [c for c in ["_source_system_id", "_table_id"] if c in df_source.columns]
    df_transformed = df_source.drop(*cols_to_drop)

    # Add Standard Metadata Columns
    df_final = df_transformed \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_system_id", F.lit(source_system_id)) \
        .withColumn("_table_id", F.lit(table_id)) \
        .withColumn("_entity_id", F.lit(entity_id)) \
        .withColumn("_operation", F.lit("I")) \
        .withColumn("_is_current", F.lit(True)) \
        .withColumn("_is_deleted", F.lit(False)) \
        .withColumn("_load_date", F.current_date()) \
        .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))

    # 6. Write to Delta (Overwrite for Initial Load)
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)

    logger.info(f"Data written to {target_path}")

    # 7. Create/Register Table in Metastore
    # Using IF NOT EXISTS to handle idempotency, though overwrite on path handles data
    create_ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING DELTA
        LOCATION '{target_path}'
    """
    spark.sql(create_ddl)

    # 8. Success: Update Control Table
    update_control_table('success', row_count, max_sync_version)

    # 9. Exit with Success JSON
    result = {
        "status": "success",
        "rows_processed": row_count,
        "sync_version": max_sync_version,
        "target_table": full_table_name
    }
    dbutils.notebook.exit(json.dumps(result))

except Exception as e:
    error_message = str(e)
    logger.error(f"Initial Load Failed: {error_message}")

    # Update Control Table with Failure
    update_control_table('failed', error_msg=error_message)

    # Exit with Failure JSON
    result = {
        "status": "failed",
        "error": error_message,
        "table_id": table_id
    }
    dbutils.notebook.exit(json.dumps(result))
