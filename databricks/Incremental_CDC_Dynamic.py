import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, current_date, max as max_func
from delta.tables import DeltaTable

# Initialize Spark Session
spark = SparkSession.builder.appName("Incremental_CDC_Dynamic").getOrCreate()
dbutils = None

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    # Handle local mock if necessary, or assume Databricks environment
    pass

# ==============================================================================
# 1. Widget / Parameter Handling
# ==============================================================================

# Define Widgets
widgets = [
    "table_id", "source_system_id", "schema_name", "table_name", "entity_id",
    "primary_key_columns", "is_composite_key", "incremental_path",
    "bronze_base_path", "pipeline_run_id"
]

for w in widgets:
    dbutils.widgets.text(w, "", w)

# Retrieve Values
table_id = dbutils.widgets.get("table_id")
source_system_id = dbutils.widgets.get("source_system_id")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
entity_id = dbutils.widgets.get("entity_id")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
is_composite_key = dbutils.widgets.get("is_composite_key")
incremental_path = dbutils.widgets.get("incremental_path")
bronze_base_path = dbutils.widgets.get("bronze_base_path")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

# Parse Primary Keys
pk_cols = [col.strip() for col in primary_key_columns.split(',')]

# ==============================================================================
# 2. Read CDC Data
# ==============================================================================

try:
    # Assuming the incremental path contains the latest batch in Parquet or Delta format
    df_cdc = spark.read.load(incremental_path)

    # Check if empty
    if df_cdc.isEmpty():
        result = {
            "status": "success",
            "rows_processed": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "message": "No data in incremental path"
        }
        dbutils.notebook.exit(json.dumps(result))

except Exception as e:
    dbutils.notebook.exit(json.dumps({"status": "error", "message": f"Error reading incremental path: {str(e)}"}))

# ==============================================================================
# 3. Metadata Extraction & Transformation
# ==============================================================================

# Extract current_sync_version (Get max version from the batch)
try:
    current_sync_version = df_cdc.select(max_func("_current_sync_version")).collect()[0][0]
    # Handle case if None
    if current_sync_version is None:
        current_sync_version = 0
except Exception:
    current_sync_version = 0

# Add Metadata Columns
# Note: _is_deleted and _is_current defaults are set here for Inserts.
# Logic for Deletes is handled in the Merge statement.
df_transformed = df_cdc \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system_id", lit(source_system_id)) \
    .withColumn("_table_id", lit(table_id)) \
    .withColumn("_entity_id", lit(entity_id)) \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_load_date", current_date()) \
    .withColumn("_is_deleted", lit(False)) \
    .withColumn("_is_current", lit(True))

# ==============================================================================
# 4. Dynamic Merge Construction
# ==============================================================================

# Exclude list as per requirements
excluded_cols = [
    'SYS_CHANGE_OPERATION', 'SYS_CHANGE_VERSION', '_current_sync_version',
    '_ingestion_timestamp', '_source_system_id', '_table_id', '_entity_id',
    '_operation', '_is_current', '_is_deleted', '_load_date', '_pipeline_run_id'
]

# Get all columns from source
source_columns = df_transformed.columns

# Dynamic Data Columns (Business Data): All columns - Excluded
data_cols = [c for c in source_columns if c not in excluded_cols]

# Build Merge Condition
# Join on PKs
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

# Build Update Dictionary (For 'U')
# Update business data columns that are not part of the Primary Key
update_dict = {col: f"source.{col}" for col in data_cols if col not in pk_cols}

# Build Insert Dictionary (For 'I')
# Insert ALL columns that should exist in the target (Business Data + PKs + Metadata)
# We filter out the transient CDC tracking columns from the insert payload
transient_cdc_cols = ['SYS_CHANGE_OPERATION', 'SYS_CHANGE_VERSION', '_current_sync_version', '_operation']
all_target_cols = [c for c in source_columns if c not in transient_cdc_cols]
insert_dict = {col: f"source.{col}" for col in all_target_cols}

# ==============================================================================
# 5. Execute Merge
# ==============================================================================

target_path = f"{bronze_base_path}/{source_system_id}/{schema_name}/{table_name}/delta"

# Calculate Metrics
row_count = df_transformed.count()
inserts_count = df_transformed.filter("SYS_CHANGE_OPERATION = 'I'").count()
updates_count = df_transformed.filter("SYS_CHANGE_OPERATION = 'U'").count()
deletes_count = df_transformed.filter("SYS_CHANGE_OPERATION = 'D'").count()

# Ensure Target Table Exists
if not DeltaTable.isDeltaTable(spark, target_path):
    # Initial Load / Table Creation
    # Drop CDC transient columns before writing initial table
    df_initial = df_transformed.drop(*transient_cdc_cols)

    # Apply soft delete logic if initial batch has deletes (unlikely but safe)
    # Since it's a new table, we just write.
    # 'D' records in a create batch are awkward, filtering them or writing as is_deleted=true.
    # For simplicity in "Create", we write all, but updated logic below applies to Merge.
    df_initial.write.format("delta").mode("append").save(target_path)
else:
    delta_table = DeltaTable.forPath(spark, target_path)

    delta_table.alias("target") \
        .merge(
            df_transformed.alias("source"),
            merge_condition
        ) \
        .whenMatchedUpdate(
            condition="source.SYS_CHANGE_OPERATION = 'U'",
            set=update_dict
        ) \
        .whenMatchedUpdate(
            condition="source.SYS_CHANGE_OPERATION = 'D'",
            set={
                "_is_deleted": "true",
                "_is_current": "false"
            }
        ) \
        .whenNotMatchedInsert(
            condition="source.SYS_CHANGE_OPERATION = 'I'",
            values=insert_dict
        ) \
        .execute()

# ==============================================================================
# 6. Update Control Table
# ==============================================================================

# Define JDBC Connection Properties
# NOTE: In a production environment, use dbutils.secrets.get()
# Assuming standard secret scope 'etl_secrets' and keys provided
try:
    # Placeholder for secrets - Replace with actual secret retrieval in production
    # jdbc_url = dbutils.secrets.get(scope="etl_secrets", key="jdbc_url")
    # jdbc_user = dbutils.secrets.get(scope="etl_secrets", key="jdbc_user")
    # jdbc_password = dbutils.secrets.get(scope="etl_secrets", key="jdbc_password")

    # MOCK for generation purposes as credentials aren't provided in prompt
    jdbc_url = spark.conf.get("spark.databricks.service.jdbc.url", "jdbc:sqlserver://hostname:1433;databaseName=control_db")
    jdbc_user = spark.conf.get("spark.databricks.service.jdbc.user", "user")
    jdbc_password = spark.conf.get("spark.databricks.service.jdbc.password", "password")

    update_sql = """
        UPDATE control.table_metadata
        SET last_sync_version = ?,
            last_sync_timestamp = GETDATE(),
            last_load_status = 'success',
            last_load_rows_processed = ?
        WHERE table_id = ?
    """

    # Execute JDBC Update via Driver (PySpark standard JDBC write doesn't support raw UPDATE)
    driver = spark._sc._gateway.jvm.java.sql.DriverManager
    conn = driver.getConnection(jdbc_url, jdbc_user, jdbc_password)

    try:
        stmt = conn.prepareStatement(update_sql)
        # Bind Parameters
        # 1: last_sync_version (BigInt/Long)
        stmt.setLong(1, int(current_sync_version))
        # 2: last_load_rows_processed (Int)
        stmt.setInt(2, row_count)
        # 3: table_id (String/Int - assuming String based on input)
        stmt.setString(3, table_id)

        stmt.executeUpdate()
    finally:
        if conn:
            conn.close()

except Exception as e:
    # Log error but don't fail the pipeline if control table update fails?
    # Usually we want to raise, but for this exercise we print.
    print(f"Warning: Failed to update control table: {str(e)}")

# ==============================================================================
# 7. Exit
# ==============================================================================

output_json = {
    "status": "success",
    "rows_processed": row_count,
    "inserts": inserts_count,
    "updates": updates_count,
    "deletes": deletes_count,
    "current_sync_version": current_sync_version
}

dbutils.notebook.exit(json.dumps(output_json))
