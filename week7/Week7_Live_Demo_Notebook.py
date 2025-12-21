# Databricks notebook source
# MAGIC %md
# MAGIC # Week 7: CDC, Idempotency & Incremental Loads
# MAGIC ## Live Demo Notebook
# MAGIC 
# MAGIC **Purpose:** Pre-built code snippets for live session demonstrations
# MAGIC 
# MAGIC **Instructor:** Run cells sequentially during live session
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Libraries

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, max, row_number, expr
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Configuration
# MAGIC 
# MAGIC **Important:** Update these values for your environment

# COMMAND ----------

# Unity Catalog Configuration
# TODO: Update these values to match your Databricks environment
CATALOG = "academy"  # Change to your catalog name
SCHEMA = "labs"  # Change to your schema name

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"✅ Using catalog: {CATALOG}, schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: CDC Concept Demo
# MAGIC 
# MAGIC **Concept:** Track only what changed instead of copying everything

# COMMAND ----------

# Day 1: Initial orders (Black Friday)
orders_day1 = spark.createDataFrame([
    (1, "Alice", "pending", 150.00, "2024-11-24 10:00:00"),
    (2, "Bob", "pending", 200.00, "2024-11-24 11:00:00"),
    (3, "Carol", "pending", 75.00, "2024-11-24 12:00:00")
], ["order_id", "customer", "status", "amount", "updated_at"])

# Write to Bronze table
orders_day1.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.orders_bronze")

print("✅ Day 1: Created 3 initial orders")
display(spark.table(f"{CATALOG}.{SCHEMA}.orders_bronze"))

# COMMAND ----------

# Day 2: Changes only! (Updates + New orders)
orders_day2 = spark.createDataFrame([
    (1, "Alice", "shipped", 150.00, "2024-11-25 14:00:00"),    # UPDATE: status changed
    (2, "Bob", "cancelled", 200.00, "2024-11-25 15:00:00"),   # UPDATE: cancelled
    (4, "David", "pending", 300.00, "2024-11-25 16:00:00")    # INSERT: new order
], ["order_id", "customer", "status", "amount", "updated_at"])

# Append changes to Bronze table
orders_day2.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.orders_bronze")

print("✅ Day 2: Added 3 change records (2 updates + 1 new)")
display(spark.table(f"{CATALOG}.{SCHEMA}.orders_bronze").orderBy("updated_at"))

# COMMAND ----------

# Query: Show only Day 2 changes
changes_day2 = spark.table(f"{CATALOG}.{SCHEMA}.orders_bronze") \
    .filter(col("updated_at") >= "2024-11-25 00:00:00")

print(f"📊 Day 2 Changes: {changes_day2.count()} records")
display(changes_day2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Watermarks & Incremental Loads
# MAGIC 
# MAGIC **Concept:** Track pipeline progress with watermarks

# COMMAND ----------

# Create pipeline state table
pipeline_state = spark.createDataFrame([
    ("order_pipeline", "2024-11-24 00:00:00", "2024-11-24 23:59:59", 3, "SUCCESS")
], ["pipeline_name", "start_time", "last_watermark", "records_processed", "status"])

pipeline_state.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.pipeline_state")

print("✅ Created pipeline state table")
display(spark.table(f"{CATALOG}.{SCHEMA}.pipeline_state"))

# COMMAND ----------

# Read watermark and process only NEW data
current_watermark = spark.table(f"{CATALOG}.{SCHEMA}.pipeline_state") \
    .filter(col("pipeline_name") == "order_pipeline") \
    .select("last_watermark").first()[0]

print(f"📖 Current watermark: {current_watermark}")

# Read only data AFTER watermark
new_orders = spark.table(f"{CATALOG}.{SCHEMA}.orders_bronze") \
    .filter(col("updated_at") > current_watermark)

print(f"✅ Found {new_orders.count()} new records to process")
display(new_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Late Arrivals with Lookback Window

# COMMAND ----------

# Simulate late arrival: Order from Day 1 arrives on Day 3
late_arrival = spark.createDataFrame([
    (1, "Alice", "delivered", 150.00, "2024-11-26 10:00:00")  # Final status update
], ["order_id", "customer", "status", "amount", "updated_at"])

late_arrival.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.orders_bronze")

print("✅ Added late arrival (Day 1 order updated on Day 3)")

# COMMAND ----------

# Use lookback window to catch late arrivals
lookback_hours = 48  # 2 days

# Calculate lookback time
from pyspark.sql.functions import to_timestamp, lit
lookback_time = expr(f"timestamp'{current_watermark}' - INTERVAL {lookback_hours} HOURS")

print(f"🔙 Lookback window starts at: {lookback_time}")

# Read data with lookback
incremental_data = spark.table(f"{CATALOG}.{SCHEMA}.orders_bronze") \
    .filter(col("updated_at") > lookback_time)

print(f"📊 With lookback: {incremental_data.count()} records (includes late arrivals)")
display(incremental_data.orderBy("updated_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Idempotent MERGE
# MAGIC 
# MAGIC **Concept:** Safe to re-run pipelines without duplicates

# COMMAND ----------

# Create Silver table (current state)
silver_initial = spark.createDataFrame([
    (1, "Alice", "pending", 150.00, "2024-11-24 10:00:00"),
    (2, "Bob", "pending", 200.00, "2024-11-24 11:00:00"),
    (3, "Carol", "pending", 75.00, "2024-11-24 12:00:00")
], ["order_id", "customer", "status", "amount", "updated_at"])

silver_initial.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.orders_silver")

print("✅ Created Silver table with initial state")
display(spark.table(f"{CATALOG}.{SCHEMA}.orders_silver"))

# COMMAND ----------

# Incoming changes (with DUPLICATES!)
incoming_changes = spark.createDataFrame([
    (1, "Alice", "shipped", 150.00, "2024-11-25 14:00:00"),
    (1, "Alice", "shipped", 150.00, "2024-11-25 14:00:00"),  # DUPLICATE!
    (2, "Bob", "cancelled", 200.00, "2024-11-25 15:00:00"),
    (4, "David", "pending", 300.00, "2024-11-25 16:00:00"),
    (4, "David", "processing", 300.00, "2024-11-25 17:00:00")  # Later update
], ["order_id", "customer", "status", "amount", "updated_at"])

print(f"📥 Incoming: {incoming_changes.count()} records (includes duplicates)")
display(incoming_changes.orderBy("order_id", "updated_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Deduplicate Source Data

# COMMAND ----------

# Keep only LATEST record per order_id
window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

deduplicated = incoming_changes.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print(f"🧹 After deduplication: {deduplicated.count()} records")
print(f"Reduced from {incoming_changes.count()} to {deduplicated.count()} records")
display(deduplicated.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Perform MERGE (Upsert)

# COMMAND ----------

# Load target Delta table
target_table = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.orders_silver")

# Perform MERGE
merge_result = target_table.alias("target").merge(
    deduplicated.alias("source"),
    "target.order_id = source.order_id"  # Match on business key
).whenMatchedUpdate(
    condition = "source.updated_at > target.updated_at",  # Only update if newer
    set = {
        "customer": "source.customer",
        "status": "source.status",
        "amount": "source.amount",
        "updated_at": "source.updated_at"
    }
).whenNotMatchedInsert(
    values = {
        "order_id": "source.order_id",
        "customer": "source.customer",
        "status": "source.status",
        "amount": "source.amount",
        "updated_at": "source.updated_at"
    }
).execute()

print("✅ MERGE completed successfully!")
print("\n📊 Final Silver table state:")
display(spark.table(f"{CATALOG}.{SCHEMA}.orders_silver").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Test Idempotency (Re-run MERGE)

# COMMAND ----------

# Capture state before re-run
before_rerun = spark.table(f"{CATALOG}.{SCHEMA}.orders_silver").orderBy("order_id").collect()
before_count = len(before_rerun)

print(f"📊 Before re-run: {before_count} records")

# Re-run the EXACT SAME MERGE
target_table = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.orders_silver")
target_table.alias("target").merge(
    deduplicated.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdate(
    condition = "source.updated_at > target.updated_at",
    set = {
        "customer": "source.customer",
        "status": "source.status",
        "amount": "source.amount",
        "updated_at": "source.updated_at"
    }
).whenNotMatchedInsert(
    values = {
        "order_id": "source.order_id",
        "customer": "source.customer",
        "status": "source.status",
        "amount": "source.amount",
        "updated_at": "source.updated_at"
    }
).execute()

# Capture state after re-run
after_rerun = spark.table(f"{CATALOG}.{SCHEMA}.orders_silver").orderBy("order_id").collect()
after_count = len(after_rerun)

print(f"📊 After re-run: {after_count} records")

# Compare
if before_rerun == after_rerun:
    print("\n🎉 SUCCESS! Pipeline is IDEMPOTENT")
    print("✅ Re-running produced identical results!")
else:
    print("\n❌ FAILURE! Results changed on re-run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Complete End-to-End Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Pipeline with Error Handling

# COMMAND ----------

def run_incremental_pipeline():
    """
    Complete incremental CDC pipeline with watermark tracking
    """
    try:
        # Step 1: Read watermark
        watermark_df = spark.table(f"{CATALOG}.{SCHEMA}.pipeline_state") \
            .filter(col("pipeline_name") == "order_pipeline")
        
        current_watermark = watermark_df.select("last_watermark").first()[0]
        print(f"📖 Current watermark: {current_watermark}")
        
        # Step 2: Read incremental data (with 24-hour lookback)
        lookback_time = expr(f"timestamp'{current_watermark}' - INTERVAL 24 HOURS")
        
        incremental = spark.table(f"{CATALOG}.{SCHEMA}.orders_bronze") \
            .filter(col("updated_at") > lookback_time)
        
        record_count = incremental.count()
        print(f"📊 Found {record_count} records to process")
        
        if record_count == 0:
            print("ℹ️ No new data to process")
            return
        
        # Step 3: Deduplicate
        window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
        deduplicated = incremental.withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        dedup_count = deduplicated.count()
        print(f"🧹 After deduplication: {dedup_count} records")
        
        # Step 4: MERGE into Silver
        target = DeltaTable.forName(
            spark,
            f"{CATALOG}.{SCHEMA}.orders_silver"
        )
        target.alias("t").merge(
            deduplicated.alias("s"),
            "t.order_id = s.order_id"
        ).whenMatchedUpdateAll(
            condition="s.updated_at > t.updated_at"
        ).whenNotMatchedInsertAll().execute()
        
        print("✅ MERGE completed successfully")
        
        # Step 5: Update watermark (ONLY after success!)
        new_watermark = incremental.agg(max("updated_at")).first()[0]
        
        updated_state = spark.createDataFrame([
            ("order_pipeline", current_watermark, new_watermark, dedup_count, "SUCCESS")
        ], ["pipeline_name", "start_time", "last_watermark", "records_processed", "status"])
        
        updated_state.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.pipeline_state")
        
        print(f"✅ Updated watermark to: {new_watermark}")
        print(f"🎉 Pipeline complete! Processed {dedup_count} records")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        # Do NOT update watermark on failure
        raise

# Run the pipeline
run_incremental_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation & Metrics

# COMMAND ----------

# Show final state
print("📊 Final Silver Table State:")
display(spark.table(f"{CATALOG}.{SCHEMA}.orders_silver").orderBy("order_id"))

print("\n📈 Pipeline State:")
display(spark.table(f"{CATALOG}.{SCHEMA}.pipeline_state"))

# COMMAND ----------

# Calculate metrics
silver_df = spark.table(f"{CATALOG}.{SCHEMA}.orders_silver")

print("📊 Summary Metrics:")
print(f"Total orders: {silver_df.count()}")
print(f"Pending orders: {silver_df.filter(col('status') == 'pending').count()}")
print(f"Shipped orders: {silver_df.filter(col('status') == 'shipped').count()}")
print(f"Cancelled orders: {silver_df.filter(col('status') == 'cancelled').count()}")
print(f"Total order value: ${silver_df.agg(F.sum('amount')).first()[0]:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to clean up demo tables
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.orders_bronze")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.orders_silver")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.pipeline_state")
# print("✅ Cleanup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **End of Live Demo Notebook**
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC - Review technical documentation
# MAGIC - Complete hands-on exercises
# MAGIC - Build ShopFast assignment
