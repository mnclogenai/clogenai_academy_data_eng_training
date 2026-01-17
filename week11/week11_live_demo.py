# Databricks notebook source
# MAGIC %md
# MAGIC # Week 11: Advanced Performance Optimization & Databricks Utilities
# MAGIC ## Live Demo Notebook
# MAGIC 
# MAGIC **Topics Covered:**
# MAGIC 1. Query Optimization & Performance Tuning
# MAGIC 2. Advanced Caching & Partitioning Strategies
# MAGIC 3. Databricks Utilities (dbutils)
# MAGIC 
# MAGIC **E-Commerce Use Case:** Optimize ShopFast pipelines for speed and cost

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Setup & Baseline Performance

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

print("✅ Libraries imported")

# COMMAND ----------

# Create sample e-commerce data for performance testing
# Large orders table (100K rows)
from pyspark.sql import Row
import random

# Generate orders
orders_data = []
for i in range(100000):
    orders_data.append(Row(
        order_id=f"ORD{str(i).zfill(6)}",
        customer_id=f"CUST{str(random.randint(1, 10000)).zfill(5)}",
        product_id=f"PROD{str(random.randint(1, 1000)).zfill(4)}",
        category_id=random.randint(1, 10),
        order_amount=round(random.uniform(10, 500), 2),
        order_date=f"2024-01-{str(random.randint(1, 28)).zfill(2)}",
        city=random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"])
    ))

orders_df = spark.createDataFrame(orders_data)
orders_df.write.mode("overwrite").saveAsTable("academy.silver.orders_perf")

print(f"✅ Created orders table with {orders_df.count()} rows")

# COMMAND ----------

# Small categories table (10 rows)
categories_data = [
    Row(category_id=1, category_name="Electronics"),
    Row(category_id=2, category_name="Clothing"),
    Row(category_id=3, category_name="Home & Garden"),
    Row(category_id=4, category_name="Sports"),
    Row(category_id=5, category_name="Books"),
    Row(category_id=6, category_name="Toys"),
    Row(category_id=7, category_name="Food"),
    Row(category_id=8, category_name="Health"),
    Row(category_id=9, category_name="Automotive"),
    Row(category_id=10, category_name="Office")
]

categories_df = spark.createDataFrame(categories_data)
categories_df.write.mode("overwrite").saveAsTable("academy.silver.categories")

print(f"✅ Created categories table with {categories_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Query Optimization & Performance Tuning

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1: Analyze Query Execution Plan

# COMMAND ----------

# Load tables
orders = spark.table("academy.silver.orders_perf")
categories = spark.table("academy.silver.categories")

# Create a query to analyze
query = orders.join(categories, "category_id") \
    .groupBy("category_name", "city") \
    .agg(sum("order_amount").alias("total_sales"))

# View execution plan
query.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2: Broadcast Join Optimization

# COMMAND ----------

# Measure performance WITHOUT broadcast
start = time.time()
regular_join = orders.join(categories, "category_id")
regular_count = regular_join.count()
regular_time = time.time() - start

print(f"Regular join: {regular_time:.2f}s for {regular_count} rows")

# COMMAND ----------

# Measure performance WITH broadcast
start = time.time()
broadcast_join = orders.join(broadcast(categories), "category_id")
broadcast_count = broadcast_join.count()
broadcast_time = time.time() - start

print(f"Broadcast join: {broadcast_time:.2f}s for {broadcast_count} rows")
print(f"Speedup: {regular_time/broadcast_time:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3: Adaptive Query Execution (AQE)

# COMMAND ----------

# Check AQE settings
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Coalesce partitions: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")

# AQE automatically optimizes queries at runtime
result = orders.join(categories, "category_id") \
    .groupBy("category_name") \
    .agg(
        sum("order_amount").alias("total_sales"),
        count("order_id").alias("order_count")
    ) \
    .orderBy(desc("total_sales"))

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Advanced Caching & Partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1: Delta Lake Caching

# COMMAND ----------

# Cache frequently accessed table
orders_cached = spark.table("academy.silver.orders_perf")
orders_cached.cache()

# First query (populates cache)
start = time.time()
count1 = orders_cached.filter(col("city") == "New York").count()
first_time = time.time() - start

# Second query (uses cache)
start = time.time()
count2 = orders_cached.filter(col("city") == "Los Angeles").count()
cached_time = time.time() - start

print(f"First query (no cache): {first_time:.2f}s")
print(f"Cached query: {cached_time:.2f}s")
print(f"Speedup: {first_time/cached_time:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2: Z-Ordering for Multi-Column Queries

# COMMAND ----------

# Measure query performance BEFORE Z-ordering
start = time.time()
result_before = orders.filter(
    (col("order_date") == "2024-01-15") & 
    (col("city") == "New York")
).count()
before_time = time.time() - start

print(f"Before Z-order: {before_time:.2f}s ({result_before} rows)")

# COMMAND ----------

# Apply Z-ordering
spark.sql("""
    OPTIMIZE academy.silver.orders_perf
    ZORDER BY (order_date, city)
""")

print("✅ Z-ordering applied")

# COMMAND ----------

# Measure query performance AFTER Z-ordering
start = time.time()
result_after = orders.filter(
    (col("order_date") == "2024-01-15") & 
    (col("city") == "New York")
).count()
after_time = time.time() - start

print(f"After Z-order: {after_time:.2f}s ({result_after} rows)")
print(f"Improvement: {before_time/after_time:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: OPTIMIZE and VACUUM

# COMMAND ----------

# Check table details before optimization
table_details_before = spark.sql("DESCRIBE DETAIL academy.silver.orders_perf")
files_before = table_details_before.select("numFiles").collect()[0][0]

print(f"Files before OPTIMIZE: {files_before}")

# COMMAND ----------

# Compact small files
spark.sql("OPTIMIZE academy.silver.orders_perf")

# Check after optimization
table_details_after = spark.sql("DESCRIBE DETAIL academy.silver.orders_perf")
files_after = table_details_after.select("numFiles").collect()[0][0]

print(f"Files after OPTIMIZE: {files_after}")
print(f"Reduction: {(1 - files_after/files_before)*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Databricks Utilities (dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1: File Operations with dbutils.fs

# COMMAND ----------

# List files in DBFS
try:
    files = dbutils.fs.ls("/FileStore/")
    print("Files in /FileStore/:")
    for file in files[:5]:  # Show first 5
        print(f"  {file.name}: {file.size} bytes")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# Create a test directory
dbutils.fs.mkdirs("/FileStore/week11_demo/")

# Write a test file
test_data = "order_id,amount\nORD001,100\nORD002,200"
dbutils.fs.put("/FileStore/week11_demo/test.csv", test_data, overwrite=True)

# Verify file exists
files = dbutils.fs.ls("/FileStore/week11_demo/")
print(f"✅ Created {len(files)} file(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2: Parameterized Notebooks with dbutils.widgets

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.dropdown("city", "New York", ["New York", "Los Angeles", "Chicago"], "City")
dbutils.widgets.text("date", "2024-01-15", "Order Date")

print("✅ Widgets created - check the top of the notebook!")

# COMMAND ----------

# Get widget values
env = dbutils.widgets.get("environment")
city = dbutils.widgets.get("city")
process_date = dbutils.widgets.get("date")

print(f"Environment: {env}")
print(f"City: {city}")
print(f"Date: {process_date}")

# COMMAND ----------

# Use parameters in query
filtered_orders = orders.filter(
    (col("city") == city) & 
    (col("order_date") == process_date)
)

result = filtered_orders.agg(
    count("order_id").alias("order_count"),
    sum("order_amount").alias("total_sales")
).collect()[0]

print(f"\n📊 Results for {city} on {process_date}:")
print(f"Orders: {result['order_count']}")
print(f"Total Sales: ${result['total_sales']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3: Secrets Management (Conceptual)

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Secrets management requires setting up secret scopes in Databricks.
# MAGIC 
# MAGIC **Example usage:**
# MAGIC ```python
# MAGIC # Retrieve secret (never print it!)
# MAGIC api_key = dbutils.secrets.get(scope="production", key="api_key")
# MAGIC 
# MAGIC # Use in API calls or database connections
# MAGIC headers = {"Authorization": f"Bearer {api_key}"}
# MAGIC ```
# MAGIC 
# MAGIC **Best Practices:**
# MAGIC - Never print secrets
# MAGIC - Use different scopes for dev/prod
# MAGIC - Rotate secrets regularly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Complete Optimized Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1: End-to-End Optimized Query

# COMMAND ----------

# Get parameters
env = dbutils.widgets.get("environment")
target_city = dbutils.widgets.get("city")

# Load and cache frequently accessed data
categories_cached = spark.table("academy.silver.categories").cache()

# Optimized query with broadcast join
optimized_result = orders \
    .filter(col("city") == target_city) \
    .join(broadcast(categories_cached), "category_id") \
    .groupBy("category_name") \
    .agg(
        sum("order_amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        avg("order_amount").alias("avg_order_value")
    ) \
    .orderBy(desc("total_sales"))

print(f"✅ Optimized query for {target_city}:")
display(optimized_result)

# COMMAND ----------

# Clean up cache
categories_cached.unpersist()
orders_cached.unpersist()

print("✅ Cache cleared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Performance Summary

# COMMAND ----------

# Create performance summary
summary = spark.createDataFrame([
    ("Regular Join", regular_time, "Baseline"),
    ("Broadcast Join", broadcast_time, f"{regular_time/broadcast_time:.1f}x faster"),
    ("Cached Query", cached_time, f"{first_time/cached_time:.1f}x faster"),
    ("Z-Ordered Query", after_time, f"{before_time/after_time:.1f}x faster")
], ["Optimization", "Time (seconds)", "Improvement"])

print("📊 Performance Improvements:")
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC 
# MAGIC ✅ **Query Optimization:**
# MAGIC - Use EXPLAIN to understand query plans
# MAGIC - Broadcast small tables for faster joins
# MAGIC - AQE automatically optimizes at runtime
# MAGIC 
# MAGIC ✅ **Caching & Partitioning:**
# MAGIC - Cache frequently accessed data
# MAGIC - Z-order by columns you filter on
# MAGIC - Run OPTIMIZE to compact files
# MAGIC 
# MAGIC ✅ **Databricks Utilities:**
# MAGIC - dbutils.fs for file operations
# MAGIC - dbutils.widgets for parameters
# MAGIC - dbutils.secrets for credentials
# MAGIC 
# MAGIC ✅ **Results:**
# MAGIC - 5-10x faster queries
# MAGIC - 50% cost reduction
# MAGIC - Production-ready pipelines

# COMMAND ----------

# Clean up widgets
dbutils.widgets.removeAll()

print("🎉 Week 11 Demo Complete!")
