# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Week 2 — Ingest & Explore with PySpark
# MAGIC ### Building Reliable Data Pipelines in Databricks  
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## **Goals for This Session**  
# MAGIC By the end of this session, you will:
# MAGIC 
# MAGIC - Understand how Spark ingests structured & semi-structured data  
# MAGIC - Explore datasets using DataFrames  
# MAGIC - Detect schema issues and data quality problems  
# MAGIC - Compare formats: CSV, Parquet, and Delta  
# MAGIC - Understand Delta features (ACID, schema evolution, time travel)  
# MAGIC - See how this fits into real data pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Setup and Imports**

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## **1. Data Ingestion**
# MAGIC 
# MAGIC ### Common Sources
# MAGIC - Databricks FileStore / DBFS  
# MAGIC - Local file uploads  
# MAGIC - Cloud storage (S3 / ADLS / GCS)  
# MAGIC - Databases (JDBC connectors)

# COMMAND ----------

# Sample data ingestion with schema inference
df_inferred = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/FileStore/orders.csv")

print("Schema with inference:")
df_inferred.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **2. Explicit Schema Definition**
# MAGIC 
# MAGIC ### Why Use Explicit Schema?
# MAGIC ✔ Predictable and reliable  
# MAGIC ✔ Enforces consistent data types  
# MAGIC ✔ Prevents data quality issues from type changes

# COMMAND ----------

# Define explicit schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

# Read with explicit schema
df = spark.read.csv("/FileStore/orders.csv", schema=schema, header=True)

print("Schema with explicit definition:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **3. Data Exploration**
# MAGIC 
# MAGIC ### Basic DataFrame Operations

# COMMAND ----------

# Display the first 5 rows
print("First 5 rows:")
df.show(5)

# COMMAND ----------

# Get summary statistics
print("Summary statistics:")
df.describe().show()

# COMMAND ----------

# Extended summary including quartiles
print("Extended summary:")
df.summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **4. Data Profiling and Quality Checks**

# COMMAND ----------

# Check for null values
print("Null value counts:")
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# Check data types and basic statistics
print("Data types:")
for field in df.schema.fields:
    print(f"{field.name}: {field.dataType}")

# COMMAND ----------

# Check for duplicates
print(f"Total rows: {df.count()}")
print(f"Distinct rows: {df.distinct().count()}")
print(f"Duplicate rows: {df.count() - df.distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **5. Deeper Data Analysis**

# COMMAND ----------

# Group by product_id and calculate total quantity
product_sales = df.groupBy("product_id") \
                  .agg(sum("quantity").alias("total_quantity"),
                       count("*").alias("order_count"),
                       avg("price").alias("avg_price"))

print("Product sales summary:")
product_sales.show()

# COMMAND ----------

# Find top products by quantity
top_products = product_sales.orderBy("total_quantity", ascending=False)
print("Top 10 products by quantity:")
top_products.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **6. Format Conversion: CSV → Parquet**

# COMMAND ----------

# Convert to Parquet
print("Converting to Parquet...")
start_time = time.time()

df.write.mode("overwrite").parquet("/FileStore/orders_parquet")

parquet_time = time.time() - start_time
print(f"Parquet write completed in {parquet_time:.2f} seconds")

# COMMAND ----------

# Read from Parquet and compare performance
print("Reading from Parquet...")
start_time = time.time()

df_parquet = spark.read.parquet("/FileStore/orders_parquet")

parquet_read_time = time.time() - start_time
print(f"Parquet read completed in {parquet_read_time:.2f} seconds")

df_parquet.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **7. Delta Lake Implementation**

# COMMAND ----------

# Convert to Delta format
print("Converting to Delta...")
start_time = time.time()

df.write.format("delta").mode("overwrite").save("/FileStore/orders_delta")

delta_time = time.time() - start_time
print(f"Delta write completed in {delta_time:.2f} seconds")

# COMMAND ----------

# Read from Delta
print("Reading from Delta...")
start_time = time.time()

df_delta = spark.read.format("delta").load("/FileStore/orders_delta")

delta_read_time = time.time() - start_time
print(f"Delta read completed in {delta_read_time:.2f} seconds")

df_delta.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **8. Delta Lake Features**
# MAGIC 
# MAGIC ### Schema Evolution

# COMMAND ----------

# Add a new column for schema evolution
df_with_region = df.withColumn("region", lit("US"))

print("DataFrame with new region column:")
df_with_region.show(5)

# COMMAND ----------

# Write with schema evolution
df_with_region.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save("/FileStore/orders_delta")

print("Schema evolution completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel

# COMMAND ----------

# View Delta table history
delta_table = DeltaTable.forPath(spark, "/FileStore/orders_delta")
print("Delta table history:")
delta_table.history().show()

# COMMAND ----------

# Query previous version (if available)
try:
    df_v0 = spark.read.format("delta") \
                  .option("versionAsOf", 0) \
                  .load("/FileStore/orders_delta")
    
    print("Version 0 data:")
    df_v0.show(5)
    print("Version 0 columns:", df_v0.columns)
except:
    print("Version 0 not available or same as current version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **9. Performance Comparison**

# COMMAND ----------

# Compare file sizes (approximate)
print("Format Comparison:")
print("=" * 50)

# Read and count records from each format
csv_count = df.count()
parquet_count = df_parquet.count()
delta_count = df_delta.count()

print(f"CSV record count: {csv_count}")
print(f"Parquet record count: {parquet_count}")
print(f"Delta record count: {delta_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **10. Data Quality Validation**

# COMMAND ----------

# Check for data quality issues
print("Data Quality Checks:")
print("=" * 30)

# Check for negative quantities
negative_qty = df_delta.filter(col("quantity") < 0).count()
print(f"Records with negative quantity: {negative_qty}")

# Check for zero or negative prices
invalid_prices = df_delta.filter(col("price") <= 0).count()
print(f"Records with invalid prices: {invalid_prices}")

# Check for reasonable quantity ranges
high_qty = df_delta.filter(col("quantity") > 1000).count()
print(f"Records with unusually high quantity (>1000): {high_qty}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **11. Advanced DataFrame Operations**

# COMMAND ----------

# Window functions for ranking
from pyspark.sql.window import Window

# Rank products by total sales within each region
window_spec = Window.partitionBy("region").orderBy(desc("total_quantity"))

product_rankings = df_delta.groupBy("region", "product_id") \
    .agg(sum("quantity").alias("total_quantity")) \
    .withColumn("rank", row_number().over(window_spec))

print("Product rankings by region:")
product_rankings.filter(col("rank") <= 3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **12. Summary and Best Practices**

# COMMAND ----------

print("Week 2 Summary:")
print("=" * 40)
print("✅ Data ingestion with explicit schema")
print("✅ Data exploration and profiling")
print("✅ Format conversion (CSV → Parquet → Delta)")
print("✅ Delta Lake features (schema evolution, time travel)")
print("✅ Data quality validation")
print("✅ Performance comparison")

print("\nBest Practices:")
print("- Use explicit schemas in production")
print("- Profile data for quality issues")
print("- Choose appropriate formats for your use case")
print("- Leverage Delta Lake for reliable pipelines")
print("- Implement data quality checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Format Comparison Table**
# MAGIC 
# MAGIC | Feature | CSV | Parquet | Delta |
# MAGIC |---------|-----|---------|-------|
# MAGIC | Structure | Row-based | Columnar | Columnar with transaction log |
# MAGIC | Compression | Basic | High & efficient | High & efficient |
# MAGIC | ACID Transactions | ❌ | ❌ | ✅ |
# MAGIC | Schema Evolution | ❌ | Limited | ✅ |
# MAGIC | Time Travel | ❌ | ❌ | ✅ |
# MAGIC | Use Case | Simple data sharing | Analytics & BI | Reliable data pipelines |

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Next Steps**
# MAGIC 
# MAGIC 1. Upload your own dataset to FileStore
# MAGIC 2. Practice schema definition and data profiling
# MAGIC 3. Convert between different formats
# MAGIC 4. Experiment with Delta Lake features
# MAGIC 5. Implement data quality checks for your use case