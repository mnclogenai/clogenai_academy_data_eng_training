# Week 3 – Data Cleaning & Transformation with Spark & PySpark

## Learning Objectives
By the end of this session, you will:
- Understand Spark's execution model and how it optimizes data transformations
- Apply ETL vs ELT concepts using the Medallion Architecture
- Implement data cleaning and validation techniques with PySpark
- Handle data quality issues and route invalid records appropriately
- Troubleshoot common Spark and Delta Lake issues
- Build a Bronze → Silver transformation project

## Quick Reference - Essential PySpark Functions

```python
# Core imports for data cleaning
from pyspark.sql.functions import (
    col, trim, lower, upper, when, isnan, isnull,
    to_date, regexp_replace, split, concat, lit
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

# Common cleaning patterns
df.withColumn("col_name", trim(col("col_name")))  # Remove whitespace
df.withColumn("col_name", when(col("col_name") == "", None).otherwise(col("col_name")))  # Empty to null
df.filter(col("col_name").isNotNull())  # Remove nulls
df.withColumn("col_name", col("col_name").cast("int"))  # Type casting

# Reading from Delta tables
spark.table("catalog.schema.table_name")  # Read Delta table
df.write.mode("overwrite").saveAsTable("catalog.schema.table_name")  # Write Delta table
```

## 1. Spark Execution Model (Driver, Executors, DAG, Lazy Evaluation)

### Spark Architecture (Conceptual View)
```
        ┌─────────────────────┐
        │   Notebook / UI     │
        │ (Databricks, IDE)   │
        └──────────┬──────────┘
                   │
                   │ 1) Submit job
                   ▼
        ┌──────────┴──────────┐
        │       Driver        │
        │  • Builds DAG       │
        │  • Schedules tasks  │
        │  • Tracks progress  │
        └──────────┬──────────┘
                   │
                   │ 2) Requests resources
                   ▼
        ┌──────────┴──────────┐
        │  Cluster Manager    │
        │  (e.g., Databricks) │
        └──────────┬──────────┘
                   │
                   │ 3) Launch executors
                   ▼
    ┌──────────────┼──────────────┐
    │              │              │
    ▼              ▼              ▼
┌─────────┐    ┌─────────┐    ┌─────────┐
│Executor1│    │Executor2│    │ExecutorN│
│• Runs   │    │• Runs   │    │• Runs   │
│  tasks  │    │  tasks  │    │  tasks  │
│• Stores │    │• Stores │    │• Stores │
│  cache  │    │  cache  │    │  cache  │
└─────────┘    └─────────┘    └─────────┘
```

### Quick Notes
- **Driver**: The main program that coordinates the Spark application (like a project manager)
- **Executors**: Worker nodes that actually process the data (like team members doing the work)
- **DAG**: Directed Acyclic Graph - Spark's execution plan showing the order of operations
- **Lazy Evaluation**: Transformations are not executed immediately, only when an action is called

### Key Points
- Spark uses a driver to coordinate work and executors to run tasks.
- Transformations are lazy; Spark builds a DAG and executes only when an action is triggered.
- `.explain()` shows the logical and physical plan Spark generates.
- Common actions: count(), show(), collect(), first().

### Example Code
```python
# Lazy evaluation in action - builds execution plan
df = spark.table("academy.week3.bronze_sample")
df_transformed = df.filter("amount > 100").select("customer", "amount")

# Only now does Spark execute the transformations
df_transformed.show()       # action triggers execution
df_transformed.explain()    # shows optimized DAG

# This connects to data cleaning: chain transformations for efficiency
cleaned_df = df \
    .filter(col("amount").isNotNull()) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("customer", trim(col("customer")))
    # All transformations above are lazy until an action is called
```

## 2. ETL vs ELT and the Medallion Architecture

### Medallion Architecture Flow
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   BRONZE    │───▶│   SILVER    │───▶│    GOLD     │
│             │    │             │    │             │
│ Raw Data    │    │ Cleaned &   │    │ Aggregated  │
│ • CSV/JSON  │    │ Validated   │    │ • Metrics   │
│ • No Schema │    │ • Schema    │    │ • Reports   │
│ • Ingested  │    │ • Quality   │    │ • Analytics │
│   As-Is     │    │   Checks    │    │   Ready     │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Data Quality Transformation Example
```
BEFORE (Bronze):                    AFTER (Silver):
┌─────────────────────┐            ┌─────────────────────┐
│ customer_name       │            │ customer_name       │
├─────────────────────┤            ├─────────────────────┤
│ " John Doe "        │     ───▶   │ john doe            │
│ "JANE SMITH"        │            │ jane smith          │
│ ""                  │            │ NULL                │
│ "Bob Jones "        │            │ bob jones           │
└─────────────────────┘            └─────────────────────┘
```

### ETL vs ELT Comparison
```
ETL (Extract, Transform, Load)     ELT (Extract, Load, Transform)
┌─────────────────────────────┐    ┌─────────────────────────────┐
│ Source → Transform → Target │    │ Source → Target → Transform │
│                             │    │                             │
│ • Transform outside target  │    │ • Transform inside target   │
│ • Limited by compute power  │    │ • Leverage target's power   │
│ • Slower for large data     │    │ • Faster for large data     │
│ • Traditional approach      │    │ • Modern lakehouse approach │
└─────────────────────────────┘    └─────────────────────────────┘
```

### Key Points
- **ETL**: Transform data before loading into the target system
- **ELT**: Load raw data first, then transform using the target system's compute power
- **Databricks/Lakehouse**: Naturally aligns with ELT approach
- **Benefits of ELT**: Faster ingestion, scalable transformations, data lineage preservation
- **Medallion Layers**:
  - Bronze → raw ingested data (ELT "Load" step)
  - Silver → cleaned and standardized (ELT "Transform" step)
  - Gold → aggregated and analytics-ready (ELT "Transform" step)

### Example Code
```python
bronze_df = spark.table("academy.week3.bronze_orders")

silver_df = bronze_df \
    .withColumn("order_id", col("order_id").cast("int")) \
    .withColumn("customer", trim(col("customer")))

silver_df.write.mode("overwrite").saveAsTable("academy.week3.silver_orders")

```

## 3. Data Cleaning & Standardization with PySpark

### Key Points
- Common cleaning operations: rename columns, trim whitespace, cast types, normalize text.
- Handle nulls: fill, drop, or convert empty strings to null.
- Use `.summary()` to validate cleaned results.
- AI tools can help draft cleaning logic, but always validate output.

### Example Code
```python
from pyspark.sql.functions import col, trim, lower, when, to_date

df = spark.table("academy.week3.bronze_customers")

cleaned = df \
    .withColumnRenamed("Customer Name", "customer_name") \
    .withColumn("customer_name", trim(lower(col("customer_name")))) \
    .withColumn("signup_date", to_date(col("signup_date"), "MM/dd/yyyy")) \
    .withColumn("rating", col("rating").cast("int")) \
    .withColumn("comments", when(col("comments") == "", None).otherwise(col("comments")))

cleaned.summary().show()

# Pro tip: This summary() call triggers execution of all chained transformations above
# demonstrating lazy evaluation from Section 1
```
## 4. Handling Missing or Invalid Records

### Key Points
- Identify rows missing required fields.
- Route invalid rows to a rejects storage location.
- Silver data should include only valid and complete records.

### Example Code
```python
valid = cleaned.filter(col("customer_name").isNotNull() & col("rating").isNotNull())
rejects = cleaned.filter(col("customer_name").isNull() | col("rating").isNull())

rejects.write.mode("overwrite").saveAsTable("academy.week3.rejects_customers")

```
## 5. Writing Silver Output in Delta Format

### Key Points
- Delta provides ACID transactions, schema enforcement, and versioning.
- Silver datasets should be written in Delta format.
- Always validate written results by reading them back.

### Example Code
```python
valid.write.mode("overwrite").saveAsTable("academy.week3.silver_customers")

spark.table("academy.week3.silver_customers").show()

```
## 6. Example End-to-End Bronze → Silver → Gold Workflow

### Example Code
```python
# Bronze → Silver: Data Cleaning
bronze_df = spark.table("academy.week3.bronze_sales")

silver_df = bronze_df \
    .withColumn("sales", col("sales").cast("double")) \
    .withColumn("product", trim(col("product"))) \
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd")) \
    .filter(col("sales").isNotNull() & col("product").isNotNull())

silver_df.write.mode("overwrite").saveAsTable("academy.week3.silver_sales")

# Silver → Gold: Business Aggregations
from pyspark.sql.functions import sum, count, avg, date_format

gold_df = silver_df \
    .withColumn("month", date_format(col("sale_date"), "yyyy-MM")) \
    .groupBy("product", "month") \
    .agg(
        sum("sales").alias("total_sales"),
        count("*").alias("transaction_count"),
        avg("sales").alias("avg_sale_amount")
    )

gold_df.write.mode("overwrite").saveAsTable("academy.week3.gold_sales_summary")
```

### Discussion Points
- Which steps belong in Bronze vs Silver vs Gold?
- What made the final result suitable for each layer?
- How does Gold layer serve business analytics needs?
- How does Spark's lazy evaluation (Section 1) optimize this multi-step workflow?
- What troubleshooting steps (Section 7) would you take if this pipeline failed?

## 7. Troubleshooting Common Issues

### Schema Mismatch Errors
```
Error: "Cannot resolve column name 'customer_id' among (customer_ID, name, email)"
Solution: Check column names with df.columns or use case-insensitive matching
```

### Memory Issues with Large Datasets
```
Error: "OutOfMemoryError" or "Task not serializable"
Solutions:
- Avoid .collect() on large DataFrames
- Use .show(n) instead of .collect()
- Increase executor memory or reduce partition size
- Use .cache() strategically for DataFrames used multiple times
```

### Performance Issues with Transformations
```
Error: Slow execution or hanging jobs
Solutions:
- Check for data skew with .describe() and .summary()
- Use .repartition() to balance data across executors
- Leverage lazy evaluation by chaining transformations before actions
- Monitor Spark UI for bottlenecks
```

### Data Validation Failures
```
Error: Unexpected data types or values after cleaning
Solutions:
- Always validate with .printSchema() after transformations
- Use .summary() to check data distributions
- Test cleaning logic on small samples first (.limit(100))
- Document assumptions and validate them with assertions
```ollect()
- Increase executor memory or reduce partition size
```

### Delta Table Conflicts
```
Error: "ConcurrentModificationException"
Solution: Use .option("mergeSchema", "true") for schema evolution
```

### Null Handling Issues
```
Error: "NullPointerException" during transformations
Solutions:
- Use .na.fill() or .na.drop() before transformations
- Check for nulls with .filter(col("column").isNotNull())
- Use when().otherwise() for conditional logic
```

### Type Casting Failures
```
Error: "NumberFormatException" when casting strings to numbers
Solution: Clean data first, then cast
df.withColumn("amount", regexp_replace(col("amount"), "[^0-9.]", "").cast("double"))
```

## 8. Key Takeaways & Success Criteria

### What Makes Quality Silver Data?
✅ **Schema Consistency**: All columns have correct data types  
✅ **Data Completeness**: Required fields are not null  
✅ **Format Standardization**: Text is trimmed, normalized case  
✅ **Validation Passed**: Summary statistics show expected ranges  
✅ **Delta Format**: ACID compliance and schema enforcement enabled  

### Performance Best Practices
- **Lazy Evaluation**: Chain transformations before triggering actions
- **Partitioning**: Consider `.repartition()` for better performance
- **Caching**: Use `.cache()` for DataFrames used multiple times
- **Schema Inference**: Avoid `inferSchema=True` in production

### Quality Checks to Always Perform
```python
# Before and after counts
bronze_df = spark.table("academy.week3.bronze_customers")
silver_df = spark.table("academy.week3.silver_customers")
rejects_df = spark.table("academy.week3.rejects_customers")

print(f"Bronze records: {bronze_df.count()}")
print(f"Silver records: {silver_df.count()}")
print(f"Rejected records: {rejects_df.count()}")

# Data quality summary
silver_df.summary().show()
silver_df.printSchema()
```

## 9. Hands-On Project Guide

### 🎯 Your Mission
Transform messy Bronze customer/transaction data into production-ready Silver tables that analysts can trust.

### 📋 Step-by-Step Checklist

#### Phase 1: Data Discovery
- [ ] Load Bronze data and examine with `.show(5)` and `.printSchema()`
- [ ] Run `.summary()` to identify data quality issues
- [ ] Document 3-5 specific problems you found

#### Phase 2: Cleaning Strategy
- [ ] Define cleaning rules for each problematic column
- [ ] Identify which records should be rejected vs cleaned
- [ ] Plan your transformation chain

#### Phase 3: Implementation
```python
# Template for your cleaning pipeline
bronze_df = spark.table("academy.week3.bronze_customers")

silver_df = bronze_df \
    .withColumnRenamed("old_name", "new_name") \
    .withColumn("clean_col", trim(lower(col("messy_col")))) \
    .withColumn("typed_col", col("string_col").cast("int")) \
    .filter(col("required_field").isNotNull())

silver_df.write.mode("overwrite").saveAsTable("academy.week3.silver_customers")
```

#### Phase 4: Validation & Output
- [ ] Compare before/after record counts
- [ ] Verify data types with `.printSchema()`
- [ ] Save Silver and rejects to Delta tables
- [ ] Test reading data back successfully

### 🏆 Success Metrics
- **Data Quality**: >95% of valid records preserved
- **Schema Compliance**: All columns have correct types
- **Documentation**: Clear explanation of cleaning decisions
- **Reproducibility**: Code runs without errors

### 📊 Expected Deliverables
- **Silver Delta Table**: Clean, typed, validated data
- **Rejects Delta Table**: Invalid records with reasons
- **Quality Report**: Before/after statistics comparison
- **Documentation**: Cleaning decisions and business rules

### 💡 Pro Tips for Your Project
- Start with small samples using `.limit(1000)` for faster iteration
- Use `.explain()` to understand Spark's execution plan
- Save intermediate results to debug transformation steps
- Always validate your assumptions with `.summary()` and `.describe()`
- Use `spark.table()` to read from Delta tables instead of file paths
- Use `.saveAsTable()` to write managed Delta tables with automatic optimization

### 🚀 What You'll Learn
By completing this project, you'll master the core skills of a data engineer:
- **Spark Architecture**: Apply lazy evaluation and DAG optimization in real transformations
- **Medallion Architecture**: Implement ELT patterns with Bronze → Silver → Gold flow
- **Data Quality**: Use PySpark functions for cleaning, validation, and error handling
- **Troubleshooting**: Debug common issues using Spark UI and error messages
- **Production Skills**: Write reliable, scalable data pipelines that power business decisions

### 🔗 How This Connects
This project integrates all concepts from today's session:
1. **Spark execution model** optimizes your transformation chains
2. **ELT approach** loads raw data first, then transforms in the lakehouse
3. **Data cleaning techniques** ensure Silver layer quality
4. **Error handling** routes bad data to rejects tables
5. **Troubleshooting skills** help debug real-world issues