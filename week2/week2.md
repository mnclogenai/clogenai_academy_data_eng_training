# 📊 Week 2 — Ingest & Explore with PySpark
### Building Reliable Data Pipelines in Databricks  

---

## **Goals for This Session**  
By the end of this session, you will:

- Understand how Spark ingests structured & semi-structured data  
- Explore datasets using DataFrames  
- Detect schema issues and data quality problems  
- Compare formats: CSV, Parquet, and Delta  
- Understand Delta features (ACID, schema evolution, time travel)  
- See how this fits into real data pipelines  

---

## **Where Ingestion Fits**  

### Data Engineering Workflow  
```
Ingest → Explore → Clean → Transform → Store → Validate → Report
```

### Week 2 Focus  
✔ Ingest  
✔ Explore  

---

## **Ingesting Data into Databricks**  

### Common Sources
- Databricks FileStore / DBFS  
- Local file uploads  
- Cloud storage (S3 / ADLS / GCS)  
- Databases (JDBC connectors)  

### Sample PySpark Code  
```python
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/FileStore/orders.csv")
```

---

## **Schema Inference vs. Explicit Schema**  

### Schema Inference  
✔ Fast  
✔ Good for initial exploration  
✘ Can misinterpret column types  
✘ Not recommended for production pipelines  

### Explicit Schema  
✔ Predictable and reliable  
✔ Enforces consistent data types  
✔ Prevents data quality issues from type changes  

### Sample Explicit Schema  
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

df = spark.read.csv("/FileStore/orders.csv", schema=schema, header=True)
```

---

## **Common Input Formats**  

### CSV  
- Row-based, plain text  
- Human-readable and easy to share  
- Can be slow to parse for large datasets  

### JSON  
- Supports flexible and nested structures  
- Common for web APIs and semi-structured data  
- Less optimized for analytical queries compared to columnar formats  

---

## **Columnar Formats: Parquet & Delta**  

### Parquet  
- An open-source columnar storage format  
- Highly compressed and efficient for reads  
- Well-suited for analytical (OLAP) workloads  

### Delta Lake  
- Built on top of Parquet, adding a transaction log  
- Provides ACID transactions for reliability  
- Enables features like schema evolution and time travel  

### Convert Data to Parquet  
```python
df.write.format("parquet").save("/FileStore/orders_parquet")
```

---

## **Benefits of Columnar Formats**  

Columnar formats are optimized for analytical workloads and offer several advantages:

- **Efficient Queries**: Only the columns required for a query are read, which significantly speeds up I/O.
- **High Compression**: Since data in a column is of the same type, it can be compressed more effectively than row-based data.
- **Lower Storage Costs**: Better compression leads to smaller file sizes and reduced storage expenses.
- **Optimized for Parallelism**: Data can be processed in parallel by column, which fits well with distributed systems like Spark.

---

## **DataFrames Overview**  

A Spark DataFrame is a distributed collection of data organized into named columns. It offers:

- A schema to define the structure of the data  
- A powerful API for transformations (e.g., `select`, `filter`, `groupBy`)
- Lazy evaluation, which allows Spark to optimize the full query plan  
- Optimized execution on a distributed cluster  

---

## **Exploring Data with DataFrames**  

### Common Exploration Commands  
```python
# Display the first 5 rows
df.show(5)

# Print the schema to understand data types
df.printSchema()

# Get summary statistics (count, mean, stddev, etc.)
df.describe().show()

# An extended summary including quartiles
df.summary().show()
```

### Deeper Exploration with Grouping and Aggregation

Beyond simple summaries, you can group data to find patterns. For example, let's find the total quantity of each product sold.

```python
from pyspark.sql.functions import sum

product_sales = df.groupBy("product_id") \
                  .agg(sum("quantity").alias("total_quantity"))

product_sales.show()

# You can also sort the results to find the most popular products
top_products = product_sales.orderBy("total_quantity", ascending=False)
top_products.show(10)
```

This kind of aggregation is a fundamental step in understanding your dataset.

---

## **Data Profiling**  

Use profiling to check for:

- **Completeness**: Are there null or missing values?  
- **Accuracy**: Do the data types match expectations?  
- **Outliers**: Are there values outside the expected range?  
- **Consistency**: Are formats for dates, numbers, and strings consistent?  

---

## **Typical Data Issues**  

- Missing values (nulls)  
- Mixed data types in a single column  
- Incorrectly formatted dates or numbers  
- Duplicate records  
- Unreasonable values (e.g., negative quantities)  
- Corrupted or incomplete entries  

---

## **Schema Problems**  

Examples of schema issues include:

- Numbers or dates stored as strings  
- Unexpected nulls in columns that should not be empty  
- Columns appearing or disappearing between files  

### Checking the Schema  
```python
df.printSchema()
```

---

## **Hands-On: CSV → Parquet → Delta**  

### Convert CSV to Parquet  
```python
df.write.parquet("/FileStore/orders_parquet")
```

### Convert Parquet to Delta  
```python
df.write.format("delta").save("/FileStore/orders_delta")
```

---

## **Delta Lake Internals**  

A Delta table consists of:

- **Parquet Data Files**: The data is stored in the efficient Parquet format.  
- **`_delta_log` Directory**: A transaction log that records every change.  
- **Commit Logs (JSON)**: Individual atomic commits are stored as JSON files.  
- **Checkpoints**: For performance, Delta periodically compacts the transaction log into a Parquet checkpoint file.  

---

## **Schema Evolution**  

### Adding a New Column  
```python
from pyspark.sql.functions import lit

df2 = df.withColumn("region", lit("US"))

# The mergeSchema option allows you to safely add new columns
df2.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save("/FileStore/orders_delta")
```

---

## **Delta Lake Feature: Time Travel**

Delta Lake's time travel feature allows you to query previous versions of your data. This is useful for debugging, auditing, or rolling back to a previous state.

### How to Use Time Travel

You can query a specific version of your Delta table using either a version number or a timestamp.

#### Querying by Version

First, view the history of your Delta table to see the available versions.

```python
from delta.tables import *

delta_table = DeltaTable.forPath(spark, "/FileStore/orders_delta")
delta_table.history().show()
```

Now, you can read a specific version. For example, to read version 0 (the original data before we added the 'region' column):

```python
df_v0 = spark.read.format("delta") \
              .option("versionAsOf", 0) \
              .load("/FileStore/orders_delta")

df_v0.show()
```

#### Querying by Timestamp

You can also query the data as it was at a specific point in time.

```python
df_timestamp = spark.read.format("delta") \
                      .option("timestampAsOf", "2023-10-27T00:00:00.000Z") \
                      .load("/FileStore/orders_delta")

df_timestamp.show()
```
This will load the data as it existed at that specific timestamp.

---

## **Format Comparison Table**  

| Feature | CSV | Parquet | Delta |
|-------------------|----------------------|-------------------|-----------------------------|
| Structure | Row-based | Columnar | Columnar with a transaction log |
| Compression | Basic (e.g., Gzip) | High & efficient | High & efficient |
| ACID Transactions | ❌ | ❌ | ✔ |
| Schema Evolution | ❌ | Limited | ✔ |
| Time Travel | ❌ | ❌ | ✔ |
| Typical Use Case | Sharing simple data | Analytics & BI | Reliable data pipelines |

---

## **Performance Characteristics**  

- **CSV**: Generally the slowest for large-scale analytics due to its row-based nature and lack of native indexing. Read performance can be a bottleneck.
- **Parquet**: Offers significantly faster read performance (often 3-10x faster than CSV) for analytical queries because of its columnar storage and compression.
- **Delta**: Provides similar read performance to Parquet, with a small overhead for reading the transaction log. It adds reliability features like ACID transactions and metadata management, which are crucial for production pipelines.

---

## **Pipeline Summary**  

```
Ingest → Explore → Compare → Improve
```

This workflow repeats across modern data engineering pipelines.

---

## **What You’ll Practice This Week**  

- Upload a dataset to FileStore  
- Explore its schema and profile for quality issues  
- Convert the data between formats  
- Compare the performance and features of each format  
- Document your findings in a notebook  
- Submit your completed notebook  

---

## **Tools to Use**  

- Databricks Notebooks  
- PySpark DataFrames  
- DBFS / FileStore  
- Parquet & Delta formats  
- AI assistants for:  
  - Code scaffolding  
  - Explanations and debugging  
  - Schema summaries  
  - Documentation  

---

## **Hands-On Expectations**  

Your notebook should include:

- Clean markdown explanations of your steps  
- Schema inspection and summary statistics  
- Code for format conversions  
- Checks to verify the contents of your Delta table  
- A short reflection on your findings  

---

## **AI Usage Tips**  

AI can help with:

- Debugging code and explaining errors  
- Suggesting transformation steps  
- Summarizing profiling results and schemas  
- Auto-generating documentation for your notebook  

Remember to always validate the correctness of AI-generated suggestions.

---

## **Real-World Relevance**  

In real data engineering projects:

- Raw data is often messy and inconsistent.  
- Schemas can change unexpectedly over time.  
- The choice of storage format has a major impact on performance and reliability.  
- Delta Lake is widely used to simplify data reliability and versioning.  

---

## **Recap**  

You learned:

- How Spark ingests data from various sources  
- How to explore and profile data using DataFrames  
- The importance of defining a schema  
- The trade-offs between CSV, Parquet, and Delta  
- How columnar formats improve performance for analytics  

---

## **Next Steps**  

Complete the following:

1. Practice / Hands-On Notebook  
2. End-of-Week Assignment  
3. Week 2 Quiz  

Submit your notebook link to **academy@clogenai.com**

---

## **Q&A**  

- Ask questions  
- Clarify concepts  
- Review next week’s preview