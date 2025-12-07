# PySpark Transformations & Modeling

## Overview

This guide covers advanced PySpark techniques for building robust data transformation pipelines using the Medallion Architecture. It focuses on schema evolution, incremental processing, analytical transformations, and Gold-layer modeling for business-ready analytics.

## Learning Objectives
By the end of this guide, you will:
- 🔄 Apply schema evolution and enforcement with Delta Lake
- 🚀 Build incremental pipelines using MERGE and timestamp-based logic
- 📊 Use joins, aggregations, and window functions for analytical metrics
- 🏆 Create Gold-layer tables for revenue, retention, and customer metrics
- 🤖 Use Databricks AI to define schemas and generate PySpark + SQL DDL
- ✅ Validate business-friendly analytical models in Databricks

---

## 1. Schema Evolution & Delta Lake Management

### Key Concepts

**Schema Evolution** enables flexible data ingestion by automatically handling:
- New columns added to incoming data
- Data type changes across versions
- Structural modifications without pipeline breaks
- Backward compatibility maintenance

**Delta Lake Features**
- ACID transactions for reliable data operations
- Time travel for historical data access
- Schema enforcement with optional evolution
- Optimized storage with Z-ordering

### Implementation Patterns

**Enable Schema Evolution**
```python
# Allow new columns to be added automatically
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("silver.orders")
```

**Schema Validation**
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

expected_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), False)
])

# Validate schema from table
df = spark.table("bronze.orders")
actual_schema = df.schema
```

**Delta Table Operations**
```python
from delta.tables import DeltaTable

# Access Delta table
delta_table = DeltaTable.forName(spark, "silver.orders")

# View table history
delta_table.history().show()

# Optimize table performance
delta_table.optimize().executeCompaction()
```

### Best Practices

- Use `mergeSchema` cautiously in production environments
- Implement schema validation at ingestion boundaries
- Monitor schema changes through Delta history
- Apply Z-ordering on frequently queried columns
- Regular OPTIMIZE operations for performance

## 2. Incremental Data Processing with MERGE

### MERGE Operation Fundamentals

**Use Cases for Incremental Processing**
- Continuous data ingestion from streaming sources
- Late-arriving events requiring updates
- Slowly changing dimensions (SCD Type 1 & 2)
- Upsert operations for transactional systems

**Basic MERGE Pattern**
```python
from delta.tables import DeltaTable

# Load incremental data
incremental_df = spark.table("bronze.orders_incremental")

# Get target Delta table
target_table = DeltaTable.forName(spark, "silver.orders")

# Execute MERGE operation
target_table.alias("target").merge(
    incremental_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**Advanced MERGE with Conditions**
```python
# Conditional updates and inserts
target_table.alias("t").merge(
    incremental_df.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdate(
    condition="s.updated_at > t.updated_at",
    set={
        "amount": "s.amount",
        "status": "s.status",
        "updated_at": "s.updated_at"
    }
).whenNotMatchedInsert(
    values={
        "order_id": "s.order_id",
        "customer_id": "s.customer_id",
        "amount": "s.amount",
        "status": "s.status",
        "created_at": "s.created_at",
        "updated_at": "s.updated_at"
    }
).execute()
```

### Performance Optimization

**Partition Strategy**
```python
# Partition by date for time-series data
df.write.format("delta") \
    .partitionBy("order_date") \
    .saveAsTable("silver.orders")
```

**Z-Order Optimization**
```python
# Optimize for join performance
spark.sql("OPTIMIZE silver.orders ZORDER BY (customer_id)")
```

## 3. Advanced Analytical Transformations

### Joins and Data Relationships

**Join Strategies**
```python
# Load tables
orders = spark.table("silver.orders")
customers = spark.table("silver.customers")

# Inner join for guaranteed matches
customer_orders = orders.join(customers, "customer_id", "inner")

# Left join to preserve all orders
enriched_orders = orders.join(customers, "customer_id", "left")

# Broadcast join for small dimension tables
from pyspark.sql.functions import broadcast
enriched_orders = orders.join(broadcast(customers), "customer_id")
```

**Complex Join Conditions**
```python
# Multiple column joins with conditions
result = orders.alias("o").join(
    customers.alias("c"),
    (col("o.customer_id") == col("c.customer_id")) & 
    (col("o.order_date") >= col("c.signup_date"))
)
```

### Aggregations and Metrics

**Basic Aggregations**
```python
from pyspark.sql.functions import sum, avg, count, max, min

customer_metrics = orders.groupBy("customer_id").agg(
    sum("amount").alias("total_revenue"),
    count("*").alias("order_count"),
    avg("amount").alias("avg_order_value"),
    max("order_date").alias("last_order_date"),
    min("order_date").alias("first_order_date")
)
```

**Advanced Aggregations**
```python
from pyspark.sql.functions import collect_list, countDistinct

detailed_metrics = orders.groupBy("customer_id").agg(
    sum("amount").alias("total_revenue"),
    countDistinct("product_id").alias("unique_products"),
    collect_list("order_id").alias("order_history")
)
```

### Window Functions for Analytics

**Running Totals and Rankings**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, sum as sum_func

# Define window specifications
customer_window = Window.partitionBy("customer_id").orderBy("order_date")
global_window = Window.orderBy(col("total_revenue").desc())

# Apply window functions
enriched_df = orders.withColumn(
    "order_sequence", row_number().over(customer_window)
).withColumn(
    "running_total", sum_func("amount").over(customer_window)
).withColumn(
    "customer_rank", rank().over(global_window)
)
```

**Lag and Lead Analysis**
```python
from pyspark.sql.functions import lag, lead, datediff

# Calculate time between orders
time_analysis = orders.withColumn(
    "prev_order_date", lag("order_date").over(customer_window)
).withColumn(
    "days_since_last_order", 
    datediff("order_date", "prev_order_date")
)
```

**Retention Analysis**
```python
# Customer retention metrics
retention_window = Window.partitionBy("customer_id").orderBy("order_date")

retention_df = orders.withColumn(
    "order_number", row_number().over(retention_window)
).withColumn(
    "is_repeat_customer", 
    when(col("order_number") > 1, True).otherwise(False)
)
```

## 4. Gold Layer Design Patterns

### Business-Ready Data Models

**Customer Lifetime Value (LTV)**
```python
def calculate_customer_ltv(orders_df, customers_df):
    """Calculate comprehensive customer LTV metrics"""
    
    # Join orders with customer data
    joined_df = orders_df.join(customers_df, "customer_id", "left")
    
    # Calculate customer metrics
    ltv_metrics = joined_df.groupBy("customer_id", "signup_date").agg(
        sum("amount").alias("total_revenue"),
        count("*").alias("total_orders"),
        avg("amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date")
    ).withColumn(
        "customer_lifespan_days",
        datediff("last_order_date", "first_order_date")
    ).withColumn(
        "ltv_score",
        col("total_revenue") / (col("customer_lifespan_days") + 1)
    )
    
    return ltv_metrics
```

**Revenue Analytics**
```python
def create_revenue_analytics(orders_df):
    """Create comprehensive revenue analytics"""
    
    revenue_metrics = orders_df.groupBy(
        date_format("order_date", "yyyy-MM").alias("month")
    ).agg(
        sum("amount").alias("monthly_revenue"),
        count("*").alias("monthly_orders"),
        countDistinct("customer_id").alias("active_customers")
    ).withColumn(
        "revenue_per_customer",
        col("monthly_revenue") / col("active_customers")
    )
    
    return revenue_metrics
```

### Gold Table Schema Design

**Dimensional Modeling**
```python
# Customer dimension
customer_dim_schema = """
CREATE TABLE gold.dim_customers (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    signup_date DATE,
    customer_segment STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
"""

# Order facts
order_fact_schema = """
CREATE TABLE gold.fact_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_date DATE,
    amount DOUBLE,
    quantity INT,
    order_status STRING,
    created_at TIMESTAMP
) USING DELTA
PARTITIONED BY (order_date)
"""
```

**Aggregated Metrics Tables**
```python
# Customer metrics mart
customer_metrics_schema = """
CREATE TABLE gold.customer_metrics (
    customer_id STRING,
    total_revenue DOUBLE,
    order_count INT,
    avg_order_value DOUBLE,
    first_order_date DATE,
    last_order_date DATE,
    customer_lifetime_days INT,
    ltv_score DOUBLE,
    retention_flag BOOLEAN,
    customer_segment STRING,
    as_of_date DATE
) USING DELTA
"""
```

## 5. AI-Assisted Development Patterns

### Schema Generation with AI

**Prompt Engineering for Schema Design**
```
Generate a Delta Lake table schema for e-commerce customer analytics including:
- Customer identification and demographics
- Revenue and order metrics
- Behavioral indicators
- Retention flags
- Segmentation attributes
```

**AI-Generated PySpark Code**
```python
# AI can help generate transformation logic
def ai_suggested_customer_segmentation(df):
    """AI-generated customer segmentation logic"""
    return df.withColumn("customer_segment",
        when(col("total_revenue") > 1000, "High Value")
        .when(col("total_revenue") > 500, "Medium Value")
        .when(col("order_count") > 5, "Frequent Buyer")
        .otherwise("New Customer")
    )
```

### Code Generation Best Practices

- Use AI for boilerplate schema definitions
- Validate AI-generated code through testing
- Customize generated code for specific business logic
- Document AI-assisted development decisions
- Review generated code for optimization opportunities

## 6. Pipeline Orchestration Patterns

### Incremental Pipeline Design

**Watermark-Based Processing**
```python
def process_incremental_orders(last_processed_timestamp):
    """Process orders incrementally using watermarks"""
    
    # Read new data since last processing
    new_orders = spark.table("bronze.orders") \
        .filter(col("created_at") > last_processed_timestamp)
    
    if new_orders.count() > 0:
        # Apply transformations
        processed_orders = transform_orders(new_orders)
        
        # Merge into target table
        merge_orders(processed_orders)
        
        # Update watermark
        new_watermark = new_orders.agg(max("created_at")).collect()[0][0]
        update_watermark("orders_pipeline", new_watermark)
    
    return new_watermark
```

**Error Handling and Recovery**
```python
def robust_pipeline_execution(pipeline_func, *args, **kwargs):
    """Execute pipeline with error handling and retry logic"""
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            result = pipeline_func(*args, **kwargs)
            log_success(pipeline_func.__name__, result)
            return result
            
        except Exception as e:
            retry_count += 1
            log_error(pipeline_func.__name__, str(e), retry_count)
            
            if retry_count >= max_retries:
                raise e
            
            time.sleep(2 ** retry_count)  # Exponential backoff
```

## 7. Performance Optimization Strategies

### Query Optimization

**Predicate Pushdown**
```python
# Filter early to reduce data movement
filtered_orders = spark.table("silver.orders") \
    .filter(col("order_date") >= "2024-01-01") \
    .filter(col("amount") > 100)
```

**Column Pruning**
```python
# Select only required columns
essential_columns = orders.select(
    "order_id", "customer_id", "amount", "order_date"
)
```

**Broadcast Joins**
```python
# Use broadcast for small dimension tables
from pyspark.sql.functions import broadcast

result = large_fact_table.join(
    broadcast(small_dimension_table), 
    "dimension_key"
)
```

### Storage Optimization

**Partitioning Strategy**
```python
# Partition by frequently filtered columns
df.write.format("delta") \
    .partitionBy("year", "month") \
    .saveAsTable("partitioned_orders")
```

**Compaction and Optimization**
```python
# Regular maintenance operations
spark.sql("OPTIMIZE silver.orders ZORDER BY (customer_id, order_date)")
spark.sql("VACUUM silver.orders RETAIN 168 HOURS")  # 7 days
```

## 8. Testing and Validation Framework

### Data Quality Checks

**Schema Validation**
```python
def validate_schema(df, expected_schema):
    """Validate DataFrame schema against expected structure"""
    actual_fields = set(df.schema.fieldNames())
    expected_fields = set(expected_schema.fieldNames())
    
    missing_fields = expected_fields - actual_fields
    extra_fields = actual_fields - expected_fields
    
    if missing_fields or extra_fields:
        raise ValueError(f"Schema mismatch: missing {missing_fields}, extra {extra_fields}")
    
    return True
```

**Business Logic Validation**
```python
def validate_business_rules(df):
    """Validate business rules and constraints"""
    
    # Check for negative amounts
    negative_amounts = df.filter(col("amount") < 0).count()
    if negative_amounts > 0:
        raise ValueError(f"Found {negative_amounts} orders with negative amounts")
    
    # Check for future dates
    future_orders = df.filter(col("order_date") > current_date()).count()
    if future_orders > 0:
        raise ValueError(f"Found {future_orders} orders with future dates")
    
    return True
```

### Unit Testing Patterns

**Transformation Testing**
```python
def test_customer_ltv_calculation():
    """Test customer LTV calculation logic"""
    
    # Create test data
    test_orders = spark.createDataFrame([
        ("cust1", "order1", 100.0, "2024-01-01"),
        ("cust1", "order2", 150.0, "2024-01-15"),
        ("cust2", "order3", 200.0, "2024-01-10")
    ], ["customer_id", "order_id", "amount", "order_date"])
    
    # Apply transformation
    result = calculate_customer_ltv(test_orders)
    
    # Validate results
    cust1_ltv = result.filter(col("customer_id") == "cust1").collect()[0]
    assert cust1_ltv["total_revenue"] == 250.0
    assert cust1_ltv["total_orders"] == 2
```

## 9. Complete Pipeline Example

### End-to-End Customer Analytics Pipeline

**Pipeline Implementation**
```python
def customer_analytics_pipeline():
    """Complete customer analytics pipeline"""
    
    # Step 1: Load and validate bronze data
    bronze_orders = spark.table("bronze.orders")
    bronze_customers = spark.table("bronze.customers")
    
    # Step 2: Apply schema evolution and validation
    validated_orders = validate_and_evolve_schema(bronze_orders)
    validated_customers = validate_and_evolve_schema(bronze_customers)
    
    # Step 3: Create silver layer with incremental processing
    silver_orders = create_silver_orders(validated_orders)
    silver_customers = create_silver_customers(validated_customers)
    
    # Step 4: Build gold layer analytics
    customer_ltv = calculate_customer_ltv(silver_orders, silver_customers)
    revenue_analytics = create_revenue_analytics(silver_orders)
    retention_analysis = calculate_retention_metrics(silver_orders)
    
    # Step 5: Save gold tables
    customer_ltv.write.format("delta").mode("overwrite").saveAsTable("gold.customer_ltv")
    revenue_analytics.write.format("delta").mode("overwrite").saveAsTable("gold.revenue_analytics")
    retention_analysis.write.format("delta").mode("overwrite").saveAsTable("gold.retention_analysis")
    
    # Step 6: Data quality validation
    validate_gold_layer_quality()
    
    return {
        'status': 'success',
        'tables_created': ['gold.customer_ltv', 'gold.revenue_analytics', 'gold.retention_analysis'],
        'timestamp': datetime.now()
    }

def validate_and_evolve_schema(df):
    """Validate and evolve schema for incoming data"""
    
    # Enable schema evolution
    return df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable("silver.validated_data")

def create_silver_orders(bronze_df):
    """Create silver layer orders with data quality checks"""
    
    # Apply business rules and transformations
    silver_df = bronze_df.filter(col("amount") > 0) \
        .filter(col("order_date") <= current_date()) \
        .withColumn("order_year", year("order_date")) \
        .withColumn("order_month", month("order_date"))
    
    # Use MERGE for incremental processing
    target_table = DeltaTable.forName(spark, "silver.orders")
    
    target_table.alias("target").merge(
        silver_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    return spark.table("silver.orders")
```

## Conclusion

Effective PySpark transformations and modeling require a combination of technical expertise and architectural thinking. The Medallion Architecture provides a structured approach to data processing, while Delta Lake enables reliable, scalable operations. By implementing proper schema evolution, incremental processing, and analytical transformations, teams can build robust data pipelines that deliver business value.

Key success factors include:
- Proper schema management and evolution strategies
- Efficient incremental processing with MERGE operations
- Advanced analytical transformations using window functions
- Well-designed Gold layer models for business consumption
- Performance optimization through partitioning and indexing
- Comprehensive testing and validation frameworks

This foundation enables teams to build maintainable, scalable data transformation pipelines that support advanced analytics and machine learning use cases.