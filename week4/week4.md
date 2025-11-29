# Week 4 – PySpark Transformations & Modeling

```mermaid
graph TB
    A[Raw Data Sources] --> B[Bronze Layer]
    B --> C[Silver Layer]
    C --> D[Gold Layer]
    D --> E[Analytics & BI]
    
    B1[Schema Evolution] --> B
    C1[Data Quality] --> C
    D1[Business Metrics] --> D
    
    style A fill:#8B0000,color:#fff
    style B fill:#8B4513,color:#fff
    style C fill:#2F4F4F,color:#fff
    style D fill:#556B2F,color:#fff
    style E fill:#483D8B,color:#fff
    style B1 fill:#191970,color:#fff
    style C1 fill:#191970,color:#fff
    style D1 fill:#191970,color:#fff
```

**Medallion Architecture Flow:**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Raw Data       │───▶│  Bronze Layer   │───▶│  Silver Layer   │───▶│   Gold Layer    │───▶│  Analytics &    │
│  Sources        │    │  (Raw Ingestion)│    │ (Clean & Valid) │    │(Business Ready) │    │      BI         │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                              ▲                        ▲                        ▲
                              │                        │                        │
                       ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
                       │   Schema    │        │    Data     │        │  Business   │
                       │  Evolution  │        │   Quality   │        │   Metrics   │
                       └─────────────┘        └─────────────┘        └─────────────┘
```

## Learning Objectives
By the end of this session, you will:
- 🔄 Apply schema evolution and enforcement with Delta Lake
- 🚀 Build incremental pipelines using MERGE and timestamp-based logic
- 📊 Use joins, aggregations, and window functions for analytical metrics
- 🏆 Create Gold-layer tables for revenue, retention, and customer metrics
- 🤖 Use Databricks AI to define schemas and generate PySpark + SQL DDL
- ✅ Validate business-friendly analytical models in Databricks

---

## 1. Schema Evolution, Enforcement & Incremental Delta Processing

### Why Schema Evolution Matters  
Modern data systems receive changing datasets: new columns, modified types, updated structures.

```mermaid
flowchart LR
    A["Orders V1 Basic Fields"] --> B["Schema Evolution Automatic Handling"]
    C["Orders V2 + New Columns"] --> B
    D["Orders V3 + Type Changes"] --> B
    B --> E["Delta Table Unified Schema"]
    
    style A fill:#8B0000,color:#fff
    style B fill:#191970,color:#fff
    style C fill:#8B4513,color:#fff
    style D fill:#556B2F,color:#fff
    style E fill:#2F4F4F,color:#fff
```

**Schema Evolution Process:**
```
┌─────────────┐
│  Orders V1  │────┐
│ Basic Fields│    │    ┌──────────────────┐    ┌─────────────────┐
└─────────────┘    ├───▶│ Schema Evolution │───▶│   Delta Table   │
┌─────────────┐    │    │ Auto Handling    │    │ Unified Schema  │
│  Orders V2  │────┤    └──────────────────┘    └─────────────────┘
│+ New Columns│    │
└─────────────┘    │
┌─────────────┐    │
│  Orders V3  │────┘
│+Type Changes│
└─────────────┘
```

Delta Lake's schema evolution + enforcement ensures:

- ✅ Reliable ingestion even when schemas shift  
- 🛡️ Protection against invalid/incorrect data  
- 🔧 Ability to process changing feeds without rebuilding pipelines  

### Example: Basic Schema Change Detection  
```python
df1 = spark.table("bronze.orders_v1")
df2 = spark.table("bronze.orders_v2")

df1.printSchema()
df2.printSchema()
```

### Enabling Schema Evolution  
```python
df2.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("silver.orders")
```

### Delta Table History  
```python
from delta.tables import DeltaTable

delta_tbl = DeltaTable.forName(spark, "silver.orders")
delta_tbl.history().show()
```

---

## 2. Incremental Data Processing & MERGE Operations

### When to Use Incremental Pipelines

```mermaid
graph TD
    A["New Data Arrives"] --> B{"Record Exists?"}
    B -->|Yes| C["UPDATE"]
    B -->|No| D["INSERT"]
    C --> E["MERGE Complete"]
    D --> E
    
    F["Continuous Ingestion"] --> A
    G["Late-arriving Events"] --> A
    H["Updated Transactions"] --> A
    I["Slowly Changing Dimensions"] --> A
    
    style A fill:#2F4F4F,color:#fff
    style B fill:#191970,color:#fff
    style C fill:#8B4513,color:#fff
    style D fill:#556B2F,color:#fff
    style E fill:#483D8B,color:#fff
    style F fill:#8B0000,color:#fff
    style G fill:#8B0000,color:#fff
    style H fill:#8B0000,color:#fff
    style I fill:#8B0000,color:#fff
```

**Use Cases:**
- 🔄 Continuous ingestion  
- ⏰ Late-arriving events  
- 💼 Updated business transactions  
- 📊 Slowly changing dimensions  

### Typical Incremental MERGE Pattern
```python
incremental_df = spark.table("bronze.orders_incremental")
delta_tbl = DeltaTable.forName(spark, "silver.orders")

delta_tbl.alias("t").merge(
    incremental_df.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### Key Advantages
- Updates and inserts in one operation  
- ACID guarantees  
- No duplication  
- No full table rewrite needed  

---

## 3. Joins, Aggregations & Window Functions

### Data Relationship Visualization

```mermaid
erDiagram
    CUSTOMERS {
        string customer_id PK
        string name
        string email
        date signup_date
    }
    ORDERS {
        string order_id PK
        string customer_id FK
        double amount
        date order_date
    }
    CUSTOMERS ||--o{ ORDERS : "places"
```

### Joining Customer and Order Data  
```python
orders = spark.table("silver.orders")
customers = spark.table("silver.customers")

joined = orders.join(customers, "customer_id", "left")
```

### Aggregations for Metrics  
```python
from pyspark.sql.functions import sum, avg, count

metrics = orders.groupBy("customer_id").agg(
    sum("amount").alias("total_revenue"),
    count("*").alias("order_count"),
    avg("amount").alias("avg_order_value")
)
```

### Window Functions  
Powerful for retention, running totals, LTV, and ordering.

```mermaid
gantt
    title Customer Journey Analysis with Window Functions
    dateFormat  YYYY-MM-DD
    section Customer A
    Order 1 ($100)    :2024-01-01, 1d
    Order 2 ($150)    :2024-02-15, 1d
    Order 3 ($200)    :2024-03-20, 1d
    section Running Totals
    LTV $100         :2024-01-01, 1d
    LTV $250         :2024-02-15, 1d
    LTV $450         :2024-03-20, 1d
```

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, sum

w = Window.partitionBy("customer_id").orderBy("order_date")

df = orders.withColumn("prev_order", lag("order_date").over(w)) \
           .withColumn("lifetime_spend", sum("amount").over(w))
```

---

## 4. Designing Gold-Layer Tables for Analytics

### Gold Layer Architecture

```mermaid
flowchart TB
    subgraph SL ["Silver Layer"]
        S1["Orders"]
        S2["Customers"]
        S3["Products"]
    end
    
    subgraph GL ["Gold Layer"]
        G1["Revenue Metrics"]
        G2["Retention Analysis"]
        G3["Customer LTV"]
        G4["Product Performance"]
    end
    
    subgraph AL ["Analytics"]
        A1["Dashboards"]
        A2["Reports"]
        A3["ML Models"]
    end
    
    S1 --> G1
    S1 --> G2
    S2 --> G2
    S1 --> G3
    S2 --> G3
    S1 --> G4
    S3 --> G4
    
    G1 --> A1
    G2 --> A1
    G3 --> A2
    G4 --> A3
    
    style S1 fill:#2F4F4F,color:#fff
    style S2 fill:#2F4F4F,color:#fff
    style S3 fill:#2F4F4F,color:#fff
    style G1 fill:#8B4513,color:#fff
    style G2 fill:#8B4513,color:#fff
    style G3 fill:#8B4513,color:#fff
    style G4 fill:#8B4513,color:#fff
    style A1 fill:#483D8B,color:#fff
    style A2 fill:#483D8B,color:#fff
    style A3 fill:#483D8B,color:#fff
```

### Why Gold Tables Exist
Gold tables serve business stakeholders:

- 💰 Revenue metrics  
- 🔄 Retention indicators  
- 📊 Customer activity signals  
- 📈 Aggregates and KPIs  
- 🎯 Dashboard-ready structures  

### Example: Gold Schema Definition (AI-assisted)
```sql
CREATE TABLE gold.customer_metrics (
    customer_id STRING,
    total_revenue DOUBLE,
    avg_order_value DOUBLE,
    order_count INT,
    lifetime_value DOUBLE,
    retention_flag BOOLEAN,
    first_order_date DATE,
    latest_order_date DATE
) USING DELTA;
```

### Writing the Gold Table  
```python
metrics.write.mode("overwrite").saveAsTable("gold.customer_metrics")
```

---

## 5. Using Databricks AI for Schema & DDL Generation

### How to Use AI in This Step
Ask Databricks AI to:

- Infer the ideal schema from sample data  
- Suggest column names, types, and constraints  
- Generate PySpark code or SQL DDL  
- Validate column-level definitions  
- Explain best practices for modeling  

### Example Prompt  
```
"Generate a Delta Lake table schema for customer revenue metrics.
Include customer_id, total_revenue, retention_flag, and running LTV."
```

### Example AI-Generated PySpark DDL
```python
ddl = """
CREATE TABLE gold.customer_metrics (
  customer_id STRING,
  total_revenue DOUBLE,
  avg_order_value DOUBLE,
  lifetime_value DOUBLE,
  retention_flag BOOLEAN
)
USING DELTA;
"""
spark.sql(ddl)
```

---

## 6. Gold Table Example: Revenue & Retention

### From Silver to Gold  
```python
from pyspark.sql.functions import sum, count, avg, min, max

gold = joined_df.groupBy("customer_id").agg(
    sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_order_value"),
    count("*").alias("order_count"),
    min("order_date").alias("first_order_date"),
    max("order_date").alias("latest_order_date")
)
```

---

## 7. Incremental Customer Metrics Pipeline

### 🎯 Pipeline Architecture Overview

```mermaid
flowchart TD
    subgraph "Data Sources"
        A1["📄 Orders V1"]
        A2["📄 Orders V2"]
        A3["📄 Orders V3"]
        A4["📄 Customers"]
    end
    
    subgraph "Processing Steps"
        B1["🔄 Schema Evolution"]
        B2["🔀 MERGE Operations"]
        B3["🔗 Joins & Aggregations"]
        B4["📊 Window Functions"]
    end
    
    subgraph "Output"
        C1["🏆 Gold Customer Metrics"]
        C2["📈 Analytics Ready"]
    end
    
    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B3
    
    B1 --> B2
    B2 --> B3
    B3 --> B4
    B4 --> C1
    C1 --> C2
    
    style B1 fill:#2F4F4F,color:#fff
    style B2 fill:#8B4513,color:#fff
    style B3 fill:#556B2F,color:#fff
    style B4 fill:#483D8B,color:#fff
    style C1 fill:#8B0000,color:#fff
```

### 🎯 Pipeline
Build a pipeline that:

1. 🔄 Handles schema evolution on multiple versions of an orders feed  
2. 🚀 Processes incremental updates using MERGE  
3. 📊 Produces customer metrics using joins, aggregations & window functions  
4. 🏆 Stores results in a Gold Delta table  
5. 🤖 Uses Databricks AI to define schema + generate DDL  

---

## 8. Troubleshooting Guide

### Common Issues & Solutions

```mermaid
flowchart TD
    A["🚨 Issue Detected"] --> B{"Issue Type?"}
    
    B -->|Schema| C["❌ Schema Mismatch"]
    B -->|Join| D["❌ Join Explosion"]
    B -->|Window| E["❌ Window Function Fail"]
    B -->|Performance| F["❌ MERGE Slow"]
    
    C --> C1["✅ Enable mergeSchema<br/>✅ Correct data types"]
    D --> D1["✅ Deduplicate keys<br/>✅ Validate join conditions"]
    E --> E1["✅ Check orderBy types<br/>✅ Ensure consistent timestamps"]
    F --> F1["✅ Z-ORDER table<br/>✅ OPTIMIZE operations"]
    
    style C fill:#8B0000,color:#fff
    style D fill:#8B0000,color:#fff
    style E fill:#8B0000,color:#fff
    style F fill:#8B0000,color:#fff
    style C1 fill:#556B2F,color:#fff
    style D1 fill:#556B2F,color:#fff
    style E1 fill:#556B2F,color:#fff
    style F1 fill:#556B2F,color:#fff
```

### Common Issues & Fixes

#### ❌ Schema mismatch  
```
Error: Cannot write incompatible data to column
```
**Fix**: Enable mergeSchema or correct types before write.

#### ❌ Many-to-many join explosion  
**Fix**: Deduplicate keys or validate join keys before joining.

#### ❌ Window functions fail  
**Fix**: Ensure orderBy uses a consistent type (date, timestamp).

#### ❌ MERGE slow  
**Fix**: Z-ORDER and OPTIMIZE the table.

---

## 9. Key Takeaways & Success Criteria


### What You Should Understand
- 🔄 Delta's schema evolution enables flexibility  
- 💰 Incremental pipelines reduce compute costs  
- 📊 Joins + aggregations + windows create analytical signals  
- 🏆 Gold tables provide consistent business metrics  
- 🤖 AI speeds up schema design & DDL creation  

### Your Notebook Must Include
- ✅ Working schema evolution demo  
- ✅ MERGE incremental logic  
- ✅ Customer metrics (revenue, order count, LTV)  
- ✅ Gold table creation + preview  
- ✅ Reflection on learnings  

---

## 10. What You'll Practice This Week


**Practice Areas:**
- 🔄 Schema changes across versions  
- 🚀 Incremental logic with MERGE  
- 📊 Analytical metrics with window functions  
- 🏆 Designing Gold models  
- 🤖 Using AI for schema generation  
- 📝 Writing high-quality documentation  

---

## Q&A  
Ask questions – Clarify concepts – Prepare for Week 4