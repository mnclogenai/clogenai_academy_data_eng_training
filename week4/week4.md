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
    
    style A fill:#ff9999
    style B fill:#ffcc99
    style C fill:#99ccff
    style D fill:#99ff99
    style E fill:#cc99ff
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
    A["Orders V1<br/>📋 Basic Fields"] --> B["Schema Evolution<br/>🔄 Automatic Handling"]
    C["Orders V2<br/>📋 + New Columns"] --> B
    D["Orders V3<br/>📋 + Type Changes"] --> B
    B --> E["Delta Table<br/>🗃️ Unified Schema"]
    
    style A fill:#ffcccc
    style C fill:#ffffcc
    style D fill:#ccffcc
    style E fill:#ccccff
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
    A["📥 New Data Arrives"] --> B{"Record Exists?"}
    B -->|Yes| C["🔄 UPDATE"]
    B -->|No| D["➕ INSERT"]
    C --> E["✅ MERGE Complete"]
    D --> E
    
    F["⏰ Continuous Ingestion"] --> A
    G["📅 Late-arriving Events"] --> A
    H["💰 Updated Transactions"] --> A
    I["🔄 Slowly Changing Dimensions"] --> A
    
    style A fill:#e1f5fe
    style C fill:#fff3e0
    style D fill:#e8f5e8
    style E fill:#f3e5f5
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
    LTV: $100         :2024-01-01, 1d
    LTV: $250         :2024-02-15, 1d
    LTV: $450         :2024-03-20, 1d
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
    subgraph "Silver Layer"
        S1["📊 Orders"]
        S2["👥 Customers"]
        S3["📦 Products"]
    end
    
    subgraph "Gold Layer - Business Metrics"
        G1["💰 Revenue Metrics"]
        G2["🔄 Retention Analysis"]
        G3["📈 Customer LTV"]
        G4["📊 Product Performance"]
    end
    
    subgraph "Analytics & BI"
        A1["📈 Dashboards"]
        A2["📊 Reports"]
        A3["🤖 ML Models"]
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
    
    style G1 fill:#ffd700
    style G2 fill:#ffd700
    style G3 fill:#ffd700
    style G4 fill:#ffd700
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
    
    style B1 fill:#e3f2fd
    style B2 fill:#fff3e0
    style B3 fill:#e8f5e8
    style B4 fill:#f3e5f5
    style C1 fill:#ffd700
```

### 🎯 Pipeline
Build a pipeline that:

1. 🔄 Handles schema evolution on multiple versions of an orders feed  
2. 🚀 Processes incremental updates using MERGE  
3. 📊 Produces customer metrics using joins, aggregations & window functions  
4. 🏆 Stores results in a Gold Delta table  
5. 🤖 Uses Databricks AI to define schema + generate DDL  

---

### 📋 Step-by-Step Checklist

```mermaid
gantt
    title Project Timeline & Phases
    dateFormat  HH:mm
    axisFormat %H:%M
    
    section Phase 1 - Schema Evolution
    Load V1,V2,V3 files    :active, p1a, 09:00, 30m
    Apply evolution        :p1b, after p1a, 20m
    Inspect history        :p1c, after p1b, 10m
    
    section Phase 2 - MERGE Logic
    Identify keys          :p2a, after p1c, 15m
    Build MERGE            :p2b, after p2a, 25m
    Validate counts        :p2c, after p2b, 10m
    
    section Phase 3 - Metrics
    Join operations        :p3a, after p2c, 20m
    Aggregations          :p3b, after p3a, 25m
    Window functions      :p3c, after p3b, 20m
    
    section Phase 4 - Gold Table
    AI schema design      :p4a, after p3c, 15m
    Generate DDL          :p4b, after p4a, 10m
    Create table          :p4c, after p4b, 15m
    
    section Phase 5 - Validation
    Schema validation     :p5a, after p4c, 10m
    Count validation      :p5b, after p5a, 10m
    Final preview         :p5c, after p5b, 10m
```

#### Phase 1 — Schema Evolution 🔄
- [ ] 📂 Load V1, V2, V3 order files  
- [ ] 🔄 Apply schema evolution  
- [ ] 📜 Inspect table history  
- [ ] 📝 Document column changes  

#### Phase 2 — Incremental MERGE Logic 🚀
- [ ] 🔑 Identify incremental key or timestamp  
- [ ] 🔀 Build MERGE logic  
- [ ] ✅ Validate row counts  

#### Phase 3 — Metrics Development 📊
- [ ] 🔗 Join orders + customers  
- [ ] 📈 Create grouped aggregations  
- [ ] 📊 Add window-based metrics (LTV, retention)  

#### Phase 4 — Gold Table 🏆
- [ ] 🤖 Use AI to propose schema  
- [ ] 📝 Generate PySpark + SQL DDL  
- [ ] 🗃️ Create and populate Gold table  

#### Phase 5 — Validation ✅
- [ ] 🔍 Validate schema  
- [ ] 📊 Validate row counts  
- [ ] 👀 Show final records  

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
    
    style C fill:#ffcdd2
    style D fill:#ffcdd2
    style E fill:#ffcdd2
    style F fill:#ffcdd2
    style C1 fill:#c8e6c9
    style D1 fill:#c8e6c9
    style E1 fill:#c8e6c9
    style F1 fill:#c8e6c9
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

### Learning Progress Tracker

```mermaid
pie title Knowledge Areas Mastered
    "Schema Evolution" : 20
    "MERGE Operations" : 20
    "Window Functions" : 20
    "Gold Layer Design" : 20
    "AI-Assisted Development" : 20
```

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

### Skills Development Roadmap

```mermaid
mindmap
  root((Week 4 Skills))
    Schema Evolution
      Version Management
      Type Compatibility
      History Tracking
    MERGE Operations
      Incremental Logic
      UPSERT Patterns
      Performance Tuning
    Analytics Functions
      Window Functions
      Aggregations
      Join Strategies
    Gold Layer Design
      Business Metrics
      KPI Development
      Data Modeling
    AI Integration
      Schema Generation
      DDL Creation
      Best Practices
```

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