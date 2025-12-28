# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Week 8 Live Demo: Gold-Layer Data Modeling & Dashboards
# MAGIC **Clogenai Academy – Data Pipeline Engineering Track**
# MAGIC 
# MAGIC ## Session Overview
# MAGIC In this live demo, we'll:
# MAGIC 1. Generate sample e-commerce data
# MAGIC 2. Create Gold-layer dimension and fact tables
# MAGIC 3. Write business analytics queries
# MAGIC 4. Prepare data for dashboard visualizations
# MAGIC 
# MAGIC **Estimated Time**: 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup: Catalog and Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use academy catalog
# MAGIC USE CATALOG academy;
# MAGIC 
# MAGIC -- Create schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Step 1: Generate Sample Data
# MAGIC 
# MAGIC We'll create sample e-commerce data for our demo:
# MAGIC - **Products**: 12 products across 3 categories
# MAGIC - **Orders**: 100 orders over 3 months (Jan-Mar 2024)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample products table
# MAGIC CREATE OR REPLACE TABLE academy.silver.products AS
# MAGIC SELECT 
# MAGIC     CONCAT('PROD', LPAD(CAST(id AS STRING), 3, '0')) AS product_id,
# MAGIC     CASE 
# MAGIC         WHEN id <= 3 THEN CONCAT('Laptop Model ', id)
# MAGIC         WHEN id <= 6 THEN CONCAT('Smartphone ', id - 3)
# MAGIC         WHEN id <= 9 THEN CONCAT('T-Shirt Style ', id - 6)
# MAGIC         ELSE CONCAT('Office Chair ', id - 9)
# MAGIC     END AS product_name,
# MAGIC     CASE 
# MAGIC         WHEN id <= 6 THEN 'Electronics'
# MAGIC         WHEN id <= 9 THEN 'Clothing'
# MAGIC         ELSE 'Home & Garden'
# MAGIC     END AS category,
# MAGIC     CASE 
# MAGIC         WHEN id <= 3 THEN 'Laptops'
# MAGIC         WHEN id <= 6 THEN 'Phones'
# MAGIC         WHEN id <= 9 THEN 'Apparel'
# MAGIC         ELSE 'Furniture'
# MAGIC     END AS subcategory,
# MAGIC     CASE 
# MAGIC         WHEN id <= 3 THEN 'TechBrand'
# MAGIC         WHEN id <= 6 THEN 'PhoneCo'
# MAGIC         WHEN id <= 9 THEN 'FashionHub'
# MAGIC         ELSE 'HomeComfort'
# MAGIC     END AS brand,
# MAGIC     CASE 
# MAGIC         WHEN id <= 3 THEN 800 + (id * 200)
# MAGIC         WHEN id <= 6 THEN 400 + ((id - 3) * 150)
# MAGIC         WHEN id <= 9 THEN 25 + ((id - 6) * 10)
# MAGIC         ELSE 150 + ((id - 9) * 50)
# MAGIC     END AS retail_price,
# MAGIC     CASE 
# MAGIC         WHEN id <= 3 THEN 600 + (id * 150)
# MAGIC         WHEN id <= 6 THEN 300 + ((id - 3) * 100)
# MAGIC         WHEN id <= 9 THEN 15 + ((id - 6) * 5)
# MAGIC         ELSE 100 + ((id - 9) * 30)
# MAGIC     END AS cost_price
# MAGIC FROM RANGE(1, 13); -- 12 products

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify products table
# MAGIC SELECT * FROM academy.silver.products ORDER BY product_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample orders table
# MAGIC CREATE OR REPLACE TABLE academy.silver.orders AS
# MAGIC SELECT 
# MAGIC     CONCAT('ORD', LPAD(CAST(id AS STRING), 4, '0')) AS order_id,
# MAGIC     CONCAT('CUST', LPAD(CAST(MOD(id * 7, 50) + 1 AS STRING), 3, '0')) AS customer_id,
# MAGIC     CONCAT('PROD', LPAD(CAST(MOD(id * 13, 12) + 1 AS STRING), 3, '0')) AS product_id,
# MAGIC     DATE_ADD('2024-01-01', CAST(MOD(id * 3, 90) AS INT)) AS order_date,
# MAGIC     TIMESTAMP(DATE_ADD('2024-01-01', CAST(MOD(id * 3, 90) AS INT))) AS order_timestamp,
# MAGIC     CAST(MOD(id * 5, 3) AS INT) + 1 AS quantity,
# MAGIC     CASE 
# MAGIC         WHEN MOD(id, 10) > 8 THEN 'Cancelled'
# MAGIC         WHEN MOD(id, 20) = 19 THEN 'Returned'
# MAGIC         ELSE 'Completed'
# MAGIC     END AS order_status
# MAGIC FROM RANGE(1, 101); -- 100 orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify orders table
# MAGIC SELECT * FROM academy.silver.orders ORDER BY order_date LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Step 2: Create Gold-Layer Dimension Table
# MAGIC 
# MAGIC **Dimension Table**: `dim_product`
# MAGIC - Contains product attributes for analysis
# MAGIC - Includes calculated field: profit_margin

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create product dimension table
# MAGIC CREATE OR REPLACE TABLE academy.gold.dim_product AS
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     subcategory,
# MAGIC     brand,
# MAGIC     retail_price,
# MAGIC     cost_price,
# MAGIC     retail_price - cost_price AS profit_margin  -- Calculated field
# MAGIC FROM academy.silver.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify dimension table
# MAGIC SELECT * FROM academy.gold.dim_product ORDER BY category, product_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 3: Create Gold-Layer Fact Table
# MAGIC 
# MAGIC **Fact Table**: `fact_sales`
# MAGIC - **Grain**: One row per order (simplified for demo)
# MAGIC - **Metrics**: quantity, unit_price, line_total, profit
# MAGIC - **Business Rule**: Only include completed orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sales fact table
# MAGIC CREATE OR REPLACE TABLE academy.gold.fact_sales AS
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     o.customer_id,
# MAGIC     o.product_id,
# MAGIC     o.order_date,
# MAGIC     o.quantity,
# MAGIC     p.retail_price AS unit_price,
# MAGIC     o.quantity * p.retail_price AS line_total,
# MAGIC     (o.quantity * p.retail_price) - (o.quantity * p.cost_price) AS profit
# MAGIC FROM academy.silver.orders o
# MAGIC JOIN academy.silver.products p ON o.product_id = p.product_id
# MAGIC WHERE o.order_status = 'Completed';  -- Business rule: only completed orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify fact table
# MAGIC SELECT COUNT(*) AS total_orders, 
# MAGIC        SUM(line_total) AS total_revenue,
# MAGIC        SUM(profit) AS total_profit
# MAGIC FROM academy.gold.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Step 4: Join Fact and Dimension
# MAGIC 
# MAGIC Demonstrate how easy it is to analyze data with star schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Sales by product with details
# MAGIC SELECT 
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     p.brand,
# MAGIC     COUNT(f.order_id) AS order_count,
# MAGIC     SUM(f.quantity) AS total_units_sold,
# MAGIC     SUM(f.line_total) AS total_revenue,
# MAGIC     SUM(f.profit) AS total_profit
# MAGIC FROM academy.gold.fact_sales f
# MAGIC JOIN academy.gold.dim_product p ON f.product_id = p.product_id
# MAGIC GROUP BY p.product_name, p.category, p.brand
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Step 5: Business Analytics Queries
# MAGIC 
# MAGIC These queries will power our dashboard visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Sales KPIs
# MAGIC **Business Question**: "What are our key sales metrics?"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate key performance indicators
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC     SUM(line_total) AS total_revenue,
# MAGIC     AVG(line_total) AS avg_order_value,
# MAGIC     SUM(profit) AS total_profit,
# MAGIC     ROUND((SUM(profit) / SUM(line_total)) * 100, 2) AS profit_margin_pct
# MAGIC FROM academy.gold.fact_sales
# MAGIC WHERE order_date >= '2024-01-01';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Revenue by Category
# MAGIC **Business Question**: "Which product categories perform best?"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue analysis by category
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     COUNT(DISTINCT f.order_id) AS order_count,
# MAGIC     SUM(f.line_total) AS total_revenue,
# MAGIC     ROUND(AVG(f.line_total), 2) AS avg_order_value,
# MAGIC     SUM(f.profit) AS total_profit
# MAGIC FROM academy.gold.fact_sales f
# MAGIC JOIN academy.gold.dim_product p ON f.product_id = p.product_id
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Monthly Sales Trend
# MAGIC **Business Question**: "How are sales trending month-over-month?"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly trend with growth calculation
# MAGIC WITH monthly_sales AS (
# MAGIC     SELECT 
# MAGIC         DATE_TRUNC('month', order_date) AS month,
# MAGIC         COUNT(DISTINCT order_id) AS order_count,
# MAGIC         SUM(line_total) AS revenue,
# MAGIC         SUM(profit) AS profit
# MAGIC     FROM academy.gold.fact_sales
# MAGIC     GROUP BY DATE_TRUNC('month', order_date)
# MAGIC )
# MAGIC SELECT 
# MAGIC     month,
# MAGIC     order_count,
# MAGIC     revenue,
# MAGIC     profit,
# MAGIC     LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
# MAGIC     revenue - LAG(revenue) OVER (ORDER BY month) AS revenue_change,
# MAGIC     ROUND(((revenue - LAG(revenue) OVER (ORDER BY month)) / 
# MAGIC      LAG(revenue) OVER (ORDER BY month) * 100), 2) AS growth_pct
# MAGIC FROM monthly_sales
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: Top Products
# MAGIC **Business Question**: "What are our best-selling products?"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 products by revenue
# MAGIC SELECT 
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     p.brand,
# MAGIC     COUNT(f.order_id) AS order_count,
# MAGIC     SUM(f.quantity) AS units_sold,
# MAGIC     SUM(f.line_total) AS total_revenue,
# MAGIC     SUM(f.profit) AS total_profit,
# MAGIC     ROUND((SUM(f.profit) / SUM(f.line_total)) * 100, 2) AS profit_margin_pct
# MAGIC FROM academy.gold.fact_sales f
# MAGIC JOIN academy.gold.dim_product p ON f.product_id = p.product_id
# MAGIC GROUP BY p.product_name, p.category, p.brand
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎨 Step 6: Dashboard Preparation
# MAGIC 
# MAGIC **Next Steps** (Demonstrated in Databricks SQL):
# MAGIC 1. Navigate to **Databricks SQL** → **Dashboards**
# MAGIC 2. Create new dashboard: "Sales Performance Dashboard"
# MAGIC 3. Add visualizations:
# MAGIC    - **Counter**: Total Revenue (from Query 1)
# MAGIC    - **Counter**: Total Orders (from Query 1)
# MAGIC    - **Bar Chart**: Revenue by Category (from Query 2)
# MAGIC    - **Line Chart**: Monthly Trend (from Query 3)
# MAGIC    - **Table**: Top Products (from Query 4)
# MAGIC 4. Arrange layout and apply styling
# MAGIC 5. Share with team

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary
# MAGIC 
# MAGIC **What We Built**:
# MAGIC - ✅ Sample e-commerce data (products + orders)
# MAGIC - ✅ Gold-layer dimension table (`dim_product`)
# MAGIC - ✅ Gold-layer fact table (`fact_sales`)
# MAGIC - ✅ 4 business analytics queries
# MAGIC - ✅ Data ready for dashboard visualizations
# MAGIC 
# MAGIC **Key Concepts**:
# MAGIC - **Star Schema**: Fact table surrounded by dimension tables
# MAGIC - **Grain**: One row per order (in our fact table)
# MAGIC - **Business Rules**: Only completed orders included
# MAGIC - **Calculated Fields**: Profit margin in dimension
# MAGIC 
# MAGIC **Next Steps**:
# MAGIC - Complete Week 8 assignment
# MAGIC - Build your own dashboard in Databricks SQL
# MAGIC - Explore optimization techniques (Z-ordering, partitioning)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Additional Practice
# MAGIC 
# MAGIC Try these exercises on your own:
# MAGIC 
# MAGIC 1. **Create a customer dimension**: `gold.dim_customer`
# MAGIC 2. **Add a date dimension**: `gold.dim_date`
# MAGIC 3. **Write a query**: Revenue by brand
# MAGIC 4. **Optimize**: Apply Z-ordering to `fact_sales`
# MAGIC 5. **Create aggregation**: Daily sales summary table

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Clogenai Academy – Data Pipeline Engineering Track**  
# MAGIC **Week 8 Live Demo Notebook**  
# MAGIC **Version 1.0 – December 2025**
