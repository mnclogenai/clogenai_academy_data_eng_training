# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Week 8: Sales Dashboard - Complete Demo
# MAGIC **Clogenai Academy – Data Pipeline Engineering Track**
# MAGIC 
# MAGIC ## What This Notebook Does
# MAGIC 1. ✅ Generates sample e-commerce data
# MAGIC 2. ✅ Creates Gold-layer tables (dimension + fact)
# MAGIC 3. ✅ Provides all 6 dashboard queries ready for visualization
# MAGIC 4. ✅ Includes instructions for building the dashboard
# MAGIC 
# MAGIC **Total Time**: 15-20 minutes
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Step 1: Setup Catalog and Schemas

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
# MAGIC ## 📦 Step 2: Generate Sample Data
# MAGIC 
# MAGIC Creating realistic e-commerce data:
# MAGIC - **12 products** across 3 categories (Electronics, Clothing, Home & Garden)
# MAGIC - **100 orders** over 3 months (Jan-Mar 2024)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample products
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
# MAGIC FROM RANGE(1, 13);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify products
# MAGIC SELECT * FROM academy.silver.products ORDER BY category, product_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample orders
# MAGIC CREATE OR REPLACE TABLE academy.silver.orders AS
# MAGIC SELECT 
# MAGIC     CONCAT('ORD', LPAD(CAST(id AS STRING), 4, '0')) AS order_id,
# MAGIC     CONCAT('CUST', LPAD(CAST(MOD(id * 7, 50) + 1 AS STRING), 3, '0')) AS customer_id,
# MAGIC     CONCAT('PROD', LPAD(CAST(MOD(id * 13, 12) + 1 AS STRING), 3, '0')) AS product_id,
# MAGIC     DATE_ADD('2024-01-01', CAST(MOD(id * 3, 90) AS INT)) AS order_date,
# MAGIC     CAST(MOD(id * 5, 3) AS INT) + 1 AS quantity,
# MAGIC     CASE 
# MAGIC         WHEN MOD(id, 10) > 8 THEN 'Cancelled'
# MAGIC         WHEN MOD(id, 20) = 19 THEN 'Returned'
# MAGIC         ELSE 'Completed'
# MAGIC     END AS order_status
# MAGIC FROM RANGE(1, 101);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify orders
# MAGIC SELECT * FROM academy.silver.orders ORDER BY order_date LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Step 3: Create Gold-Layer Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create product dimension
# MAGIC CREATE OR REPLACE TABLE academy.gold.dim_product AS
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     subcategory,
# MAGIC     brand,
# MAGIC     retail_price,
# MAGIC     cost_price,
# MAGIC     retail_price - cost_price AS profit_margin
# MAGIC FROM academy.silver.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify dimension
# MAGIC SELECT * FROM academy.gold.dim_product ORDER BY category, product_name;

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
# MAGIC WHERE o.order_status = 'Completed';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify fact table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(line_total) AS total_revenue,
# MAGIC     SUM(profit) AS total_profit
# MAGIC FROM academy.gold.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 DASHBOARD QUERIES
# MAGIC 
# MAGIC The following queries are ready to be used for dashboard visualizations.
# MAGIC 
# MAGIC **Instructions**:
# MAGIC 1. Run each query below to verify results
# MAGIC 2. In Databricks SQL, create a new query for each
# MAGIC 3. Copy the SQL from below
# MAGIC 4. Add visualization to each query
# MAGIC 5. Add all visualizations to a dashboard
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Query 1: Total Revenue (KPI Counter)
# MAGIC 
# MAGIC **Visualization Type**: Counter  
# MAGIC **Format**: Currency ($#,##0)  
# MAGIC **Color**: Green

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DASHBOARD QUERY 1: Total Revenue
# MAGIC SELECT 
# MAGIC     SUM(line_total) AS total_revenue
# MAGIC FROM academy.gold.fact_sales
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND order_date <= '2024-03-31';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Query 2: Total Orders (KPI Counter)
# MAGIC 
# MAGIC **Visualization Type**: Counter  
# MAGIC **Format**: Number (#,##0)  
# MAGIC **Color**: Blue

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DASHBOARD QUERY 2: Total Orders
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT order_id) AS total_orders
# MAGIC FROM academy.gold.fact_sales
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND order_date <= '2024-03-31';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💵 Query 3: Average Order Value (KPI Counter)
# MAGIC 
# MAGIC **Visualization Type**: Counter  
# MAGIC **Format**: Currency ($#,##0.00)  
# MAGIC **Color**: Purple

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DASHBOARD QUERY 3: Average Order Value
# MAGIC SELECT 
# MAGIC     ROUND(AVG(line_total), 2) AS avg_order_value
# MAGIC FROM academy.gold.fact_sales
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND order_date <= '2024-03-31';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Query 4: Monthly Revenue Trend (Line Chart)
# MAGIC 
# MAGIC **Visualization Type**: Line Chart  
# MAGIC **X-Axis**: month (format as MMM YYYY)  
# MAGIC **Y-Axis**: revenue (format as currency)  
# MAGIC **Line Color**: Blue

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DASHBOARD QUERY 4: Monthly Revenue Trend
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('month', order_date) AS month,
# MAGIC     SUM(line_total) AS revenue,
# MAGIC     COUNT(DISTINCT order_id) AS order_count
# MAGIC FROM academy.gold.fact_sales
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND order_date <= '2024-03-31'
# MAGIC GROUP BY DATE_TRUNC('month', order_date)
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Query 5: Revenue by Category (Bar Chart)
# MAGIC 
# MAGIC **Visualization Type**: Bar Chart (Horizontal)  
# MAGIC **X-Axis**: total_revenue (format as currency)  
# MAGIC **Y-Axis**: category  
# MAGIC **Colors**: Electronics (Blue), Clothing (Orange), Home & Garden (Green)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DASHBOARD QUERY 5: Revenue by Category
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     SUM(f.line_total) AS total_revenue,
# MAGIC     COUNT(DISTINCT f.order_id) AS order_count
# MAGIC FROM academy.gold.fact_sales f
# MAGIC JOIN academy.gold.dim_product p ON f.product_id = p.product_id
# MAGIC WHERE f.order_date >= '2024-01-01'
# MAGIC   AND f.order_date <= '2024-03-31'
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 Query 6: Top 10 Products (Table)
# MAGIC 
# MAGIC **Visualization Type**: Table  
# MAGIC **Columns**: product, category, units_sold, revenue, profit_margin_pct  
# MAGIC **Format**: Revenue as currency, profit_margin_pct as percentage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DASHBOARD QUERY 6: Top 10 Products
# MAGIC SELECT 
# MAGIC     p.product_name AS product,
# MAGIC     p.category,
# MAGIC     SUM(f.quantity) AS units_sold,
# MAGIC     SUM(f.line_total) AS revenue,
# MAGIC     ROUND((SUM(f.profit) / SUM(f.line_total)) * 100, 1) AS profit_margin_pct
# MAGIC FROM academy.gold.fact_sales f
# MAGIC JOIN academy.gold.dim_product p ON f.product_id = p.product_id
# MAGIC WHERE f.order_date >= '2024-01-01'
# MAGIC   AND f.order_date <= '2024-03-31'
# MAGIC GROUP BY p.product_name, p.category
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎨 Building the Dashboard
# MAGIC 
# MAGIC ## Step-by-Step Instructions
# MAGIC 
# MAGIC ### 1. Navigate to Databricks SQL
# MAGIC - Click on **SQL** in the left sidebar
# MAGIC - Ensure your SQL warehouse is running
# MAGIC 
# MAGIC ### 2. Create Queries
# MAGIC For each of the 6 queries above:
# MAGIC 1. Click **Queries** → **Create Query**
# MAGIC 2. Name the query (e.g., "Dashboard - Total Revenue")
# MAGIC 3. Copy the SQL from the corresponding cell above
# MAGIC 4. Paste and run to verify results
# MAGIC 5. Click **Save**
# MAGIC 
# MAGIC ### 3. Add Visualizations
# MAGIC For each saved query:
# MAGIC 1. Click **Add Visualization**
# MAGIC 2. Select the chart type (Counter, Line Chart, Bar Chart, or Table)
# MAGIC 3. Configure settings:
# MAGIC    - **Query 1-3 (Counters)**: Select value column, set format, choose color
# MAGIC    - **Query 4 (Line Chart)**: X-axis = month, Y-axis = revenue
# MAGIC    - **Query 5 (Bar Chart)**: X-axis = total_revenue, Y-axis = category
# MAGIC    - **Query 6 (Table)**: All columns, format revenue and percentage
# MAGIC 4. Click **Save**
# MAGIC 
# MAGIC ### 4. Create Dashboard
# MAGIC 1. Go to **Dashboards** → **Create Dashboard**
# MAGIC 2. Name: "Sales Performance Dashboard"
# MAGIC 3. Description: "Executive sales metrics for Q1 2024"
# MAGIC 4. Click **Create**
# MAGIC 
# MAGIC ### 5. Add Widgets to Dashboard
# MAGIC 1. Click **Add** → **Visualization**
# MAGIC 2. Select each visualization you created
# MAGIC 3. Arrange in this layout:
# MAGIC    ```
# MAGIC    Row 1: [Total Revenue] [Total Orders] [Avg Order Value]
# MAGIC    Row 2: [Monthly Revenue Trend - full width]
# MAGIC    Row 3: [Revenue by Category] [Top 10 Products]
# MAGIC    ```
# MAGIC 4. Resize widgets to match the layout
# MAGIC 
# MAGIC ### 6. Polish & Share
# MAGIC 1. Click **Done Editing**
# MAGIC 2. Review the final dashboard
# MAGIC 3. Click **Share** to grant access to team members
# MAGIC 4. Set up auto-refresh schedule (optional)
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verification Checklist
# MAGIC 
# MAGIC Before building the dashboard, verify:
# MAGIC 
# MAGIC - [ ] `academy.silver.products` table has 12 rows
# MAGIC - [ ] `academy.silver.orders` table has ~100 rows
# MAGIC - [ ] `academy.gold.dim_product` table has 12 rows
# MAGIC - [ ] `academy.gold.fact_sales` table has 85-95 rows (completed orders only)
# MAGIC - [ ] All 6 dashboard queries return results
# MAGIC - [ ] SQL warehouse is running
# MAGIC 
# MAGIC **If any checks fail, re-run the corresponding cells above.**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick verification query
# MAGIC SELECT 
# MAGIC     'academy.silver.products' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM academy.silver.products
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'academy.silver.orders' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM academy.silver.orders
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'academy.gold.dim_product' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM academy.gold.dim_product
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'academy.gold.fact_sales' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM academy.gold.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Additional Resources
# MAGIC 
# MAGIC - **Dashboard Layout Reference**: See `dashboard_layout_reference.md`
# MAGIC - **Dashboard Demo Guide**: See `dashboard_demo_guide.md`
# MAGIC - **Week 8 Assignment**: See `week8_real_world_assignment.md`
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🎓 What You Learned
# MAGIC 
# MAGIC ✅ Creating Gold-layer dimension and fact tables  
# MAGIC ✅ Writing business analytics queries  
# MAGIC ✅ Preparing data for dashboard visualizations  
# MAGIC ✅ Building interactive Databricks dashboards  
# MAGIC 
# MAGIC **Next Steps**: Complete the Week 8 assignment to build your own dashboard!
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC *Clogenai Academy – Data Pipeline Engineering Track*  
# MAGIC *Week 8 Complete Dashboard Demo*  
# MAGIC *Version 1.0 – December 2025*
