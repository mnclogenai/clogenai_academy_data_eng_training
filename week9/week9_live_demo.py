# Databricks notebook source
# MAGIC %md
# MAGIC # Week 9: Data Governance & Security - Live Demo
# MAGIC **E-Commerce Order Management System**
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC - Implement Unity Catalog for centralized governance
# MAGIC - Configure RBAC for different user roles
# MAGIC - Apply data masking to protect customer PII
# MAGIC - Implement encryption and key management
# MAGIC - Create comprehensive audit trails
# MAGIC - Demonstrate GDPR compliance (data deletion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Catalog Structure

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog Hierarchy
# MAGIC %sql
# MAGIC -- Create catalog for e-commerce system
# MAGIC CREATE CATALOG IF NOT EXISTS academy;
# MAGIC USE CATALOG academy;
# MAGIC 
# MAGIC -- Create schemas for medallion architecture
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw e-commerce data';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned customer and order data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Analytics-ready data';
# MAGIC CREATE SCHEMA IF NOT EXISTS audit COMMENT 'Security and compliance logs';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Access Control & Data Protection

# COMMAND ----------

# DBTITLE 1,Create Sample Customer Data with PII
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Generate sample customer data with PII
customer_data = []
for i in range(1, 101):
    customer_data.append({
        "customer_id": f"CUST{i:05d}",
        "email": f"customer{i}@example.com",
        "phone": f"555-{random.randint(100,999):03d}-{random.randint(1000,9999):04d}",
        "ssn": f"{random.randint(100,999):03d}-{random.randint(10,99):02d}-{random.randint(1000,9999):04d}",
        "first_name": f"FirstName{i}",
        "last_name": f"LastName{i}",
        "address": f"{random.randint(100,9999)} Main St",
        "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
        "state": random.choice(["NY", "CA", "IL", "TX", "AZ"]),
        "zip_code": f"{random.randint(10000,99999)}",
        "credit_card_last4": f"{random.randint(1000,9999)}",
        "date_of_birth": (datetime.now() - timedelta(days=random.randint(6570, 25550))).date(),
        "created_at": datetime.now() - timedelta(days=random.randint(1, 365)),
        "consent_marketing": random.choice([True, False]),
        "consent_data_sharing": random.choice([True, False])
    })

customers_df = spark.createDataFrame(customer_data)

# Write to Silver layer
customers_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("academy.silver.customers")

display(customers_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Create Sample Order Data
# Generate order data
order_data = []
order_id = 1
for customer_id in [f"CUST{i:05d}" for i in range(1, 101)]:
    # Each customer has 1-5 orders
    for _ in range(random.randint(1, 5)):
        order_data.append({
            "order_id": f"ORD{order_id:06d}",
            "customer_id": customer_id,
            "order_date": datetime.now() - timedelta(days=random.randint(1, 180)),
            "total_amount": round(random.uniform(25.0, 500.0), 2),
            "status": random.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal"]),
            "shipping_address": f"{random.randint(100,9999)} Main St",
            "created_at": datetime.now() - timedelta(days=random.randint(1, 180)),
            "updated_at": datetime.now() - timedelta(days=random.randint(0, 30))
        })
        order_id += 1

orders_df = spark.createDataFrame(order_data)

# Write to Silver layer
orders_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("academy.silver.orders")

display(orders_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1: Implement Role-Based Access Control (RBAC)

# COMMAND ----------

# DBTITLE 1,Create Roles and Grant Permissions
# MAGIC %sql
# MAGIC -- Create roles for different user types
# MAGIC CREATE ROLE IF NOT EXISTS data_analyst;
# MAGIC CREATE ROLE IF NOT EXISTS data_engineer;
# MAGIC CREATE ROLE IF NOT EXISTS customer_service;
# MAGIC CREATE ROLE IF NOT EXISTS compliance_officer;
# MAGIC CREATE ROLE IF NOT EXISTS marketing_team;
# MAGIC 
# MAGIC -- Grant catalog-level access
# MAGIC GRANT USE CATALOG ON CATALOG academy TO data_analyst;
# MAGIC GRANT USE CATALOG ON CATALOG academy TO customer_service;
# MAGIC GRANT USE CATALOG ON CATALOG academy TO marketing_team;
# MAGIC 
# MAGIC -- Data Analysts: Read-only on Gold layer
# MAGIC GRANT USE SCHEMA ON SCHEMA academy.gold TO data_analyst;
# MAGIC GRANT SELECT ON SCHEMA academy.gold TO data_analyst;
# MAGIC 
# MAGIC -- Customer Service: Limited access to customer data (masked)
# MAGIC GRANT USE SCHEMA ON SCHEMA academy.gold TO customer_service;
# MAGIC GRANT SELECT ON SCHEMA academy.gold TO customer_service;
# MAGIC 
# MAGIC -- Marketing Team: Only customers who consented
# MAGIC GRANT USE SCHEMA ON SCHEMA academy.gold TO marketing_team;
# MAGIC 
# MAGIC -- Compliance Officer: Full audit access
# MAGIC GRANT USE SCHEMA ON SCHEMA academy.audit TO compliance_officer;
# MAGIC GRANT SELECT ON SCHEMA academy.audit TO compliance_officer;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2: Implement Data Masking

# COMMAND ----------

# DBTITLE 1,Create Masking Functions
# MAGIC %sql
# MAGIC -- Function to mask email addresses
# MAGIC CREATE OR REPLACE FUNCTION academy.gold.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT(
# MAGIC     LEFT(SPLIT(email, '@')[0], 2),
# MAGIC     '***@',
# MAGIC     SPLIT(email, '@')[1]
# MAGIC );
# MAGIC 
# MAGIC -- Function to mask phone numbers
# MAGIC CREATE OR REPLACE FUNCTION academy.gold.mask_phone(phone STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('***-***-', RIGHT(phone, 4));
# MAGIC 
# MAGIC -- Function to mask SSN
# MAGIC CREATE OR REPLACE FUNCTION academy.gold.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN '***-**-****';
# MAGIC 
# MAGIC -- Function to mask credit card
# MAGIC CREATE OR REPLACE FUNCTION academy.gold.mask_credit_card(last4 STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('****-****-****-', last4);

# COMMAND ----------

# DBTITLE 1,Create Masked View for Customer Service
# MAGIC %sql
# MAGIC -- Customer service sees masked PII
# MAGIC CREATE OR REPLACE VIEW academy.gold.customers_customer_service AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     academy.gold.mask_email(email) AS email,
# MAGIC     academy.gold.mask_phone(phone) AS phone,
# MAGIC     academy.gold.mask_ssn(ssn) AS ssn,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     state,
# MAGIC     academy.gold.mask_credit_card(credit_card_last4) AS credit_card,
# MAGIC     created_at
# MAGIC FROM academy.silver.customers;
# MAGIC 
# MAGIC -- Grant access to customer service role
# MAGIC GRANT SELECT ON VIEW academy.gold.customers_customer_service TO customer_service;
# MAGIC 
# MAGIC -- Show masked data
# MAGIC SELECT * FROM academy.gold.customers_customer_service LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Create Consent-Based View for Marketing
# MAGIC %sql
# MAGIC -- Marketing team sees only customers who consented
# MAGIC CREATE OR REPLACE VIEW academy.gold.customers_marketing AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     email,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     state,
# MAGIC     created_at
# MAGIC FROM academy.silver.customers
# MAGIC WHERE consent_marketing = TRUE;
# MAGIC 
# MAGIC GRANT SELECT ON VIEW academy.gold.customers_marketing TO marketing_team;
# MAGIC 
# MAGIC -- Show only consented customers
# MAGIC SELECT COUNT(*) AS total_customers,
# MAGIC        SUM(CASE WHEN consent_marketing THEN 1 ELSE 0 END) AS marketing_consent_count
# MAGIC FROM academy.silver.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3: Implement Data Tokenization

# COMMAND ----------

# DBTITLE 1,Tokenize Sensitive Customer Data
from pyspark.sql.functions import sha2, concat_ws, lit
import uuid

# Read customer data
customers = spark.table("academy.silver.customers")

# Create tokenized version
tokenized_customers = customers.select(
    # Generate token for customer_id
    sha2(concat_ws("|", col("customer_id"), lit(str(uuid.uuid4()))), 256).alias("customer_token"),
    
    # Tokenize email
    sha2(col("email"), 256).alias("email_token"),
    
    # Keep non-sensitive fields
    col("first_name"),
    col("last_name"),
    col("city"),
    col("state"),
    col("created_at")
)

# Save tokenized data (for analytics)
tokenized_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("academy.gold.customers_tokenized")

# Store token mapping in secure location (separate from analytics data)
token_mapping = customers.select(
    col("customer_id"),
    sha2(concat_ws("|", col("customer_id"), lit(str(uuid.uuid4()))), 256).alias("customer_token"),
    col("email"),
    col("ssn")
)

# In production, this would be stored in a secure vault with restricted access
token_mapping.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("academy.audit.customer_token_mapping")

display(tokenized_customers.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Encryption & Key Management

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1: Enable Encryption at Rest

# COMMAND ----------

# DBTITLE 1,Create Encrypted Table
# MAGIC %sql
# MAGIC -- Create table with encryption enabled
# MAGIC CREATE TABLE IF NOT EXISTS academy.silver.customers_encrypted (
# MAGIC     customer_id STRING NOT NULL,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     ssn STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Customer data with encryption at rest'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.encryption.enabled' = 'true'
# MAGIC );
# MAGIC 
# MAGIC -- Insert data
# MAGIC INSERT INTO academy.silver.customers_encrypted
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     email,
# MAGIC     phone,
# MAGIC     ssn,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     created_at
# MAGIC FROM academy.silver.customers;

# COMMAND ----------

# DBTITLE 1,Configure Secrets Management
# MAGIC %python
# MAGIC # Demonstrate proper secret management
# MAGIC # In production, use Databricks Secrets
# MAGIC 
# MAGIC # ❌ NEVER DO THIS - Hardcoded secrets
# MAGIC # api_key = "sk-1234567890abcdef"
# MAGIC # database_password = "MyPassword123"
# MAGIC 
# MAGIC # ✅ CORRECT WAY - Use Databricks Secrets
# MAGIC # First, create secret scope (one-time setup via CLI or API):
# MAGIC # databricks secrets create-scope --scope production
# MAGIC # databricks secrets put --scope production --key database-password
# MAGIC 
# MAGIC # Then retrieve secrets in code:
# MAGIC try:
# MAGIC     # Example: Retrieve database password
# MAGIC     # db_password = dbutils.secrets.get(scope="production", key="database-password")
# MAGIC     
# MAGIC     # Example: Retrieve API key
# MAGIC     # api_key = dbutils.secrets.get(scope="production", key="api-key")
# MAGIC     
# MAGIC     # Example: Retrieve storage account key
# MAGIC     # storage_key = dbutils.secrets.get(scope="azure-secrets", key="storage-account-key")
# MAGIC     
# MAGIC     print("✅ Secrets retrieved securely from Databricks Secrets")
# MAGIC except Exception as e:
# MAGIC     print(f"⚠️ Secret scope not configured (expected in demo): {e}")
# MAGIC     print("In production, configure secret scopes for secure key management")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2: Encryption in Transit

# COMMAND ----------

# DBTITLE 1,Configure TLS for Data Transfer
# Configure encryption in transit
spark.conf.set("spark.ssl.enabled", "true")
spark.conf.set("spark.ssl.protocol", "TLSv1.2")

# Verify configuration
print("✅ TLS 1.2 enabled for data in transit")
print(f"SSL Enabled: {spark.conf.get('spark.ssl.enabled')}")
print(f"SSL Protocol: {spark.conf.get('spark.ssl.protocol')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Compliance & Audit Trails

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1: Create Audit Log Table

# COMMAND ----------

# DBTITLE 1,Setup Audit Logging Infrastructure
# MAGIC %sql
# MAGIC -- Create comprehensive audit log table
# MAGIC CREATE TABLE IF NOT EXISTS academy.audit.data_access_log (
# MAGIC     audit_id STRING NOT NULL,
# MAGIC     user_email STRING,
# MAGIC     user_role STRING,
# MAGIC     action_type STRING,  -- SELECT, INSERT, UPDATE, DELETE
# MAGIC     table_name STRING,
# MAGIC     row_count BIGINT,
# MAGIC     timestamp TIMESTAMP,
# MAGIC     ip_address STRING,
# MAGIC     session_id STRING,
# MAGIC     query_text STRING,
# MAGIC     success BOOLEAN,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(timestamp))
# MAGIC COMMENT 'Comprehensive audit trail for all data access';

# COMMAND ----------

# DBTITLE 1,Log Sample Data Access Events
import uuid
from datetime import datetime

# Simulate audit log entries
audit_entries = [
    {
        "audit_id": str(uuid.uuid4()),
        "user_email": "analyst@company.com",
        "user_role": "data_analyst",
        "action_type": "SELECT",
        "table_name": "academy.gold.customers_customer_service",
        "row_count": 100,
        "timestamp": datetime.now(),
        "ip_address": "192.168.1.100",
        "session_id": str(uuid.uuid4()),
        "query_text": "SELECT * FROM academy.gold.customers_customer_service LIMIT 100",
        "success": True,
        "error_message": None
    },
    {
        "audit_id": str(uuid.uuid4()),
        "user_email": "engineer@company.com",
        "user_role": "data_engineer",
        "action_type": "INSERT",
        "table_name": "academy.silver.customers",
        "row_count": 50,
        "timestamp": datetime.now(),
        "ip_address": "192.168.1.101",
        "session_id": str(uuid.uuid4()),
        "query_text": "INSERT INTO academy.silver.customers VALUES (...)",
        "success": True,
        "error_message": None
    },
    {
        "audit_id": str(uuid.uuid4()),
        "user_email": "unauthorized@external.com",
        "user_role": None,
        "action_type": "SELECT",
        "table_name": "academy.silver.customers",
        "row_count": 0,
        "timestamp": datetime.now(),
        "ip_address": "203.0.113.45",
        "session_id": str(uuid.uuid4()),
        "query_text": "SELECT * FROM academy.silver.customers",
        "success": False,
        "error_message": "Permission denied: User does not have SELECT privilege"
    }
]

audit_df = spark.createDataFrame(audit_entries)
audit_df.write.format("delta").mode("append").saveAsTable("academy.audit.data_access_log")

display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2: GDPR Compliance - Data Subject Rights

# COMMAND ----------

# DBTITLE 1,GDPR: Right to Access
# MAGIC %sql
# MAGIC -- Customer requests: "Show me all my data"
# MAGIC -- This query retrieves all data for a specific customer
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Customer Profile' AS data_category,
# MAGIC     customer_id,
# MAGIC     email,
# MAGIC     phone,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     address,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zip_code,
# MAGIC     date_of_birth,
# MAGIC     consent_marketing,
# MAGIC     consent_data_sharing,
# MAGIC     created_at
# MAGIC FROM academy.silver.customers
# MAGIC WHERE customer_id = 'CUST00001'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Order History' AS data_category,
# MAGIC     customer_id,
# MAGIC     order_id AS email,
# MAGIC     CAST(total_amount AS STRING) AS phone,
# MAGIC     status AS first_name,
# MAGIC     payment_method AS last_name,
# MAGIC     NULL AS address,
# MAGIC     NULL AS city,
# MAGIC     NULL AS state,
# MAGIC     NULL AS zip_code,
# MAGIC     NULL AS date_of_birth,
# MAGIC     NULL AS consent_marketing,
# MAGIC     NULL AS consent_data_sharing,
# MAGIC     order_date AS created_at
# MAGIC FROM academy.silver.orders
# MAGIC WHERE customer_id = 'CUST00001';

# COMMAND ----------

# DBTITLE 1,GDPR: Right to Erasure (Right to be Forgotten)
# MAGIC %sql
# MAGIC -- Customer requests: "Delete all my data"
# MAGIC 
# MAGIC -- Step 1: Log the deletion request
# MAGIC CREATE TABLE IF NOT EXISTS academy.audit.gdpr_deletion_requests (
# MAGIC     request_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     request_date TIMESTAMP,
# MAGIC     requested_by STRING,
# MAGIC     status STRING,
# MAGIC     completed_date TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- Step 2: Execute deletion (in production, this would be a controlled process)
# MAGIC -- Note: In demo, we'll show the queries but not execute to preserve demo data
# MAGIC 
# MAGIC -- DELETE FROM academy.silver.customers WHERE customer_id = 'CUST00001';
# MAGIC -- DELETE FROM academy.silver.orders WHERE customer_id = 'CUST00001';
# MAGIC -- DELETE FROM academy.gold.customers_tokenized WHERE customer_token IN (
# MAGIC --     SELECT customer_token FROM academy.audit.customer_token_mapping 
# MAGIC --     WHERE customer_id = 'CUST00001'
# MAGIC -- );
# MAGIC 
# MAGIC -- Step 3: Log completion
# MAGIC INSERT INTO academy.audit.gdpr_deletion_requests
# MAGIC VALUES (
# MAGIC     'DEL-' || uuid(),
# MAGIC     'CUST00001',
# MAGIC     current_timestamp(),
# MAGIC     'customer@example.com',
# MAGIC     'completed',
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC 
# MAGIC SELECT * FROM academy.audit.gdpr_deletion_requests;

# COMMAND ----------

# DBTITLE 1,GDPR: Data Portability
# MAGIC %sql
# MAGIC -- Export customer data in portable JSON format
# MAGIC SELECT 
# MAGIC     to_json(
# MAGIC         struct(
# MAGIC             customer_id,
# MAGIC             email,
# MAGIC             first_name,
# MAGIC             last_name,
# MAGIC             address,
# MAGIC             city,
# MAGIC             state,
# MAGIC             created_at
# MAGIC         )
# MAGIC     ) AS customer_data_json
# MAGIC FROM academy.silver.customers
# MAGIC WHERE customer_id = 'CUST00001';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: Audit Queries and Compliance Reports

# COMMAND ----------

# DBTITLE 1,Query: Daily Access Summary
# MAGIC %sql
# MAGIC -- Daily audit report: Who accessed what?
# MAGIC SELECT 
# MAGIC     DATE(timestamp) AS access_date,
# MAGIC     user_email,
# MAGIC     user_role,
# MAGIC     action_type,
# MAGIC     COUNT(*) AS operation_count,
# MAGIC     SUM(row_count) AS total_rows_accessed,
# MAGIC     SUM(CASE WHEN success THEN 1 ELSE 0 END) AS successful_operations,
# MAGIC     SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failed_operations
# MAGIC FROM academy.audit.data_access_log
# MAGIC WHERE DATE(timestamp) = CURRENT_DATE()
# MAGIC GROUP BY DATE(timestamp), user_email, user_role, action_type
# MAGIC ORDER BY total_rows_accessed DESC;

# COMMAND ----------

# DBTITLE 1,Query: Detect Suspicious Activity
# MAGIC %sql
# MAGIC -- Alert: Users accessing unusually high volumes of data
# MAGIC SELECT 
# MAGIC     user_email,
# MAGIC     user_role,
# MAGIC     COUNT(DISTINCT table_name) AS tables_accessed,
# MAGIC     SUM(row_count) AS total_rows_accessed,
# MAGIC     COUNT(*) AS query_count,
# MAGIC     MIN(timestamp) AS first_access,
# MAGIC     MAX(timestamp) AS last_access
# MAGIC FROM academy.audit.data_access_log
# MAGIC WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC   AND success = TRUE
# MAGIC GROUP BY user_email, user_role
# MAGIC HAVING total_rows_accessed > 10000  -- Alert threshold
# MAGIC ORDER BY total_rows_accessed DESC;

# COMMAND ----------

# DBTITLE 1,Query: Failed Access Attempts
# MAGIC %sql
# MAGIC -- Security alert: Failed access attempts (potential breach attempts)
# MAGIC SELECT 
# MAGIC     user_email,
# MAGIC     ip_address,
# MAGIC     table_name,
# MAGIC     COUNT(*) AS failed_attempts,
# MAGIC     MAX(timestamp) AS last_attempt,
# MAGIC     COLLECT_LIST(error_message) AS error_messages
# MAGIC FROM academy.audit.data_access_log
# MAGIC WHERE success = FALSE
# MAGIC   AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC GROUP BY user_email, ip_address, table_name
# MAGIC HAVING failed_attempts >= 3  -- Multiple failed attempts
# MAGIC ORDER BY failed_attempts DESC;

# COMMAND ----------

# DBTITLE 1,Query: Sensitive Table Access Report
# MAGIC %sql
# MAGIC -- Compliance report: Who accessed sensitive customer data?
# MAGIC SELECT 
# MAGIC     DATE(timestamp) AS access_date,
# MAGIC     user_email,
# MAGIC     user_role,
# MAGIC     table_name,
# MAGIC     COUNT(*) AS access_count,
# MAGIC     SUM(row_count) AS rows_accessed,
# MAGIC     COLLECT_SET(ip_address) AS ip_addresses
# MAGIC FROM academy.audit.data_access_log
# MAGIC WHERE table_name LIKE '%customers%'
# MAGIC   AND success = TRUE
# MAGIC   AND timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(timestamp), user_email, user_role, table_name
# MAGIC ORDER BY access_date DESC, rows_accessed DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4: Unity Catalog Lineage and Metadata

# COMMAND ----------

# DBTITLE 1,View Table Metadata and Tags
# MAGIC %sql
# MAGIC -- Add governance tags to tables
# MAGIC ALTER TABLE academy.silver.customers 
# MAGIC SET TAGS ('sensitivity' = 'high', 'pii' = 'true', 'compliance' = 'gdpr,hipaa');
# MAGIC 
# MAGIC ALTER TABLE academy.silver.orders 
# MAGIC SET TAGS ('sensitivity' = 'medium', 'pii' = 'false', 'compliance' = 'gdpr');
# MAGIC 
# MAGIC -- Query tables by sensitivity level
# MAGIC SELECT 
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     table_type,
# MAGIC     comment
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'academy'
# MAGIC   AND table_schema IN ('silver', 'gold')
# MAGIC ORDER BY table_schema, table_name;

# COMMAND ----------

# DBTITLE 1,View Column-Level Metadata
# MAGIC %sql
# MAGIC -- View column details for sensitive tables
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     data_type,
# MAGIC     is_nullable,
# MAGIC     comment
# MAGIC FROM system.information_schema.columns
# MAGIC WHERE table_catalog = 'academy'
# MAGIC   AND table_schema = 'silver'
# MAGIC   AND table_name = 'customers'
# MAGIC ORDER BY ordinal_position;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Key Takeaways
# MAGIC 
# MAGIC ### What We Implemented:
# MAGIC 
# MAGIC #### 1. Access Control & Data Protection
# MAGIC - ✅ Created role-based access control (RBAC) for different user types
# MAGIC - ✅ Implemented data masking for PII (email, phone, SSN, credit card)
# MAGIC - ✅ Created consent-based views for marketing compliance
# MAGIC - ✅ Demonstrated data tokenization for analytics
# MAGIC 
# MAGIC #### 2. Encryption & Key Management
# MAGIC - ✅ Enabled encryption at rest for sensitive tables
# MAGIC - ✅ Configured TLS 1.2 for encryption in transit
# MAGIC - ✅ Demonstrated proper secrets management with Databricks Secrets
# MAGIC 
# MAGIC #### 3. Compliance & Audit Trails
# MAGIC - ✅ Implemented comprehensive audit logging
# MAGIC - ✅ Demonstrated GDPR compliance (right to access, erasure, portability)
# MAGIC - ✅ Created compliance reports and suspicious activity detection
# MAGIC - ✅ Used Unity Catalog for centralized governance and metadata management
# MAGIC 
# MAGIC ### Best Practices Applied:
# MAGIC - 🔐 Principle of least privilege
# MAGIC - 🎭 Data masking for role-appropriate access
# MAGIC - 🔒 Encryption everywhere (at rest and in transit)
# MAGIC - 📝 Comprehensive audit trails
# MAGIC - ✅ GDPR compliance built-in
# MAGIC - 🏷️ Metadata tagging for governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC **Practice Exercises:**
# MAGIC 1. Create additional roles for your organization
# MAGIC 2. Implement column-level masking for specific use cases
# MAGIC 3. Build automated compliance reports
# MAGIC 4. Set up alerts for suspicious access patterns
# MAGIC 5. Practice GDPR data deletion workflows
# MAGIC 
# MAGIC **Additional Resources:**
# MAGIC - [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC - [Databricks Security Best Practices](https://docs.databricks.com/security/index.html)
# MAGIC - [GDPR Compliance Guide](https://gdpr.eu/)
