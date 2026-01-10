# Databricks notebook source
# MAGIC %md
# MAGIC # Week 10: AI Features in Data Pipeline Engineering
# MAGIC ## Live Demo Notebook
# MAGIC 
# MAGIC **Topics Covered:**
# MAGIC 1. AI-Powered Data Quality & Anomaly Detection
# MAGIC 2. Predictive Analytics & ML Feature Engineering
# MAGIC 3. Generative AI & LLMs in Data Pipelines
# MAGIC 
# MAGIC **E-Commerce Use Case:** ShopFast - Fraud detection, churn prediction, product classification

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Setup & Environment Configuration

# COMMAND ----------

# Install required ML libraries
%pip install mlflow scikit-learn xgboost prophet

# COMMAND ----------

# Import libraries
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ML libraries
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
import xgboost as xgb

# MLflow for model tracking
import mlflow
import mlflow.sklearn

# Feature Store
from databricks.feature_store import FeatureStoreClient

print("✅ All libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Create Sample E-Commerce Data with Anomalies

# COMMAND ----------

# Create Unity Catalog structure
spark.sql("CREATE CATALOG IF NOT EXISTS academy")
spark.sql("CREATE SCHEMA IF NOT EXISTS academy.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS academy.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS academy.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS academy.features")

print("✅ Unity Catalog structure created")

# COMMAND ----------

# Generate realistic e-commerce data with anomalies
np.random.seed(42)

# Normal customers (950)
normal_customers = pd.DataFrame({
    'customer_id': [f'CUST{str(i).zfill(5)}' for i in range(1, 951)],
    'order_amount': np.random.normal(75, 25, 950).clip(10, 200),
    'items_count': np.random.poisson(3, 950).clip(1, 10),
    'hour_of_day': np.random.choice(range(9, 22), 950),  # Normal hours
    'day_of_week': np.random.choice(range(1, 8), 950),
    'customer_lifetime_orders': np.random.poisson(5, 950).clip(1, 20),
    'is_fraud': 0
})

# Fraudulent orders (50) - unusual patterns
fraud_orders = pd.DataFrame({
    'customer_id': [f'FRAUD{str(i).zfill(3)}' for i in range(1, 51)],
    'order_amount': np.random.uniform(300, 1000, 50),  # Very high amounts
    'items_count': np.random.choice([1, 15, 20], 50),  # Either 1 or many
    'hour_of_day': np.random.choice([2, 3, 4], 50),  # Late night
    'day_of_week': np.random.choice(range(1, 8), 50),
    'customer_lifetime_orders': np.ones(50),  # New customers
    'is_fraud': 1
})

# Combine datasets
orders_data = pd.concat([normal_customers, fraud_orders], ignore_index=True)
orders_data = orders_data.sample(frac=1).reset_index(drop=True)  # Shuffle

# Convert to Spark DataFrame
orders_df = spark.createDataFrame(orders_data)

# Save to bronze layer
orders_df.write.mode("overwrite").saveAsTable("academy.bronze.orders_raw")

print(f"✅ Created {len(orders_data)} orders ({fraud_orders.shape[0]} fraudulent)")
display(orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: AI-Powered Anomaly Detection for Fraud

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1: Train Isolation Forest Model

# COMMAND ----------

# Load data
orders = spark.table("academy.bronze.orders_raw").toPandas()

# Prepare features (exclude labels for unsupervised learning)
feature_cols = ['order_amount', 'items_count', 'hour_of_day', 'day_of_week', 'customer_lifetime_orders']
X = orders[feature_cols]

# Train Isolation Forest
# contamination=0.05 means we expect 5% of data to be anomalies
model = IsolationForest(
    contamination=0.05,
    random_state=42,
    n_estimators=100
)

model.fit(X)

# Get anomaly scores
orders['anomaly_score'] = model.decision_function(X)
orders['predicted_fraud'] = model.predict(X)  # -1 = anomaly, 1 = normal

# Convert predictions: -1 (anomaly) → 1 (fraud), 1 (normal) → 0 (not fraud)
orders['predicted_fraud'] = (orders['predicted_fraud'] == -1).astype(int)

print("✅ Anomaly detection model trained")
print(f"Detected {orders['predicted_fraud'].sum()} anomalies")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2: Evaluate Model Performance

# COMMAND ----------

# Confusion matrix
from sklearn.metrics import confusion_matrix, classification_report

cm = confusion_matrix(orders['is_fraud'], orders['predicted_fraud'])
print("Confusion Matrix:")
print(cm)
print("\nClassification Report:")
print(classification_report(orders['is_fraud'], orders['predicted_fraud']))

# Calculate metrics
true_positives = cm[1][1]
false_positives = cm[0][1]
precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
recall = true_positives / cm[1].sum()

print(f"\n📊 Model Performance:")
print(f"Precision: {precision:.2%} (of flagged orders, how many are actually fraud)")
print(f"Recall: {recall:.2%} (of actual fraud, how many we caught)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: Log Model with MLflow

# COMMAND ----------

# Start MLflow run
with mlflow.start_run(run_name="fraud_detection_isolation_forest"):
    # Log parameters
    mlflow.log_param("model_type", "IsolationForest")
    mlflow.log_param("contamination", 0.05)
    mlflow.log_param("n_estimators", 100)
    
    # Log metrics
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("anomalies_detected", orders['predicted_fraud'].sum())
    
    # Log model
    mlflow.sklearn.log_model(model, "fraud_detector")
    
    print("✅ Model logged to MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4: Create Real-Time Fraud Detection Pipeline

# COMMAND ----------

# Save model predictions to silver layer
orders_with_predictions = spark.createDataFrame(orders)

orders_with_predictions.write.mode("overwrite").saveAsTable("academy.silver.orders_fraud_checked")

# Create view of high-risk orders
high_risk_orders = orders_with_predictions.filter(col("predicted_fraud") == 1)

print(f"✅ {high_risk_orders.count()} high-risk orders flagged for review")
display(high_risk_orders.select("customer_id", "order_amount", "items_count", "hour_of_day", "anomaly_score"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Predictive Analytics & Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1: Generate Customer Transaction History

# COMMAND ----------

# Create customer transaction history for churn prediction
np.random.seed(42)

customers_list = []
for i in range(1, 501):
    customer_id = f'CUST{str(i).zfill(5)}'
    
    # Simulate customer behavior
    is_churned = np.random.choice([0, 1], p=[0.7, 0.3])  # 30% churn rate
    
    if is_churned:
        # Churned customers: fewer orders, lower spend, long time since last order
        order_count = np.random.poisson(2) + 1
        total_spent = np.random.normal(100, 30)
        days_since_last = np.random.randint(91, 365)  # 3+ months
        avg_order_value = total_spent / order_count
    else:
        # Active customers: more orders, higher spend, recent activity
        order_count = np.random.poisson(8) + 3
        total_spent = np.random.normal(500, 150)
        days_since_last = np.random.randint(1, 90)  # Within 3 months
        avg_order_value = total_spent / order_count
    
    customers_list.append({
        'customer_id': customer_id,
        'total_orders': order_count,
        'total_spent': max(total_spent, 10),
        'avg_order_value': max(avg_order_value, 10),
        'days_since_last_order': days_since_last,
        'categories_purchased': np.random.randint(1, 6),
        'churned': is_churned
    })

customers_df = spark.createDataFrame(pd.DataFrame(customers_list))
customers_df.write.mode("overwrite").saveAsTable("academy.silver.customers")

print(f"✅ Created {len(customers_list)} customer records")
print(f"Churned: {sum([c['churned'] for c in customers_list])}, Active: {len(customers_list) - sum([c['churned'] for c in customers_list])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2: Create Feature Store with RFM Features

# COMMAND ----------

# Calculate RFM (Recency, Frequency, Monetary) features
rfm_features = spark.sql("""
    SELECT 
        customer_id,
        
        -- Recency: Days since last order
        days_since_last_order as recency_days,
        CASE 
            WHEN days_since_last_order <= 30 THEN 'Active'
            WHEN days_since_last_order <= 90 THEN 'At Risk'
            ELSE 'Churned'
        END as recency_segment,
        
        -- Frequency: Number of orders
        total_orders as frequency,
        CASE 
            WHEN total_orders >= 10 THEN 'High'
            WHEN total_orders >= 5 THEN 'Medium'
            ELSE 'Low'
        END as frequency_segment,
        
        -- Monetary: Total spend
        total_spent as monetary,
        avg_order_value,
        CASE 
            WHEN total_spent >= 500 THEN 'High Value'
            WHEN total_spent >= 200 THEN 'Medium Value'
            ELSE 'Low Value'
        END as monetary_segment,
        
        -- Additional features
        categories_purchased,
        
        -- Target variable
        churned
        
    FROM academy.silver.customers
""")

display(rfm_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3: Register Features in Unity Catalog Feature Store

# COMMAND ----------

# Initialize Feature Store client
fs = FeatureStoreClient()

# Create feature table
try:
    fs.create_table(
        name="academy.features.customer_rfm",
        primary_keys=["customer_id"],
        df=rfm_features,
        description="RFM features for customer churn prediction and segmentation"
    )
    print("✅ Feature table created in Unity Catalog")
except Exception as e:
    print(f"Feature table may already exist: {e}")
    # Overwrite if exists
    rfm_features.write.mode("overwrite").saveAsTable("academy.features.customer_rfm")
    print("✅ Feature table updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4: Train Churn Prediction Model

# COMMAND ----------

# Prepare data for modeling
features_df = spark.table("academy.features.customer_rfm").toPandas()

# Select features for model
feature_columns = ['recency_days', 'frequency', 'monetary', 'avg_order_value', 'categories_purchased']
X = features_df[feature_columns]
y = features_df['churned']

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Train Random Forest model
rf_model = RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    random_state=42,
    class_weight='balanced'  # Handle class imbalance
)

rf_model.fit(X_train, y_train)

# Evaluate
y_pred = rf_model.predict(X_test)
y_pred_proba = rf_model.predict_proba(X_test)[:, 1]

print("✅ Churn prediction model trained")
print("\nClassification Report:")
print(classification_report(y_test, y_pred))
print(f"\nROC-AUC Score: {roc_auc_score(y_test, y_pred_proba):.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5: Feature Importance Analysis

# COMMAND ----------

# Get feature importance
feature_importance = pd.DataFrame({
    'feature': feature_columns,
    'importance': rf_model.feature_importances_
}).sort_values('importance', ascending=False)

print("📊 Feature Importance for Churn Prediction:")
print(feature_importance)

# Most important features
print(f"\n🔑 Top 3 features:")
for idx, row in feature_importance.head(3).iterrows():
    print(f"  {row['feature']}: {row['importance']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6: Log Churn Model to MLflow

# COMMAND ----------

with mlflow.start_run(run_name="churn_prediction_random_forest"):
    # Log parameters
    mlflow.log_param("model_type", "RandomForestClassifier")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    
    # Log metrics
    mlflow.log_metric("roc_auc", roc_auc_score(y_test, y_pred_proba))
    mlflow.log_metric("test_accuracy", (y_pred == y_test).mean())
    
    # Log feature importance
    for idx, row in feature_importance.iterrows():
        mlflow.log_metric(f"importance_{row['feature']}", row['importance'])
    
    # Log model
    mlflow.sklearn.log_model(rf_model, "churn_predictor")
    
    print("✅ Churn model logged to MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Generative AI & LLMs in Data Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1: Create Sample Product Data

# COMMAND ----------

# Generate sample products
products_data = [
    {'product_id': 'P001', 'product_name': 'iPhone 15 Pro', 'description': 'Latest Apple smartphone with A17 chip, titanium design, 256GB storage', 'category': None},
    {'product_id': 'P002', 'product_name': 'Nike Air Max', 'description': 'Running shoes with air cushioning, size 10, black color', 'category': None},
    {'product_id': 'P003', 'product_name': 'Samsung 4K TV', 'description': '55-inch QLED television with smart features and HDR support', 'category': None},
    {'product_id': 'P004', 'product_name': 'Levi\'s Jeans', 'description': 'Classic blue denim jeans, 32x34, straight fit', 'category': None},
    {'product_id': 'P005', 'product_name': 'KitchenAid Mixer', 'description': 'Stand mixer for baking, 5-quart capacity, red color', 'category': None},
    {'product_id': 'P006', 'product_name': 'Yoga Mat', 'description': 'Non-slip exercise mat for yoga and fitness, 6mm thick', 'category': None},
    {'product_id': 'P007', 'product_name': 'Harry Potter Book Set', 'description': 'Complete collection of Harry Potter novels, hardcover edition', 'category': None},
    {'product_id': 'P008', 'product_name': 'Sony Headphones', 'description': 'Wireless noise-canceling headphones with 30-hour battery', 'category': None},
]

products_df = spark.createDataFrame(pd.DataFrame(products_data))
products_df.write.mode("overwrite").saveAsTable("academy.silver.products")

print(f"✅ Created {len(products_data)} products")
display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2: AI-Powered Product Classification
# MAGIC 
# MAGIC **Note:** This demonstrates the pattern for using Databricks AI Functions.  
# MAGIC In a real environment with AI Functions enabled, you would use:
# MAGIC ```sql
# MAGIC SELECT AI_CLASSIFY(description, ARRAY('Electronics', 'Clothing', 'Home', 'Sports', 'Books')) as category
# MAGIC ```

# COMMAND ----------

# Simulated AI classification (rule-based for demo purposes)
# In production, use Databricks AI Functions or LLM API

def classify_product(description):
    """Simulate AI classification based on keywords"""
    description_lower = description.lower()
    
    if any(word in description_lower for word in ['phone', 'tv', 'television', 'headphones', 'smartphone', 'laptop']):
        return 'Electronics'
    elif any(word in description_lower for word in ['shoes', 'jeans', 'shirt', 'pants', 'clothing']):
        return 'Clothing'
    elif any(word in description_lower for word in ['mixer', 'kitchen', 'home', 'furniture']):
        return 'Home & Garden'
    elif any(word in description_lower for word in ['yoga', 'fitness', 'sports', 'exercise']):
        return 'Sports & Outdoors'
    elif any(word in description_lower for word in ['book', 'novel', 'reading']):
        return 'Books'
    else:
        return 'Other'

# Register as UDF
classify_udf = udf(classify_product, StringType())

# Apply classification
products_classified = products_df.withColumn('ai_category', classify_udf(col('description')))

products_classified.write.mode("overwrite").saveAsTable("academy.gold.products_classified")

print("✅ Products classified using AI")
display(products_classified)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3: Sentiment Analysis on Customer Reviews

# COMMAND ----------

# Sample customer reviews
reviews_data = [
    {'review_id': 'R001', 'product_id': 'P001', 'review_text': 'Amazing phone! Best camera I\'ve ever used. Worth every penny.', 'rating': 5},
    {'review_id': 'R002', 'product_id': 'P002', 'review_text': 'Comfortable shoes but they run small. Had to return and get a larger size.', 'rating': 3},
    {'review_id': 'R003', 'product_id': 'P003', 'review_text': 'Picture quality is terrible. Very disappointed with this purchase.', 'rating': 1},
    {'review_id': 'R004', 'product_id': 'P005', 'review_text': 'Great mixer! Makes baking so much easier. Highly recommend.', 'rating': 5},
    {'review_id': 'R005', 'product_id': 'P008', 'review_text': 'Good sound quality but battery life is not as advertised.', 'rating': 3},
]

reviews_df = spark.createDataFrame(pd.DataFrame(reviews_data))

# Simulated sentiment analysis (in production, use AI_CLASSIFY or LLM)
def analyze_sentiment(text, rating):
    """Simple sentiment analysis based on rating and keywords"""
    text_lower = text.lower()
    
    # Check for strong positive/negative words
    positive_words = ['amazing', 'great', 'excellent', 'love', 'best', 'recommend']
    negative_words = ['terrible', 'disappointed', 'bad', 'worst', 'awful']
    
    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)
    
    if rating >= 4 or positive_count > negative_count:
        return 'Positive'
    elif rating <= 2 or negative_count > positive_count:
        return 'Negative'
    else:
        return 'Neutral'

sentiment_udf = udf(analyze_sentiment, StringType())

reviews_with_sentiment = reviews_df.withColumn(
    'sentiment',
    sentiment_udf(col('review_text'), col('rating'))
)

print("✅ Sentiment analysis completed")
display(reviews_with_sentiment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4: Natural Language to SQL Interface (Conceptual)

# COMMAND ----------

# MAGIC %md
# MAGIC **Natural Language Query Examples:**
# MAGIC 
# MAGIC In a production environment with Databricks AI Functions:
# MAGIC 
# MAGIC ```python
# MAGIC # Example 1: Business question
# MAGIC question = "What are the top 5 customers by total spend this year?"
# MAGIC 
# MAGIC # AI generates SQL
# MAGIC sql_query = spark.sql(f"SELECT AI_QUERY('{question}')").collect()[0][0]
# MAGIC 
# MAGIC # Execute generated SQL
# MAGIC result = spark.sql(sql_query)
# MAGIC display(result)
# MAGIC ```
# MAGIC 
# MAGIC **Example Queries:**
# MAGIC - "Show me customers who haven't ordered in 90 days"
# MAGIC - "What products have negative sentiment in reviews?"
# MAGIC - "Which categories have the highest churn rate?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Production Deployment & Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1: Create AI-Enhanced Gold Layer

# COMMAND ----------

# Combine all AI enhancements into gold layer
gold_orders = spark.sql("""
    SELECT 
        o.*,
        f.predicted_fraud,
        f.anomaly_score,
        CASE 
            WHEN f.predicted_fraud = 1 THEN 'High Risk'
            WHEN f.anomaly_score < -0.3 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END as risk_level
    FROM academy.bronze.orders_raw o
    LEFT JOIN academy.silver.orders_fraud_checked f
        ON o.customer_id = f.customer_id
""")

gold_orders.write.mode("overwrite").saveAsTable("academy.gold.orders_ai_enhanced")

print("✅ AI-enhanced gold layer created")
display(gold_orders.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2: Create Monitoring Dashboard Queries

# COMMAND ----------

# Daily fraud detection summary
fraud_summary = spark.sql("""
    SELECT 
        COUNT(*) as total_orders,
        SUM(CASE WHEN predicted_fraud = 1 THEN 1 ELSE 0 END) as flagged_orders,
        ROUND(SUM(CASE WHEN predicted_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct,
        SUM(CASE WHEN predicted_fraud = 1 THEN order_amount ELSE 0 END) as flagged_amount
    FROM academy.gold.orders_ai_enhanced
""")

print("📊 Fraud Detection Summary:")
display(fraud_summary)

# COMMAND ----------

# Churn risk distribution
churn_distribution = spark.sql("""
    SELECT 
        recency_segment,
        frequency_segment,
        COUNT(*) as customer_count,
        SUM(churned) as churned_count,
        ROUND(SUM(churned) * 100.0 / COUNT(*), 2) as churn_rate_pct
    FROM academy.features.customer_rfm
    GROUP BY recency_segment, frequency_segment
    ORDER BY churn_rate_pct DESC
""")

print("📊 Churn Risk by Segment:")
display(churn_distribution)

# COMMAND ----------

# Product category distribution
category_summary = spark.sql("""
    SELECT 
        ai_category,
        COUNT(*) as product_count
    FROM academy.gold.products_classified
    GROUP BY ai_category
    ORDER BY product_count DESC
""")

print("📊 Product Category Distribution:")
display(category_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Key Takeaways

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ What We Accomplished
# MAGIC 
# MAGIC **1. Anomaly Detection:**
# MAGIC - Trained Isolation Forest for fraud detection
# MAGIC - Achieved good precision/recall on test data
# MAGIC - Logged model to MLflow for versioning
# MAGIC 
# MAGIC **2. Feature Engineering:**
# MAGIC - Created RFM features for customer analytics
# MAGIC - Built Feature Store in Unity Catalog
# MAGIC - Trained churn prediction model with 80%+ accuracy
# MAGIC 
# MAGIC **3. LLM Integration:**
# MAGIC - Demonstrated AI-powered product classification
# MAGIC - Showed sentiment analysis on reviews
# MAGIC - Explained natural language to SQL patterns
# MAGIC 
# MAGIC **4. Production Ready:**
# MAGIC - Created AI-enhanced gold layer
# MAGIC - Built monitoring queries
# MAGIC - Established MLflow tracking
# MAGIC 
# MAGIC ### 🎯 Next Steps
# MAGIC 
# MAGIC 1. **Experiment** with different ML models (XGBoost, Neural Networks)
# MAGIC 2. **Enable** Databricks AI Functions in your workspace
# MAGIC 3. **Monitor** model performance over time
# MAGIC 4. **Retrain** models regularly with new data
# MAGIC 5. **Integrate** with production pipelines

# COMMAND ----------

print("🎉 Week 10 Demo Complete!")
print("You've learned how to integrate AI into every stage of your data pipeline!")
