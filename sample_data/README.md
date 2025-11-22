# Sample Data Files for Week 2 Notebook

This directory contains sample data files to use with the Week 2 PySpark notebook.

## Files

### orders.csv
- Clean orders data with 30 records
- Schema: order_id, product_id, quantity, price, order_date
- Used for main notebook exercises

### products.json
- Product catalog in JSON format (10 products)
- Semi-structured data example
- Schema: product_id, name, category, brand, price, in_stock

### orders_with_issues.csv
- Orders data with intentional quality issues
- Used for data validation exercises
- Issues include: negative quantities, zero prices, null values, invalid dates

## Usage in Databricks

1. Upload these files to Databricks FileStore
2. Update file paths in the notebook to match your FileStore location
3. Example path: `/FileStore/shared_uploads/your_email/orders.csv`

## File Upload Instructions

1. In Databricks workspace, go to Data → Create Table
2. Upload files to FileStore
3. Copy the generated file paths
4. Update notebook code with correct paths