"""
Data Generator for Week 2 Notebook
Generates additional sample data for testing
"""

import csv
import json
import random
from datetime import datetime, timedelta

def generate_orders(num_records=100, filename="generated_orders.csv"):
    """Generate orders CSV data"""
    product_ids = list(range(101, 121))
    start_date = datetime(2024, 1, 1)
    
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['order_id', 'product_id', 'quantity', 'price', 'order_date'])
        
        for i in range(num_records):
            order_id = 3000 + i
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 10)
            price = round(random.uniform(10.0, 200.0), 2)
            order_date = start_date + timedelta(days=random.randint(0, 90))
            
            writer.writerow([order_id, product_id, quantity, price, order_date.strftime('%Y-%m-%d')])
    
    print(f"Generated {num_records} orders in {filename}")

def generate_products_json(num_products=20, filename="generated_products.json"):
    """Generate products JSON data"""
    categories = ["Electronics", "Kitchen", "Sports", "Office", "Travel", "Home"]
    brands = ["TechCorp", "HomeGoods", "SportMax", "OfficeMax", "TravelPro", "HomePlus"]
    
    with open(filename, 'w') as file:
        for i in range(num_products):
            product = {
                "product_id": 201 + i,
                "name": f"Product {201 + i}",
                "category": random.choice(categories),
                "brand": random.choice(brands),
                "price": round(random.uniform(5.0, 300.0), 2),
                "in_stock": random.choice([True, False])
            }
            file.write(json.dumps(product) + '\n')
    
    print(f"Generated {num_products} products in {filename}")

if __name__ == "__main__":
    generate_orders(50, "generated_orders.csv")
    generate_products_json(15, "generated_products.json")