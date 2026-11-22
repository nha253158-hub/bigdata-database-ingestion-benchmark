"""
High-Speed Streaming Data Generator for Big Data Benchmarks
Generates multiple CSV files each with 1,000,000+ rows (or custom sizes).
Uses buffered chunk writing for minimal RAM overhead and extreme speed.
"""

import os
import time
import random
import argparse
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

COUNTRIES = ["Vietnam", "United States", "Japan", "Germany", "Singapore", "Australia", "United Kingdom", "France", "Canada", "Korea"]
STATUSES = ["COMPLETED", "PENDING", "CANCELLED", "SHIPPED", "REFUNDED"]
PRODUCTS = [
    ("Laptop Pro 16", 1850.0),
    ("Wireless Mouse", 25.5),
    ("Mechanical Keyboard", 89.0),
    ("4K Monitor 27in", 340.0),
    ("USB-C Hub Multiport", 45.0),
    ("Noise Cancelling Headphones", 199.0),
    ("External SSD 1TB", 110.0),
    ("Ergonomic Desk Chair", 280.0),
    ("Standing Desk Frame", 350.0),
    ("Webcam HD 1080p", 65.0)
]

def generate_users(filepath: str, n_rows: int = 1_000_000, chunk_size: int = 100_000):
    print(f"[*] Generating {n_rows:,} rows -> {filepath}...")
    start_time = time.time()
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    start_date = datetime(2023, 1, 1)
    
    with open(filepath, "w", encoding="utf-8", buffering=1024*1024) as f:
        f.write("user_id,username,email,country,created_at\n")
        
        lines = []
        for i in range(1, n_rows + 1):
            country = COUNTRIES[i % len(COUNTRIES)]
            user_date = (start_date + timedelta(seconds=i * 17 % 60000000)).strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"{i},user_{i},user_{i}@example.com,{country},{user_date}\n")
            
            if i % chunk_size == 0:
                f.writelines(lines)
                lines.clear()
                print(f"    Users: {i:,}/{n_rows:,} ({(i/n_rows)*100:.1f}%)")
        
        if lines:
            f.writelines(lines)
            
    elapsed = time.time() - start_time
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"[+] Done users: {n_rows:,} rows in {elapsed:.2f}s ({n_rows/elapsed:,.0f} rows/s) | Size: {file_size_mb:.2f} MB\n")


def generate_orders_and_items(orders_path: str, items_path: str, n_orders: int = 1_000_000, n_users: int = 1_000_000, chunk_size: int = 100_000):
    print(f"[*] Generating {n_orders:,} orders & items -> {orders_path}, {items_path}...")
    start_time = time.time()
    
    start_date = datetime(2024, 1, 1)
    item_id_counter = 1
    
    with open(orders_path, "w", encoding="utf-8", buffering=1024*1024) as f_orders, \
         open(items_path, "w", encoding="utf-8", buffering=1024*1024) as f_items:
        
        f_orders.write("order_id,user_id,order_date,total_amount,status\n")
        f_items.write("item_id,order_id,product_name,quantity,unit_price,subtotal\n")
        
        order_lines = []
        item_lines = []
        
        for order_id in range(1, n_orders + 1):
            user_id = ((order_id * 31) % n_users) + 1
            order_date = (start_date + timedelta(seconds=order_id * 23 % 30000000)).strftime("%Y-%m-%d")
            status = STATUSES[order_id % len(STATUSES)]
            
            # 1-3 items per order
            num_items = (order_id % 3) + 1
            order_total = 0.0
            
            for _ in range(num_items):
                prod_name, unit_price = PRODUCTS[(item_id_counter * 7) % len(PRODUCTS)]
                quantity = (item_id_counter % 4) + 1
                subtotal = round(quantity * unit_price, 2)
                order_total += subtotal
                
                item_lines.append(f"{item_id_counter},{order_id},{prod_name},{quantity},{unit_price:.2f},{subtotal:.2f}\n")
                item_id_counter += 1
                
            order_lines.append(f"{order_id},{user_id},{order_date},{order_total:.2f},{status}\n")
            
            if order_id % chunk_size == 0:
                f_orders.writelines(order_lines)
                f_items.writelines(item_lines)
                order_lines.clear()
                item_lines.clear()
                print(f"    Orders: {order_id:,}/{n_orders:,} ({(order_id/n_orders)*100:.1f}%) | Total Items: {item_id_counter-1:,}")
                
        if order_lines:
            f_orders.writelines(order_lines)
        if item_lines:
            f_items.writelines(item_lines)
            
    elapsed = time.time() - start_time
    total_items = item_id_counter - 1
    orders_size_mb = os.path.getsize(orders_path) / (1024 * 1024)
    items_size_mb = os.path.getsize(items_path) / (1024 * 1024)
    print(f"[+] Done orders: {n_orders:,} rows ({orders_size_mb:.2f} MB)")
    print(f"[+] Done items: {total_items:,} rows ({items_size_mb:.2f} MB)")
    print(f"[+] Total time: {elapsed:.2f}s ({(n_orders + total_items)/elapsed:,.0f} total rows/s)\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Big Data CSV Generator")
    parser.add_argument("--users", type=int, default=1_000_000, help="Number of user records (default: 1,000,000)")
    parser.add_argument("--orders", type=int, default=1_000_000, help="Number of order records (default: 1,000,000)")
    args = parser.parse_args()
    
    os.makedirs(DATA_DIR, exist_ok=True)
    users_file = os.path.join(DATA_DIR, "users_1m.csv")
    orders_file = os.path.join(DATA_DIR, "orders_1m.csv")
    items_file = os.path.join(DATA_DIR, "order_items_2m.csv")
    
    print("=" * 65)
    print(f"GENERATING MULTI-FILE SYNTHETIC DATASET (>1M ROWS PER FILE)")
    print("=" * 65)
    generate_users(users_file, n_rows=args.users)
    generate_orders_and_items(orders_file, items_file, n_orders=args.orders, n_users=args.users)
    print("[*] All datasets generated successfully in ./data/")
