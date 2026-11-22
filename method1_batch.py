"""
Method 1: Standard Batch Ingestion (Pandas chunksize + to_sql / executemany)
A common, developer-friendly approach using high-level abstractions.
"""

import os
import time
import sqlite3
import psutil
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "method1_test.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

def init_db(db_path: str):
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.close()

def insert_file_pandas(conn, csv_file: str, table_name: str, chunksize: int = 50_000):
    total_rows = 0
    start_time = time.time()
    
    # Read and insert in chunks using standard Pandas to_sql
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        chunk.to_sql(table_name, conn, if_exists="append", index=False)
        total_rows += len(chunk)
        
    elapsed = time.time() - start_time
    return total_rows, elapsed

def run_method1(chunksize: int = 50_000):
    print("=" * 65)
    print(f"RUNNING METHOD 1: Standard Pandas to_sql (Chunksize={chunksize:,})")
    print("=" * 65)
    
    init_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    process = psutil.Process(os.getpid())
    
    mem_before = process.memory_info().rss / (1024 * 1024)
    total_start = time.time()
    
    files_to_load = [
        ("users", os.path.join(DATA_DIR, "users_1m.csv")),
        ("orders", os.path.join(DATA_DIR, "orders_1m.csv")),
        ("order_items", os.path.join(DATA_DIR, "order_items_2m.csv"))
    ]
    
    results = {}
    total_all_rows = 0
    
    for table, file_path in files_to_load:
        if not os.path.exists(file_path):
            print(f"[-] File not found: {file_path}. Please run generate_mock_data.py first.")
            return None
        print(f"[*] Ingesting into table '{table}' from {os.path.basename(file_path)}...")
        rows, duration = insert_file_pandas(conn, file_path, table, chunksize=chunksize)
        results[table] = {"rows": rows, "time": duration, "speed": rows / duration if duration > 0 else 0}
        total_all_rows += rows
        print(f"    Loaded {rows:,} rows in {duration:.2f}s ({rows/duration:,.0f} rows/s)")
        
    conn.commit()
    conn.close()
    
    total_elapsed = time.time() - total_start
    mem_after = process.memory_info().rss / (1024 * 1024)
    db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    
    metrics = {
        "method": "Method 1: Pandas to_sql Chunking",
        "total_rows": total_all_rows,
        "total_time_sec": total_elapsed,
        "overall_speed_rows_sec": total_all_rows / total_elapsed,
        "memory_peak_mb": mem_after,
        "memory_delta_mb": mem_after - mem_before,
        "db_size_mb": db_size_mb,
        "details": results
    }
    
    print("-" * 65)
    print(f"[SUMMARY] Total Rows: {total_all_rows:,} | Total Time: {total_elapsed:.2f}s | Speed: {total_all_rows/total_elapsed:,.0f} rows/s")
    print(f"[RESOURCES] RAM Delta: {metrics['memory_delta_mb']:.2f} MB | Peak: {mem_after:.2f} MB | DB Size: {db_size_mb:.2f} MB\n")
    return metrics

if __name__ == "__main__":
    run_method1()
