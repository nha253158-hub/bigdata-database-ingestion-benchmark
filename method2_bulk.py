"""
Method 2: High-Performance Bulk Insertion (Raw Streaming + Engine Tuning + Explicit Transactions)
Optimized for high-throughput big data workloads with minimal memory footprint.
"""

import os
import csv
import time
import sqlite3
import psutil

DB_PATH = os.path.join(os.path.dirname(__file__), "method2_test.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

def init_db(db_path: str):
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.close()

def optimize_sqlite_connection(conn):
    """
    Apply database engine optimizations for massive batch inserts:
    - synchronous = OFF: Avoids waiting for OS disk flush after every batch.
    - journal_mode = MEMORY / WAL: Keeps transaction journal in RAM.
    - cache_size = -64000: Allocates 64MB RAM page cache.
    - temp_store = MEMORY: Uses memory for temporary tables and sorting.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = MEMORY;")
    cursor.execute("PRAGMA cache_size = -64000;")
    cursor.execute("PRAGMA temp_store = MEMORY;")
    cursor.close()

def batch_generator(csv_reader, batch_size=100_000):
    batch = []
    for row in csv_reader:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def insert_file_optimized(conn, csv_file: str, table_name: str, batch_size: int = 100_000):
    start_time = time.time()
    total_rows = 0
    
    with open(csv_file, "r", encoding="utf-8", buffering=1024*1024) as f:
        reader = csv.reader(f)
        headers = next(reader)
        placeholders = ",".join(["?"] * len(headers))
        sql = f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({placeholders})"
        
        cursor = conn.cursor()
        conn.execute("BEGIN TRANSACTION;")
        
        for batch in batch_generator(reader, batch_size=batch_size):
            cursor.executemany(sql, batch)
            total_rows += len(batch)
            
        conn.commit()
        cursor.close()
        
    elapsed = time.time() - start_time
    return total_rows, elapsed

def run_method2(batch_size: int = 100_000):
    print("=" * 65)
    print(f"RUNNING METHOD 2: Optimized Streaming Bulk (Batch Size={batch_size:,})")
    print("=" * 65)
    
    init_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    optimize_sqlite_connection(conn)
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
        rows, duration = insert_file_optimized(conn, file_path, table, batch_size=batch_size)
        results[table] = {"rows": rows, "time": duration, "speed": rows / duration if duration > 0 else 0}
        total_all_rows += rows
        print(f"    Loaded {rows:,} rows in {duration:.2f}s ({rows/duration:,.0f} rows/s)")
        
    conn.close()
    
    total_elapsed = time.time() - total_start
    mem_after = process.memory_info().rss / (1024 * 1024)
    db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    
    metrics = {
        "method": "Method 2: Optimized Low-Level Bulk",
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
    run_method2()
