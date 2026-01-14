"""
Export SQLite data to CSV files for easy import to PostgreSQL.
This is a backup method if direct migration doesn't work.

Usage: python export_data.py
"""
import pandas as pd
from sqlalchemy import create_engine
import os
import time

SQLITE_PATH = 'forecast.db'
EXPORT_DIR = 'data_export'

# Create export directory
os.makedirs(EXPORT_DIR, exist_ok=True)

print("=" * 60)
print("EXPORTING DATA TO CSV")
print("=" * 60)

engine = create_engine(f'sqlite:///{SQLITE_PATH}')

TABLES = [
    'fba_inventory',
    'awd_inventory',
    'products', 
    'units_sold',
    'seasonality',
]

total_start = time.perf_counter()
stats = {}

for table in TABLES:
    start = time.perf_counter()
    try:
        df = pd.read_sql_table(table, engine)
        filepath = f'{EXPORT_DIR}/{table}.csv'
        df.to_csv(filepath, index=False)
        elapsed = time.perf_counter() - start
        stats[table] = {'rows': len(df), 'time': elapsed}
        print(f"  [{table}] {len(df):,} rows -> {filepath} ({elapsed:.2f}s)")
    except Exception as e:
        print(f"  [{table}] ERROR: {e}")

total_time = time.perf_counter() - total_start
total_rows = sum(s['rows'] for s in stats.values())

print("\n" + "=" * 60)
print(f"EXPORT COMPLETE: {total_rows:,} rows in {total_time:.2f}s")
print(f"Files saved to: {EXPORT_DIR}/")
print("=" * 60)
