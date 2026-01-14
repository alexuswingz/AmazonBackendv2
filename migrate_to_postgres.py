"""
Fast Data Migration: SQLite → PostgreSQL

This script transfers all data from local SQLite to Railway PostgreSQL
using pandas bulk operations for maximum speed.

Usage:
  1. Set DATABASE_URL environment variable to your PostgreSQL URL
  2. Run: python migrate_to_postgres.py
"""
import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, text

# Source: Local SQLite
SQLITE_PATH = 'forecast.db'

# Target: PostgreSQL (from environment or command line)
POSTGRES_URL = os.getenv('DATABASE_URL', '')

if len(sys.argv) > 1:
    POSTGRES_URL = sys.argv[1]

if not POSTGRES_URL:
    print("ERROR: No PostgreSQL URL provided!")
    print("Usage: python migrate_to_postgres.py <postgresql://...>")
    print("Or set DATABASE_URL environment variable")
    sys.exit(1)

# Fix Railway's postgres:// to postgresql://
if POSTGRES_URL.startswith('postgres://'):
    POSTGRES_URL = POSTGRES_URL.replace('postgres://', 'postgresql://', 1)

print("=" * 60)
print("FAST DATA MIGRATION: SQLite → PostgreSQL")
print("=" * 60)

# Create engines
print(f"\n[SOURCE] SQLite: {SQLITE_PATH}")
print(f"[TARGET] PostgreSQL: {POSTGRES_URL[:50]}...")

sqlite_engine = create_engine(f'sqlite:///{SQLITE_PATH}')
postgres_engine = create_engine(POSTGRES_URL)

# Tables to migrate (in order due to foreign keys)
TABLES = [
    'fba_inventory',
    'awd_inventory', 
    'products',
    'units_sold',
    'seasonality',
    'forecast_cache'
]

total_start = time.perf_counter()

# Step 1: Create schema in PostgreSQL
print("\n[1/3] Creating PostgreSQL schema...")
from app import create_app, db
app = create_app('production')
with app.app_context():
    db.create_all()
    print("    [OK] Schema created with indexes")

# Step 2: Migrate data table by table
print("\n[2/3] Migrating data...")
stats = {}

for table in TABLES:
    start = time.perf_counter()
    
    try:
        # Read from SQLite
        df = pd.read_sql_table(table, sqlite_engine)
        row_count = len(df)
        
        if row_count == 0:
            print(f"    [{table}] Empty - skipping")
            continue
        
        # Clear existing data in PostgreSQL
        with postgres_engine.connect() as conn:
            conn.execute(text(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE'))
            conn.commit()
        
        # Bulk insert to PostgreSQL (fastest method)
        df.to_sql(
            table, 
            postgres_engine, 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        elapsed = time.perf_counter() - start
        rate = row_count / elapsed if elapsed > 0 else 0
        stats[table] = {'rows': row_count, 'time': elapsed, 'rate': rate}
        print(f"    [{table}] {row_count:,} rows in {elapsed:.2f}s ({rate:.0f} rows/sec)")
        
    except Exception as e:
        print(f"    [{table}] ERROR: {e}")

# Step 3: Optimize database
print("\n[3/3] Optimizing PostgreSQL...")
with postgres_engine.connect() as conn:
    # ANALYZE updates statistics for query planner
    conn.execute(text("ANALYZE"))
    conn.commit()
print("    [OK] Database optimized")

# Summary
total_time = time.perf_counter() - total_start
total_rows = sum(s['rows'] for s in stats.values())

print("\n" + "=" * 60)
print("MIGRATION COMPLETE")
print("=" * 60)
print(f"\nTotal rows migrated: {total_rows:,}")
print(f"Total time: {total_time:.2f}s")
print(f"Average rate: {total_rows/total_time:.0f} rows/sec")
print("\nPer-table breakdown:")
for table, s in stats.items():
    print(f"  - {table}: {s['rows']:,} rows ({s['time']:.2f}s)")
