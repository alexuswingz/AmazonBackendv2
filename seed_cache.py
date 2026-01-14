"""
FAST Cache Seeder - Seeds forecast cache directly to PostgreSQL.

Run: python seed_cache.py

This bypasses the HTTP timeout issue by running locally.
"""
import os
import sys
import time
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import pandas as pd

# PostgreSQL connection - use environment variable
import os
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable")
    print("Example: set DATABASE_URL=postgresql://user:pass@host:port/db")
    sys.exit(1)

print("=" * 60)
print("FAST FORECAST CACHE SEEDER")
print("=" * 60)

start_time = time.perf_counter()

# Connect to PostgreSQL
engine = create_engine(DATABASE_URL)
print(f"[1/5] Connected to PostgreSQL")

# Load all data in bulk (FAST)
print("[2/5] Loading all data from PostgreSQL...")

# Get all products
products_df = pd.read_sql("SELECT asin, brand, product_name, size FROM products", engine)
print(f"      Products: {len(products_df):,}")

# Get all sales data with first sale dates
sales_query = """
SELECT 
    asin,
    MIN(week_date) as first_sale,
    MAX(week_date) as last_sale,
    COUNT(*) as weeks_count,
    SUM(units) as total_units,
    AVG(units) as avg_units
FROM units_sold 
WHERE units > 0
GROUP BY asin
"""
sales_agg_df = pd.read_sql(sales_query, engine)
print(f"      Sales aggregates: {len(sales_agg_df):,}")

# Get recent sales (last 12 weeks) for better averages
recent_sales_query = """
SELECT asin, AVG(units) as recent_avg
FROM units_sold
WHERE week_date >= CURRENT_DATE - INTERVAL '84 days'
GROUP BY asin
"""
recent_df = pd.read_sql(recent_sales_query, engine)
recent_lookup = dict(zip(recent_df['asin'], recent_df['recent_avg']))
print(f"      Recent sales: {len(recent_df):,}")

# Get inventory totals
fba_query = """
SELECT 
    asin,
    SUM(COALESCE(available, 0)) as fba_available,
    SUM(COALESCE(inbound_quantity, 0)) as fba_inbound,
    SUM(COALESCE(total_reserved_quantity, 0)) as fba_reserved
FROM fba_inventory
GROUP BY asin
"""
fba_df = pd.read_sql(fba_query, engine)
fba_lookup = {row['asin']: row for _, row in fba_df.iterrows()}
print(f"      FBA inventory: {len(fba_df):,}")

awd_query = """
SELECT 
    asin,
    SUM(COALESCE(available_in_awd_units, 0)) as awd_available,
    SUM(COALESCE(inbound_to_awd_units, 0)) as awd_inbound,
    SUM(COALESCE(reserved_in_awd_units, 0)) as awd_reserved,
    SUM(COALESCE(outbound_to_fba_units, 0)) as awd_outbound
FROM awd_inventory
GROUP BY asin
"""
awd_df = pd.read_sql(awd_query, engine)
awd_lookup = {row['asin']: row for _, row in awd_df.iterrows()}
print(f"      AWD inventory: {len(awd_df):,}")

# Merge data
print("[3/5] Calculating forecasts...")

# Settings
AMAZON_DOI_GOAL = 93  # days
INBOUND_LEAD_TIME = 30  # days
MANUFACTURE_LEAD_TIME = 7  # days
TOTAL_LEAD_TIME = INBOUND_LEAD_TIME + MANUFACTURE_LEAD_TIME
TOTAL_DOI_GOAL = AMAZON_DOI_GOAL + TOTAL_LEAD_TIME

today = date.today()
now = datetime.utcnow()
expires_at = now + timedelta(hours=24)

cache_entries = []
success = 0
skipped = 0

for _, product in products_df.iterrows():
    asin = product['asin']
    
    # Get sales data
    sales_row = sales_agg_df[sales_agg_df['asin'] == asin]
    
    if sales_row.empty or sales_row.iloc[0]['weeks_count'] < 4:
        skipped += 1
        continue
    
    sales = sales_row.iloc[0]
    first_sale = pd.to_datetime(sales['first_sale']).date()
    
    # Calculate product age
    age_days = (today - first_sale).days
    age_months = age_days / 30.44
    
    # Determine algorithm based on age
    if age_months >= 18:
        algorithm = "18m+"
    elif age_months >= 6:
        algorithm = "6-18m"
    else:
        algorithm = "0-6m"
    
    # Get inventory
    fba = fba_lookup.get(asin, {})
    awd = awd_lookup.get(asin, {})
    
    fba_available = int(fba.get('fba_available', 0) or 0)
    fba_inbound = int(fba.get('fba_inbound', 0) or 0)
    fba_reserved = int(fba.get('fba_reserved', 0) or 0)
    awd_available = int(awd.get('awd_available', 0) or 0)
    awd_inbound = int(awd.get('awd_inbound', 0) or 0)
    awd_reserved = int(awd.get('awd_reserved', 0) or 0)
    awd_outbound = int(awd.get('awd_outbound', 0) or 0)
    
    total_inventory = (fba_available + fba_inbound + fba_reserved + 
                       awd_available + awd_inbound + awd_reserved + awd_outbound)
    
    # Use recent average if available, else overall average
    avg_weekly = recent_lookup.get(asin, sales['avg_units'])
    if avg_weekly is None or avg_weekly <= 0:
        avg_weekly = sales['avg_units']
    if avg_weekly is None or avg_weekly <= 0:
        avg_weekly = 1  # Prevent division by zero
    
    # Calculate DOI
    doi_total = (total_inventory / avg_weekly) * 7 if avg_weekly > 0 else 0
    doi_fba = (fba_available / avg_weekly) * 7 if avg_weekly > 0 else 0
    
    # Calculate units needed
    lead_time_weeks = TOTAL_LEAD_TIME / 7
    doi_goal_weeks = AMAZON_DOI_GOAL / 7
    total_weeks_needed = lead_time_weeks + doi_goal_weeks
    
    units_needed = avg_weekly * total_weeks_needed
    units_to_make = max(0, int(units_needed - total_inventory))
    
    cache_entries.append({
        'asin': asin,
        'algorithm': algorithm,
        'computed_at': now,
        'expires_at': expires_at,
        'units_to_make': units_to_make,
        'doi_total_days': round(doi_total, 1),
        'doi_fba_available_days': round(doi_fba, 1),
        'unit_needed_total': round(units_needed, 1),
        'sales_velocity_adjustment': 0,
        'settings_hash': 'default'
    })
    success += 1

print(f"      Calculated: {success:,} products")
print(f"      Skipped: {skipped:,} (insufficient data)")

# Clear and insert cache
print("[4/5] Writing to forecast_cache table...")

with engine.begin() as conn:
    # Clear existing cache
    conn.execute(text("DELETE FROM forecast_cache"))
    
    # Bulk insert using pandas
    if cache_entries:
        cache_df = pd.DataFrame(cache_entries)
        cache_df.to_sql('forecast_cache', conn, if_exists='append', index=False, method='multi', chunksize=500)

print(f"      Inserted: {len(cache_entries):,} cache entries")

# Analyze table
print("[5/5] Optimizing database...")
with engine.begin() as conn:
    conn.execute(text("ANALYZE forecast_cache"))

elapsed = time.perf_counter() - start_time

print("\n" + "=" * 60)
print("CACHE SEEDING COMPLETE!")
print("=" * 60)
print(f"Products cached: {success:,}")
print(f"Products skipped: {skipped:,}")
print(f"Total time: {elapsed:.2f}s")
print(f"Rate: {success / elapsed:.0f} products/sec")
print("\nTest your API:")
print("https://web-production-e39d6.up.railway.app/api/forecast/all")
