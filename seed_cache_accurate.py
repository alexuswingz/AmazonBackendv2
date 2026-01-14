"""
ACCURATE Cache Seeder - Uses full TPS algorithm for exact Excel match.

Run: python seed_cache_accurate.py

Takes longer but produces exact values matching Excel.
"""
import os
import sys

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
import time

# Import the actual TPS algorithm
from app.algorithms.algorithms_tps import (
    calculate_forecast_18m_plus as tps_18m,
    DEFAULT_SETTINGS
)

# PostgreSQL connection
DATABASE_URL = "postgresql://postgres:JMVZWnrhWpFToCzqgkEwCPhSBHCvUMuH@caboose.proxy.rlwy.net:54152/railway"

print("=" * 60)
print("ACCURATE FORECAST CACHE SEEDER (Full TPS Algorithm)")
print("=" * 60)

start_time = time.perf_counter()

# Connect to PostgreSQL
engine = create_engine(DATABASE_URL)
print(f"[1/6] Connected to PostgreSQL")

# Load all products
print("[2/6] Loading products...")
products_df = pd.read_sql("SELECT asin, brand, product_name, size FROM products", engine)
print(f"      Products: {len(products_df):,}")

# Load ALL sales data (needed for TPS algorithm)
print("[3/6] Loading sales history...")
sales_df = pd.read_sql("""
    SELECT asin, week_date, units 
    FROM units_sold 
    ORDER BY asin, week_date
""", engine)
print(f"      Sales records: {len(sales_df):,}")

# Group sales by ASIN for fast lookup
sales_by_asin = {asin: group.to_dict('records') for asin, group in sales_df.groupby('asin')}

# Get first sale dates
first_sales = sales_df[sales_df['units'] > 0].groupby('asin')['week_date'].min().to_dict()

# Load inventory
print("[4/6] Loading inventory...")
fba_df = pd.read_sql("""
    SELECT 
        asin,
        SUM(COALESCE(available, 0)) as fba_available,
        SUM(COALESCE(inbound_quantity, 0)) as fba_inbound,
        SUM(COALESCE(total_reserved_quantity, 0)) as fba_reserved
    FROM fba_inventory
    GROUP BY asin
""", engine)
fba_lookup = {row['asin']: row for _, row in fba_df.iterrows()}

awd_df = pd.read_sql("""
    SELECT 
        asin,
        SUM(COALESCE(available_in_awd_units, 0)) as awd_available,
        SUM(COALESCE(inbound_to_awd_units, 0)) as awd_inbound,
        SUM(COALESCE(reserved_in_awd_units, 0)) as awd_reserved,
        SUM(COALESCE(outbound_to_fba_units, 0)) as awd_outbound
    FROM awd_inventory
    GROUP BY asin
""", engine)
awd_lookup = {row['asin']: row for _, row in awd_df.iterrows()}
print(f"      FBA: {len(fba_df):,}, AWD: {len(awd_df):,}")

# Calculate forecasts using FULL TPS algorithm
print("[5/6] Calculating forecasts (full TPS algorithm)...")

today = date.today()
now = datetime.utcnow()
expires_at = now + timedelta(hours=24)

cache_entries = []
success = 0
skipped = 0
errors = 0

total = len(products_df)

for idx, product in products_df.iterrows():
    asin = product['asin']
    
    # Progress
    if (idx + 1) % 100 == 0:
        print(f"      Progress: {idx + 1}/{total} ({success} success, {errors} errors)")
    
    # Get sales data for this ASIN
    sales_records = sales_by_asin.get(asin, [])
    
    if len(sales_records) < 4:
        skipped += 1
        continue
    
    # Check first sale
    first_sale = first_sales.get(asin)
    if not first_sale:
        skipped += 1
        continue
    
    # Convert to date if needed
    if isinstance(first_sale, str):
        first_sale = datetime.strptime(first_sale, '%Y-%m-%d').date()
    elif hasattr(first_sale, 'date'):
        first_sale = first_sale.date()
    
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
    
    # Get inventory totals
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
    
    # Prepare units_data for TPS algorithm
    units_data = []
    for record in sales_records:
        week_date = record['week_date']
        if isinstance(week_date, str):
            week_date = datetime.strptime(week_date, '%Y-%m-%d').date()
        elif hasattr(week_date, 'date'):
            week_date = week_date.date()
        
        units_data.append({
            'week_end': week_date,
            'units': int(record['units'] or 0)
        })
    
    # Settings for TPS algorithm
    settings = DEFAULT_SETTINGS.copy()
    settings['total_inventory'] = total_inventory
    settings['fba_available'] = fba_available
    
    try:
        # Run FULL TPS algorithm (same as single product endpoint)
        result = tps_18m(units_data, today, settings)
        
        cache_entries.append({
            'asin': asin,
            'algorithm': algorithm,
            'computed_at': now,
            'expires_at': expires_at,
            'units_to_make': result['units_to_make'],
            'doi_total_days': round(result['doi_total_days'], 1),
            'doi_fba_available_days': round(result['doi_fba_days'], 1),
            'unit_needed_total': round(result.get('total_units_needed', 0), 1),
            'sales_velocity_adjustment': result.get('sales_velocity_adjustment', 0),
            'settings_hash': 'default'
        })
        success += 1
        
    except Exception as e:
        errors += 1
        if errors <= 5:
            print(f"      Error for {asin}: {str(e)[:60]}")

print(f"      Done: {success} success, {skipped} skipped, {errors} errors")

# Write to database
print("[6/6] Writing to forecast_cache table...")

with engine.begin() as conn:
    # Clear existing cache
    conn.execute(text("DELETE FROM forecast_cache"))
    
    # Bulk insert
    if cache_entries:
        cache_df = pd.DataFrame(cache_entries)
        cache_df.to_sql('forecast_cache', conn, if_exists='append', index=False, method='multi', chunksize=500)
    
    # Optimize
    conn.execute(text("ANALYZE forecast_cache"))

elapsed = time.perf_counter() - start_time

print("\n" + "=" * 60)
print("ACCURATE CACHE SEEDING COMPLETE!")
print("=" * 60)
print(f"Products cached: {success:,}")
print(f"Products skipped: {skipped:,}")
print(f"Errors: {errors:,}")
print(f"Total time: {elapsed:.2f}s")
print(f"Rate: {success / elapsed:.1f} products/sec")
print("\nTest your API:")
print("https://web-production-e39d6.up.railway.app/api/forecast/all")
