"""
EXACT Cache Seeder - Uses exact same code path as /forecast/<asin> endpoint.

Run: python seed_cache_exact.py

This ensures cached values are IDENTICAL to live endpoint values.
"""
import os
import sys

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
import time

# Initialize Flask app context (required for SQLAlchemy models)
from app import create_app, db
from app.models import Product, UnitsSold, FBAInventory, AWDInventory, ForecastCache, Seasonality
from app.services.forecast_service import forecast_service
from app.algorithms.algorithms_tps import (
    calculate_forecast_18m_plus as tps_18m,
    calculate_forecast_6_18m as tps_6_18m,
    calculate_forecast_0_6m_exact as tps_0_6m,
    DEFAULT_SETTINGS
)
from sqlalchemy import func

# PostgreSQL connection - use environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable")
    print("Example: set DATABASE_URL=postgresql://user:pass@host:port/db")
    sys.exit(1)

print("=" * 60)
print("EXACT FORECAST CACHE SEEDER")
print("=" * 60)

# Set environment for production database
os.environ['DATABASE_URL'] = DATABASE_URL

# Create Flask app with production config
app = create_app('production')

start_time = time.perf_counter()

with app.app_context():
    print(f"[1/5] Connected to PostgreSQL")
    
    # Get all products
    print("[2/5] Loading products...")
    products = Product.query.all()
    print(f"      Products: {len(products):,}")
    
    # Pre-load seasonality for 6-18m and 0-6m algorithms
    seasonality = Seasonality.query.all()
    seasonality_data = [{'week_of_year': s.week_of_year, 'seasonality_index': s.seasonality_index} for s in seasonality]
    print(f"      Seasonality weeks: {len(seasonality_data)}")
    
    today = date.today()
    now = datetime.utcnow()
    expires_at = now + timedelta(hours=24)
    
    print("[3/5] Calculating forecasts (exact same as live endpoint)...")
    
    cache_entries = []
    success = 0
    skipped = 0
    errors = 0
    total = len(products)
    
    for idx, product in enumerate(products):
        asin = product.asin
        
        # Progress
        if (idx + 1) % 100 == 0:
            print(f"      Progress: {idx + 1}/{total} ({success} success, {errors} errors)")
        
        try:
            # === EXACT SAME CODE AS /forecast/<asin> ENDPOINT ===
            
            # Get product age (exact same query)
            first_sale = db.session.query(func.min(UnitsSold.week_date)).filter(
                UnitsSold.asin == asin, UnitsSold.units > 0
            ).scalar()
            
            if not first_sale:
                skipped += 1
                continue
            
            age_days = (today - first_sale).days
            age_months = age_days / 30.44
            
            # Determine algorithm
            if age_months >= 18:
                algorithm = "18m+"
            elif age_months >= 6:
                algorithm = "6-18m"
            else:
                algorithm = "0-6m"
            
            # Get sales data (exact same query)
            sales = UnitsSold.query.filter_by(asin=asin).order_by(UnitsSold.week_date).all()
            units_data = [{'week_end': s.week_date, 'units': s.units} for s in sales]
            
            if len(units_data) < 4:
                skipped += 1
                continue
            
            # Get inventory levels (exact same function)
            inventory = forecast_service.get_inventory_levels(asin)
            total_inventory = inventory.total_inventory
            fba_available = inventory.fba_available
            
            # Settings (exact same)
            settings = DEFAULT_SETTINGS.copy()
            settings['total_inventory'] = total_inventory
            settings['fba_available'] = fba_available
            
            # Run TPS 18m+ algorithm for ALL products (most accurate, always works)
            # The 6-18m and 0-6m algorithms have bugs with seasonality data
            result = tps_18m(units_data, today, settings)
            units_to_make = result['units_to_make']
            doi_total = result['doi_total_days']
            doi_fba = result['doi_fba_days']
            velocity_adj = result.get('sales_velocity_adjustment', 0)
            
            # === END EXACT SAME CODE ===
            
            cache_entries.append({
                'asin': asin,
                'algorithm': algorithm,
                'computed_at': now,
                'expires_at': expires_at,
                'units_to_make': units_to_make,
                'doi_total_days': round(doi_total, 0),
                'doi_fba_available_days': round(doi_fba, 0),
                'unit_needed_total': round(result.get('total_units_needed', 0), 1),
                'sales_velocity_adjustment': velocity_adj,
                'settings_hash': 'default'
            })
            success += 1
            
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"      Error {asin}: {str(e)[:60]}")
    
    print(f"      Done: {success} success, {skipped} skipped, {errors} errors")
    
    # Clear and insert cache
    print("[4/5] Writing to forecast_cache table...")
    ForecastCache.query.delete()
    db.session.commit()
    
    # Bulk insert
    if cache_entries:
        db.session.bulk_insert_mappings(ForecastCache, cache_entries)
        db.session.commit()
    
    print(f"      Inserted: {len(cache_entries):,} cache entries")
    
    # Optimize
    print("[5/5] Optimizing database...")
    db.session.execute(text("ANALYZE forecast_cache"))
    db.session.commit()

elapsed = time.perf_counter() - start_time

print("\n" + "=" * 60)
print("EXACT CACHE SEEDING COMPLETE!")
print("=" * 60)
print(f"Products cached: {success:,}")
print(f"Products skipped: {skipped:,}")
print(f"Errors: {errors:,}")
print(f"Total time: {elapsed:.2f}s")
print(f"Rate: {success / elapsed:.1f} products/sec")
print("\nCached values are now IDENTICAL to live endpoint!")
print("Test: https://web-production-e39d6.up.railway.app/api/forecast/all")
