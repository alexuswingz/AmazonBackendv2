"""
Fast sync all data from Excel file (8) to the database
"""
import sys
sys.path.insert(0, '.')

import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import time

EXCEL_PATH = r'C:\Users\User\OneDrive\Desktop\NewData\V2.2 AutoForecast 1000 Bananas 2026.1.7 (8).xlsx'
POSTGRES_URL = 'postgresql://postgres:JMVZWnrhWpFToCzqgkEwCPhSBHCvUMuH@caboose.proxy.rlwy.net:54152/railway'

engine = create_engine(POSTGRES_URL)

def sync_fba_inventory():
    """Sync FBA Inventory"""
    print("  Syncing FBA Inventory...", end=" ", flush=True)
    
    fba_df = pd.read_excel(EXCEL_PATH, sheet_name='FBAInventory')
    
    fba_df = fba_df.rename(columns={
        'snapshot-date': 'snapshot_date',
        'product-name': 'product_name'
    })
    
    # Filter out header rows and rows without valid ASIN
    fba_df = fba_df[fba_df['asin'].notna()]
    fba_df = fba_df[~fba_df['asin'].astype(str).str.contains('asin', case=False, na=False)]
    fba_df['asin'] = fba_df['asin'].astype(str)
    
    # Keep essential columns only
    fba_df = fba_df[['snapshot_date', 'sku', 'fnsku', 'asin', 'product_name', 'condition', 'available']]
    
    # Convert available to numeric
    fba_df['available'] = pd.to_numeric(fba_df['available'], errors='coerce').fillna(0).astype(int)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fba_inventory"))
    
    fba_df.to_sql('fba_inventory', engine, if_exists='append', index=False, method='multi', chunksize=200)
    
    print(f"{len(fba_df)} rows")
    return len(fba_df)

def sync_awd_inventory():
    """Sync AWD Inventory"""
    print("  Syncing AWD Inventory...", end=" ", flush=True)
    
    awd_df = pd.read_excel(EXCEL_PATH, sheet_name='AWDInventory', header=3)
    
    awd_df = awd_df.rename(columns={
        'Product Name': 'product_name',
        'SKU': 'sku',
        'FNSKU': 'fnsku',
        'ASIN': 'asin',
        'Available in AWD (units)': 'available_in_awd_units',
        'Available in AWD (cases)': 'available_in_awd_cases',
    })
    
    awd_df = awd_df[awd_df['asin'].notna()]
    awd_df = awd_df[~awd_df['asin'].astype(str).str.contains('ASIN|asin', case=False, na=False)]
    awd_df['asin'] = awd_df['asin'].astype(str)
    
    awd_df = awd_df[['product_name', 'sku', 'fnsku', 'asin', 'available_in_awd_units', 'available_in_awd_cases']]
    
    # Convert to numeric
    awd_df['available_in_awd_units'] = pd.to_numeric(awd_df['available_in_awd_units'], errors='coerce').fillna(0).astype(int)
    awd_df['available_in_awd_cases'] = pd.to_numeric(awd_df['available_in_awd_cases'], errors='coerce').fillna(0).astype(int)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE awd_inventory"))
    
    awd_df.to_sql('awd_inventory', engine, if_exists='append', index=False, method='multi', chunksize=200)
    
    print(f"{len(awd_df)} rows")
    return len(awd_df)

def refresh_cache():
    """Refresh forecast cache"""
    print("  Refreshing forecast cache...", end=" ", flush=True)
    
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        calculate_forecast_6_18m as tps_6_18m,
        calculate_forecast_0_6m_exact as tps_0_6m,
        DEFAULT_SETTINGS
    )
    
    with engine.connect() as conn:
        products = [r[0] for r in conn.execute(text("SELECT DISTINCT asin FROM products")).fetchall()]
        first_sales = {r[0]: r[1] for r in conn.execute(text("SELECT asin, MIN(week_date) FROM units_sold WHERE units > 0 GROUP BY asin")).fetchall()}
        
        sales_by_asin = {}
        for asin, week_date, units in conn.execute(text("SELECT asin, week_date, units FROM units_sold ORDER BY asin, week_date")).fetchall():
            sales_by_asin.setdefault(asin, []).append({'week_end': str(week_date), 'units': units})
        
        seasonality_data = [{'week_of_year': s[0], 'sv_smooth_env_97': s[1], 'seasonality_index': s[2]} 
                           for s in conn.execute(text("SELECT week_of_year, sv_smooth_env_97, seasonality_index FROM seasonality ORDER BY week_of_year")).fetchall()]
        
        psv_by_asin = {}
        for asin, week_date, sv in conn.execute(text("SELECT asin, week_date, search_volume FROM product_search_volume")).fetchall():
            psv_by_asin.setdefault(asin, []).append({'week_date': str(week_date), 'search_volume': sv})
        
        vine_by_asin = {}
        for asin, claim_date, units in conn.execute(text("SELECT asin, claim_date, units_claimed FROM vine_claims")).fetchall():
            vine_by_asin.setdefault(asin, []).append({'claim_date': str(claim_date), 'units_claimed': units})
        
        fba_totals = {r[0]: int(r[1] or 0) for r in conn.execute(text("SELECT asin, SUM(available) FROM fba_inventory GROUP BY asin")).fetchall()}
        awd_totals = {r[0]: int(r[1] or 0) for r in conn.execute(text("SELECT asin, SUM(available_in_awd_units) FROM awd_inventory GROUP BY asin")).fetchall()}
    
    today = date.today()
    now = datetime.utcnow()
    expires_at = now + timedelta(hours=24)
    results = []
    
    for asin in products:
        first_sale = first_sales.get(asin)
        units_data = sales_by_asin.get(asin, [])
        if not first_sale or len(units_data) < 4:
            continue
        
        age_months = (today - first_sale).days / 30.44
        algorithm = "18m+" if age_months >= 18 else ("6-18m" if age_months >= 6 else "0-6m")
        
        total_inv = fba_totals.get(asin, 0) + awd_totals.get(asin, 0)
        fba_avail = fba_totals.get(asin, 0)
        
        settings = DEFAULT_SETTINGS.copy()
        settings['total_inventory'] = total_inv
        settings['fba_available'] = fba_avail
        
        try:
            if algorithm == "0-6m":
                result = tps_0_6m(units_data, seasonality_data, vine_by_asin.get(asin, []), today, settings, psv_by_asin.get(asin, []))
            elif algorithm == "6-18m":
                result = tps_6_18m(units_data, seasonality_data, today, settings, vine_by_asin.get(asin, []), psv_by_asin.get(asin, []))
            else:
                result = tps_18m(units_data, today, settings)
        except:
            try:
                result = tps_18m(units_data, today, settings)
                algorithm = f"{algorithm}->18m+"
            except:
                continue
        
        results.append({
            'asin': asin, 'algorithm': algorithm,
            'units_to_make': int(result.get('units_to_make', 0)),
            'doi_total_days': round(result.get('doi_total_days', 0), 2),
            'doi_fba_available_days': round(result.get('doi_fba_days', 0), 2),
            'unit_needed_total': round(result.get('total_units_needed', 0), 2),
            'sales_velocity_adjustment': round(result.get('sales_velocity_adjustment', 0), 4),
            'computed_at': now, 'expires_at': expires_at, 'settings_hash': 'default'
        })
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE forecast_cache"))
    
    pd.DataFrame(results).to_sql('forecast_cache', engine, if_exists='append', index=False, method='multi', chunksize=200)
    
    print(f"{len(results)} products")
    return len(results)

def main():
    print("=" * 50)
    print("FAST SYNC FROM EXCEL FILE (8)")
    print("=" * 50)
    
    start = time.perf_counter()
    fba = sync_fba_inventory()
    awd = sync_awd_inventory()
    cache = refresh_cache()
    total = time.perf_counter() - start
    
    print("=" * 50)
    print(f"DONE in {total:.1f}s | FBA: {fba} | AWD: {awd} | Cache: {cache}")

if __name__ == '__main__':
    main()
