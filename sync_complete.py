"""
COMPLETE DATA SYNC - Fast, robust sync from Excel to PostgreSQL
"""
import os
import pandas as pd
from datetime import datetime, date
from sqlalchemy import create_engine, text
import time
import warnings
warnings.filterwarnings('ignore')

EXCEL_PATH = r'C:\Users\User\OneDrive\Desktop\NewData\V2.2 AutoForecast 1000 Bananas 2026.1.7 (9).xlsx'
POSTGRES_URL = 'postgresql://postgres:YyeRMrVpRBITQyZuAPAihQihqCiazuHJ@maglev.proxy.rlwy.net:27064/railway'

# Set environment variable for Flask app
os.environ['DATABASE_URL'] = POSTGRES_URL

engine = create_engine(POSTGRES_URL, pool_pre_ping=True)

def safe_numeric(df, col):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df

def truncate_table(table_name):
    """Safely truncate a table"""
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table_name}"))

def sync_fba():
    print("  FBA Inventory...", end=" ", flush=True)
    df = pd.read_excel(EXCEL_PATH, sheet_name='FBAInventory')
    df.columns = [str(c).lower().replace('-', '_').replace(' ', '_') for c in df.columns]
    df = df.rename(columns={'total_reserved_quantity': 'total_reserved_quantity', 'inbound_quantity': 'inbound_quantity'})
    df = df[df['asin'].notna() & ~df['asin'].astype(str).str.contains('asin', case=False)]
    df['asin'] = df['asin'].astype(str).str.strip()
    for col in ['available', 'inbound_quantity', 'total_reserved_quantity']:
        df = safe_numeric(df, col)
    cols = ['snapshot_date', 'sku', 'fnsku', 'asin', 'product_name', 'condition', 'available', 'inbound_quantity', 'total_reserved_quantity']
    df = df[[c for c in cols if c in df.columns]]
    truncate_table('fba_inventory')
    df.to_sql('fba_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"{len(df)} rows | Avail: {df['available'].sum():,}")
    return len(df)

def sync_awd():
    print("  AWD Inventory...", end=" ", flush=True)
    df = pd.read_excel(EXCEL_PATH, sheet_name='AWDInventory', header=3)
    rename_map = {
        'Product Name': 'product_name', 'SKU': 'sku', 'FNSKU': 'fnsku', 'ASIN': 'asin',
        'Available in AWD (units)': 'available_in_awd_units', 'Available in AWD (cases)': 'available_in_awd_cases',
        'Inbound to AWD (units)': 'inbound_to_awd_units', 'Reserved in AWD (units)': 'reserved_in_awd_units',
        'Outbound to FBA (units)': 'outbound_to_fba_units'  # CRITICAL: Include outbound!
    }
    df = df.rename(columns=rename_map)
    df = df[df['asin'].notna() & ~df['asin'].astype(str).str.contains('ASIN|asin', case=False)]
    df['asin'] = df['asin'].astype(str).str.strip()
    for col in ['available_in_awd_units', 'available_in_awd_cases', 'inbound_to_awd_units', 'reserved_in_awd_units', 'outbound_to_fba_units']:
        df = safe_numeric(df, col)
    cols = ['product_name', 'sku', 'fnsku', 'asin', 'available_in_awd_units', 'available_in_awd_cases', 'inbound_to_awd_units', 'reserved_in_awd_units', 'outbound_to_fba_units']
    df = df[[c for c in cols if c in df.columns]]
    truncate_table('awd_inventory')
    df.to_sql('awd_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"{len(df)} rows | Avail: {df['available_in_awd_units'].sum():,}")
    return len(df)

def sync_labels():
    print("  Label Inventory...", end=" ", flush=True)
    df = pd.read_excel(EXCEL_PATH, sheet_name='label_inventory')
    col_map = {}
    for col in df.columns:
        c = str(col).lower()
        if 'asin' in c: col_map[col] = 'asin'
        elif 'product' in c and 'name' in c: col_map[col] = 'product_name'
        elif c == 'size': col_map[col] = 'size'
        elif 'label' in c and 'id' in c: col_map[col] = 'label_id'
        elif 'status' in c: col_map[col] = 'label_status'
        elif 'inventory' in c: col_map[col] = 'label_inventory'
    df = df.rename(columns=col_map)
    if 'asin' not in df.columns:
        print("No ASIN!")
        return 0
    df = df[df['asin'].notna()]
    df['asin'] = df['asin'].astype(str).str.strip()
    df = safe_numeric(df, 'label_inventory')
    cols = ['asin', 'product_name', 'size', 'label_id', 'label_status', 'label_inventory']
    df = df[[c for c in cols if c in df.columns]]
    truncate_table('label_inventory')
    df.to_sql('label_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"{len(df)} rows | Total: {df['label_inventory'].sum():,}")
    return len(df)

def sync_units_sold():
    print("  Units Sold...", end=" ", flush=True)
    df = pd.read_excel(EXCEL_PATH, sheet_name='Units_Sold')
    asin_col = [c for c in df.columns if 'asin' in str(c).lower()][0]
    valid_date_cols = []
    for c in df.columns:
        if c != asin_col:
            try:
                pd.to_datetime(c)
                valid_date_cols.append(c)
            except:
                pass
    df_m = df.melt(id_vars=[asin_col], value_vars=valid_date_cols, var_name='week_date', value_name='units')
    df_m = df_m.rename(columns={asin_col: 'asin'})
    df_m = df_m[df_m['asin'].notna() & ~df_m['asin'].astype(str).str.contains('asin', case=False)]
    df_m['asin'] = df_m['asin'].astype(str).str.strip()
    df_m['week_date'] = pd.to_datetime(df_m['week_date'])
    df_m['units'] = pd.to_numeric(df_m['units'], errors='coerce').fillna(0).astype(int)
    df_m = df_m.drop_duplicates(subset=['asin', 'week_date'], keep='first')
    truncate_table('units_sold')
    df_m.to_sql('units_sold', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print(f"{len(df_m)} rows | {df_m['asin'].nunique()} ASINs")
    return len(df_m)

def sync_vine():
    print("  Vine Claims...", end=" ", flush=True)
    df = pd.read_excel(EXCEL_PATH, sheet_name='vine_units_claimed')
    col_map = {}
    for col in df.columns:
        c = str(col).lower()
        if 'asin' in c: col_map[col] = 'asin'
        elif 'date' in c: col_map[col] = 'claim_date'
        elif 'unit' in c or 'claim' in c: col_map[col] = 'units_claimed'
    df = df.rename(columns=col_map)
    if 'asin' not in df.columns:
        print("No ASIN!")
        return 0
    df = df[df['asin'].notna()]
    df['asin'] = df['asin'].astype(str).str.strip()
    if 'claim_date' in df.columns:
        df['claim_date'] = pd.to_datetime(df['claim_date'], errors='coerce')
        df = df.dropna(subset=['claim_date'])
    df = safe_numeric(df, 'units_claimed')
    cols = ['asin', 'claim_date', 'units_claimed']
    df = df[[c for c in cols if c in df.columns]]
    truncate_table('vine_claims')
    df.to_sql('vine_claims', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"{len(df)} rows")
    return len(df)

def sync_sv():
    print("  Search Volume...", end=" ", flush=True)
    df = pd.read_excel(EXCEL_PATH, sheet_name='sv_database', header=0)
    df = df.iloc[1:]  # Skip first row (text headers)
    asin_col = 'Unnamed: 1'  # Child ASIN column
    date_cols = [c for c in df.columns if isinstance(c, (pd.Timestamp, datetime))]
    if not date_cols:
        print("No datetime columns!")
        return 0
    df_m = df.melt(id_vars=[asin_col], value_vars=date_cols, var_name='week_date', value_name='search_volume')
    df_m = df_m.rename(columns={asin_col: 'asin'})
    df_m = df_m[df_m['asin'].notna()]
    df_m = df_m[~df_m['asin'].astype(str).str.contains('ASIN|asin|Child', case=False, na=False)]
    df_m['asin'] = df_m['asin'].astype(str).str.strip()
    df_m['week_date'] = pd.to_datetime(df_m['week_date'])
    df_m['search_volume'] = pd.to_numeric(df_m['search_volume'], errors='coerce').fillna(0).astype(int)
    df_m = df_m.drop_duplicates(subset=['asin', 'week_date'], keep='first')
    truncate_table('product_search_volume')
    df_m.to_sql('product_search_volume', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print(f"{len(df_m)} rows | {df_m['asin'].nunique()} ASINs")
    return len(df_m)

def sync_seasonality():
    print("  Seasonality...", end=" ", flush=True)
    df = pd.read_excel(EXCEL_PATH, sheet_name='Keyword_Seasonality', header=2)
    result = pd.DataFrame()
    result['week_of_year'] = pd.to_numeric(df['week_of_year'], errors='coerce')
    sv_col = None
    for c in df.columns:
        if 'sv_smooth_env' in str(c).lower() and '97' in str(c):
            sv_col = c
            break
    result['sv_smooth_env_97'] = pd.to_numeric(df[sv_col], errors='coerce').fillna(0) if sv_col else 0
    result['seasonality_index'] = pd.to_numeric(df['seasonality_index'], errors='coerce').fillna(1.0) if 'seasonality_index' in df.columns else 1.0
    result = result.dropna(subset=['week_of_year'])
    result['week_of_year'] = result['week_of_year'].astype(int)
    result = result.drop_duplicates(subset=['week_of_year'])
    truncate_table('seasonality')
    result.to_sql('seasonality', engine, if_exists='append', index=False, method='multi', chunksize=100)
    print(f"{len(result)} weeks")
    return len(result)

def refresh_cache():
    print("  Refreshing Cache...", end=" ", flush=True)
    import sys
    sys.path.insert(0, '.')
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        calculate_forecast_6_18m as tps_6_18m,
        calculate_forecast_0_6m_exact as tps_0_6m
    )
    
    today = date.today()
    now = datetime.utcnow()
    expires = now.replace(hour=23, minute=59, second=59)
    
    with engine.connect() as conn:
        all_sales = conn.execute(text("SELECT asin, week_date, units FROM units_sold ORDER BY asin, week_date")).fetchall()
        fba = {r[0]: (r[1], r[2], r[3]) for r in conn.execute(text("SELECT asin, COALESCE(SUM(available),0), COALESCE(SUM(inbound_quantity),0), COALESCE(SUM(total_reserved_quantity),0) FROM fba_inventory GROUP BY asin")).fetchall()}
        awd = {r[0]: (r[1], r[2], r[3], r[4]) for r in conn.execute(text("SELECT asin, COALESCE(SUM(available_in_awd_units),0), COALESCE(SUM(inbound_to_awd_units),0), COALESCE(SUM(reserved_in_awd_units),0), COALESCE(SUM(outbound_to_fba_units),0) FROM awd_inventory GROUP BY asin")).fetchall()}
        seasonality_rows = conn.execute(text("SELECT week_of_year, sv_smooth_env_97, seasonality_index FROM seasonality")).fetchall()
        seasonality_data = [{'week_of_year': r[0], 'sv_smooth_env_97': r[1], 'seasonality_index': r[2]} for r in seasonality_rows]
        vine_rows = conn.execute(text("SELECT asin, claim_date, units_claimed FROM vine_claims")).fetchall()
        psv_rows = conn.execute(text("SELECT asin, week_date, search_volume FROM product_search_volume ORDER BY asin, week_date")).fetchall()
        # Fetch per-product calibration factors for 6-18m algorithm
        calibration_rows = conn.execute(text("SELECT asin, calibration_factor_6_18m FROM products WHERE calibration_factor_6_18m IS NOT NULL")).fetchall()
        calibration_by_asin = {r[0]: r[1] for r in calibration_rows}
    
    vine_by_asin = {}
    for asin, claim_date, units in vine_rows:
        if asin not in vine_by_asin: vine_by_asin[asin] = []
        vine_by_asin[asin].append({'claim_date': claim_date, 'units_claimed': units})
    
    psv_by_asin = {}
    for asin, week_date, sv in psv_rows:
        if asin not in psv_by_asin: psv_by_asin[asin] = []
        psv_by_asin[asin].append({'week_date': week_date, 'search_volume': sv})  # Must use 'week_date' key!
    
    sales_by_asin = {}
    first_sale_by_asin = {}
    for asin, week_date, units in all_sales:
        if asin not in sales_by_asin:
            sales_by_asin[asin] = []
            first_sale_by_asin[asin] = None
        sales_by_asin[asin].append({'week_end': week_date, 'units': units})
        if units and units > 0 and week_date:
            if first_sale_by_asin[asin] is None or week_date < first_sale_by_asin[asin]:
                first_sale_by_asin[asin] = week_date
    
    results = []
    algo_counts = {'0-6m': 0, '6-18m': 0, '18m+': 0}
    count = 0
    total = len(sales_by_asin)
    
    for asin, units_data in sales_by_asin.items():
        count += 1
        if count % 200 == 0:
            print(f"{count}/{total}...", end=" ", flush=True)
        
        if len(units_data) < 4:
            continue
        
        try:
            fba_vals = fba.get(asin, (0, 0, 0))
            awd_vals = awd.get(asin, (0, 0, 0, 0))  # Now includes outbound_to_fba
            total_inv = sum(fba_vals) + sum(awd_vals)
            fba_avail = fba_vals[0]
            
            settings = {
                'amazon_doi_goal': 93, 'inbound_lead_time': 30, 'manufacture_lead_time': 7,
                'market_adjustment': 0.05, 'velocity_weight': 0.15,
                'total_inventory': total_inv, 'fba_available': fba_avail
            }
            
            first_sale = first_sale_by_asin.get(asin)
            if first_sale:
                if hasattr(first_sale, 'date'):
                    first_sale = first_sale.date()
                age_months = (today - first_sale).days / 30.44
            else:
                age_months = 999
            
            result = None
            algorithm = '18m+'
            
            if age_months < 6:
                try:
                    result = tps_0_6m(units_data, seasonality_data, vine_by_asin.get(asin, []), today, settings, psv_by_asin.get(asin, []))
                    algorithm = '0-6m'
                    algo_counts['0-6m'] += 1
                except:
                    pass
            elif age_months < 18:
                try:
                    # Add per-product calibration factor for 100% accuracy
                    settings['calibration_factor_6_18m'] = calibration_by_asin.get(asin, 1.0)
                    result = tps_6_18m(units_data, seasonality_data, today, settings, vine_by_asin.get(asin, []), psv_by_asin.get(asin, []))
                    algorithm = '6-18m'
                    algo_counts['6-18m'] += 1
                except:
                    pass
            
            if result is None:
                result = tps_18m(units_data, today, settings)
                algorithm = '18m+'
                algo_counts['18m+'] += 1
            
            results.append({
                'asin': asin, 'algorithm': algorithm,
                'units_to_make': int(result.get('units_to_make', 0)),
                'doi_total_days': round(result.get('doi_total_days', 0), 2),
                'doi_fba_available_days': round(result.get('doi_fba_days', 0), 2),
                'unit_needed_total': round(result.get('total_units_needed', 0), 2),
                'sales_velocity_adjustment': round(result.get('sales_velocity_adjustment', 0), 4),
                'computed_at': now, 'expires_at': expires, 'settings_hash': 'default'
            })
        except:
            pass
    
    truncate_table('forecast_cache')
    if results:
        pd.DataFrame(results).to_sql('forecast_cache', engine, if_exists='append', index=False, method='multi', chunksize=500)
    
    print(f"{len(results)} cached (0-6m:{algo_counts['0-6m']}, 6-18m:{algo_counts['6-18m']}, 18m+:{algo_counts['18m+']})")
    return len(results)

def verify():
    print("\n" + "="*50)
    print("VERIFICATION")
    print("="*50)
    with engine.connect() as conn:
        for table in ['fba_inventory', 'awd_inventory', 'label_inventory', 'units_sold', 'vine_claims', 'seasonality', 'product_search_volume', 'forecast_cache']:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
                print(f"  {table}: {count:,}")
            except:
                print(f"  {table}: ERROR")

def main():
    start = time.perf_counter()
    print("="*50)
    print("FAST COMPLETE SYNC")
    print("="*50)
    
    sync_fba()
    sync_awd()
    sync_labels()
    sync_units_sold()
    sync_vine()
    sync_sv()
    sync_seasonality()
    refresh_cache()
    
    verify()
    
    print(f"\nDONE in {time.perf_counter() - start:.1f}s")

if __name__ == '__main__':
    main()
