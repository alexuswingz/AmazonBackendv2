"""Fast cache refresh using bulk operations - similar to seeding speed"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
from io import StringIO
import time

POSTGRES_URL = 'postgresql://postgres:YyeRMrVpRBITQyZuAPAihQihqCiazuHJ@maglev.proxy.rlwy.net:27064/railway'
engine = create_engine(POSTGRES_URL)

from app.algorithms.algorithms_tps import (
    calculate_forecast_18m_plus as tps_18m,
    calculate_forecast_6_18m as tps_6_18m,
    calculate_forecast_0_6m_exact as tps_0_6m,
    DEFAULT_SETTINGS
)

def fast_copy_insert(data, table_name):
    """Fast bulk insert using PostgreSQL COPY."""
    if not data:
        return
    
    buffer = StringIO()
    columns = list(data[0].keys())
    
    for row in data:
        values = []
        for col in columns:
            val = row[col]
            if val is None:
                values.append('\\N')
            elif isinstance(val, datetime):
                values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                values.append(str(val))
        buffer.write(','.join(values) + '\n')
    
    buffer.seek(0)
    col_str = ', '.join(columns)
    
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table_name} ({col_str}) FROM STDIN WITH CSV NULL '\\N'", buffer)
        conn.commit()
    finally:
        conn.close()

def refresh_cache():
    start_time = time.perf_counter()
    print("=" * 60)
    print("FAST CACHE REFRESH")
    print("=" * 60)
    
    # =========================================================
    # STEP 1: Bulk fetch ALL data (single queries)
    # =========================================================
    print("\n[1/4] Fetching all data...")
    fetch_start = time.perf_counter()
    
    with engine.connect() as conn:
        # All products
        products = [r[0] for r in conn.execute(text("SELECT DISTINCT asin FROM products")).fetchall()]
        print(f"  Products: {len(products)}")
        
        # First sale dates
        first_sales_raw = conn.execute(text("""
            SELECT asin, MIN(week_date) FROM units_sold WHERE units > 0 GROUP BY asin
        """)).fetchall()
        first_sales = {r[0]: r[1] for r in first_sales_raw}
        print(f"  First sales: {len(first_sales)}")
        
        # ALL sales data grouped by ASIN
        all_sales = conn.execute(text("""
            SELECT asin, week_date, units FROM units_sold ORDER BY asin, week_date
        """)).fetchall()
        
        sales_by_asin = {}
        for asin, week_date, units in all_sales:
            if asin not in sales_by_asin:
                sales_by_asin[asin] = []
            sales_by_asin[asin].append({'week_end': str(week_date), 'units': units})
        print(f"  Sales records: {len(all_sales)}")
        
        # Seasonality (global)
        seasonality_raw = conn.execute(text("""
            SELECT week_of_year, sv_smooth_env_97, seasonality_index FROM seasonality ORDER BY week_of_year
        """)).fetchall()
        seasonality_data = [{'week_of_year': s[0], 'sv_smooth_env_97': s[1], 'seasonality_index': s[2]} for s in seasonality_raw]
        
        # Product search volume
        psv_raw = conn.execute(text("""
            SELECT asin, week_date, search_volume FROM product_search_volume ORDER BY asin, week_date
        """)).fetchall()
        
        psv_by_asin = {}
        for asin, week_date, sv in psv_raw:
            if asin not in psv_by_asin:
                psv_by_asin[asin] = []
            psv_by_asin[asin].append({'week_date': str(week_date), 'search_volume': sv})
        print(f"  Product SV ASINs: {len(psv_by_asin)}")
        
        # Vine claims
        vine_raw = conn.execute(text("SELECT asin, claim_date, units_claimed FROM vine_claims")).fetchall()
        vine_by_asin = {}
        for asin, claim_date, units in vine_raw:
            if asin not in vine_by_asin:
                vine_by_asin[asin] = []
            vine_by_asin[asin].append({'claim_date': str(claim_date), 'units_claimed': units})
        
        # Inventory totals - include inbound and reserved for Total Inventory
        # Excel Total Inventory = FBA available + inbound + reserved + AWD available + outbound_to_fba
        fba_raw = conn.execute(text("""
            SELECT asin, 
                   SUM(COALESCE(available, 0)) as available,
                   SUM(COALESCE(inbound_quantity, 0)) as inbound,
                   SUM(COALESCE(total_reserved_quantity, 0)) as reserved
            FROM fba_inventory GROUP BY asin
        """)).fetchall()
        fba_totals = {r[0]: int(r[1] or 0) + int(r[2] or 0) + int(r[3] or 0) for r in fba_raw}
        fba_available = {r[0]: int(r[1] or 0) for r in fba_raw}
        
        awd_raw = conn.execute(text("""
            SELECT asin,
                   SUM(COALESCE(available_in_awd_units, 0)) as available,
                   SUM(COALESCE(inbound_to_awd_units, 0)) as inbound,
                   SUM(COALESCE(reserved_in_awd_units, 0)) as reserved,
                   SUM(COALESCE(outbound_to_fba_units, 0)) as outbound
            FROM awd_inventory GROUP BY asin
        """)).fetchall()
        awd_totals = {r[0]: int(r[1] or 0) + int(r[2] or 0) + int(r[3] or 0) + int(r[4] or 0) for r in awd_raw}
    
    fetch_time = time.perf_counter() - fetch_start
    print(f"  Fetch time: {fetch_time:.2f}s")
    
    # =========================================================
    # STEP 2: Calculate forecasts (in memory)
    # =========================================================
    print("\n[2/4] Calculating forecasts...")
    calc_start = time.perf_counter()
    
    today = date.today()
    now = datetime.utcnow()
    expires_at = now + timedelta(hours=24)
    
    results = []
    success = 0
    errors = 0
    skipped = 0
    
    for asin in products:
        try:
            first_sale = first_sales.get(asin)
            units_data = sales_by_asin.get(asin, [])
            
            if not first_sale or len(units_data) < 4:
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
            
            # Get inventory (Total = FBA available+inbound+reserved + AWD available+outbound)
            total_inv = fba_totals.get(asin, 0) + awd_totals.get(asin, 0)
            fba_avail = fba_available.get(asin, 0)
            
            settings = DEFAULT_SETTINGS.copy()
            settings['total_inventory'] = total_inv
            settings['fba_available'] = fba_avail
            
            result = None
            product_sv = psv_by_asin.get(asin, [])
            vine_claims = vine_by_asin.get(asin, [])
            
            # Use appropriate algorithm based on product age
            try:
                if algorithm == "0-6m":
                    result = tps_0_6m(units_data, seasonality_data, vine_claims, today, settings, product_sv)
                elif algorithm == "6-18m":
                    result = tps_6_18m(units_data, seasonality_data, today, settings, vine_claims, product_sv)
                else:  # 18m+
                    result = tps_18m(units_data, today, settings)
            except Exception as e:
                pass
            
            # Fallback: Try 18m+ if primary failed
            if result is None and algorithm != "18m+":
                try:
                    result = tps_18m(units_data, today, settings)
                    algorithm = f"{algorithm}->18m+"
                except:
                    pass
            
            # Fallback to 6-18m
            if result is None:
                try:
                    result = tps_6_18m(units_data, seasonality_data, today, settings, vine_claims, product_sv)
                    algorithm = "6-18m"
                except:
                    pass
            
            # Simple fallback
            if result is None:
                recent_units = [d['units'] for d in units_data[-12:]]
                avg_weekly = sum(recent_units) / len(recent_units) if recent_units else 0
                lead_time_weeks = 37 / 7
                doi_goal_weeks = 93 / 7
                units_needed = avg_weekly * (lead_time_weeks + doi_goal_weeks)
                
                result = {
                    'units_to_make': max(0, int(units_needed - total_inv)),
                    'doi_total_days': (total_inv / avg_weekly * 7) if avg_weekly > 0 else 0,
                    'doi_fba_days': (fba_avail / avg_weekly * 7) if avg_weekly > 0 else 0,
                    'total_units_needed': units_needed,
                    'sales_velocity_adjustment': 0
                }
                algorithm = "simple"
            
            results.append({
                'asin': asin,
                'algorithm': algorithm,
                'units_to_make': int(result.get('units_to_make', 0)),
                'doi_total_days': round(result.get('doi_total_days', 0), 2),
                'doi_fba_available_days': round(result.get('doi_fba_days', 0), 2),
                'unit_needed_total': round(result.get('total_units_needed', 0), 2),
                'sales_velocity_adjustment': round(result.get('sales_velocity_adjustment', 0), 4),
                'computed_at': now,
                'expires_at': expires_at,
                'settings_hash': 'default'
            })
            success += 1
            
        except Exception as e:
            errors += 1
    
    calc_time = time.perf_counter() - calc_start
    print(f"  Calculated: {success} forecasts in {calc_time:.2f}s")
    print(f"  Skipped: {skipped}, Errors: {errors}")
    
    # =========================================================
    # STEP 3: Clear old cache and insert new (in transaction)
    # =========================================================
    print("\n[3/4] Clearing old cache...")
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE forecast_cache")
        conn.commit()
        print("  Cache cleared successfully")
    finally:
        conn.close()
    
    # =========================================================
    # STEP 4: Bulk insert new cache (COPY)
    # =========================================================
    print("\n[4/4] Inserting new cache...")
    insert_start = time.perf_counter()
    
    fast_copy_insert(results, 'forecast_cache')
    
    insert_time = time.perf_counter() - insert_start
    print(f"  Inserted {len(results)} rows in {insert_time:.2f}s")
    
    # =========================================================
    # DONE
    # =========================================================
    total_time = time.perf_counter() - start_time
    
    print("\n" + "=" * 60)
    print("CACHE REFRESH COMPLETE")
    print("=" * 60)
    print(f"  Total products: {len(products)}")
    print(f"  Cached: {success}")
    print(f"  Skipped: {skipped}")
    print(f"  Errors: {errors}")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  Rate: {success/total_time:.0f} products/sec")

if __name__ == '__main__':
    refresh_cache()
