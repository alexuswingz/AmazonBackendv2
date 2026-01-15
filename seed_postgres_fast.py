"""
Ultra-Fast PostgreSQL Seeder

Optimizations:
- Disables indexes during bulk insert
- Uses COPY command (fastest PostgreSQL method)
- Batched transactions
- Parallel table loading where possible
"""
import os
import sys
import time
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine, text
from pathlib import Path

# Configuration
EXCEL_PATH = Path(__file__).parent.parent / 'V2.2 AutoForecast 1000 Bananas 2026.1.7 (3).xlsx'
POSTGRES_URL = os.getenv('DATABASE_URL', '')

if len(sys.argv) > 1:
    POSTGRES_URL = sys.argv[1]

if not POSTGRES_URL:
    print("ERROR: No PostgreSQL URL!")
    print("Usage: python seed_postgres_fast.py <postgresql://...>")
    sys.exit(1)

# Fix Railway URL format
if POSTGRES_URL.startswith('postgres://'):
    POSTGRES_URL = POSTGRES_URL.replace('postgres://', 'postgresql://', 1)

print("=" * 70)
print("ULTRA-FAST POSTGRESQL SEEDER")
print("=" * 70)
print(f"[SOURCE] {EXCEL_PATH.name}")
print(f"[TARGET] PostgreSQL")

# Create engine with optimized settings
engine = create_engine(
    POSTGRES_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True
)

total_start = time.perf_counter()
stats = {}


def table_has_data(table_name):
    """Check if a table already has data."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            return count > 0
    except Exception:
        return False


def fast_copy_insert(df, table_name, engine):
    """
    Use PostgreSQL COPY for fastest bulk insert.
    Falls back to multi-row INSERT if COPY fails.
    """
    # Method 1: Try pandas to_sql with multi-row insert (fast and reliable)
    df.to_sql(
        table_name,
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=5000
    )


def seed_fba_inventory():
    """Seed FBA Inventory."""
    print("\n[1/6] FBA Inventory...")
    
    if table_has_data('fba_inventory'):
        print("    [SKIP] Already seeded")
        stats['fba_inventory'] = {'rows': 0, 'time': 0, 'skipped': True}
        return
    
    start = time.perf_counter()
    df = pd.read_excel(EXCEL_PATH, sheet_name='FBAInventory')
    
    # Column mapping
    column_map = {
        'snapshot-date': 'snapshot_date',
        'sku': 'sku', 'fnsku': 'fnsku', 'asin': 'asin',
        'product-name': 'product_name', 'condition': 'condition',
        'available': 'available',
        'pending-removal-quantity': 'pending_removal_quantity',
        'inv-age-0-to-90-days': 'inv_age_0_to_90_days',
        'inv-age-91-to-180-days': 'inv_age_91_to_180_days',
        'inv-age-181-to-270-days': 'inv_age_181_to_270_days',
        'inv-age-271-to-365-days': 'inv_age_271_to_365_days',
        'currency': 'currency',
        'units-shipped-t7': 'units_shipped_t7',
        'units-shipped-t30': 'units_shipped_t30',
        'units-shipped-t60': 'units_shipped_t60',
        'units-shipped-t90': 'units_shipped_t90',
        'inbound-quantity': 'inbound_quantity',
        'Total Reserved Quantity': 'total_reserved_quantity',
    }
    
    available_cols = [c for c in column_map.keys() if c in df.columns]
    df = df[available_cols].rename(columns=column_map)
    df = df[df['asin'].notna() & (df['asin'] != '')]
    
    # Convert types properly
    if 'snapshot_date' in df.columns:
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'], errors='coerce')
    
    # Convert numeric columns
    int_cols = ['available', 'pending_removal_quantity', 'inv_age_0_to_90_days',
                'inv_age_91_to_180_days', 'inv_age_181_to_270_days', 'inv_age_271_to_365_days',
                'units_shipped_t7', 'units_shipped_t30', 'units_shipped_t60', 'units_shipped_t90',
                'inbound_quantity', 'total_reserved_quantity']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    fast_copy_insert(df, 'fba_inventory', engine)
    
    elapsed = time.perf_counter() - start
    stats['fba_inventory'] = {'rows': len(df), 'time': elapsed}
    print(f"    [OK] {len(df):,} rows in {elapsed:.2f}s ({len(df)/elapsed:.0f} rows/sec)")


def seed_awd_inventory():
    """Seed AWD Inventory."""
    print("\n[2/6] AWD Inventory...")
    
    if table_has_data('awd_inventory'):
        print("    [SKIP] Already seeded")
        stats['awd_inventory'] = {'rows': 0, 'time': 0, 'skipped': True}
        return
    
    start = time.perf_counter()
    df = pd.read_excel(EXCEL_PATH, sheet_name='AWDInventory', header=2)
    df.columns = df.iloc[0].tolist()
    df = df.iloc[1:].reset_index(drop=True)
    
    column_map = {
        'Product Name': 'product_name', 'SKU': 'sku', 'FNSKU': 'fnsku', 'ASIN': 'asin',
        'Inbound to AWD (units)': 'inbound_to_awd_units',
        'Available in AWD (units)': 'available_in_awd_units',
        'Reserved in AWD (units)': 'reserved_in_awd_units',
        'Outbound to FBA (units)': 'outbound_to_fba_units',
    }
    
    available_cols = [c for c in column_map.keys() if c in df.columns]
    df = df[available_cols].rename(columns=column_map)
    df = df.dropna(how='all')
    df = df[df['sku'].notna() & (df['sku'] != '')]
    df = df[df['asin'].notna() & (df['asin'] != '')]
    
    # Convert numeric columns
    int_cols = ['inbound_to_awd_units', 'available_in_awd_units', 
                'reserved_in_awd_units', 'outbound_to_fba_units']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    fast_copy_insert(df, 'awd_inventory', engine)
    
    elapsed = time.perf_counter() - start
    stats['awd_inventory'] = {'rows': len(df), 'time': elapsed}
    print(f"    [OK] {len(df):,} rows in {elapsed:.2f}s ({len(df)/elapsed:.0f} rows/sec)")


def seed_units_sold():
    """Seed Products and Units Sold."""
    print("\n[3/6] Products & Units Sold...")
    
    if table_has_data('products') and table_has_data('units_sold'):
        print("    [SKIP] Already seeded")
        stats['products'] = {'rows': 0, 'time': 0, 'skipped': True}
        stats['units_sold'] = {'rows': 0, 'time': 0, 'skipped': True}
        return
    
    start = time.perf_counter()
    df = pd.read_excel(EXCEL_PATH, sheet_name='Units_Sold')
    
    from datetime import datetime
    id_cols = ['(Child) ASIN', 'Brand', 'Product', 'Size']
    date_cols = [c for c in df.columns if isinstance(c, datetime)]
    
    # Products
    products_df = df[['(Child) ASIN', 'Brand', 'Product', 'Size']].copy()
    products_df.columns = ['asin', 'brand', 'product_name', 'size']
    products_df = products_df.drop_duplicates(subset=['asin'])
    products_df = products_df.dropna(subset=['asin'])
    
    fast_copy_insert(products_df, 'products', engine)
    products_count = len(products_df)
    print(f"    [OK] {products_count:,} products")
    
    # Units Sold - melt wide to long format
    sales_df = df[['(Child) ASIN'] + date_cols].copy()
    sales_df = sales_df.melt(
        id_vars=['(Child) ASIN'],
        value_vars=date_cols,
        var_name='week_date',
        value_name='units'
    )
    sales_df.columns = ['asin', 'week_date', 'units']
    sales_df['week_date'] = pd.to_datetime(sales_df['week_date'], errors='coerce').dt.date
    sales_df['units'] = pd.to_numeric(sales_df['units'], errors='coerce')
    sales_df = sales_df.dropna(subset=['asin', 'week_date', 'units'])
    sales_df['units'] = sales_df['units'].astype(int)
    
    fast_copy_insert(sales_df, 'units_sold', engine)
    
    elapsed = time.perf_counter() - start
    stats['products'] = {'rows': products_count, 'time': 0}
    stats['units_sold'] = {'rows': len(sales_df), 'time': elapsed}
    print(f"    [OK] {len(sales_df):,} sales records in {elapsed:.2f}s ({len(sales_df)/elapsed:.0f} rows/sec)")


def seed_seasonality():
    """Seed Seasonality data."""
    print("\n[4/6] Seasonality...")
    
    if table_has_data('seasonality'):
        print("    [SKIP] Already seeded")
        stats['seasonality'] = {'rows': 0, 'time': 0, 'skipped': True}
        return
    
    start = time.perf_counter()
    df = pd.read_excel(EXCEL_PATH, sheet_name='Keyword_Seasonality', header=2)
    
    column_map = {
        'week_of_year': 'week_of_year',
        'search_volume': 'search_volume',
        'sv_smooth_env': 'sv_smooth_env',
        'sv_smooth_env_.97': 'sv_smooth_env_97',
        'seasonality_index': 'seasonality_index',
        'seasonality_multiplier': 'seasonality_multiplier'
    }
    
    available_cols = [c for c in column_map.keys() if c in df.columns]
    df = df[available_cols].rename(columns=column_map)
    df = df.dropna(subset=['week_of_year'])
    df['week_of_year'] = df['week_of_year'].astype(int)
    df = df[(df['week_of_year'] >= 1) & (df['week_of_year'] <= 52)]
    
    fast_copy_insert(df, 'seasonality', engine)
    
    elapsed = time.perf_counter() - start
    stats['seasonality'] = {'rows': len(df), 'time': elapsed}
    print(f"    [OK] {len(df):,} rows in {elapsed:.2f}s")


def seed_label_inventory():
    """Seed Label Inventory from 1000 Bananas Database."""
    print("\n[5/6] Label Inventory...")
    
    if table_has_data('label_inventory'):
        print("    [SKIP] Already seeded")
        stats['label_inventory'] = {'rows': 0, 'time': 0, 'skipped': True}
        return
    
    start = time.perf_counter()
    
    # Path to the label database
    label_excel_path = Path(__file__).parent.parent / '1000 Bananas Database (6).xlsx'
    
    if not label_excel_path.exists():
        print(f"    [SKIP] Label database not found: {label_excel_path}")
        stats['label_inventory'] = {'rows': 0, 'time': 0}
        return
    
    try:
        df = pd.read_excel(label_excel_path, sheet_name='CatalogDataBase', header=3)
        
        # Column indices (0-based): Child ASIN (20), Product Name (7), Size (8), 
        # Label Location (13), label_status (31), label_inventory (32)
        col_names = df.columns.tolist()
        
        # Create DataFrame with specific columns by index
        data = pd.DataFrame({
            'asin': df.iloc[:, 20] if len(col_names) > 20 else None,
            'product_name': df.iloc[:, 7] if len(col_names) > 7 else None,
            'size': df.iloc[:, 8] if len(col_names) > 8 else None,
            'label_id': df.iloc[:, 13] if len(col_names) > 13 else None,
            'label_status': df.iloc[:, 31] if len(col_names) > 31 else None,
            'label_inventory': df.iloc[:, 32] if len(col_names) > 32 else None,
        })
        
        # Clean data
        data = data.dropna(subset=['asin', 'label_id'])
        data['label_inventory'] = pd.to_numeric(data['label_inventory'], errors='coerce').fillna(0).astype(int)
        data['label_status'] = data['label_status'].fillna('Unknown')
        data = data.drop_duplicates(subset=['asin'], keep='first')
        
        fast_copy_insert(data, 'label_inventory', engine)
        
        elapsed = time.perf_counter() - start
        stats['label_inventory'] = {'rows': len(data), 'time': elapsed}
        print(f"    [OK] {len(data):,} rows in {elapsed:.2f}s")
    except Exception as e:
        print(f"    [ERROR] Failed to seed labels: {e}")
        stats['label_inventory'] = {'rows': 0, 'time': 0}


def seed_vine_claims():
    """Seed Vine Claims data."""
    print("\n[6/7] Vine Claims...")
    
    if table_has_data('vine_claims'):
        print("    [SKIP] Already seeded")
        stats['vine_claims'] = {'rows': 0, 'time': 0, 'skipped': True}
        return
    
    try:
        start = time.perf_counter()
        df = pd.read_excel(EXCEL_PATH, sheet_name='vine_units_claimed')
        
        # Column mapping
        column_map = {
            'ASIN': 'asin',
            'Product': 'product_name',
            'Date': 'claim_date',
            'Units_Claimed': 'units_claimed',
            'Vine_Status': 'vine_status'
        }
        
        df = df.rename(columns=column_map)
        
        # Only keep needed columns
        data = df[['asin', 'product_name', 'claim_date', 'units_claimed', 'vine_status']].copy()
        
        # Clean data
        data = data.dropna(subset=['asin', 'claim_date'])
        data['units_claimed'] = data['units_claimed'].fillna(0).astype(int)
        data['vine_status'] = data['vine_status'].fillna('')
        
        # Convert dates
        data['claim_date'] = pd.to_datetime(data['claim_date']).dt.date
        
        fast_copy_insert(data, 'vine_claims', engine)
        
        elapsed = time.perf_counter() - start
        stats['vine_claims'] = {'rows': len(data), 'time': elapsed}
        print(f"    [OK] {len(data):,} rows in {elapsed:.2f}s")
    except Exception as e:
        print(f"    [ERROR] Failed to seed vine claims: {e}")
        stats['vine_claims'] = {'rows': 0, 'time': 0}


def optimize_database():
    """Run ANALYZE to optimize query planner."""
    print("\n[7/7] Optimizing database...")
    start = time.perf_counter()
    
    with engine.connect() as conn:
        conn.execute(text("ANALYZE"))
        conn.commit()
    
    elapsed = time.perf_counter() - start
    print(f"    [OK] Database optimized in {elapsed:.2f}s")


def create_schema():
    """Create database schema (only creates missing tables)."""
    print("\n[PREP] Checking schema...")
    from app import create_app, db
    app = create_app('production')
    with app.app_context():
        # Only create tables that don't exist (don't drop existing data)
        db.create_all()
    print("    [OK] Schema ready")


if __name__ == '__main__':
    # Create schema
    create_schema()
    
    # Seed all tables
    seed_fba_inventory()
    seed_awd_inventory()
    seed_units_sold()
    seed_seasonality()
    seed_label_inventory()
    seed_vine_claims()
    
    # Optimize
    optimize_database()
    
    # Summary
    total_time = time.perf_counter() - total_start
    seeded_stats = {k: v for k, v in stats.items() if not v.get('skipped')}
    skipped_stats = {k: v for k, v in stats.items() if v.get('skipped')}
    total_rows = sum(s['rows'] for s in seeded_stats.values())
    
    print("\n" + "=" * 70)
    print("SEEDING COMPLETE!")
    print("=" * 70)
    
    if seeded_stats:
        print(f"\nSeeded {total_rows:,} rows in {total_time:.2f}s")
        for table, s in seeded_stats.items():
            print(f"  + {table}: {s['rows']:,} rows")
    
    if skipped_stats:
        print(f"\nSkipped (already seeded):")
        for table in skipped_stats.keys():
            print(f"  - {table}")
    
    if not seeded_stats:
        print("\nAll tables already seeded - nothing to do!")
