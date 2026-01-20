"""
Full sync from new Excel file V2.2 AutoForecast 1000 Bananas 2026.1.7 (9).xlsx
"""
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

EXCEL = '../V2.2 AutoForecast 1000 Bananas 2026.1.7 (9).xlsx'
DATABASE_URL = 'postgresql://postgres:JMVZWnrhWpFToCzqgkEwCPhSBHCvUMuH@caboose.proxy.rlwy.net:54152/railway'
engine = create_engine(DATABASE_URL)

def sync_fba_inventory():
    """Sync FBA inventory from Excel"""
    print("Syncing FBA inventory...")
    df = pd.read_excel(EXCEL, sheet_name='FBAInventory')
    
    # Rename columns to match DB schema
    col_map = {
        'snapshot-date': 'snapshot_date',
        'product-name': 'product_name',
        'pending-removal-quantity': 'pending_removal_quantity',
        'inv-age-0-to-90-days': 'inv_age_0_to_90_days',
        'inv-age-91-to-180-days': 'inv_age_91_to_180_days',
        'inv-age-181-to-270-days': 'inv_age_181_to_270_days',
        'inv-age-271-to-365-days': 'inv_age_271_to_365_days',
        'units-shipped-t7': 'units_shipped_t7',
        'units-shipped-t30': 'units_shipped_t30',
        'units-shipped-t60': 'units_shipped_t60',
        'units-shipped-t90': 'units_shipped_t90',
        'your-price': 'your_price',
        'sales-price': 'sales_price',
        'lowest-price-new-plus-shipping': 'lowest_price_new_plus_shipping',
        'lowest-price-used': 'lowest_price_used',
        'recommended-action': 'recommended_action',
        'sell-through': 'sell_through',
        'item-volume': 'item_volume',
        'volume-unit-measurement': 'volume_unit_measurement',
        'storage-type': 'storage_type',
        'storage-volume': 'storage_volume',
        'product-group': 'product_group',
        'sales-rank': 'sales_rank',
        'days-of-supply': 'days_of_supply',
        'estimated-excess-quantity': 'estimated_excess_quantity',
        'weeks-of-cover-t30': 'weeks_of_cover_t30',
        'weeks-of-cover-t90': 'weeks_of_cover_t90',
        'featuredoffer-price': 'featuredoffer_price',
        'sales-shipped-last-7-days': 'sales_shipped_last_7_days',
        'sales-shipped-last-30-days': 'sales_shipped_last_30_days',
        'sales-shipped-last-60-days': 'sales_shipped_last_60_days',
        'sales-shipped-last-90-days': 'sales_shipped_last_90_days',
        'inbound-quantity': 'inbound_quantity',
        'inbound-working': 'inbound_working',
        'inbound-shipped': 'inbound_shipped',
        'inbound-received': 'inbound_received',
        'total-reserved-quantity': 'total_reserved_quantity',
        'Total Reserved Quantity': 'total_reserved_quantity',  # Capital version
        'unfulfillable-quantity': 'unfulfillable_quantity',
        'estimated-storage-cost-next-month': 'estimated_storage_cost_next_month',
        'historical-days-of-supply': 'historical_days_of_supply',
        'fba-minimum-inventory-level': 'fba_minimum_inventory_level',
        'fba-inventory-level-health-status': 'fba_inventory_level_health_status',
        'inventory-supply-at-fba': 'inventory_supply_at_fba',
    }
    df = df.rename(columns=col_map)
    
    # Handle snapshot_date
    df = df[df['snapshot_date'].notna()]
    df = df[~df['snapshot_date'].astype(str).str.contains('snapshot', case=False)]
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'], errors='coerce')
    df = df[df['snapshot_date'].notna()]
    
    # Fill numeric columns
    numeric_cols = ['available', 'inbound_quantity', 'total_reserved_quantity', 'unfulfillable_quantity']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fba_inventory RESTART IDENTITY"))
    
    # Select only columns that exist in DB
    db_cols = ['snapshot_date', 'sku', 'fnsku', 'asin', 'product_name', 'condition', 
               'available', 'pending_removal_quantity', 'inv_age_0_to_90_days',
               'inv_age_91_to_180_days', 'inv_age_181_to_270_days', 'inv_age_271_to_365_days',
               'currency', 'units_shipped_t7', 'units_shipped_t30', 'units_shipped_t60',
               'units_shipped_t90', 'alert', 'your_price', 'sales_price',
               'lowest_price_new_plus_shipping', 'lowest_price_used', 'recommended_action',
               'sell_through', 'item_volume', 'volume_unit_measurement', 'storage_type',
               'storage_volume', 'marketplace', 'product_group', 'sales_rank', 'days_of_supply',
               'estimated_excess_quantity', 'weeks_of_cover_t30', 'weeks_of_cover_t90',
               'featuredoffer_price', 'sales_shipped_last_7_days', 'sales_shipped_last_30_days',
               'sales_shipped_last_60_days', 'sales_shipped_last_90_days', 'inbound_quantity',
               'inbound_working', 'inbound_shipped', 'inbound_received', 'total_reserved_quantity',
               'unfulfillable_quantity', 'estimated_storage_cost_next_month',
               'historical_days_of_supply', 'fba_minimum_inventory_level',
               'fba_inventory_level_health_status', 'inventory_supply_at_fba', 'supplier']
    
    existing_cols = [c for c in db_cols if c in df.columns]
    df_insert = df[existing_cols].copy()
    
    df_insert.to_sql('fba_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  FBA inventory: {len(df_insert)} rows")
    return len(df_insert)


def sync_awd_inventory():
    """Sync AWD inventory from Excel"""
    print("Syncing AWD inventory...")
    # Header is on row 3
    df = pd.read_excel(EXCEL, sheet_name='AWDInventory', header=3)
    
    col_map = {
        'Product Name': 'product_name',
        'SKU': 'sku',
        'FNSKU': 'fnsku',
        'ASIN': 'asin',
        'Inbound to AWD (units)': 'inbound_to_awd_units',
        'Inbound to AWD (cases)': 'inbound_to_awd_cases',
        'Available in AWD (units)': 'available_in_awd_units',
        'Available in AWD (cases)': 'available_in_awd_cases',
        'Reserved in AWD (units)': 'reserved_in_awd_units',
        'Reserved in AWD (cases)': 'reserved_in_awd_cases',
        'Outbound Order (units)': 'outbound_order_units',
        'Outbound Order (cases)': 'outbound_order_cases',
        'Researching (units)': 'researching_units',
        'Researching (cases)': 'researching_cases',
        'Outbound to FBA (units)': 'outbound_to_fba_units',
        'Available in FBA (units)': 'available_in_fba_units',
        'Available in FBA (days)': 'available_in_fba_days',
        'Reserved in FBA (units)': 'reserved_in_fba_units',
        'Customer Order (units)': 'customer_order_units',
        'FC Processing (units)': 'fc_processing_units',
        'FC Transfer (units)': 'fc_transfer_units',
        'Days of Supply (days)': 'days_of_supply',
        'Auto Replenishment Ratio (percent)': 'auto_replenishment_ratio',
    }
    df = df.rename(columns=col_map)
    
    # Handle lowercase asin column if exists
    if 'asin' not in df.columns and 'ASIN' in df.columns:
        df['asin'] = df['ASIN']
    
    # Filter out rows with missing ASIN
    df = df[df['asin'].notna() & (df['asin'] != '')]
    
    # Clean all numeric columns
    numeric_int_cols = ['inbound_to_awd_units', 'inbound_to_awd_cases', 'available_in_awd_units', 
                        'available_in_awd_cases', 'reserved_in_awd_units', 'reserved_in_awd_cases',
                        'outbound_order_units', 'outbound_order_cases', 'researching_units', 
                        'researching_cases', 'outbound_to_fba_units', 'available_in_fba_units',
                        'available_in_fba_days', 'reserved_in_fba_units', 'customer_order_units',
                        'fc_processing_units', 'fc_transfer_units']
    for col in numeric_int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Float columns
    numeric_float_cols = ['days_of_supply', 'auto_replenishment_ratio']
    for col in numeric_float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE awd_inventory RESTART IDENTITY"))
    
    db_cols = ['product_name', 'sku', 'fnsku', 'asin', 'inbound_to_awd_units', 'inbound_to_awd_cases',
               'available_in_awd_units', 'available_in_awd_cases', 'reserved_in_awd_units',
               'reserved_in_awd_cases', 'outbound_order_units', 'outbound_order_cases',
               'researching_units', 'researching_cases', 'outbound_to_fba_units',
               'available_in_fba_units', 'available_in_fba_days', 'reserved_in_fba_units',
               'customer_order_units', 'fc_processing_units', 'fc_transfer_units',
               'days_of_supply', 'auto_replenishment_ratio']
    
    existing_cols = [c for c in db_cols if c in df.columns]
    df_insert = df[existing_cols].copy()
    
    df_insert.to_sql('awd_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  AWD inventory: {len(df_insert)} rows")
    return len(df_insert)


def sync_label_inventory():
    """Sync label inventory from Excel"""
    print("Syncing label inventory...")
    df = pd.read_excel(EXCEL, sheet_name='label_inventory')
    
    col_map = {
        '(Child) ASIN': 'asin',
        'Product Name': 'product_name',
        'Size': 'size',
        'Label ID': 'label_id',
        'Label Status': 'label_status',
        'label_inventory': 'label_inventory',
    }
    df = df.rename(columns=col_map)
    
    df['asin'] = df['asin'].astype(str)
    df['label_inventory'] = pd.to_numeric(df['label_inventory'], errors='coerce').fillna(0).astype(int)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE label_inventory"))
    
    db_cols = ['asin', 'product_name', 'size', 'label_id', 'label_status', 'label_inventory']
    existing_cols = [c for c in db_cols if c in df.columns]
    df_insert = df[existing_cols].copy()
    
    df_insert.to_sql('label_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  Label inventory: {len(df_insert)} rows")
    return len(df_insert)


def sync_units_sold():
    """Sync units sold from Excel - wide format with dates as columns"""
    print("Syncing units sold...")
    df = pd.read_excel(EXCEL, sheet_name='Units_Sold')
    
    # The sheet has: (Child) ASIN, Brand, Product, Size, then date columns
    # We need to melt it to long format
    
    id_cols = ['(Child) ASIN', 'Brand', 'Product', 'Size']
    id_cols_present = [c for c in id_cols if c in df.columns]
    
    # Get date columns (everything that's a datetime or can be parsed as date)
    date_cols = [c for c in df.columns if c not in id_cols]
    
    # Melt the dataframe
    df_melted = df.melt(id_vars=id_cols_present, value_vars=date_cols, 
                        var_name='date', value_name='units_ordered')
    
    # Rename ASIN column
    df_melted = df_melted.rename(columns={'(Child) ASIN': 'asin'})
    
    df_melted['asin'] = df_melted['asin'].astype(str)
    df_melted['week_date'] = pd.to_datetime(df_melted['date'], errors='coerce')
    df_melted = df_melted[df_melted['week_date'].notna()]
    df_melted['units'] = pd.to_numeric(df_melted['units_ordered'], errors='coerce').fillna(0).astype(int)
    
    # Drop rows with zero sales to save space
    df_melted = df_melted[df_melted['units'] > 0]
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE units_sold RESTART IDENTITY"))
    
    df_insert = df_melted[['asin', 'week_date', 'units']].copy()
    df_insert.to_sql('units_sold', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print(f"  Units sold: {len(df_insert)} rows")
    return len(df_insert)


def sync_vine_claims():
    """Sync vine claims from Excel"""
    print("Syncing vine claims...")
    df = pd.read_excel(EXCEL, sheet_name='vine_units_claimed')
    
    col_map = {
        'ASIN': 'asin',
        'Date': 'claim_date',
        'Units_Claimed': 'units_claimed',
    }
    df = df.rename(columns=col_map)
    
    if 'asin' not in df.columns:
        print("  ERROR: No ASIN column found")
        return 0
    
    df = df[df['asin'].notna() & (df['asin'] != '')]
    df['asin'] = df['asin'].astype(str)
    df['claim_date'] = pd.to_datetime(df['claim_date'], errors='coerce')
    df = df[df['claim_date'].notna()]
    df['units_claimed'] = pd.to_numeric(df.get('units_claimed', 1), errors='coerce').fillna(1).astype(int)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE vine_claims RESTART IDENTITY"))
    
    df_insert = df[['asin', 'claim_date', 'units_claimed']].copy()
    df_insert.to_sql('vine_claims', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print(f"  Vine claims: {len(df_insert)} rows")
    return len(df_insert)


def sync_product_search_volume():
    """Sync product search volume from sv_database - wide format with dates as columns"""
    print("Syncing product search volume...")
    # Skip first row which is header names
    df = pd.read_excel(EXCEL, sheet_name='sv_database', header=0)
    
    # First 3 columns are: (Parent) ASIN, (Child) ASIN, Title
    # Remaining columns are week dates
    
    # Rename columns
    first_cols = list(df.columns[:3])
    df = df.rename(columns={first_cols[1]: 'asin'})  # (Child) ASIN
    
    # Get date columns (everything except first 3 cols)
    date_cols = [c for c in df.columns if c not in first_cols and c != 'asin' and 'Unnamed' not in str(c)]
    
    # Skip header row if present
    df = df[df['asin'] != '(Child) ASIN']
    
    # Melt to long format
    df_melted = df.melt(id_vars=['asin'], value_vars=date_cols,
                        var_name='week_end', value_name='search_volume')
    
    df_melted = df_melted[df_melted['asin'].notna() & (df_melted['asin'] != '')]
    df_melted['asin'] = df_melted['asin'].astype(str)
    df_melted['week_date'] = pd.to_datetime(df_melted['week_end'], errors='coerce')
    df_melted = df_melted[df_melted['week_date'].notna()]
    df_melted['search_volume'] = pd.to_numeric(df_melted['search_volume'], errors='coerce').fillna(0).astype(float)
    
    # Drop rows with zero search volume
    df_melted = df_melted[df_melted['search_volume'] > 0]
    
    # De-duplicate - keep first occurrence
    df_melted = df_melted.drop_duplicates(subset=['asin', 'week_date'], keep='first')
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE product_search_volume RESTART IDENTITY"))
    
    df_insert = df_melted[['asin', 'week_date', 'search_volume']].copy()
    df_insert.to_sql('product_search_volume', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print(f"  Product search volume: {len(df_insert)} rows")
    return len(df_insert)


def sync_seasonality():
    """Sync global seasonality from Keyword_Seasonality"""
    print("Syncing seasonality...")
    df = pd.read_excel(EXCEL, sheet_name='Keyword_Seasonality', header=2)
    
    print(f"  Columns: {list(df.columns)[:15]}")
    
    col_map = {
        'week_of_year': 'week_of_year',
        'sv_smooth_env_.97': 'sv_smooth_env_97',
        'seasonality_index': 'seasonality_index',
    }
    df = df.rename(columns=col_map)
    
    df['week_of_year'] = pd.to_numeric(df.get('week_of_year', 0), errors='coerce').fillna(0).astype(int)
    df['sv_smooth_env_97'] = pd.to_numeric(df.get('sv_smooth_env_97', 0), errors='coerce').fillna(0)
    df['seasonality_index'] = pd.to_numeric(df.get('seasonality_index', 0), errors='coerce').fillna(0)
    
    df = df[df['week_of_year'] > 0]
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE seasonality RESTART IDENTITY"))
    
    df_insert = df[['week_of_year', 'sv_smooth_env_97', 'seasonality_index']].copy()
    df_insert.to_sql('seasonality', engine, if_exists='append', index=False, method='multi', chunksize=100)
    print(f"  Seasonality: {len(df_insert)} rows")
    return len(df_insert)


def verify_data():
    """Verify the synced data for B0F2MMYLD8"""
    print("\n" + "="*60)
    print("VERIFICATION FOR B0F2MMYLD8")
    print("="*60)
    
    with engine.connect() as conn:
        # FBA
        fba = conn.execute(text("""
            SELECT available, inbound_quantity, total_reserved_quantity
            FROM fba_inventory WHERE asin = 'B0F2MMYLD8'
        """)).fetchone()
        if fba:
            avail, inb, res = fba[0] or 0, fba[1] or 0, fba[2] or 0
            print(f"FBA: available={avail}, inbound={inb}, reserved={res}")
            print(f"     Total FBA = {avail + inb + res}")
        
        # AWD
        awd = conn.execute(text("""
            SELECT available_in_awd_units, inbound_to_awd_units, reserved_in_awd_units
            FROM awd_inventory WHERE asin = 'B0F2MMYLD8'
        """)).fetchone()
        if awd:
            avail, inb, res = awd[0] or 0, awd[1] or 0, awd[2] or 0
            print(f"AWD: available={avail}, inbound={inb}, reserved={res}")
            print(f"     Total AWD = {avail + inb + res}")
        
        # Label
        label = conn.execute(text("""
            SELECT label_inventory FROM label_inventory WHERE asin = 'B0F2MMYLD8'
        """)).fetchone()
        if label:
            print(f"Label inventory: {label[0]}")
        
        # Expected from UI:
        print("\nExpected from UI:")
        print("  FBA: Total=984, Available=245, Reserved=739, Inbound=0")
        print("  AWD: Total=200, Available=0, Outbound=200")
        print("  Label: 4,175")


if __name__ == '__main__':
    print("="*60)
    print("SYNCING ALL DATA FROM NEW EXCEL FILE")
    print("="*60)
    print()
    
    sync_fba_inventory()
    sync_awd_inventory()
    sync_label_inventory()
    sync_units_sold()
    sync_vine_claims()
    sync_product_search_volume()
    sync_seasonality()
    
    verify_data()
    
    print("\n✅ All data synced!")
