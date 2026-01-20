"""Quick data sync - inventory and seasonality from Excel"""
import pandas as pd
from sqlalchemy import create_engine, text
import time

EXCEL = r'C:\Users\User\OneDrive\Desktop\NewData\V2.2 AutoForecast 1000 Bananas 2026.1.7 (8).xlsx'
DB = 'postgresql://postgres:JMVZWnrhWpFToCzqgkEwCPhSBHCvUMuH@caboose.proxy.rlwy.net:54152/railway'
engine = create_engine(DB)

start = time.time()
print("Syncing data from Excel...")

# ============================================
# SEASONALITY from Keyword_Seasonality sheet
# ============================================
print("\n[1] Syncing Keyword_Seasonality...")
season = pd.read_excel(EXCEL, sheet_name='Keyword_Seasonality', header=2)
# Columns: week_of_year, raw_sv, ..., sv_smooth_env_.97, seasonality_index
season = season.rename(columns={
    'sv_smooth_env_.97': 'sv_smooth_env_97'
})
season = season[['week_of_year', 'sv_smooth_env_97', 'seasonality_index']]
season = season[season['week_of_year'].notna()]
season['week_of_year'] = pd.to_numeric(season['week_of_year'], errors='coerce').fillna(0).astype(int)
season['sv_smooth_env_97'] = pd.to_numeric(season['sv_smooth_env_97'], errors='coerce').fillna(0)
season['seasonality_index'] = pd.to_numeric(season['seasonality_index'], errors='coerce').fillna(0)
season = season[season['week_of_year'] > 0]

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE seasonality"))
season.to_sql('seasonality', engine, if_exists='append', index=False, method='multi')
print(f"  Seasonality: {len(season)} rows")

# ============================================
# INVENTORY
# ============================================
print("\n[2] Syncing FBA Inventory...")

# FBA - include inbound and reserved
fba = pd.read_excel(EXCEL, sheet_name='FBAInventory')
fba = fba.rename(columns={
    'snapshot-date': 'snapshot_date', 
    'product-name': 'product_name',
    'inbound-quantity': 'inbound_quantity',
    'Total Reserved Quantity': 'total_reserved_quantity'
})
fba = fba[fba['asin'].notna()]
fba = fba[fba['snapshot_date'].apply(lambda x: not isinstance(x, str) or 'snapshot' not in x.lower())]
fba['asin'] = fba['asin'].astype(str)

# Select columns including inbound and reserved
cols = ['snapshot_date', 'sku', 'fnsku', 'asin', 'product_name', 'condition', 'available', 'inbound_quantity', 'total_reserved_quantity']
for c in cols:
    if c not in fba.columns:
        fba[c] = 0
fba = fba[cols]

fba['available'] = pd.to_numeric(fba['available'], errors='coerce').fillna(0).astype(int)
fba['inbound_quantity'] = pd.to_numeric(fba['inbound_quantity'], errors='coerce').fillna(0).astype(int)
fba['total_reserved_quantity'] = pd.to_numeric(fba['total_reserved_quantity'], errors='coerce').fillna(0).astype(int)

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE fba_inventory"))
fba.to_sql('fba_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
print(f"  FBA: {len(fba)} rows")

# AWD - include all inventory components
awd = pd.read_excel(EXCEL, sheet_name='AWDInventory', header=3)
awd = awd.rename(columns={
    'Product Name': 'product_name', 'SKU': 'sku', 'FNSKU': 'fnsku', 'ASIN': 'asin',
    'Available in AWD (units)': 'available_in_awd_units', 
    'Available in AWD (cases)': 'available_in_awd_cases',
    'Inbound to AWD (units)': 'inbound_to_awd_units',
    'Reserved in AWD (units)': 'reserved_in_awd_units',
    'Outbound to FBA (units)': 'outbound_to_fba_units'
})
awd = awd[awd['asin'].notna()]
awd = awd[awd['asin'].apply(lambda x: not isinstance(x, str) or 'asin' not in x.lower())]
awd['asin'] = awd['asin'].astype(str)

cols = ['product_name', 'sku', 'fnsku', 'asin', 'available_in_awd_units', 'available_in_awd_cases', 
        'inbound_to_awd_units', 'reserved_in_awd_units', 'outbound_to_fba_units']
for c in cols:
    if c not in awd.columns:
        awd[c] = 0
awd = awd[cols]

for c in ['available_in_awd_units', 'available_in_awd_cases', 'inbound_to_awd_units', 'reserved_in_awd_units', 'outbound_to_fba_units']:
    awd[c] = pd.to_numeric(awd[c], errors='coerce').fillna(0).astype(int)

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE awd_inventory"))
awd.to_sql('awd_inventory', engine, if_exists='append', index=False, method='multi', chunksize=500)
print(f"  AWD: {len(awd)} rows")

print(f"DONE in {time.time()-start:.1f}s")
