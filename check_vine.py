from sqlalchemy import create_engine, text

POSTGRES_URL = 'postgresql://postgres:JMVZWnrhWpFToCzqgkEwCPhSBHCvUMuH@caboose.proxy.rlwy.net:54152/railway'
engine = create_engine(POSTGRES_URL)

with engine.connect() as conn:
    # Clean up: truncate and re-insert unique records
    conn.execute(text('TRUNCATE vine_claims'))
    conn.commit()
    print('Cleared vine_claims table')

# Now re-seed properly
import pandas as pd
df = pd.read_excel('../V2.2 AutoForecast 1000 Bananas 2026.1.7 (3).xlsx', sheet_name='vine_units_claimed')
df = df.rename(columns={
    'ASIN': 'asin',
    'Product': 'product_name', 
    'Date': 'claim_date',
    'Units_Claimed': 'units_claimed',
    'Vine_Status': 'vine_status'
})
df = df[['asin', 'product_name', 'claim_date', 'units_claimed', 'vine_status']].copy()
df = df.dropna(subset=['asin', 'claim_date'])
df['units_claimed'] = df['units_claimed'].fillna(0).astype(int)
df['vine_status'] = df['vine_status'].fillna('')
df['claim_date'] = pd.to_datetime(df['claim_date']).dt.date

# Remove duplicates
df = df.drop_duplicates(subset=['asin', 'claim_date'])
print(f'Inserting {len(df)} unique vine claims')

df.to_sql('vine_claims', engine, if_exists='append', index=False, method='multi')

# Verify
with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM vine_claims'))
    print('Final count:', result.scalar())
    
    # Check specific ASINs
    for asin in ['B0DPH77GSG', 'B0D4G6QJ7D']:
        result = conn.execute(text(f"SELECT asin, claim_date, units_claimed FROM vine_claims WHERE asin = '{asin}'"))
        rows = result.fetchall()
        print(f'{asin}:', rows if rows else 'No vine claims')
