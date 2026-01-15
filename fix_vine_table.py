from sqlalchemy import create_engine, text
import pandas as pd

POSTGRES_URL = 'postgresql://postgres:JMVZWnrhWpFToCzqgkEwCPhSBHCvUMuH@caboose.proxy.rlwy.net:54152/railway'
engine = create_engine(POSTGRES_URL)

with engine.connect() as conn:
    # Drop and recreate the table with proper schema
    conn.execute(text('DROP TABLE IF EXISTS vine_claims'))
    conn.execute(text('''
        CREATE TABLE vine_claims (
            id SERIAL PRIMARY KEY,
            asin VARCHAR(50) NOT NULL,
            product_name TEXT,
            claim_date DATE NOT NULL,
            units_claimed INTEGER DEFAULT 0,
            vine_status VARCHAR(100)
        )
    '''))
    conn.execute(text('CREATE INDEX ix_vine_claims_asin ON vine_claims(asin)'))
    conn.execute(text('CREATE INDEX ix_vine_claims_asin_date ON vine_claims(asin, claim_date)'))
    conn.commit()
    print('Table recreated with proper schema')

# Now re-seed
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
df = df.drop_duplicates(subset=['asin', 'claim_date'])

df.to_sql('vine_claims', engine, if_exists='append', index=False, method='multi')

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM vine_claims'))
    print(f'Inserted {result.scalar()} rows')
    
    # Check specific ASINs
    for asin in ['B0DPH77GSG', 'B0D4G6QJ7D']:
        result = conn.execute(text(f"SELECT asin, claim_date, units_claimed FROM vine_claims WHERE asin = '{asin}'"))
        rows = result.fetchall()
        print(f'{asin}: {rows if rows else "No vine claims"}')
