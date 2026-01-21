from sqlalchemy import create_engine, text
engine = create_engine('postgresql://postgres:YyeRMrVpRBITQyZuAPAihQihqCiazuHJ@maglev.proxy.rlwy.net:27064/railway')
with engine.connect() as conn:
    r = conn.execute(text("SELECT algorithm, units_to_make, doi_total_days FROM forecast_cache WHERE asin = 'B0FM3HF8JK'")).fetchone()
    if r:
        print(f'Cache: algorithm={r[0]}, units_to_make={r[1]}, doi_total={r[2]}')
    else:
        print('No cache entry found')
