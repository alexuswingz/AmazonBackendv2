"""Compare our seasonality calculation with Excel's values"""
import sys
sys.path.insert(0, '.')
from sqlalchemy import create_engine, text
from app.algorithms.algorithms_tps import calculate_per_product_seasonality

POSTGRES_URL = 'postgresql://postgres:YyeRMrVpRBITQyZuAPAihQihqCiazuHJ@maglev.proxy.rlwy.net:27064/railway'
engine = create_engine(POSTGRES_URL)

asin = 'B0FM3HF8JK'

# Excel values from screenshot (seasonality_index column J)
EXCEL_SEASONALITY = {
    1: 0.06, 2: 0.07, 3: 0.09, 4: 0.13, 5: 0.17, 6: 0.21, 7: 0.28, 8: 0.38,
    9: 0.50, 10: 0.62, 11: 0.75, 12: 0.86, 13: 0.94, 14: 0.97, 15: 0.99, 16: 1.00,
    17: 0.99, 18: 0.94, 19: 0.86, 20: 0.76, 21: 0.68, 22: 0.62, 23: 0.59, 24: 0.56,
    25: 0.54, 26: 0.57, 27: 0.60, 28: 0.53, 29: 0.44, 30: 0.37, 31: 0.33, 32: 0.32,
    33: 0.31, 34: 0.30, 35: 0.29, 36: 0.27, 37: 0.25, 38: 0.24, 39: 0.23, 40: 0.22,
    41: 0.18, 42: 0.14, 43: 0.10, 44: 0.08, 45: 0.07, 46: 0.06, 47: 0.05, 48: 0.04,
    49: 0.04, 50: 0.04, 51: 0.05, 52: 0.05
}

# Get product search volume from database
with engine.connect() as conn:
    psv = conn.execute(text('SELECT week_date, search_volume FROM product_search_volume WHERE asin = :a ORDER BY week_date'), {'a': asin}).fetchall()
    product_sv = [{'week_date': r[0], 'search_volume': r[1]} for r in psv]

print(f'Product SV rows: {len(product_sv)}')

# Calculate our seasonality
our_seasonality = calculate_per_product_seasonality(product_sv)

# Compare
print(f'\n{"Week":<6} {"Excel":<8} {"Ours":<8} {"Diff":<8} {"Match?":<8}')
print('-' * 45)

mismatches = 0
for week in range(1, 53):
    excel_val = EXCEL_SEASONALITY.get(week, 0)
    our_val = our_seasonality.get(week, 0)
    diff = abs(excel_val - our_val)
    match = "OK" if diff < 0.02 else "DIFF"
    if diff >= 0.02:
        mismatches += 1
    print(f'{week:<6} {excel_val:<8.2f} {our_val:<8.2f} {diff:<8.3f} {match:<8}')

print(f'\nMismatches (>0.02): {mismatches}')
print(f'Match rate: {((52 - mismatches) / 52 * 100):.1f}%')

# Key weeks for 0-6m algorithm
print(f'\n=== KEY WEEKS ===')
print(f'Week 3 (current/winter): Excel={EXCEL_SEASONALITY.get(3)}, Ours={our_seasonality.get(3)}')
print(f'Week 16 (peak/summer): Excel={EXCEL_SEASONALITY.get(16)}, Ours={our_seasonality.get(16)}')
