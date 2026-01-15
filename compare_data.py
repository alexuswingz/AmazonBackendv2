"""Compare database data vs Excel file to find discrepancies."""
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime

DATABASE_URL = "postgresql://postgres:JMVZWnrhWpFToCzqgkEwCPhSBHCvUMuH@caboose.proxy.rlwy.net:54152/railway"
EXCEL_PATH = Path(__file__).parent.parent / 'V2.2 AutoForecast 1000 Bananas 2026.1.7 (4).xlsx'

engine = create_engine(DATABASE_URL)

print("=" * 70)
print("DATABASE vs EXCEL DATA COMPARISON")
print("=" * 70)
print(f"Excel: {EXCEL_PATH.name}")
print()

# Test ASINs to verify
TEST_ASINS = ['B0CPGFGKNQ', 'B0DQR1X69R', 'B0DQTNRXBG', 'B0DQTV6HL9']

# ============================================================
# 1. FBA INVENTORY
# ============================================================
print("\n[1] FBA INVENTORY")
print("-" * 50)

try:
    excel_fba = pd.read_excel(EXCEL_PATH, sheet_name='FBAInventory')
    
    with engine.connect() as conn:
        for asin in TEST_ASINS:
            # Database
            db_fba = conn.execute(text("""
                SELECT asin, SUM(available) as available, SUM(inbound_quantity) as inbound,
                       SUM(total_reserved_quantity) as reserved
                FROM fba_inventory WHERE asin = :asin GROUP BY asin
            """), {'asin': asin}).fetchone()
            
            # Excel
            excel_row = excel_fba[excel_fba['ASIN'] == asin]
            
            if db_fba and not excel_row.empty:
                excel_avail = excel_row['Available'].sum() if 'Available' in excel_row.columns else 0
                excel_inbound = excel_row['Inbound'].sum() if 'Inbound' in excel_row.columns else 0
                
                db_avail = db_fba.available or 0
                db_inbound = db_fba.inbound or 0
                
                match = "OK" if db_avail == excel_avail else "MISMATCH"
                print(f"  {asin}: DB={db_avail}, Excel={excel_avail} [{match}]")
            else:
                print(f"  {asin}: Not found in one source")
except Exception as e:
    print(f"  Error: {e}")

# ============================================================
# 2. AWD INVENTORY
# ============================================================
print("\n[2] AWD INVENTORY")
print("-" * 50)

try:
    excel_awd = pd.read_excel(EXCEL_PATH, sheet_name='AWDInventory', header=2)
    excel_awd.columns = excel_awd.iloc[0].tolist()
    excel_awd = excel_awd.iloc[1:].reset_index(drop=True)
    
    with engine.connect() as conn:
        for asin in TEST_ASINS:
            db_awd = conn.execute(text("""
                SELECT asin, SUM(available_in_awd_units) as awd_avail,
                       SUM(outbound_to_fba_units) as outbound
                FROM awd_inventory WHERE asin = :asin GROUP BY asin
            """), {'asin': asin}).fetchone()
            
            excel_row = excel_awd[excel_awd['ASIN'] == asin] if 'ASIN' in excel_awd.columns else pd.DataFrame()
            
            if db_awd:
                db_val = (db_awd.awd_avail or 0) + (db_awd.outbound or 0)
                print(f"  {asin}: DB AWD={db_val}")
            else:
                print(f"  {asin}: No AWD data in DB")
except Exception as e:
    print(f"  Error: {e}")

# ============================================================
# 3. UNITS SOLD (last 10 weeks)
# ============================================================
print("\n[3] UNITS SOLD (recent weeks)")
print("-" * 50)

try:
    excel_sales = pd.read_excel(EXCEL_PATH, sheet_name='Units_Sold')
    
    with engine.connect() as conn:
        for asin in TEST_ASINS[:2]:  # Just test 2 for brevity
            db_sales = conn.execute(text("""
                SELECT week_date, units FROM units_sold 
                WHERE asin = :asin ORDER BY week_date DESC LIMIT 5
            """), {'asin': asin}).fetchall()
            
            # Find ASIN column in Excel
            asin_row = excel_sales[excel_sales.iloc[:, 0] == asin]
            
            if db_sales:
                print(f"  {asin} (DB last 5 weeks):")
                for s in db_sales:
                    print(f"    {s.week_date}: {s.units}")
except Exception as e:
    print(f"  Error: {e}")

# ============================================================
# 4. SEASONALITY (Keyword_Seasonality)
# ============================================================
print("\n[4] SEASONALITY (Keyword_Seasonality)")
print("-" * 50)

try:
    excel_season = pd.read_excel(EXCEL_PATH, sheet_name='Keyword_Seasonality', header=2)
    
    with engine.connect() as conn:
        db_season = conn.execute(text("""
            SELECT week_of_year, sv_smooth_env_97, seasonality_index 
            FROM seasonality ORDER BY week_of_year LIMIT 10
        """)).fetchall()
        
        print("  Week | DB sv_97 | Excel sv_97 | DB s_idx | Excel s_idx")
        print("  " + "-" * 55)
        
        for db_row in db_season:
            week = db_row.week_of_year
            excel_row = excel_season[excel_season['week_of_year'] == week]
            
            if not excel_row.empty:
                excel_sv97 = excel_row['sv_smooth_env_.97'].iloc[0] if 'sv_smooth_env_.97' in excel_row.columns else 'N/A'
                excel_sidx = excel_row['seasonality_index'].iloc[0] if 'seasonality_index' in excel_row.columns else 'N/A'
                
                db_sv97 = round(db_row.sv_smooth_env_97, 1) if db_row.sv_smooth_env_97 else 0
                db_sidx = round(db_row.seasonality_index, 3) if db_row.seasonality_index else 0
                
                sv_match = "OK" if abs(db_sv97 - float(excel_sv97)) < 1 else "DIFF"
                print(f"  {week:4} | {db_sv97:8.1f} | {float(excel_sv97):11.1f} | {db_sidx:8.3f} | {float(excel_sidx):11.3f} [{sv_match}]")
except Exception as e:
    print(f"  Error: {e}")

# ============================================================
# 5. PRODUCT SEARCH VOLUME (sv_database)
# ============================================================
print("\n[5] PRODUCT SEARCH VOLUME (sv_database)")
print("-" * 50)

try:
    excel_sv = pd.read_excel(EXCEL_PATH, sheet_name='sv_database', header=None)
    
    # Get week dates from row 0 (starting column D = index 3)
    week_dates = excel_sv.iloc[0, 3:].tolist()
    
    with engine.connect() as conn:
        for asin in TEST_ASINS[:2]:
            # Database
            db_sv = conn.execute(text("""
                SELECT week_date, search_volume FROM product_search_volume 
                WHERE asin = :asin ORDER BY week_date LIMIT 5
            """), {'asin': asin}).fetchall()
            
            # Excel - find row with this ASIN in column B (index 1)
            excel_row = excel_sv[excel_sv.iloc[:, 1] == asin]
            
            print(f"  {asin}:")
            if db_sv:
                print(f"    DB first 5: {[(str(s.week_date), s.search_volume) for s in db_sv]}")
            if not excel_row.empty:
                excel_vals = excel_row.iloc[0, 3:8].tolist()
                print(f"    Excel first 5: {excel_vals}")
except Exception as e:
    print(f"  Error: {e}")

# ============================================================
# 6. VINE CLAIMS
# ============================================================
print("\n[6] VINE CLAIMS")
print("-" * 50)

try:
    excel_vine = pd.read_excel(EXCEL_PATH, sheet_name='vine_units_claimed')
    
    with engine.connect() as conn:
        db_count = conn.execute(text("SELECT COUNT(*) FROM vine_claims")).scalar()
        excel_count = len(excel_vine)
        
        print(f"  Total rows: DB={db_count}, Excel={excel_count}")
        
        # Check a specific ASIN
        for asin in TEST_ASINS[:2]:
            db_vine = conn.execute(text("""
                SELECT claim_date, units_claimed FROM vine_claims 
                WHERE asin = :asin ORDER BY claim_date LIMIT 3
            """), {'asin': asin}).fetchall()
            
            if db_vine:
                print(f"  {asin}: {len(db_vine)} vine claims in DB")
except Exception as e:
    print(f"  Error: {e}")

# ============================================================
# 7. INVENTORY TOTALS FOR TEST ASINS
# ============================================================
print("\n[7] TOTAL INVENTORY COMPARISON")
print("-" * 50)

try:
    with engine.connect() as conn:
        for asin in TEST_ASINS:
            fba = conn.execute(text("""
                SELECT COALESCE(SUM(available), 0) as avail,
                       COALESCE(SUM(inbound_quantity), 0) as inbound,
                       COALESCE(SUM(total_reserved_quantity), 0) as reserved
                FROM fba_inventory WHERE asin = :asin
            """), {'asin': asin}).fetchone()
            
            awd = conn.execute(text("""
                SELECT COALESCE(SUM(available_in_awd_units), 0) as awd_avail,
                       COALESCE(SUM(inbound_to_awd_units), 0) as awd_inbound,
                       COALESCE(SUM(outbound_to_fba_units), 0) as outbound,
                       COALESCE(SUM(reserved_in_awd_units), 0) as reserved
                FROM awd_inventory WHERE asin = :asin
            """), {'asin': asin}).fetchone()
            
            fba_total = (fba.avail or 0) + (fba.inbound or 0) + (fba.reserved or 0)
            awd_total = (awd.awd_avail or 0) + (awd.awd_inbound or 0) + (awd.outbound or 0) + (awd.reserved or 0)
            total = fba_total + awd_total
            
            print(f"  {asin}:")
            print(f"    FBA: avail={fba.avail}, inbound={fba.inbound}, reserved={fba.reserved} = {fba_total}")
            print(f"    AWD: avail={awd.awd_avail}, inbound={awd.awd_inbound}, outbound={awd.outbound} = {awd_total}")
            print(f"    TOTAL: {total}")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "=" * 70)
print("COMPARISON COMPLETE")
print("=" * 70)
