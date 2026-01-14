"""
Label Inventory Seeder - Seeds label inventory from 1000 Bananas Database.

Run: python seed_labels.py

This seeds the label_inventory table from CatalogDataBase sheet.
"""
import os
import sys
import time
import pandas as pd
from datetime import datetime

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Excel file path
EXCEL_PATH = '../1000 Bananas Database (6).xlsx'

print("=" * 60)
print("LABEL INVENTORY SEEDER")
print("=" * 60)

start_time = time.perf_counter()

print("[1/4] Reading label data from Excel...")

# Read CatalogDataBase sheet
df = pd.read_excel(EXCEL_PATH, sheet_name='CatalogDataBase', header=None)

# Extract relevant columns (row 5 onwards is data)
data = df.iloc[5:, [20, 7, 8, 13, 31, 32]].copy()
data.columns = ['asin', 'product_name', 'size', 'label_id', 'label_status', 'label_inventory']

# Clean data
data = data.dropna(subset=['asin'])
data = data.drop_duplicates(subset=['asin'], keep='first')  # Remove duplicate ASINs
data['label_inventory'] = pd.to_numeric(data['label_inventory'], errors='coerce').fillna(0).astype(int)
data['label_status'] = data['label_status'].fillna('Unknown')
data['label_id'] = data['label_id'].fillna('')

print(f"      Found {len(data)} products with label data")

print("[2/4] Initializing Flask app...")

from app import create_app, db
from app.models import LabelInventory
from sqlalchemy import text

app = create_app('development')

with app.app_context():
    print("[3/4] Recreating label_inventory table...")
    
    # Drop and recreate the table properly (including index)
    with db.engine.connect() as conn:
        conn.execute(text("DROP INDEX IF EXISTS ix_label_inventory_label_id"))
        conn.execute(text("DROP INDEX IF EXISTS ix_label_inventory_asin"))
        conn.execute(text("DROP TABLE IF EXISTS label_inventory"))
        conn.commit()
    
    # Create table with proper schema
    LabelInventory.__table__.create(db.engine, checkfirst=True)
    
    print("[4/4] Inserting label inventory...")
    
    now = datetime.utcnow()
    entries = []
    
    for _, row in data.iterrows():
        entries.append(LabelInventory(
            asin=str(row['asin']),
            product_name=str(row['product_name']) if pd.notna(row['product_name']) else None,
            size=str(row['size']) if pd.notna(row['size']) else None,
            label_id=str(row['label_id']) if pd.notna(row['label_id']) else None,
            label_status=str(row['label_status']),
            label_inventory=int(row['label_inventory']),
            updated_at=now
        ))
    
    # Bulk insert
    db.session.bulk_save_objects(entries)
    db.session.commit()
    
    # Verify
    count = LabelInventory.query.count()
    total_labels = db.session.query(db.func.sum(LabelInventory.label_inventory)).scalar()
    zero_labels = LabelInventory.query.filter(LabelInventory.label_inventory == 0).count()

elapsed = time.perf_counter() - start_time

print()
print("=" * 60)
print("LABEL SEEDING COMPLETE!")
print("=" * 60)
print(f"Products with labels: {count}")
print(f"Total labels in stock: {total_labels:,}")
print(f"Products with 0 labels: {zero_labels}")
print(f"Time: {elapsed:.2f}s")
