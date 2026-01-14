"""
Initialize PostgreSQL database with schema and indexes.
Run this after deploying to Railway to create tables.

Usage: python init_db.py
"""
from app import create_app, db

print("Initializing PostgreSQL database...")

app = create_app('production')
with app.app_context():
    db.create_all()
    print("[OK] All tables and indexes created!")
    
    # Show created tables
    from sqlalchemy import inspect
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    print(f"\nTables created: {len(tables)}")
    for table in tables:
        indexes = inspector.get_indexes(table)
        print(f"  - {table} ({len(indexes)} indexes)")
