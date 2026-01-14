"""
Database Utilities for Performance Optimization.

Supports both SQLite and PostgreSQL with automatic detection.
"""
from sqlalchemy import event, text
from app import db


def apply_sqlite_optimizations(app):
    """
    Apply database-specific performance optimizations.
    
    For SQLite: Applies PRAGMAs for WAL mode, cache, etc.
    For PostgreSQL: No special connection-level settings needed.
    """
    # Check if using SQLite
    db_url = str(app.config.get('SQLALCHEMY_DATABASE_URI', ''))
    is_sqlite = db_url.startswith('sqlite')
    
    if not is_sqlite:
        app.logger.info("Using PostgreSQL - no connection PRAGMAs needed")
        return
    
    @event.listens_for(db.engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        
        # WAL mode - Allows concurrent reads while writing
        cursor.execute("PRAGMA journal_mode=WAL")
        
        # Memory-mapped I/O - Faster file access (256MB)
        cursor.execute("PRAGMA mmap_size=268435456")
        
        # Cache size - 64MB cache
        cursor.execute("PRAGMA cache_size=-64000")
        
        # Synchronous mode - NORMAL balances speed and safety
        cursor.execute("PRAGMA synchronous=NORMAL")
        
        # Temp store in memory
        cursor.execute("PRAGMA temp_store=MEMORY")
        
        # Foreign keys enforcement
        cursor.execute("PRAGMA foreign_keys=ON")
        
        cursor.close()


def analyze_tables():
    """
    Run ANALYZE on all tables to update query planner statistics.
    
    Call this after bulk data loading for optimal query plans.
    """
    with db.engine.connect() as conn:
        conn.execute(text("ANALYZE"))
        conn.commit()
    print("[DB] ANALYZE complete - query planner statistics updated")


def vacuum_database():
    """
    Run VACUUM to rebuild the database file.
    
    Reclaims space and defragments the database.
    Call periodically or after large deletions.
    """
    with db.engine.connect() as conn:
        conn.execute(text("VACUUM"))
        conn.commit()
    print("[DB] VACUUM complete - database optimized")


def get_index_stats():
    """Get statistics about indexes for debugging."""
    with db.engine.connect() as conn:
        result = conn.execute(text("""
            SELECT name, tbl_name 
            FROM sqlite_master 
            WHERE type='index' 
            ORDER BY tbl_name, name
        """))
        indexes = result.fetchall()
    
    print("\n[DB] Index Statistics:")
    print("-" * 50)
    current_table = None
    for name, table in indexes:
        if table != current_table:
            print(f"\n  {table}:")
            current_table = table
        print(f"    - {name}")
    print("-" * 50)
    return indexes


def get_table_stats():
    """Get row counts for all tables."""
    tables = ['products', 'units_sold', 'fba_inventory', 'awd_inventory', 'forecast_cache']
    stats = {}
    
    with db.engine.connect() as conn:
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                stats[table] = count
            except Exception:
                stats[table] = 'N/A'
    
    print("\n[DB] Table Statistics:")
    print("-" * 30)
    for table, count in stats.items():
        print(f"  {table}: {count:,}" if isinstance(count, int) else f"  {table}: {count}")
    print("-" * 30)
    return stats


def explain_query(query_string: str):
    """
    Show query execution plan for debugging slow queries.
    
    Usage:
        explain_query("SELECT * FROM units_sold WHERE asin = 'B073ZNQWCM'")
    """
    with db.engine.connect() as conn:
        result = conn.execute(text(f"EXPLAIN QUERY PLAN {query_string}"))
        plan = result.fetchall()
    
    print(f"\n[DB] Query Plan for: {query_string[:50]}...")
    print("-" * 60)
    for row in plan:
        print(f"  {row}")
    print("-" * 60)
    return plan
