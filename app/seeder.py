"""
Fast data seeding module using pandas bulk operations.
Avoids ORM loops - uses direct SQL bulk inserts for maximum performance.
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
from sqlalchemy import text


class DataSeeder:
    """High-performance data seeder using pandas to_sql bulk operations."""
    
    # Column mappings for clean database field names
    FBA_COLUMN_MAP = {
        'snapshot-date': 'snapshot_date',
        'sku': 'sku',
        'fnsku': 'fnsku',
        'asin': 'asin',
        'product-name': 'product_name',
        'condition': 'condition',
        'available': 'available',
        'pending-removal-quantity': 'pending_removal_quantity',
        'inv-age-0-to-90-days': 'inv_age_0_to_90_days',
        'inv-age-91-to-180-days': 'inv_age_91_to_180_days',
        'inv-age-181-to-270-days': 'inv_age_181_to_270_days',
        'inv-age-271-to-365-days': 'inv_age_271_to_365_days',
        'currency': 'currency',
        'units-shipped-t7': 'units_shipped_t7',
        'units-shipped-t30': 'units_shipped_t30',
        'units-shipped-t60': 'units_shipped_t60',
        'units-shipped-t90': 'units_shipped_t90',
        'alert': 'alert',
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
        'marketplace': 'marketplace',
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
        'Total Reserved Quantity': 'total_reserved_quantity',
        'unfulfillable-quantity': 'unfulfillable_quantity',
        'estimated-storage-cost-next-month': 'estimated_storage_cost_next_month',
        'historical-days-of-supply': 'historical_days_of_supply',
        'fba-minimum-inventory-level': 'fba_minimum_inventory_level',
        'fba-inventory-level-health-status': 'fba_inventory_level_health_status',
        'Inventory Supply at FBA': 'inventory_supply_at_fba',
        'supplier': 'supplier',
    }
    
    AWD_COLUMN_MAP = {
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
    
    def __init__(self, excel_path: str, db_engine):
        """
        Initialize seeder with Excel file path and database engine.
        
        Args:
            excel_path: Path to the Excel file containing source data
            db_engine: SQLAlchemy engine for database operations
        """
        self.excel_path = Path(excel_path)
        self.engine = db_engine
        self._stats = {}
    
    def seed_all(self, drop_existing: bool = True) -> dict:
        """
        Seed all tables from Excel file using bulk operations.
        
        Args:
            drop_existing: If True, truncate tables before seeding
            
        Returns:
            Dictionary with seeding statistics
        """
        total_start = time.perf_counter()
        
        print("=" * 60)
        print("PRODUCT FORECASTING DATABASE SEEDING")
        print("=" * 60)
        
        # Clear existing data if requested
        if drop_existing:
            self._truncate_tables()
        
        # Seed in order: FBA -> AWD -> Products -> Units Sold -> Seasonality
        self._seed_fba_inventory()
        self._seed_awd_inventory()
        self._seed_units_sold()
        self._seed_seasonality()
        
        # Run ANALYZE for optimal query planning
        print("\n[5/5] Optimizing database...")
        self._optimize_database()
        
        total_time = time.perf_counter() - total_start
        self._stats['total_time'] = f"{total_time:.2f}s"
        
        print("=" * 60)
        print(f"SEEDING COMPLETE - Total time: {total_time:.2f}s")
        print("=" * 60)
        
        return self._stats
    
    def _optimize_database(self):
        """Run ANALYZE to update query planner statistics."""
        start = time.perf_counter()
        with self.engine.connect() as conn:
            conn.execute(text("ANALYZE"))
            conn.commit()
        elapsed = time.perf_counter() - start
        self._stats['optimization'] = {'time': f"{elapsed:.2f}s"}
        print(f"    [OK] Database optimized in {elapsed:.2f}s")
    
    def _truncate_tables(self):
        """Truncate all tables to prepare for fresh seeding."""
        print("\n[PREP] Clearing existing data...")
        with self.engine.connect() as conn:
            conn.execute(text("DELETE FROM units_sold"))
            conn.execute(text("DELETE FROM products"))
            conn.execute(text("DELETE FROM awd_inventory"))
            conn.execute(text("DELETE FROM fba_inventory"))
            conn.execute(text("DELETE FROM seasonality"))
            conn.commit()
        print("    [OK] Tables cleared")
    
    def _seed_fba_inventory(self):
        """Bulk insert FBA Inventory data."""
        print("\n[1/5] Seeding FBA Inventory...")
        start = time.perf_counter()
        
        # Read Excel sheet
        df = pd.read_excel(self.excel_path, sheet_name='FBAInventory')
        
        # Select and rename columns
        available_cols = [c for c in self.FBA_COLUMN_MAP.keys() if c in df.columns]
        df = df[available_cols].rename(columns=self.FBA_COLUMN_MAP)
        
        # Convert datetime columns
        if 'snapshot_date' in df.columns:
            df['snapshot_date'] = pd.to_datetime(df['snapshot_date'], errors='coerce')
        
        # Filter out rows with null required fields (ASIN is required)
        df = df[df['asin'].notna() & (df['asin'] != '')]
        
        # Bulk insert using pandas to_sql (fastest method)
        df.to_sql('fba_inventory', self.engine, if_exists='append', index=False,
                  method='multi', chunksize=500)
        
        elapsed = time.perf_counter() - start
        self._stats['fba_inventory'] = {'rows': len(df), 'time': f"{elapsed:.2f}s"}
        print(f"    [OK] Inserted {len(df):,} rows in {elapsed:.2f}s")
    
    def _seed_awd_inventory(self):
        """Bulk insert AWD Inventory data."""
        print("\n[2/5] Seeding AWD Inventory...")
        start = time.perf_counter()
        
        # Read Excel sheet - skip header rows
        df = pd.read_excel(self.excel_path, sheet_name='AWDInventory', header=2)
        
        # Get column names from row 2 and filter valid data
        df.columns = df.iloc[0].tolist()
        df = df.iloc[1:].reset_index(drop=True)
        
        # Select and rename columns
        available_cols = [c for c in self.AWD_COLUMN_MAP.keys() if c in df.columns]
        df = df[available_cols].rename(columns=self.AWD_COLUMN_MAP)
        
        # Remove empty rows and rows with null required fields
        df = df.dropna(how='all')
        df = df[df['sku'].notna() & (df['sku'] != '')]
        df = df[df['asin'].notna() & (df['asin'] != '')]  # ASIN is required
        
        # Convert numeric columns
        numeric_cols = [c for c in df.columns if c not in ['product_name', 'sku', 'fnsku', 'asin']]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Bulk insert
        df.to_sql('awd_inventory', self.engine, if_exists='append', index=False,
                  method='multi', chunksize=500)
        
        elapsed = time.perf_counter() - start
        self._stats['awd_inventory'] = {'rows': len(df), 'time': f"{elapsed:.2f}s"}
        print(f"    [OK] Inserted {len(df):,} rows in {elapsed:.2f}s")
    
    def _seed_units_sold(self):
        """
        Bulk insert Units Sold data.
        Uses melt operation to transform wide format to normalized long format.
        """
        print("\n[3/5] Seeding Units Sold (Products + Sales)...")
        start = time.perf_counter()
        
        # Read Excel sheet
        df = pd.read_excel(self.excel_path, sheet_name='Units_Sold')
        
        # Identify date columns (all datetime columns after the first 4 metadata columns)
        id_cols = ['(Child) ASIN', 'Brand', 'Product', 'Size']
        date_cols = [c for c in df.columns if isinstance(c, datetime) or 
                     (isinstance(c, str) and c not in id_cols and c != 'Unnamed: 114')]
        
        # Step 1: Create products table
        products_df = df[['(Child) ASIN', 'Brand', 'Product', 'Size']].copy()
        products_df.columns = ['asin', 'brand', 'product_name', 'size']
        products_df = products_df.drop_duplicates(subset=['asin'])
        products_df = products_df.dropna(subset=['asin'])
        
        products_df.to_sql('products', self.engine, if_exists='append', index=False,
                           method='multi', chunksize=500)
        
        products_count = len(products_df)
        print(f"    [OK] Inserted {products_count:,} products")
        
        # Step 2: Transform wide to long format and insert units_sold
        # Use pd.melt for vectorized transformation (no loops!)
        sales_df = df[['(Child) ASIN'] + date_cols].copy()
        sales_df = sales_df.melt(
            id_vars=['(Child) ASIN'],
            value_vars=date_cols,
            var_name='week_date',
            value_name='units'
        )
        sales_df.columns = ['asin', 'week_date', 'units']
        
        # Convert week_date to proper date
        sales_df['week_date'] = pd.to_datetime(sales_df['week_date'], errors='coerce').dt.date
        
        # Convert units to integer, drop nulls
        sales_df['units'] = pd.to_numeric(sales_df['units'], errors='coerce')
        sales_df = sales_df.dropna(subset=['asin', 'week_date', 'units'])
        sales_df['units'] = sales_df['units'].astype(int)
        
        # Bulk insert - using smaller chunks for large dataset
        sales_df.to_sql('units_sold', self.engine, if_exists='append', index=False,
                        method='multi', chunksize=1000)
        
        elapsed = time.perf_counter() - start
        self._stats['products'] = {'rows': products_count, 'time': 'included'}
        self._stats['units_sold'] = {'rows': len(sales_df), 'time': f"{elapsed:.2f}s"}
        print(f"    [OK] Inserted {len(sales_df):,} sales records in {elapsed:.2f}s")
    
    def _seed_seasonality(self):
        """Bulk insert Seasonality data from Keyword_Seasonality sheet."""
        print("\n[4/5] Seeding Seasonality...")
        start = time.perf_counter()
        
        # Read Keyword_Seasonality sheet - skip the header row
        df = pd.read_excel(self.excel_path, sheet_name='Keyword_Seasonality', header=2)
        
        # Select relevant columns
        column_map = {
            'week_of_year': 'week_of_year',
            'search_volume': 'search_volume',
            'sv_smooth_env': 'sv_smooth_env',
            'sv_smooth_env_.97': 'sv_smooth_env_97',
            'seasonality_index': 'seasonality_index',
            'seasonality_multiplier': 'seasonality_multiplier'
        }
        
        # Filter columns that exist
        available_cols = [c for c in column_map.keys() if c in df.columns]
        df = df[available_cols].rename(columns=column_map)
        
        # Drop rows without week_of_year
        df = df.dropna(subset=['week_of_year'])
        
        # Convert week_of_year to integer
        df['week_of_year'] = df['week_of_year'].astype(int)
        
        # Only keep weeks 1-52
        df = df[(df['week_of_year'] >= 1) & (df['week_of_year'] <= 52)]
        
        # Convert numeric columns
        for col in ['search_volume', 'sv_smooth_env', 'sv_smooth_env_97', 
                    'seasonality_index', 'seasonality_multiplier']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Bulk insert
        df.to_sql('seasonality', self.engine, if_exists='append', index=False,
                  method='multi', chunksize=100)
        
        elapsed = time.perf_counter() - start
        self._stats['seasonality'] = {'rows': len(df), 'time': f"{elapsed:.2f}s"}
        print(f"    [OK] Inserted {len(df):,} seasonality records in {elapsed:.2f}s")


def seed_database(app, excel_path: str):
    """
    Convenience function to seed database within Flask app context.
    
    Args:
        app: Flask application instance
        excel_path: Path to Excel source file
    """
    from app import db
    
    with app.app_context():
        # Create all tables first
        db.create_all()
        
        # Run seeder
        seeder = DataSeeder(excel_path, db.engine)
        stats = seeder.seed_all(drop_existing=True)
        
        return stats
