"""
Database models for product forecasting.

Optimized with comprehensive indexing for fast queries:
- Primary keys with auto-increment
- Single-column indexes for common lookups
- Composite indexes for multi-column queries
- Covering indexes for frequent query patterns
"""
from app import db
from datetime import datetime


class FBAInventory(db.Model):
    """
    FBA Inventory snapshot data.
    
    Index Strategy:
    - asin: Most common lookup (product queries)
    - sku: SKU-based lookups
    - snapshot_date: Time-series queries
    - (asin, snapshot_date): Composite for product history
    """
    __tablename__ = 'fba_inventory'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    snapshot_date = db.Column(db.DateTime, index=True)
    sku = db.Column(db.String(200), index=True)
    fnsku = db.Column(db.String(100))
    asin = db.Column(db.String(50), index=True, nullable=False)
    product_name = db.Column(db.Text)
    condition = db.Column(db.String(50))
    available = db.Column(db.Integer, default=0)
    pending_removal_quantity = db.Column(db.Integer, default=0)
    inv_age_0_to_90_days = db.Column(db.Integer, default=0)
    inv_age_91_to_180_days = db.Column(db.Integer, default=0)
    inv_age_181_to_270_days = db.Column(db.Integer, default=0)
    inv_age_271_to_365_days = db.Column(db.Integer, default=0)
    currency = db.Column(db.String(10))
    units_shipped_t7 = db.Column(db.Integer, default=0)
    units_shipped_t30 = db.Column(db.Integer, default=0)
    units_shipped_t60 = db.Column(db.Integer, default=0)
    units_shipped_t90 = db.Column(db.Integer, default=0)
    alert = db.Column(db.String(200))
    your_price = db.Column(db.Float)
    sales_price = db.Column(db.Float)
    lowest_price_new_plus_shipping = db.Column(db.Float)
    lowest_price_used = db.Column(db.Float)
    recommended_action = db.Column(db.String(200))
    sell_through = db.Column(db.Float)
    item_volume = db.Column(db.Float)
    volume_unit_measurement = db.Column(db.String(50))
    storage_type = db.Column(db.String(50))
    storage_volume = db.Column(db.Float)
    marketplace = db.Column(db.String(20))
    product_group = db.Column(db.String(200))
    sales_rank = db.Column(db.Integer)
    days_of_supply = db.Column(db.Integer)
    estimated_excess_quantity = db.Column(db.Integer)
    weeks_of_cover_t30 = db.Column(db.Float)
    weeks_of_cover_t90 = db.Column(db.Float)
    featuredoffer_price = db.Column(db.Float)
    sales_shipped_last_7_days = db.Column(db.Float)
    sales_shipped_last_30_days = db.Column(db.Float)
    sales_shipped_last_60_days = db.Column(db.Float)
    sales_shipped_last_90_days = db.Column(db.Float)
    inbound_quantity = db.Column(db.Integer, default=0)
    inbound_working = db.Column(db.Integer, default=0)
    inbound_shipped = db.Column(db.Integer, default=0)
    inbound_received = db.Column(db.Integer, default=0)
    total_reserved_quantity = db.Column(db.Integer, default=0)
    unfulfillable_quantity = db.Column(db.Integer, default=0)
    estimated_storage_cost_next_month = db.Column(db.Float)
    historical_days_of_supply = db.Column(db.Float)
    fba_minimum_inventory_level = db.Column(db.Float)
    fba_inventory_level_health_status = db.Column(db.String(100))
    inventory_supply_at_fba = db.Column(db.Integer)
    supplier = db.Column(db.String(200))
    
    __table_args__ = (
        # Composite index for product history queries
        db.Index('ix_fba_asin_snapshot', 'asin', 'snapshot_date'),
        # Composite index for inventory aggregation
        db.Index('ix_fba_asin_available', 'asin', 'available'),
        # Index for supplier filtering
        db.Index('ix_fba_supplier', 'supplier'),
    )
    
    def __repr__(self):
        return f'<FBAInventory {self.sku} @ {self.snapshot_date}>'


class AWDInventory(db.Model):
    """
    AWD (Amazon Warehousing and Distribution) Inventory data.
    
    Index Strategy:
    - asin: Primary lookup key
    - sku: SKU-based lookups
    - Composite for inventory queries
    """
    __tablename__ = 'awd_inventory'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    product_name = db.Column(db.Text)
    sku = db.Column(db.String(200), index=True)
    fnsku = db.Column(db.String(100))
    asin = db.Column(db.String(50), index=True, nullable=False)
    inbound_to_awd_units = db.Column(db.Integer, default=0)
    inbound_to_awd_cases = db.Column(db.Integer, default=0)
    available_in_awd_units = db.Column(db.Integer, default=0)
    available_in_awd_cases = db.Column(db.Integer, default=0)
    reserved_in_awd_units = db.Column(db.Integer, default=0)
    reserved_in_awd_cases = db.Column(db.Integer, default=0)
    outbound_order_units = db.Column(db.Integer, default=0)
    outbound_order_cases = db.Column(db.Integer, default=0)
    researching_units = db.Column(db.Integer, default=0)
    researching_cases = db.Column(db.Integer, default=0)
    outbound_to_fba_units = db.Column(db.Integer, default=0)
    available_in_fba_units = db.Column(db.Integer, default=0)
    available_in_fba_days = db.Column(db.Integer, default=0)
    reserved_in_fba_units = db.Column(db.Integer, default=0)
    customer_order_units = db.Column(db.Integer, default=0)
    fc_processing_units = db.Column(db.Integer, default=0)
    fc_transfer_units = db.Column(db.Integer, default=0)
    days_of_supply = db.Column(db.Integer, default=0)
    auto_replenishment_ratio = db.Column(db.Float)
    
    __table_args__ = (
        # Composite index for inventory aggregation queries
        db.Index('ix_awd_asin_available', 'asin', 'available_in_awd_units'),
    )
    
    def __repr__(self):
        return f'<AWDInventory {self.sku}>'


class Product(db.Model):
    """
    Product master data extracted from Units Sold.
    
    Index Strategy:
    - asin: Unique, primary lookup (most queries use ASIN)
    - brand: Brand-based filtering
    """
    __tablename__ = 'products'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    asin = db.Column(db.String(50), unique=True, index=True, nullable=False)
    brand = db.Column(db.String(200), index=True)
    product_name = db.Column(db.Text)
    size = db.Column(db.String(100))
    
    # Relationship to sales data (lazy='dynamic' for efficient large dataset queries)
    sales = db.relationship('UnitsSold', backref='product', lazy='dynamic')
    
    __table_args__ = (
        # Covering index for common product listing queries
        db.Index('ix_products_brand_asin', 'brand', 'asin'),
    )
    
    def __repr__(self):
        return f'<Product {self.asin}>'


class UnitsSold(db.Model):
    """
    Weekly units sold data - normalized format for time series.
    
    Index Strategy:
    - asin: Product lookup (most common)
    - week_date: Time-series queries
    - (asin, week_date): Composite for product sales history (CRITICAL)
    - (week_date, asin): Reverse composite for date-range queries
    - product_id: Foreign key lookups
    """
    __tablename__ = 'units_sold'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), index=True)
    asin = db.Column(db.String(50), index=True, nullable=False)
    week_date = db.Column(db.Date, index=True, nullable=False)
    units = db.Column(db.Integer, default=0)
    
    __table_args__ = (
        # CRITICAL: Composite index for sales history queries (most used)
        db.Index('ix_units_sold_asin_week', 'asin', 'week_date'),
        # Reverse composite for date-range across products
        db.Index('ix_units_sold_week_asin', 'week_date', 'asin'),
        # Covering index for aggregation queries
        db.Index('ix_units_sold_asin_units', 'asin', 'units'),
        # Index for finding products with sales above threshold
        db.Index('ix_units_sold_units', 'units'),
    )
    
    def __repr__(self):
        return f'<UnitsSold {self.asin} @ {self.week_date}: {self.units}>'


class Seasonality(db.Model):
    """
    Category-level seasonality data by week of year.
    
    From Keyword_Seasonality sheet - 52 rows for each week.
    Used by 6-18m and 0-6m algorithms.
    """
    __tablename__ = 'seasonality'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    week_of_year = db.Column(db.Integer, unique=True, index=True, nullable=False)
    search_volume = db.Column(db.Float, default=0)
    sv_smooth_env = db.Column(db.Float, default=0)  # Smoothed search volume
    sv_smooth_env_97 = db.Column(db.Float, default=0)  # sv_smooth_env * 0.97
    seasonality_index = db.Column(db.Float, default=1.0)
    seasonality_multiplier = db.Column(db.Float, default=1.0)
    
    def __repr__(self):
        return f'<Seasonality Week {self.week_of_year}: idx={self.seasonality_index}>'


class ForecastCache(db.Model):
    """
    Cache table for computed forecast results.
    
    Stores pre-computed forecasts to avoid recalculation.
    Indexed for fast lookup by ASIN and freshness checks.
    """
    __tablename__ = 'forecast_cache'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    asin = db.Column(db.String(50), index=True, nullable=False)
    algorithm = db.Column(db.String(50), index=True)  # '18m+', '6-18m', '0-6m'
    computed_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    expires_at = db.Column(db.DateTime, index=True)
    
    # Cached results (JSON)
    units_to_make = db.Column(db.Integer)
    doi_total_days = db.Column(db.Float)
    doi_fba_available_days = db.Column(db.Float)
    unit_needed_total = db.Column(db.Float)
    sales_velocity_adjustment = db.Column(db.Float)
    
    # Settings used (for cache invalidation)
    settings_hash = db.Column(db.String(64), index=True)
    
    __table_args__ = (
        # Composite for cache lookup
        db.Index('ix_forecast_cache_asin_algo', 'asin', 'algorithm'),
        # For cache expiration queries
        db.Index('ix_forecast_cache_expires', 'expires_at'),
        # Unique constraint for cache key
        db.UniqueConstraint('asin', 'algorithm', 'settings_hash', name='uq_forecast_cache'),
    )
    
    def __repr__(self):
        return f'<ForecastCache {self.asin} ({self.algorithm})>'


class LabelInventory(db.Model):
    """
    Label inventory for each product.
    
    Tracks how many labels are in stock for each ASIN.
    Used to calculate labels needed based on production forecast.
    """
    __tablename__ = 'label_inventory'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    asin = db.Column(db.String(50), unique=True, nullable=False)
    product_name = db.Column(db.Text)
    size = db.Column(db.String(50))
    label_id = db.Column(db.String(50))  # e.g., LBL-PLANT-494
    label_status = db.Column(db.String(50))  # e.g., "Up to Date"
    label_inventory = db.Column(db.Integer, default=0)  # Labels in stock
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<LabelInventory {self.asin}: {self.label_inventory} labels>'


class VineClaims(db.Model):
    """
    Vine units claimed data.
    
    Tracks vine program claims by ASIN and date.
    Used to adjust units sold in 0-6m and 6-18m algorithms.
    """
    __tablename__ = 'vine_claims'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    asin = db.Column(db.String(50), index=True, nullable=False)
    product_name = db.Column(db.Text)
    claim_date = db.Column(db.Date, index=True, nullable=False)
    units_claimed = db.Column(db.Integer, default=0)
    vine_status = db.Column(db.String(100))  # e.g., "Awaiting Reviews", "Concluded"
    
    __table_args__ = (
        # Composite index for ASIN + date lookups
        db.Index('ix_vine_claims_asin_date', 'asin', 'claim_date'),
    )
    
    def __repr__(self):
        return f'<VineClaims {self.asin} @ {self.claim_date}: {self.units_claimed}>'
