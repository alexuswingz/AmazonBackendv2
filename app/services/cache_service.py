"""
Forecast Cache Service - Pre-compute and cache forecasts for fast retrieval.

This allows the /forecast/all endpoint to return 1000+ products instantly.
Run refresh_all_forecasts() periodically (e.g., daily) to update cache.
"""
from datetime import datetime, date, timedelta
from typing import Dict, List, Any
from sqlalchemy import func

from app import db
from app.models import Product, UnitsSold, ForecastCache, Seasonality
from app.services.forecast_service import forecast_service
from app.algorithms.algorithms_tps import (
    calculate_forecast_18m_plus as tps_18m,
    calculate_forecast_6_18m as tps_6_18m,
    calculate_forecast_0_6m_exact as tps_0_6m,
    DEFAULT_SETTINGS
)


class CacheService:
    """Service for managing forecast cache."""
    
    CACHE_DURATION_HOURS = 24  # Cache validity
    
    def get_all_cached_forecasts(self, brand_filter: str = None) -> List[Dict]:
        """
        Get all forecasts from cache - FAST (simple DB read).
        
        Returns list of forecasts for all products.
        """
        query = db.session.query(
            ForecastCache.asin,
            ForecastCache.algorithm,
            ForecastCache.units_to_make,
            ForecastCache.doi_total_days,
            ForecastCache.doi_fba_available_days,
            ForecastCache.computed_at,
            Product.brand,
            Product.product_name,
            Product.size
        ).join(
            Product, ForecastCache.asin == Product.asin
        )
        
        if brand_filter:
            query = query.filter(Product.brand.ilike(f'%{brand_filter}%'))
        
        results = query.order_by(Product.product_name).all()
        
        return [{
            'brand': r.brand or 'TPS Plant Foods',
            'product': r.product_name,
            'size': r.size,
            'asin': r.asin,
            'units_to_make': r.units_to_make or 0,
            'algorithm': r.algorithm,
            'doi_total_days': round(r.doi_total_days or 0, 0),
            'doi_fba_days': round(r.doi_fba_available_days or 0, 0),
            'last_updated': r.computed_at.isoformat() if r.computed_at else None
        } for r in results]
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        total = ForecastCache.query.count()
        expired = ForecastCache.query.filter(
            ForecastCache.expires_at < datetime.utcnow()
        ).count()
        
        latest = db.session.query(func.max(ForecastCache.computed_at)).scalar()
        
        return {
            'total_cached': total,
            'expired': expired,
            'valid': total - expired,
            'last_refresh': latest.isoformat() if latest else None
        }
    
    def refresh_all_forecasts(self, batch_size: int = 50) -> Dict:
        """
        Refresh ALL product forecasts - run this periodically.
        
        Optimized with batch processing and bulk inserts.
        """
        import time
        start_time = time.perf_counter()
        
        # Get all products
        products = Product.query.all()
        total_products = len(products)
        
        print(f"[CACHE] Refreshing forecasts for {total_products} products...")
        
        # Pre-fetch seasonality data once
        seasonality_data = [
            {'week_of_year': s.week_of_year, 'seasonality_index': s.seasonality_index}
            for s in Seasonality.query.all()
        ]
        
        # Pre-fetch all first sale dates in one query
        first_sales = dict(
            db.session.query(
                UnitsSold.asin,
                func.min(UnitsSold.week_date)
            ).filter(UnitsSold.units > 0).group_by(UnitsSold.asin).all()
        )
        
        today = date.today()
        now = datetime.utcnow()
        expires_at = now + timedelta(hours=self.CACHE_DURATION_HOURS)
        
        success_count = 0
        error_count = 0
        results = []
        
        for i, product in enumerate(products):
            asin = product.asin
            
            try:
                # Get first sale date from pre-fetched dict
                first_sale = first_sales.get(asin)
                
                if not first_sale:
                    continue
                
                age_days = (today - first_sale).days
                age_months = age_days / 30.44
                
                # Determine algorithm
                if age_months >= 18:
                    algorithm = "18m+"
                elif age_months >= 6:
                    algorithm = "6-18m"
                else:
                    algorithm = "0-6m"
                
                # Get sales data
                sales = UnitsSold.query.filter_by(asin=asin).order_by(UnitsSold.week_date).all()
                units_data = [{'week_end': s.week_date, 'units': s.units} for s in sales]
                
                if len(units_data) < 4:
                    continue
                
                # Get inventory
                inventory = forecast_service.get_inventory_levels(asin)
                
                # Settings
                settings = DEFAULT_SETTINGS.copy()
                settings['total_inventory'] = inventory.total_inventory
                settings['fba_available'] = inventory.fba_available
                
                # Run appropriate algorithm (with fallback to 18m+ if others fail)
                try:
                    if algorithm == "18m+":
                        result = tps_18m(units_data, today, settings)
                    elif algorithm == "6-18m":
                        if seasonality_data:
                            result = tps_6_18m(units_data, today, settings, seasonality_data)
                        else:
                            result = tps_18m(units_data, today, settings)
                            algorithm = "18m+ (fallback)"
                    else:  # 0-6m
                        if seasonality_data:
                            result = tps_0_6m(units_data, today, settings, seasonality_data)
                        else:
                            result = tps_18m(units_data, today, settings)
                            algorithm = "18m+ (fallback)"
                except Exception as algo_error:
                    # Fallback to 18m+ if specific algorithm fails
                    result = tps_18m(units_data, today, settings)
                    algorithm = "18m+ (fallback)"
                
                # Prepare cache entry
                cache_entry = {
                    'asin': asin,
                    'algorithm': algorithm,
                    'units_to_make': result['units_to_make'],
                    'doi_total_days': result['doi_total_days'],
                    'doi_fba_available_days': result['doi_fba_days'],
                    'unit_needed_total': result.get('total_units_needed', 0),
                    'sales_velocity_adjustment': result.get('sales_velocity_adjustment', 0),
                    'computed_at': now,
                    'expires_at': expires_at,
                    'settings_hash': 'default'
                }
                results.append(cache_entry)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"[CACHE] Error for {asin}: {str(e)[:50]}")
            
            # Progress update
            if (i + 1) % 100 == 0:
                print(f"[CACHE] Progress: {i + 1}/{total_products}")
        
        # Bulk upsert to cache table
        if results:
            print(f"[CACHE] Saving {len(results)} forecasts to cache...")
            
            # Clear old cache and insert new
            ForecastCache.query.delete()
            
            # Bulk insert
            db.session.bulk_insert_mappings(ForecastCache, results)
            db.session.commit()
        
        elapsed = time.perf_counter() - start_time
        
        stats = {
            'total_products': total_products,
            'success': success_count,
            'errors': error_count,
            'time_seconds': round(elapsed, 2),
            'rate': round(success_count / elapsed, 1) if elapsed > 0 else 0
        }
        
        print(f"[CACHE] Complete! {success_count} forecasts in {elapsed:.2f}s ({stats['rate']} products/sec)")
        
        return stats


# Singleton
cache_service = CacheService()
