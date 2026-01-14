"""
Forecast Service - Business logic layer for forecasting.

Handles data retrieval from database and algorithm execution.
"""
import pandas as pd
from datetime import datetime, date
from typing import Optional, Dict, Any

from app import db
from app.models import Product, UnitsSold, FBAInventory, AWDInventory
from app.algorithms.forecast_18m_plus import (
    Forecast18MonthPlus, 
    ForecastSettings, 
    InventoryLevels,
    ForecastResult
)
from app.algorithms.algorithms_tps import (
    calculate_forecast_18m_plus as tps_forecast_18m_plus,
    DEFAULT_SETTINGS as TPS_DEFAULT_SETTINGS
)


class ForecastService:
    """Service for running forecasts on products."""
    
    @staticmethod
    def get_product_age_months(asin: str) -> float:
        """Calculate product age in months from first sale date."""
        first_sale = db.session.query(
            db.func.min(UnitsSold.week_date)
        ).filter(
            UnitsSold.asin == asin,
            UnitsSold.units > 0
        ).scalar()
        
        if not first_sale:
            return 0.0
        
        today = datetime.today().date()
        days = (today - first_sale).days
        return days / 30.44  # Average days per month
    
    @staticmethod
    def get_sales_history(asin: str) -> pd.DataFrame:
        """Get sales history for a product."""
        sales = UnitsSold.query.filter_by(asin=asin).order_by(UnitsSold.week_date).all()
        
        if not sales:
            return pd.DataFrame(columns=['week_date', 'units'])
        
        return pd.DataFrame([
            {'week_date': s.week_date, 'units': s.units}
            for s in sales
        ])
    
    @staticmethod
    def get_inventory_levels(asin: str) -> InventoryLevels:
        """
        Get current inventory levels for a product.
        
        Aggregates across all SKUs for the same ASIN (important for products
        with multiple SKU variations).
        """
        from sqlalchemy import func
        
        # Get FBA inventory - SUM across all SKUs for this ASIN
        fba_agg = db.session.query(
            func.coalesce(func.sum(FBAInventory.available), 0).label('available'),
            func.coalesce(func.sum(FBAInventory.total_reserved_quantity), 0).label('reserved'),
            func.coalesce(func.sum(FBAInventory.inbound_quantity), 0).label('inbound')
        ).filter(FBAInventory.asin == asin).first()
        
        fba_available = int(fba_agg.available) if fba_agg else 0
        fba_reserved = int(fba_agg.reserved) if fba_agg else 0
        fba_inbound = int(fba_agg.inbound) if fba_agg else 0
        
        # Get AWD inventory - SUM across all SKUs for this ASIN
        awd_agg = db.session.query(
            func.coalesce(func.sum(AWDInventory.available_in_awd_units), 0).label('available'),
            func.coalesce(func.sum(AWDInventory.reserved_in_awd_units), 0).label('reserved'),
            func.coalesce(func.sum(AWDInventory.inbound_to_awd_units), 0).label('inbound'),
            func.coalesce(func.sum(AWDInventory.outbound_to_fba_units), 0).label('outbound')
        ).filter(AWDInventory.asin == asin).first()
        
        awd_available = int(awd_agg.available) if awd_agg else 0
        awd_reserved = int(awd_agg.reserved) if awd_agg else 0
        awd_inbound = int(awd_agg.inbound) if awd_agg else 0
        awd_outbound = int(awd_agg.outbound) if awd_agg else 0
        
        total = (fba_available + fba_reserved + fba_inbound + 
                 awd_available + awd_reserved + awd_inbound + awd_outbound)
        
        return InventoryLevels(
            total_inventory=total,
            fba_available=fba_available,
            fba_reserved=fba_reserved,
            fba_inbound=fba_inbound,
            awd_available=awd_available,
            awd_reserved=awd_reserved,
            awd_inbound=awd_inbound,
            awd_outbound_to_fba=awd_outbound
        )
    
    @staticmethod
    def determine_algorithm(product_age_months: float) -> str:
        """Determine which algorithm to use based on product age."""
        if product_age_months >= 18:
            return "18m+"
        elif product_age_months >= 6:
            return "6-18m"
        else:
            return "0-6m"
    
    def run_forecast(
        self,
        asin: str,
        settings: Optional[ForecastSettings] = None,
        force_algorithm: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run forecast for a product.
        
        Args:
            asin: Product ASIN
            settings: Optional custom forecast settings
            force_algorithm: Force a specific algorithm ('18m+', '6-18m', '0-6m')
            
        Returns:
            Dictionary with forecast results and metadata
        """
        # Get product info
        product = Product.query.filter_by(asin=asin).first()
        
        if not product:
            return {'error': f'Product not found: {asin}'}
        
        # Get product age
        product_age_months = self.get_product_age_months(asin)
        
        # Determine algorithm
        algorithm = force_algorithm or self.determine_algorithm(product_age_months)
        
        # Get data
        sales_history = self.get_sales_history(asin)
        inventory = self.get_inventory_levels(asin)
        
        if len(sales_history) < 4:
            return {
                'error': 'Insufficient sales history',
                'asin': asin,
                'sales_weeks': len(sales_history)
            }
        
        # Run appropriate algorithm
        if algorithm == "18m+":
            result = self._run_18m_plus_forecast(sales_history, inventory, settings)
        else:
            # For now, use 18m+ for all - other algorithms to be implemented
            result = self._run_18m_plus_forecast(sales_history, inventory, settings)
        
        return {
            'asin': asin,
            'product_name': product.product_name,
            'product_age_months': round(product_age_months, 2),
            'algorithm_used': algorithm,
            'inventory': {
                'total': inventory.total_inventory,
                'fba_available': inventory.fba_available,
                'fba_reserved': inventory.fba_reserved,
                'fba_inbound': inventory.fba_inbound,
                'awd_available': inventory.awd_available,
            },
            'forecast_result': {
                'units_to_make': result.units_to_make,
                'doi_total_days': round(result.doi_total_days, 2),
                'doi_fba_available_days': round(result.doi_fba_available_days, 2),
                'unit_needed_total': round(result.unit_needed_total, 2),
                'sales_velocity_adjustment': round(result.sales_velocity_adjustment * 100, 2),
            },
            'settings_used': {
                'amazon_doi_goal': result.settings_used.amazon_doi_goal,
                'inbound_lead_time': result.settings_used.inbound_lead_time,
                'manufacture_lead_time': result.settings_used.manufacture_lead_time,
                'total_lead_time': result.settings_used.total_lead_time,
                'total_doi_goal': result.settings_used.total_doi_goal,
                'market_adjustment': result.settings_used.market_adjustment * 100,
                'sales_velocity_adj_weight': result.settings_used.sales_velocity_adj_weight * 100,
            }
        }
    
    def _run_18m_plus_forecast(
        self,
        sales_history: pd.DataFrame,
        inventory: InventoryLevels,
        settings: Optional[ForecastSettings] = None
    ) -> ForecastResult:
        """Run the 18+ month forecast algorithm."""
        algorithm = Forecast18MonthPlus(settings)
        return algorithm.calculate(sales_history, inventory)
    
    def run_forecast_tps(
        self,
        asin: str,
        custom_settings: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Run forecast using the proven TPS algorithm.
        
        This is the exact Excel formula replication algorithm.
        
        For best results, provide the 'sales_velocity_adjustment' from your 
        spreadsheet's Settings B60 value (as decimal, e.g., 0.6105 for 61.05%).
        Set 'auto_velocity': False to use the provided value instead of dynamic calculation.
        """
        # Get product info
        product = Product.query.filter_by(asin=asin).first()
        if not product:
            return {'error': f'Product not found: {asin}'}
        
        # Get product age
        product_age_months = self.get_product_age_months(asin)
        
        # Get sales data as list of dicts (TPS format)
        sales = UnitsSold.query.filter_by(asin=asin).order_by(UnitsSold.week_date).all()
        units_data = [{'week_end': s.week_date, 'units': s.units} for s in sales]
        
        if len(units_data) < 4:
            return {'error': 'Insufficient sales history', 'asin': asin}
        
        # Get inventory levels
        inventory = self.get_inventory_levels(asin)
        
        # Prepare settings
        settings = TPS_DEFAULT_SETTINGS.copy()
        settings['total_inventory'] = inventory.total_inventory
        settings['fba_available'] = inventory.fba_available
        
        if custom_settings:
            settings.update(custom_settings)
        
        # Run TPS algorithm
        result = tps_forecast_18m_plus(units_data, date.today(), settings)
        
        return {
            'asin': asin,
            'product_name': product.product_name,
            'product_age_months': round(product_age_months, 2),
            'algorithm_used': '18m+ (TPS)',
            'inventory': {
                'total': inventory.total_inventory,
                'fba_available': inventory.fba_available,
            },
            'forecast_result': {
                'units_to_make': result['units_to_make'],
                'doi_total_days': result['doi_total_days'],
                'doi_fba_days': result['doi_fba_days'],
                'total_units_needed': round(result['total_units_needed'], 1),
                'lead_time_days': result['lead_time_days'],
                'sales_velocity_adjustment': round(result.get('sales_velocity_adjustment', 0) * 100, 2),
                'adjustment_factor': round(result.get('adjustment_factor', 1.0), 4),
            },
            'settings_used': {
                'amazon_doi_goal': settings.get('amazon_doi_goal', 93),
                'inbound_lead_time': settings.get('inbound_lead_time', 30),
                'manufacture_lead_time': settings.get('manufacture_lead_time', 7),
                'market_adjustment': settings.get('market_adjustment', 0.05) * 100,
                'velocity_weight': settings.get('velocity_weight', 0.15) * 100,
            },
            'forecasts': result.get('forecasts', [])[:12]  # First 12 weeks
        }
    
    def get_forecast_details(
        self,
        asin: str,
        settings: Optional[ForecastSettings] = None
    ) -> Dict[str, Any]:
        """
        Get detailed forecast data including weekly projections.
        
        Returns full forecast dataframe for visualization.
        """
        # Get data
        sales_history = self.get_sales_history(asin)
        inventory = self.get_inventory_levels(asin)
        
        if len(sales_history) < 4:
            return {'error': 'Insufficient sales history'}
        
        # Run algorithm
        algorithm = Forecast18MonthPlus(settings)
        result = algorithm.calculate(sales_history, inventory)
        
        # Convert forecast data to serializable format
        df = result.forecast_data.copy()
        
        # Select key columns for output
        output_cols = [
            'week_end', 'week_number', 'units_sold', 
            'units_final_smooth_85', 'prior_year_smooth_85',
            'adj_forecast', 'final_adj_forecast',
            'total_inventory_remaining', 'fba_available_remaining',
            'weekly_units_needed'
        ]
        
        available_cols = [c for c in output_cols if c in df.columns]
        df = df[available_cols].copy()
        
        # Convert dates to string
        df['week_end'] = df['week_end'].dt.strftime('%Y-%m-%d')
        
        # Replace NaN with None for JSON
        df = df.where(pd.notnull(df), None)
        
        return {
            'asin': asin,
            'summary': {
                'units_to_make': result.units_to_make,
                'doi_total_days': round(result.doi_total_days, 2),
                'doi_fba_available_days': round(result.doi_fba_available_days, 2),
                'sales_velocity_adjustment': round(result.sales_velocity_adjustment * 100, 2),
            },
            'weekly_data': df.to_dict(orient='records')
        }


# Singleton instance
forecast_service = ForecastService()
