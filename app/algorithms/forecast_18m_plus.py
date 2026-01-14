"""
18+ Month Forecast Algorithm

This algorithm is used for products with age >= 18 months.
It uses prior year data with smoothing and adjustments for forecasting.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple


@dataclass
class ForecastSettings:
    """Global settings for the forecast algorithm."""
    amazon_doi_goal: int = 93          # Days of inventory to cover at Amazon
    inbound_lead_time: int = 30        # Shipping time (days)
    manufacture_lead_time: int = 7     # Production time (days)
    market_adjustment: float = 0.05    # 5% market adjustment on final output
    sales_velocity_adj_weight: float = 0.15  # Weight of velocity adjustment (15%)
    
    @property
    def total_lead_time(self) -> int:
        """Production + Shipping time."""
        return self.manufacture_lead_time + self.inbound_lead_time
    
    @property
    def total_doi_goal(self) -> int:
        """Total Days of Inventory goal (Amazon + Lead Time)."""
        return self.amazon_doi_goal + self.total_lead_time


@dataclass
class InventoryLevels:
    """Current inventory levels across all locations."""
    total_inventory: int = 0
    fba_available: int = 0
    fba_reserved: int = 0
    fba_inbound: int = 0
    awd_available: int = 0
    awd_reserved: int = 0
    awd_inbound: int = 0
    awd_outbound_to_fba: int = 0


@dataclass
class ForecastResult:
    """Result of the forecast calculation."""
    units_to_make: int
    doi_total_days: float
    doi_fba_available_days: float
    unit_needed_total: float
    sales_velocity_adjustment: float
    forecast_data: pd.DataFrame
    settings_used: ForecastSettings


class Forecast18MonthPlus:
    """
    18+ Month Forecast Algorithm.
    
    Uses historical sales data with smoothing, prior year seasonality,
    and configurable adjustments to forecast future demand.
    """
    
    def __init__(self, settings: Optional[ForecastSettings] = None):
        """Initialize with optional custom settings."""
        self.settings = settings or ForecastSettings()
    
    def calculate(
        self,
        sales_history: pd.DataFrame,
        inventory: InventoryLevels,
        forecast_weeks: int = 52
    ) -> ForecastResult:
        """
        Calculate the 18+ month forecast.
        
        Args:
            sales_history: DataFrame with 'week_date' and 'units' columns
            inventory: Current inventory levels
            forecast_weeks: Number of weeks to forecast (default 52)
            
        Returns:
            ForecastResult with units to make and supporting data
        """
        # Step 1: Prepare and smooth historical data
        df = self._prepare_historical_data(sales_history)
        
        # Step 2: Apply smoothing pipeline
        df = self._apply_smoothing_pipeline(df)
        
        # Step 3: Get prior year data for seasonality
        df = self._add_prior_year_data(df)
        
        # Step 4: Calculate sales velocity adjustment
        sales_velocity_adj = self._calculate_sales_velocity_adjustment(df)
        
        # Step 5: Generate forecast for future weeks
        df = self._generate_forecast(df, forecast_weeks, sales_velocity_adj)
        
        # Step 6: Calculate inventory tracking (DOI)
        df = self._calculate_inventory_tracking(df, inventory.total_inventory)
        df = self._calculate_fba_inventory_tracking(df, inventory.fba_available)
        
        # Step 7: Calculate production numbers
        df, unit_needed_total = self._calculate_production_numbers(df, inventory)
        
        # Step 8: Calculate final results
        units_to_make = max(0, int(unit_needed_total - inventory.total_inventory))
        doi_total = self._calculate_doi(df, 'total_inventory_remaining')
        doi_fba = self._calculate_doi(df, 'fba_available_remaining')
        
        return ForecastResult(
            units_to_make=units_to_make,
            doi_total_days=doi_total,
            doi_fba_available_days=doi_fba,
            unit_needed_total=unit_needed_total,
            sales_velocity_adjustment=sales_velocity_adj,
            forecast_data=df,
            settings_used=self.settings
        )
    
    def _prepare_historical_data(self, sales_history: pd.DataFrame) -> pd.DataFrame:
        """Prepare historical sales data with proper date indexing."""
        df = sales_history.copy()
        df = df.rename(columns={'week_date': 'week_end', 'units': 'units_sold'})
        df['week_end'] = pd.to_datetime(df['week_end'])
        df = df.sort_values('week_end').reset_index(drop=True)
        df['week_number'] = range(1, len(df) + 1)
        return df
    
    def _apply_smoothing_pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the smoothing pipeline to historical data.
        
        Pipeline:
        1. units_peak_env: 4-week max envelope
        2. units_peak_env_offset: Offset average
        3. units_smooth_env: 3-week moving average
        4. units_final_curve: Max of units, peak_env_offset, smooth_env
        5. units_final_smooth: Weighted 12-week lookback average
        6. units_final_smooth_85: 85% of final_smooth
        """
        n = len(df)
        
        # 1. Peak envelope (4-week rolling max)
        df['units_peak_env'] = df['units_sold'].rolling(window=4, min_periods=1).max()
        
        # 2. Peak envelope offset (2-row average)
        df['units_peak_env_offset'] = df['units_peak_env'].rolling(window=2, min_periods=1).mean()
        
        # 3. Smooth envelope (3-week moving average)
        df['units_smooth_env'] = df['units_peak_env_offset'].rolling(window=3, min_periods=1).mean()
        
        # 4. Final curve (max of units, peak_env_offset, smooth_env)
        df['units_final_curve'] = df[['units_sold', 'units_peak_env_offset', 'units_smooth_env']].max(axis=1)
        
        # 5. Final smooth (weighted average with 12-week lookback)
        df['units_final_smooth'] = self._calculate_weighted_smooth(df['units_final_curve'])
        
        # 6. Final smooth × 0.85
        df['units_final_smooth_85'] = df['units_final_smooth'] * 0.85
        
        return df
    
    def _calculate_weighted_smooth(self, series: pd.Series, lookback: int = 12) -> pd.Series:
        """
        Calculate weighted smooth average with exponential decay.
        
        Uses a weighted average where recent weeks have more influence.
        Weights: 0.25 each for different time periods.
        """
        result = pd.Series(index=series.index, dtype=float)
        
        for i in range(len(series)):
            if i < 3:
                result.iloc[i] = series.iloc[:i+1].mean() if i > 0 else series.iloc[i]
            else:
                # Weighted average of different time windows
                # Similar to Excel formula with multiple OFFSET/IFERROR calls
                weights = []
                values = []
                
                # Recent 4 weeks (25% weight)
                recent_4 = series.iloc[max(0, i-3):i+1].mean()
                weights.append(0.25)
                values.append(recent_4)
                
                # Weeks 5-8 (25% weight)
                if i >= 7:
                    mid_4 = series.iloc[max(0, i-7):max(0, i-3)].mean()
                    weights.append(0.25)
                    values.append(mid_4)
                
                # Weeks 9-12 (25% weight)
                if i >= 11:
                    older_4 = series.iloc[max(0, i-11):max(0, i-7)].mean()
                    weights.append(0.25)
                    values.append(older_4)
                
                # Overall average (25% weight)
                overall = series.iloc[:i+1].mean()
                weights.append(0.25)
                values.append(overall)
                
                # Normalize weights
                total_weight = sum(weights)
                result.iloc[i] = sum(v * w / total_weight for v, w in zip(values, weights))
        
        return result
    
    def _add_prior_year_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add prior year smoothed data for seasonality."""
        df['prior_year_smooth_85'] = np.nan
        df['prior_year_smooth_future'] = np.nan
        
        # For each row, look back 52 weeks for prior year data
        for i in range(len(df)):
            if i >= 52:
                df.loc[df.index[i], 'prior_year_smooth_85'] = df.loc[df.index[i-52], 'units_final_smooth_85']
        
        # For future forecasting, use prior year data
        today = pd.Timestamp.today().normalize()
        future_mask = df['week_end'] >= today
        
        for i in df[future_mask].index:
            idx = df.index.get_loc(i)
            if idx >= 52:
                df.loc[i, 'prior_year_smooth_future'] = df.loc[df.index[idx-52], 'units_final_smooth_85']
        
        return df
    
    def _calculate_sales_velocity_adjustment(self, df: pd.DataFrame) -> float:
        """
        Calculate sales velocity adjustment based on recent vs historical performance.
        
        Compares recent weeks' performance to prior year same period.
        Returns a percentage adjustment (e.g., -0.50 means 50% below prior year).
        """
        today = pd.Timestamp.today().normalize()
        
        # Get last 8 weeks of actual sales
        recent_mask = (df['week_end'] < today) & (df['week_end'] >= today - timedelta(weeks=8))
        recent_df = df[recent_mask]
        
        if len(recent_df) < 4:
            return 0.0  # Not enough data
        
        recent_avg = recent_df['units_final_smooth_85'].mean()
        
        # Get prior year same period
        prior_year_avg = recent_df['prior_year_smooth_85'].mean()
        
        if pd.isna(prior_year_avg) or prior_year_avg == 0:
            return 0.0
        
        # Calculate percentage change
        velocity_adj = (recent_avg - prior_year_avg) / prior_year_avg
        
        return velocity_adj
    
    def _generate_forecast(
        self,
        df: pd.DataFrame,
        forecast_weeks: int,
        sales_velocity_adj: float
    ) -> pd.DataFrame:
        """Generate forecast for future weeks."""
        today = pd.Timestamp.today().normalize()
        
        # Find the last date in data
        last_date = df['week_end'].max()
        
        # Extend dataframe for forecast period if needed
        forecast_end = today + timedelta(weeks=forecast_weeks)
        
        if last_date < forecast_end:
            # Generate future weeks
            future_dates = pd.date_range(
                start=last_date + timedelta(weeks=1),
                end=forecast_end,
                freq='W-SAT'  # Weekly on Saturday
            )
            
            future_df = pd.DataFrame({
                'week_end': future_dates,
                'week_number': range(len(df) + 1, len(df) + len(future_dates) + 1),
                'units_sold': np.nan
            })
            
            df = pd.concat([df, future_df], ignore_index=True)
        
        # Calculate adjusted forecast for future weeks
        df['adj_forecast'] = np.nan
        df['final_adj_forecast'] = np.nan
        
        future_mask = df['week_end'] >= today
        market_adj = self.settings.market_adjustment
        velocity_weight = self.settings.sales_velocity_adj_weight
        
        for i in df[future_mask].index:
            idx = df.index.get_loc(i)
            
            # Base forecast from prior year
            if idx >= 52:
                base = df.loc[df.index[idx-52], 'units_final_smooth_85']
            else:
                # Use average if no prior year data
                base = df['units_final_smooth_85'].mean()
            
            if pd.isna(base):
                base = df['units_sold'].mean()
            
            # Apply adjustments
            # adj_forecast = base × (1 + market_adj) × (1 + velocity_adj × weight)
            adj_forecast = base * (1 + market_adj) * (1 + sales_velocity_adj * velocity_weight)
            
            df.loc[i, 'adj_forecast'] = adj_forecast
            df.loc[i, 'final_adj_forecast'] = adj_forecast
        
        return df
    
    def _calculate_inventory_tracking(
        self,
        df: pd.DataFrame,
        total_inventory: int
    ) -> pd.DataFrame:
        """Calculate running inventory and DOI for total inventory."""
        today = pd.Timestamp.today().normalize()
        
        df['total_inventory_remaining'] = np.nan
        df['cumulative_forecast'] = np.nan
        
        # Calculate cumulative forecast from today forward
        future_mask = df['week_end'] >= today
        future_df = df[future_mask].copy()
        
        if len(future_df) > 0:
            cumsum = future_df['final_adj_forecast'].fillna(0).cumsum()
            df.loc[future_df.index, 'cumulative_forecast'] = cumsum.values
            df.loc[future_df.index, 'total_inventory_remaining'] = total_inventory - cumsum.values
        
        return df
    
    def _calculate_fba_inventory_tracking(
        self,
        df: pd.DataFrame,
        fba_available: int
    ) -> pd.DataFrame:
        """Calculate running inventory and DOI for FBA available inventory."""
        today = pd.Timestamp.today().normalize()
        
        df['fba_available_remaining'] = np.nan
        
        future_mask = df['week_end'] >= today
        
        if 'cumulative_forecast' in df.columns:
            df.loc[future_mask, 'fba_available_remaining'] = (
                fba_available - df.loc[future_mask, 'cumulative_forecast'].fillna(0)
            )
        
        return df
    
    def _calculate_doi(self, df: pd.DataFrame, inventory_col: str) -> float:
        """Calculate Days of Inventory until stockout."""
        today = pd.Timestamp.today().normalize()
        
        future_df = df[df['week_end'] >= today].copy()
        
        if len(future_df) == 0:
            return 0.0
        
        # Find when inventory goes negative
        negative_mask = future_df[inventory_col] <= 0
        
        if not negative_mask.any():
            # Inventory doesn't run out in forecast period
            last_date = future_df['week_end'].max()
            return (last_date - today).days
        
        # Find first week where inventory goes negative
        first_negative_idx = negative_mask.idxmax()
        stockout_date = future_df.loc[first_negative_idx, 'week_end']
        
        # Calculate fractional week
        if first_negative_idx > future_df.index[0]:
            prev_idx = future_df.index[future_df.index.get_loc(first_negative_idx) - 1]
            prev_remaining = future_df.loc[prev_idx, inventory_col]
            week_forecast = future_df.loc[first_negative_idx, 'final_adj_forecast']
            
            if week_forecast > 0:
                fraction = prev_remaining / week_forecast
                stockout_date = future_df.loc[prev_idx, 'week_end'] + timedelta(days=fraction * 7)
        
        return max(0, (stockout_date - today).days)
    
    def _calculate_production_numbers(
        self,
        df: pd.DataFrame,
        inventory: InventoryLevels
    ) -> Tuple[pd.DataFrame, float]:
        """
        Calculate weekly units needed and total units needed for production.
        
        Units needed = forecast within the lead time + DOI goal window
        """
        today = pd.Timestamp.today().normalize()
        lead_time = self.settings.total_lead_time
        doi_goal = self.settings.total_doi_goal
        
        df['weekly_units_needed'] = np.nan
        
        # Calculate units needed for each future week within the planning window
        future_mask = df['week_end'] >= today
        planning_end = today + timedelta(days=doi_goal)
        planning_mask = future_mask & (df['week_end'] <= planning_end)
        
        # Weekly units needed = adjusted forecast for weeks within planning window
        df.loc[planning_mask, 'weekly_units_needed'] = df.loc[planning_mask, 'final_adj_forecast']
        
        # Total units needed
        unit_needed_total = df['weekly_units_needed'].sum()
        
        # Apply lead time consideration
        # Add extra weeks for lead time buffer
        lead_time_end = planning_end + timedelta(days=lead_time)
        lead_time_mask = (df['week_end'] > planning_end) & (df['week_end'] <= lead_time_end)
        
        lead_time_units = df.loc[lead_time_mask, 'final_adj_forecast'].sum()
        unit_needed_total += lead_time_units
        
        return df, unit_needed_total
