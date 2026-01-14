"""
18+ Month Forecast Algorithm V2 - Exact Excel Match

This implementation exactly replicates the Excel formulas.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class ForecastSettings:
    """Settings matching Excel Settings sheet."""
    amazon_doi_goal: int = 93           # B45
    inbound_lead_time: int = 30         # B46
    manufacture_lead_time: int = 7      # B47
    market_adjustment: float = 0.05     # B59 (5%)
    sales_velocity_adj_weight: float = 0.15  # B61 (15%)
    
    @property
    def total_lead_time(self) -> int:
        return self.manufacture_lead_time + self.inbound_lead_time
    
    @property
    def total_doi_goal(self) -> int:
        return self.amazon_doi_goal + self.total_lead_time


@dataclass
class InventoryData:
    """Inventory levels."""
    total_inventory: int = 0
    fba_available: int = 0


@dataclass 
class ForecastResult:
    """Forecast calculation results."""
    units_to_make: int
    doi_total_days: float
    doi_fba_available_days: float
    sales_velocity_adjustment: float
    unit_needed_total: float
    forecast_df: pd.DataFrame


class Forecast18MonthPlusV2:
    """
    Exact Excel algorithm implementation.
    """
    
    # Column H: 11-week weighted average (offsets -5 to +5)
    SMOOTH_WEIGHTS_H = [1, 2, 4, 7, 11, 13, 11, 7, 4, 2, 1]  # Total: 63
    SMOOTH_OFFSETS_H = [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]
    
    # Column L: 7-week weighted average (offsets -3 to +3)
    SMOOTH_WEIGHTS_L = [1, 3, 5, 7, 5, 3, 1]  # Total: 25
    SMOOTH_OFFSETS_L = [-3, -2, -1, 0, 1, 2, 3]
    
    def __init__(self, settings: Optional[ForecastSettings] = None):
        self.settings = settings or ForecastSettings()
        self.today = pd.Timestamp.today().normalize()
    
    def calculate(
        self,
        sales_history: pd.DataFrame,
        inventory: InventoryData,
    ) -> ForecastResult:
        """Run the complete forecast calculation."""
        # Step 1: Prepare base dataframe
        df = self._prepare_dataframe(sales_history)
        
        # Step 2: Calculate smoothing (Column I)
        df = self._calc_units_final_smooth_85(df)
        
        # Step 3: Prior year columns (J, K, L)
        df = self._calc_prior_year_columns(df)
        
        # Step 4: Sales velocity adjustment (Column N) - EXACT Excel formula
        df, sales_velocity_adj = self._calc_sales_velocity_adjustment_exact(df)
        
        # Step 5: Forecast columns (O, P)
        df = self._calc_forecast_columns(df, sales_velocity_adj)
        
        # Step 6: Total inventory tracking (Q, R, S, T, U, V)
        df, doi_total = self._calc_inventory_tracking_exact(
            df, inventory.total_inventory, 'total'
        )
        
        # Step 7: FBA inventory tracking (W, X, Y, Z, AA, AB)
        df, doi_fba = self._calc_inventory_tracking_exact(
            df, inventory.fba_available, 'fba'
        )
        
        # Step 8: Production numbers (AC, AD, AE)
        df, unit_needed_total, units_to_make = self._calc_production_numbers(
            df, inventory.total_inventory
        )
        
        return ForecastResult(
            units_to_make=units_to_make,
            doi_total_days=doi_total,
            doi_fba_available_days=doi_fba,
            sales_velocity_adjustment=sales_velocity_adj,
            unit_needed_total=unit_needed_total,
            forecast_df=df
        )
    
    def _prepare_dataframe(self, sales_history: pd.DataFrame) -> pd.DataFrame:
        """Prepare base dataframe with dates extended for forecast."""
        df = sales_history.copy()
        df = df.rename(columns={'week_date': 'week_end', 'units': 'units_sold'})
        df['week_end'] = pd.to_datetime(df['week_end'])
        df = df.sort_values('week_end').reset_index(drop=True)
        
        # Extend to 52 weeks into future
        last_date = df['week_end'].max()
        future_end = self.today + timedelta(weeks=52)
        
        if last_date < future_end:
            future_dates = pd.date_range(
                start=last_date + timedelta(days=7),
                end=future_end,
                freq='W-SAT'
            )
            future_df = pd.DataFrame({
                'week_end': future_dates,
                'units_sold': np.nan
            })
            df = pd.concat([df, future_df], ignore_index=True)
        
        df['row_num'] = range(len(df))
        return df
    
    def _calc_units_final_smooth_85(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Column I: units_final_smooth_.85
        
        Pipeline: C -> D -> E -> F -> G -> H -> I
        - D: units_peak_env = MAX(OFFSET(C,-2,0,4)) - 4-row window (2 before, current, 1 after)
        - E: units_peak_env_offset = (D[i] + D[i+1]) / 2 - FORWARD looking average
        - F: units_smooth_env = AVERAGE(OFFSET(E,-1,0,3)) - 3-row centered average
        - G: units_final_curve = MAX(C, E, F)
        - H: units_final_smooth (11-week weighted avg of G, only for historical)
        - I: H * 0.85
        """
        n = len(df)
        C = df['units_sold']
        
        # Column D: MAX(OFFSET(C,-2,0,4)) - max of [i-2, i-1, i, i+1]
        D = pd.Series(index=df.index, dtype=float)
        for i in range(n):
            start = max(0, i - 2)
            end = min(n, i + 2)  # i+1 inclusive, so i+2 for slicing
            window = C.iloc[start:end]
            D.iloc[i] = window.max() if len(window) > 0 else np.nan
        df['units_peak_env'] = D
        
        # Column E: (D[i] + D[i+1]) / 2 - forward-looking average
        E = pd.Series(index=df.index, dtype=float)
        for i in range(n):
            if i + 1 < n:
                E.iloc[i] = (D.iloc[i] + D.iloc[i + 1]) / 2
            else:
                E.iloc[i] = D.iloc[i]  # Last row - just use current
        df['units_peak_env_offset'] = E
        
        # Column F: AVERAGE(OFFSET(E,-1,0,3)) - 3-row window [i-1, i, i+1]
        F = pd.Series(index=df.index, dtype=float)
        for i in range(n):
            start = max(0, i - 1)
            end = min(n, i + 2)  # i+1 inclusive
            window = E.iloc[start:end]
            F.iloc[i] = window.mean() if len(window) > 0 else np.nan
        df['units_smooth_env'] = F
        
        # Column G: MAX(C, E, F)
        df['units_final_curve'] = df[['units_sold', 'units_peak_env_offset', 'units_smooth_env']].max(axis=1)
        
        # Column H: 11-week weighted average of G (only for historical dates)
        df['units_final_smooth'] = self._calc_column_h(df)
        
        # Column I: H * 0.85
        df['units_final_smooth_85'] = np.where(
            df['units_final_smooth'].notna(),
            df['units_final_smooth'] * 0.85,
            np.nan
        )
        
        return df
    
    def _calc_column_h(self, df: pd.DataFrame) -> pd.Series:
        """
        Column H: units_final_smooth
        
        11-week weighted average of Column G with weights: 1,2,4,7,11,13,11,7,4,2,1
        Only calculated for rows where A <= TODAY
        """
        result = pd.Series(index=df.index, dtype=float)
        G = df['units_final_curve']
        
        for i in range(len(df)):
            # Only calculate for historical dates (A <= TODAY)
            if df.loc[df.index[i], 'week_end'] > self.today:
                continue
            
            values = []
            weights = []
            
            for offset, weight in zip(self.SMOOTH_OFFSETS_H, self.SMOOTH_WEIGHTS_H):
                idx = i + offset
                if 0 <= idx < len(df):
                    val = G.iloc[idx]
                    if not pd.isna(val) and val > 0:
                        values.append(val)
                        weights.append(weight)
            
            if weights:
                result.iloc[i] = sum(v * w for v, w in zip(values, weights)) / sum(weights)
        
        return result
    
    def _calc_prior_year_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate J, K, L columns for prior year data.
        
        J: prior_year_units_final_smooth_.85 (52-week offset of I)
        K: prior_year_units_peak_env (2-week max of J)
        L: prior_year_final_smooth (7-week weighted avg of K: 1,3,5,7,5,3,1)
        """
        n = len(df)
        
        # Column J: 52-week offset of Column I
        df['prior_year_smooth_85'] = np.nan
        for i in range(52, n):
            df.loc[df.index[i], 'prior_year_smooth_85'] = df.loc[df.index[i-52], 'units_final_smooth_85']
        
        # Column K: MAX(OFFSET(J,-2,0,2)) - 2-week max looking back
        df['prior_year_peak_env'] = np.nan
        for i in range(n):
            # OFFSET(J,-2,0,2) means start 2 rows back, get 2 rows
            start_idx = max(0, i - 1)  # -1 because OFFSET(-2,0,2) gets rows at -2 and -1
            window = df['prior_year_smooth_85'].iloc[start_idx:i+1]
            valid = window.dropna()
            if len(valid) > 0:
                df.loc[df.index[i], 'prior_year_peak_env'] = valid.max()
        
        # Column L: 7-week weighted average of K (weights: 1,3,5,7,5,3,1)
        df['prior_year_final_smooth'] = np.nan
        for i in range(n):
            values = []
            weights = []
            
            for offset, weight in zip(self.SMOOTH_OFFSETS_L, self.SMOOTH_WEIGHTS_L):
                idx = i + offset
                if 0 <= idx < n:
                    val = df.loc[df.index[idx], 'prior_year_peak_env']
                    if not pd.isna(val) and val > 0:
                        values.append(val)
                        weights.append(weight)
            
            if weights:
                df.loc[df.index[i], 'prior_year_final_smooth'] = (
                    sum(v * w for v, w in zip(values, weights)) / sum(weights)
                )
        
        return df
    
    def _calc_sales_velocity_adjustment_exact(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
        """
        Calculate Column N: sales_velocity_adj_weighted - EXACT Excel formula
        
        Formula (for row with enough history):
        (
          0.25 * (I[n]/7) +
          0.25 * (SUM(I[n-1]:I[n])/14) +
          0.25 * (SUM(I[n-3]:I[n])/28) +
          0.25 * (SUM(I[n-5]:I[n])/42)
        ) / (
          0.25 * (L[n]/7) +
          0.25 * (SUM(L[n-1]:L[n])/14) +
          0.25 * (SUM(L[n-3]:L[n])/28) +
          0.25 * (SUM(L[n-5]:L[n])/42)
        ) - 1
        """
        df['sales_velocity_adj'] = np.nan
        
        # Only calculate for historical rows (before today)
        historical_mask = df['week_end'] < self.today
        
        for i in df[historical_mask].index:
            idx = df.index.get_loc(i)
            
            # Need at least 6 weeks of history for full formula
            if idx < 5:
                df.loc[i, 'sales_velocity_adj'] = 0
                continue
            
            # Get Column I (current) and Column L (prior year) values
            I = df['units_final_smooth_85']
            L = df['prior_year_final_smooth']
            
            # Calculate current velocity (numerator)
            current_1wk = I.iloc[idx] / 7 if not pd.isna(I.iloc[idx]) else 0
            current_2wk = I.iloc[max(0, idx-1):idx+1].sum() / 14
            current_4wk = I.iloc[max(0, idx-3):idx+1].sum() / 28
            current_6wk = I.iloc[max(0, idx-5):idx+1].sum() / 42
            
            current_velocity = 0.25 * (current_1wk + current_2wk + current_4wk + current_6wk)
            
            # Calculate prior year velocity (denominator)
            prior_1wk = L.iloc[idx] / 7 if not pd.isna(L.iloc[idx]) else 0
            prior_2wk = L.iloc[max(0, idx-1):idx+1].sum() / 14
            prior_4wk = L.iloc[max(0, idx-3):idx+1].sum() / 28
            prior_6wk = L.iloc[max(0, idx-5):idx+1].sum() / 42
            
            prior_velocity = 0.25 * (prior_1wk + prior_2wk + prior_4wk + prior_6wk)
            
            # Calculate adjustment
            if prior_velocity > 0:
                velocity_adj = (current_velocity / prior_velocity) - 1
                df.loc[i, 'sales_velocity_adj'] = velocity_adj
            else:
                df.loc[i, 'sales_velocity_adj'] = 0
        
        # Settings B60: Get LAST non-empty value from Column N
        # =INDEX(FILTER(N:N, N:N<>""), COUNT(FILTER(N:N, N:N<>"")))
        valid_velocity = df[df['sales_velocity_adj'].notna()]['sales_velocity_adj']
        sales_velocity_adj = valid_velocity.iloc[-1] if len(valid_velocity) > 0 else 0
        
        return df, sales_velocity_adj
    
    def _calc_forecast_columns(self, df: pd.DataFrame, sales_velocity_adj: float) -> pd.DataFrame:
        """
        Calculate O and P columns.
        
        Column O: adj_forecast = L * (1 + (velocity * weight) + market_adj)
        Column P: final_adj_forecast_offset = (O[current] + O[next]) / 2
        """
        market_adj = self.settings.market_adjustment
        velocity_weight = self.settings.sales_velocity_adj_weight
        
        # Column O: adj_forecast
        df['adj_forecast'] = np.nan
        future_mask = df['week_end'] >= self.today
        
        for i in df[future_mask].index:
            prior_year_smooth = df.loc[i, 'prior_year_final_smooth']
            
            if pd.isna(prior_year_smooth):
                continue
            
            # O = L * (1 + (B60 * B61) + B59)
            adj = prior_year_smooth * (1 + (sales_velocity_adj * velocity_weight) + market_adj)
            df.loc[i, 'adj_forecast'] = adj
        
        # Column P: final_adj_forecast_offset = (O + O_next) / 2
        df['final_adj_forecast'] = np.nan
        forecast_end = self.today + timedelta(days=365)
        
        for i in df[future_mask].index:
            idx = df.index.get_loc(i)
            week_date = df.loc[i, 'week_end']
            
            if week_date > forecast_end:
                continue
            
            current_o = df.loc[i, 'adj_forecast']
            next_o = df.loc[df.index[idx + 1], 'adj_forecast'] if idx + 1 < len(df) else current_o
            
            if pd.isna(current_o):
                continue
            
            if pd.isna(next_o):
                next_o = current_o
            
            df.loc[i, 'final_adj_forecast'] = (current_o + next_o) / 2
        
        return df
    
    def _calc_inventory_tracking_exact(
        self,
        df: pd.DataFrame,
        initial_inventory: int,
        inv_type: str
    ) -> Tuple[pd.DataFrame, float]:
        """
        Calculate inventory tracking - EXACT Excel formulas.
        
        Column Q: total_inventory_remaining = Inventory!$A$2 - SUM($P$first_forecast:P[row])
        Column R: total_inventory_start_of_week = Q + P
        Column S: fraction_of_week_until_runout = IF(Q<=0, R/P, "")
        Column T: mid_week_runout_date = A - 7 + (S * 7)
        Column U: final_runout_date = INDEX(T, first non-empty)
        Column V: DOI = U - TODAY()
        """
        prefix = 'total' if inv_type == 'total' else 'fba'
        
        df[f'{prefix}_remaining'] = np.nan
        df[f'{prefix}_start_of_week'] = np.nan
        df[f'{prefix}_fraction_runout'] = np.nan
        df[f'{prefix}_runout_date'] = pd.NaT
        
        # Find first forecast row (where final_adj_forecast is not NaN)
        future_mask = df['week_end'] >= self.today
        forecast_rows = df[future_mask & df['final_adj_forecast'].notna()].index.tolist()
        
        if not forecast_rows:
            return df, 0
        
        first_forecast_idx = forecast_rows[0]
        
        # Calculate cumulative sum from first forecast row
        cumulative = 0
        runout_date = None
        
        for i in forecast_rows:
            forecast = df.loc[i, 'final_adj_forecast']
            week_date = df.loc[i, 'week_end']
            
            if pd.isna(forecast):
                continue
            
            cumulative += forecast
            
            # Column Q/W: remaining = initial - cumulative
            remaining = initial_inventory - cumulative
            df.loc[i, f'{prefix}_remaining'] = remaining
            
            # Column R/X: start_of_week = remaining + forecast
            start_of_week = remaining + forecast
            df.loc[i, f'{prefix}_start_of_week'] = start_of_week
            
            # Column S/Y: fraction when inventory runs out
            if remaining <= 0 and start_of_week > 0 and forecast > 0:
                fraction = start_of_week / forecast
                df.loc[i, f'{prefix}_fraction_runout'] = fraction
                
                # Column T/Z: mid_week_runout_date = A - 7 + (fraction * 7)
                runout = week_date - timedelta(days=7) + timedelta(days=fraction * 7)
                df.loc[i, f'{prefix}_runout_date'] = runout
                
                # Column U/AA: first non-empty runout date
                if runout_date is None:
                    runout_date = runout
        
        # Column V/AB: DOI = runout_date - TODAY()
        if runout_date is not None:
            doi = (runout_date - self.today).days
        else:
            doi = 365  # Max if doesn't run out
        
        return df, max(0, doi)
    
    def _calc_production_numbers(
        self,
        df: pd.DataFrame,
        total_inventory: int
    ) -> Tuple[pd.DataFrame, float, int]:
        """
        Calculate production columns - EXACT Excel formula.
        
        Column AC: weekly_units_needed
        = P * MAX(0, MIN(TODAY + B45 + B46 + B47, A) - MAX(TODAY, A-7)) / 7
        
        Column AD: unit_needed_total = SUM(AC)
        Column AE: Units to Make = MAX(0, AD - inventory)
        """
        df['weekly_units_needed'] = np.nan
        
        # Planning window end: TODAY + DOI_goal + lead_times
        window_end = self.today + timedelta(
            days=self.settings.amazon_doi_goal + 
                 self.settings.inbound_lead_time + 
                 self.settings.manufacture_lead_time
        )
        
        future_mask = df['week_end'] >= self.today
        
        for i in df[future_mask].index:
            week_end = df.loc[i, 'week_end']
            week_start = week_end - timedelta(days=7)
            forecast = df.loc[i, 'final_adj_forecast']
            
            if pd.isna(forecast) or forecast == 0:
                continue
            
            # Excel: P * MAX(0, MIN(window_end, A) - MAX(TODAY, A-7)) / 7
            period_end = min(window_end, week_end)
            period_start = max(self.today, week_start)
            
            days_in_window = (period_end - period_start).days
            
            if days_in_window > 0:
                units_needed = forecast * days_in_window / 7
                df.loc[i, 'weekly_units_needed'] = units_needed
        
        # Column AD: sum of all weekly units needed
        unit_needed_total = df['weekly_units_needed'].sum()
        if pd.isna(unit_needed_total):
            unit_needed_total = 0
        
        # Column AE: Units to Make = MAX(0, AD - inventory)
        units_to_make = max(0, int(round(unit_needed_total - total_inventory)))
        
        return df, unit_needed_total, units_to_make
