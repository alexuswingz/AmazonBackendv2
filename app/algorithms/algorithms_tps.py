"""
Forecasting Algorithms - EXACT Excel AutoForecast Formula Replication

This module replicates the exact formulas from the Excel AutoForecast system.
All calculations match cell-for-cell with the Excel spreadsheet.

Excel Formula References:
- H3: units_final_smooth (weights: 1,2,4,7,11,13,11,7,4,2,1)
- I3: units_final_smooth_85 = H3 * 0.85
- L3: prior_year_final_smooth (weights: 1,3,5,7,5,3,1)
- O3: adj_forecast = L3 * (1 + market_adj + velocity_adj * velocity_weight)
- P3: final_adj_forecast_offset = (O3 + O4) / 2
- AC3: weekly_units_needed = P3 * overlap_fraction
- AE3: units_to_make = MAX(0, SUM(AC) - inventory)
- V3: doi_total = runout_date - TODAY()
"""

from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
import statistics


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def safe_max(values: List[float], default: float = 0) -> float:
    """Safe max that handles empty lists"""
    filtered = [v for v in values if v is not None and v != 0]
    return max(filtered) if filtered else default


def safe_avg(values: List[float], default: float = 0) -> float:
    """Safe average that handles empty lists"""
    filtered = [v for v in values if v is not None]
    return statistics.mean(filtered) if filtered else default


def parse_date(d) -> Optional[date]:
    """Parse date from various formats"""
    if d is None:
        return None
    if isinstance(d, date):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        try:
            return datetime.strptime(d.split()[0], '%Y-%m-%d').date()
        except:
            return None
    return None


def weighted_average(values: List[float], weights: List[int], center_idx: int) -> float:
    """
    Calculate weighted average centered on an index.
    Replicates Excel OFFSET-based weighted average formulas.
    
    Args:
        values: List of all values
        weights: Weight values (e.g., [1,2,4,7,11,13,11,7,4,2,1])
        center_idx: Index to center the weights on
    
    Returns:
        Weighted average, handling missing values like Excel IFERROR
    """
    n = len(values)
    half_len = len(weights) // 2
    
    weighted_sum = 0
    weight_sum = 0
    
    for i, w in enumerate(weights):
        idx = center_idx - half_len + i
        if 0 <= idx < n and values[idx] is not None and values[idx] > 0:
            weighted_sum += values[idx] * w
            weight_sum += w  # Only count weight if value exists (SIGN logic)
    
    return weighted_sum / weight_sum if weight_sum > 0 else 0


# =============================================================================
# EXCEL SETTINGS (from Settings sheet)
# =============================================================================

DEFAULT_SETTINGS = {
    # Global Settings (B45-B50)
    'amazon_doi_goal': 93,           # B45: Days of inventory to cover
    'inbound_lead_time': 30,         # B46: Shipping time
    'manufacture_lead_time': 7,      # B47: Production time
    # Total lead time = 93 + 30 + 7 = 130 days
    
    # 18+ Month Algorithm Settings (B59-B61)
    'market_adjustment': 0.05,       # B59: 5% market growth
    'sales_velocity_adjustment': 0.10,  # B60: 10% velocity adjustment
    'velocity_weight': 0.15,         # B61: 15% weight on velocity
}


# =============================================================================
# COLUMN G: units_final_curve
# Formula: =MAX(C, E, F) where C=units, E=peak_env_offset, F=smooth_env
# =============================================================================

def calculate_units_final_curve(units_data: List[Dict], extend_weeks: int = 10) -> List[float]:
    """
    Calculate Column G (units_final_curve) exactly as Excel does.
    
    Steps:
    - D: units_peak_env = IF(C="","",MAX(OFFSET(C,-2,0,4)))
    - E: units_peak_env_offset = (D + D_next) / 2
    - F: units_smooth_env = AVERAGE(OFFSET(E,-1,0,3))
    - G: units_final_curve = MAX(C, E, F)
    
    CRITICAL: 
    - D is BLANK only when C is truly EMPTY (None/""), NOT when C=0!
    - Excel treats 0 as a valid value, "" as blank
    - For future weeks (extended), C is blank, so D is blank
    - F smoothing propagates into future weeks, making G non-zero
    """
    n_original = len(units_data)
    if n_original == 0:
        return []
    
    # Extend data with synthetic future weeks (C=None for blank) for proper smoothing
    units = [d.get('units', 0) for d in units_data]  # Keep 0 as 0, not convert to None
    
    # Add extend_weeks more weeks with C=None (blank, like Excel's future rows)
    for _ in range(extend_weeks):
        units.append(None)  # None = blank, different from 0
    
    n = len(units)  # Extended length
    
    # Column D: Peak envelope - IF(C="","",MAX(OFFSET(C,-2,0,4)))
    # CRITICAL: D is blank only when C is EMPTY (None/""), NOT when C=0!
    # Excel treats 0 and "" differently - 0 is a valid value, "" is blank
    peak_env = []
    for i in range(n):
        # Check if C is truly blank (None or empty string), NOT zero
        # For extended future weeks (beyond original data), C is blank
        c_val = units[i]
        is_blank = c_val is None or (isinstance(c_val, str) and c_val == "")
        
        # For extended weeks (index >= n_original), treat as blank
        if i >= n_original:
            peak_env.append(0)
        elif is_blank:
            peak_env.append(0)
        else:
            # OFFSET(C,-2,0,4) means 4 rows: [i-2, i-1, i, i+1]
            indices = [i-2, i-1, i, i+1]
            values = [units[j] for j in indices if 0 <= j < n and units[j] is not None]
            peak_env.append(max(values) if values else 0)
    
    # Column E: Peak envelope offset = (D[i] + D[i+1]) / 2
    peak_env_offset = []
    for i in range(n):
        d_current = peak_env[i]
        d_next = peak_env[i + 1] if i + 1 < n else 0
        peak_env_offset.append((d_current + d_next) / 2)
    
    # Column F: Smooth envelope = AVERAGE(OFFSET(E,-1,0,3)) = avg of E[i-1], E[i], E[i+1]
    smooth_env = []
    for i in range(n):
        indices = [i-1, i, i+1]
        values = [peak_env_offset[j] for j in indices if 0 <= j < n]
        smooth_env.append(sum(values) / len(values) if values else 0)
    
    # Column G: Final curve = MAX(C, E, F)
    # Handle None values (blank C for future weeks)
    final_curve = []
    for i in range(n):
        c_val = units[i] if units[i] is not None else 0
        final_curve.append(max(c_val, peak_env_offset[i], smooth_env[i]))
    
    # Return the extended G values (includes future weeks for H calculation)
    return final_curve


# =============================================================================
# COLUMN H: units_final_smooth
# Excel Formula: Weighted average with weights [1,2,4,7,11,13,11,7,4,2,1]
# =============================================================================

def calculate_units_final_smooth(units_final_curve: List[float], original_length: int = None) -> List[float]:
    """
    Calculate Column H (units_final_smooth) exactly as Excel does.
    
    Excel formula H3:
    = (G[-5]*1 + G[-4]*2 + G[-3]*4 + G[-2]*7 + G[-1]*11 + G[0]*13 + 
       G[+1]*11 + G[+2]*7 + G[+3]*4 + G[+4]*2 + G[+5]*1) / sum_of_weights
    
    Weights: [1, 2, 4, 7, 11, 13, 11, 7, 4, 2, 1] (sum = 63)
    
    Args:
        units_final_curve: Extended G values (includes synthetic future weeks)
        original_length: If provided, only return this many H values (original data length)
    """
    weights = [1, 2, 4, 7, 11, 13, 11, 7, 4, 2, 1]
    
    # If original_length not specified, use full length
    if original_length is None:
        original_length = len(units_final_curve)
    
    result = []
    # Calculate H for original data rows (using extended G for proper windowing)
    for i in range(original_length):
        result.append(weighted_average(units_final_curve, weights, i))
    
    return result


# =============================================================================
# COLUMN I: units_final_smooth_85
# Excel Formula: =IF(H3="","",H3*0.85)
# =============================================================================

def calculate_units_final_smooth_85(units_final_smooth: List[float]) -> List[float]:
    """Column I = Column H × 0.85"""
    return [v * 0.85 if v else 0 for v in units_final_smooth]


# =============================================================================
# COLUMN K: prior_year_units_peak_env
# Gets Column I (units_final_smooth_85) from 52 weeks ago
# =============================================================================

def get_prior_year_peak_env(units_data: List[Dict], today: date, extend_weeks: int = 10) -> List[float]:
    """
    Get prior year's smoothed values (Column I) aligned with current weeks.
    Maps each week to the same week 52 weeks earlier.
    
    Excel Column K is the I value (units_final_smooth_85) from 52 weeks prior.
    
    CRITICAL: Must extend J and K calculations to include future weeks so that
    L weighted average can use K[+1], K[+2], K[+3] values.
    """
    n = len(units_data)
    if n == 0:
        return []
    
    # Calculate full chain: G → H → I (with extended future weeks for proper smoothing)
    final_curve = calculate_units_final_curve(units_data, extend_weeks=extend_weeks)  # Column G
    final_smooth = calculate_units_final_smooth(final_curve, original_length=n)  # Column H
    final_smooth_85 = calculate_units_final_smooth_85(final_smooth)  # Column I
    
    # Create lookup by week
    week_lookup = {}
    week_dates = []
    for i, d in enumerate(units_data):
        week_end = parse_date(d.get('week_end'))
        if week_end:
            week_lookup[week_end] = final_smooth_85[i]  # Use Column I values
            week_dates.append(week_end)
        else:
            week_dates.append(None)
    
    # Extend week dates into the future (for J/K extension)
    last_date = week_dates[-1] if week_dates else today
    extended_week_dates = list(week_dates)
    for i in range(1, extend_weeks + 1):
        future_date = last_date + timedelta(days=7 * i)
        extended_week_dates.append(future_date)
    
    # Column J: Prior year I values (52 weeks = 364 days offset)
    # Extended to include future weeks
    prior_year_j = []
    for week_end in extended_week_dates:
        if week_end:
            prior_week = week_end - timedelta(days=364)  # 52 weeks = 364 days
            prior_year_j.append(week_lookup.get(prior_week, 0))
        else:
            prior_year_j.append(0)
    
    # Column K: Rolling 2-week MAX of J values
    # Excel: K3 = MAX(OFFSET(J3, -2, 0, 2)) = MAX(J1, J2)
    # Extended to include future weeks
    prior_year_k = []
    for i in range(len(prior_year_j)):
        if i < 2:
            prior_year_k.append(prior_year_j[i])
        else:
            prior_year_k.append(max(prior_year_j[i-2], prior_year_j[i-1]))
    
    return prior_year_k


# =============================================================================
# COLUMN L: prior_year_final_smooth
# Excel Formula: Weighted average with weights [1,3,5,7,5,3,1]
# =============================================================================

def calculate_prior_year_final_smooth(prior_year_peak_env: List[float], original_length: int = None) -> List[float]:
    """
    Calculate Column L (prior_year_final_smooth) exactly as Excel does.
    
    Excel formula L3:
    = (K[-3]*1 + K[-2]*3 + K[-1]*5 + K[0]*7 + K[+1]*5 + K[+2]*3 + K[+3]*1) / sum_of_weights
    
    Weights: [1, 3, 5, 7, 5, 3, 1] (sum = 25)
    
    Args:
        prior_year_peak_env: Extended K values (includes future weeks)
        original_length: If provided, only return this many L values (original data length)
    """
    weights = [1, 3, 5, 7, 5, 3, 1]
    
    # If original_length not specified, use full length
    if original_length is None:
        original_length = len(prior_year_peak_env)
    
    result = []
    # Calculate L for original data rows (using extended K for proper windowing)
    for i in range(original_length):
        result.append(weighted_average(prior_year_peak_env, weights, i))
    
    return result


# =============================================================================
# COLUMN N: sales_velocity_adj_weighted (DYNAMIC CALCULATION)
# Compares current year (I) to prior year (L) performance
# B60 = LAST non-empty value in Column N
# =============================================================================

def calculate_column_n_velocity(
    units_final_smooth_85: List[float],  # Column I
    prior_year_final_smooth: List[float],  # Column L
    week_dates: List[date],
    today: date
) -> List[float]:
    """
    Calculate Column N (sales_velocity_adj_weighted) for all rows.
    
    Excel formula N[i]:
    = IF(A[i] >= TODAY(), "",
        IFERROR(
          ( (0.25 * I[i]/7) + (0.25 * SUM(I[i-1:i])/14) + (0.25 * SUM(I[i-3:i])/28) + (0.25 * SUM(I[i-5:i])/42) )
          /
          ( (0.25 * L[i]/7) + (0.25 * SUM(L[i-1:i])/14) + (0.25 * SUM(L[i-3:i])/28) + (0.25 * SUM(L[i-5:i])/42) )
          - 1,
          0
        )
      )
    
    Returns list of N values for each row.
    """
    n = len(units_final_smooth_85)
    I = units_final_smooth_85
    L = prior_year_final_smooth
    
    # Ensure L is same length as I
    while len(L) < n:
        L.append(0)
    
    N_values = []
    
    for idx in range(n):
        week_end = week_dates[idx] if idx < len(week_dates) else None
        
        # Only calculate for historical rows (A < TODAY)
        if week_end is None or week_end >= today:
            N_values.append(None)
            continue
        
        # Need at least 6 weeks of data for full calculation
        # But we'll calculate what we can with available data
        
        def safe_get(values, i):
            """Safely get value at index, return 0 if invalid"""
            if 0 <= i < len(values) and values[i] is not None and values[i] > 0:
                return values[i]
            return 0
        
        def safe_sum_range(values, start, end):
            """Sum values from start to end (inclusive)"""
            total = 0
            for i in range(max(0, start), min(len(values), end + 1)):
                val = safe_get(values, i)
                total += val
            return total
        
        # Current year (Column I) - weighted daily averages
        i_1w = safe_get(I, idx) / 7  # 1 week
        i_2w = safe_sum_range(I, idx-1, idx) / 14  # 2 weeks
        i_4w = safe_sum_range(I, idx-3, idx) / 28  # 4 weeks
        i_6w = safe_sum_range(I, idx-5, idx) / 42  # 6 weeks
        
        current_avg = 0.25 * i_1w + 0.25 * i_2w + 0.25 * i_4w + 0.25 * i_6w
        
        # Prior year (Column L) - weighted daily averages
        l_1w = safe_get(L, idx) / 7
        l_2w = safe_sum_range(L, idx-1, idx) / 14
        l_4w = safe_sum_range(L, idx-3, idx) / 28
        l_6w = safe_sum_range(L, idx-5, idx) / 42
        
        prior_avg = 0.25 * l_1w + 0.25 * l_2w + 0.25 * l_4w + 0.25 * l_6w
        
        # Velocity = (current / prior) - 1
        # IFERROR returns 0 if calculation fails
        if prior_avg > 0:
            velocity = (current_avg / prior_avg) - 1
        else:
            velocity = 0.0
        
        N_values.append(velocity)
    
    return N_values


def calculate_sales_velocity_adjustment(
    units_final_smooth_85: List[float],  # Column I
    prior_year_final_smooth: List[float],  # Column L
    week_dates: List[date],
    today: date
) -> float:
    """
    Calculate B60 = LAST non-empty value in Column N.
    
    Excel formula B60:
    =INDEX(
      FILTER('forecast_18m+'!N3:N, 'forecast_18m+'!N3:N<>""),
      COUNT(FILTER('forecast_18m+'!N3:N, 'forecast_18m+'!N3:N<>""))
    )
    
    Returns the most recent historical velocity adjustment.
    """
    # Calculate all N values
    N_values = calculate_column_n_velocity(
        units_final_smooth_85,
        prior_year_final_smooth,
        week_dates,
        today
    )
    
    # Get the LAST non-empty (non-None) value
    last_valid = 0.0
    for val in N_values:
        if val is not None:
            last_valid = val
    
    return last_valid


# =============================================================================
# COLUMN O: adj_forecast
# Excel Formula: =L3 * (1 + market_adj + velocity_adj * velocity_weight)
# =============================================================================

def calculate_adj_forecast(
    prior_year_smooth: List[float],
    week_dates: List[date],
    today: date,
    market_adjustment: float = 0.05,
    sales_velocity_adjustment: float = 0.10,
    velocity_weight: float = 0.15
) -> List[float]:
    """
    Calculate Column O (adj_forecast) exactly as Excel does.
    
    Excel formula O3:
    =IF(TODAY() <= A3,
        L3 * (1 + Settings!$B$59 + Settings!$B$60 * Settings!$B$61),
        "")
    
    = L3 * (1 + 0.05 + velocity_adj * 0.15)
    """
    # Combined adjustment factor
    adjustment = 1 + market_adjustment + (sales_velocity_adjustment * velocity_weight)
    
    result = []
    for i, (smooth_val, week_end) in enumerate(zip(prior_year_smooth, week_dates)):
        if week_end and week_end >= today:
            result.append(smooth_val * adjustment)
        else:
            result.append(0)  # Empty for past dates
    
    return result


# =============================================================================
# COLUMN P: final_adj_forecast_offset
# Excel Formula: =IF(AND(A3>=TODAY(), A3<=TODAY()+365), (O3+O4)/2, "")
# =============================================================================

def calculate_final_forecast(adj_forecast: List[float], week_dates: List[date], today: date) -> List[float]:
    """
    Calculate Column P (final_adj_forecast_offset) exactly as Excel does.
    
    Averages current and next week's forecast for smoothing.
    """
    n = len(adj_forecast)
    result = []
    
    for i in range(n):
        week_end = week_dates[i] if i < len(week_dates) else None
        
        if week_end and today <= week_end <= today + timedelta(days=365):
            current = adj_forecast[i]
            next_val = adj_forecast[i + 1] if i + 1 < n else current
            result.append((current + next_val) / 2)
        else:
            result.append(0)
    
    return result


# =============================================================================
# COLUMN AC: weekly_units_needed
# Excel Formula: =P3 * overlap_fraction_with_lead_time
# =============================================================================

def calculate_weekly_units_needed(
    forecasts: List[float],
    week_dates: List[date],
    today: date,
    lead_time_days: int = 130  # 93 + 30 + 7
) -> List[float]:
    """
    Calculate Column AC (weekly_units_needed) exactly as Excel does.
    
    Excel formula AC3:
    =IF(OR($A3="", $P3=""), "",
        $P3 * MAX(0,
            MIN(TODAY() + lead_time, $A3) - MAX(TODAY(), $A3-7)
        ) / 7
    )
    
    This calculates the portion of each week's forecast that falls within
    the lead time window [TODAY, TODAY + lead_time].
    """
    lead_time_end = today + timedelta(days=lead_time_days)
    result = []
    
    for forecast, week_end in zip(forecasts, week_dates):
        if not week_end or not forecast:
            result.append(0)
            continue
        
        week_start = week_end - timedelta(days=7)
        
        # Calculate overlap: MAX(0, MIN(lead_time_end, week_end) - MAX(today, week_start))
        period_start = max(today, week_start)
        period_end = min(lead_time_end, week_end)
        
        overlap_days = (period_end - period_start).days
        if overlap_days > 0:
            fraction = overlap_days / 7
            result.append(forecast * fraction)
        else:
            result.append(0)
    
    return result


# =============================================================================
# COLUMN AE: units_to_make
# Excel Formula: =MAX(0, AD3 - Inventory!$A$2) where AD3 = SUM(AC:AC)
# =============================================================================

def calculate_units_to_make(
    weekly_units_needed: List[float],
    total_inventory: int
) -> int:
    """
    Calculate Column AE (units_to_make) exactly as Excel does.
    
    Excel formula:
    - AD3 = SUM(AC3:AC) (total units needed during lead time)
    - AE3 = MAX(0, AD3 - Inventory)
    """
    total_needed = sum(weekly_units_needed)
    units_to_make = max(0, total_needed - total_inventory)
    return int(round(units_to_make))


# =============================================================================
# DOI CALCULATION (Columns Q, R, S, T, U, V)
# =============================================================================

def calculate_doi(
    forecasts: List[float],
    week_dates: List[date],
    inventory: int,
    today: date
) -> Dict:
    """
    Calculate DOI exactly as Excel does using iterative inventory drawdown.
    
    Excel formulas:
    - Q3 (inventory_remaining) = Inventory - cumulative_sum(P)
    - R3 (inventory_start_of_week) = previous Q value
    - S3 (fraction) = IF(Q3 <= 0, R3/P3, "")
    - T3 (runout_date) = week_start + S3 * 7
    - U3 = first non-empty T value
    - V3 (DOI) = U3 - TODAY()
    """
    if not forecasts or not week_dates:
        return {'doi_days': 0, 'runout_date': None}
    
    cumulative = 0
    runout_date = None
    
    for i, (forecast, week_end) in enumerate(zip(forecasts, week_dates)):
        if not week_end or week_end < today:
            continue
        
        if forecast <= 0:
            continue
        
        week_start = week_end - timedelta(days=7)
        inventory_at_start = inventory - cumulative
        cumulative += forecast
        inventory_remaining = inventory - cumulative
        
        # S3: When inventory runs out (Q <= 0), calculate fraction
        if inventory_remaining <= 0 and runout_date is None:
            # fraction = inventory_at_start / forecast
            if forecast > 0:
                fraction = inventory_at_start / forecast
                fraction = max(0, min(1, fraction))
                # T3: runout_date = week_start + fraction * 7
                runout_date = week_start + timedelta(days=fraction * 7)
            else:
                runout_date = week_start
            break
    
    # If inventory never runs out in forecast period
    if runout_date is None:
        if forecasts:
            avg_weekly = sum(f for f in forecasts if f > 0) / max(1, len([f for f in forecasts if f > 0]))
            if avg_weekly > 0:
                weeks_left = inventory / avg_weekly
                runout_date = today + timedelta(weeks=weeks_left)
            else:
                runout_date = today + timedelta(days=365)
        else:
            runout_date = today + timedelta(days=365)
    
    # V3: DOI = runout_date - TODAY()
    doi_days = (runout_date - today).days if runout_date else 0
    
    return {
        'doi_days': max(0, doi_days),
        'runout_date': runout_date
    }


# =============================================================================
# MAIN 18m+ FORECAST FUNCTION (Combines all columns)
# =============================================================================

def calculate_forecast_18m_plus(
    units_data: List[Dict],
    today: date = None,
    settings: Dict = None
) -> Dict:
    """
    Calculate complete 18m+ forecast exactly as Excel does.
    
    This replicates the entire forecast_18m+ sheet calculation chain:
    G → H → I → K → L → O → P → Q/R/S/T/U → V (DOI) → AC → AD → AE (Units to Make)
    
    Key insight: For future weeks, K gets the I value from 52 weeks ago.
    """
    if today is None:
        today = date.today()
    
    if settings is None:
        settings = DEFAULT_SETTINGS.copy()
    
    if not units_data:
        return {
            'units_to_make': 0,
            'doi_total_days': 0,
            'doi_fba_days': 0,
            'forecasts': [],
            'settings': settings
        }
    
    # Extract data
    n = len(units_data)
    week_dates = [parse_date(d.get('week_end')) for d in units_data]
    
    # Column G: units_final_curve (extended with synthetic future weeks for proper smoothing)
    final_curve = calculate_units_final_curve(units_data, extend_weeks=10)
    
    # Column H: units_final_smooth (weights: 1,2,4,7,11,13,11,7,4,2,1)
    # Pass original length to only get H values for actual data rows
    final_smooth = calculate_units_final_smooth(final_curve, original_length=n)
    
    # Column I: units_final_smooth_85
    final_smooth_85 = calculate_units_final_smooth_85(final_smooth)
    
    # Create lookup of I values by date for prior year mapping
    i_value_lookup = {}
    for i, d in enumerate(units_data):
        week_end = parse_date(d.get('week_end'))
        if week_end:
            i_value_lookup[week_end] = final_smooth_85[i]
    
    # Generate extended week dates (data + 52 future weeks)
    last_date = week_dates[-1] if week_dates else today
    extended_dates = list(week_dates)
    
    # Add 104 weeks of future dates for full coverage
    for i in range(1, 105):
        future_date = last_date + timedelta(days=7 * i)
        if future_date not in extended_dates:
            extended_dates.append(future_date)
    
    # Column J: Prior year I values (52 weeks = 364 days offset)
    # Excel: J60 = I8 means 52-row offset
    extended_j = []
    for week_end in extended_dates:
        if week_end:
            prior_week = week_end - timedelta(days=364)  # 52 weeks = 364 days
            j_val = i_value_lookup.get(prior_week, 0)
            extended_j.append(j_val)
        else:
            extended_j.append(0)
    
    # Column K: Rolling 2-week MAX of J values
    # Excel: K3 = MAX(OFFSET(J3, -2, 0, 2)) = MAX(J1, J2)
    # K[i] = MAX(J[i-2], J[i-1])
    extended_k = []
    for i in range(len(extended_j)):
        if i < 2:
            # Not enough prior data, use current J value
            extended_k.append(extended_j[i])
        else:
            # MAX of previous 2 J values
            k_val = max(extended_j[i-2], extended_j[i-1])
            extended_k.append(k_val)
    
    # Calculate L (weighted average of K) for all dates
    # Weights: [1, 3, 5, 7, 5, 3, 1]
    weights_L = [1, 3, 5, 7, 5, 3, 1]
    extended_L = []
    for i in range(len(extended_k)):
        extended_L.append(weighted_average(extended_k, weights_L, i))
    
    # Calculate dynamic velocity adjustment
    # Use dynamic calculation unless explicit velocity is provided AND auto_velocity is False
    use_dynamic = settings.get('auto_velocity', True)  # Default to dynamic
    
    if use_dynamic:
        # Calculate dynamic velocity from Column N logic
        dynamic_velocity = calculate_sales_velocity_adjustment(
            final_smooth_85,  # Column I
            extended_L[:n],   # Column L (just the original data portion)
            week_dates,
            today
        )
        velocity_adj = dynamic_velocity
    else:
        velocity_adj = settings.get('sales_velocity_adjustment', 0.10)
    
    # Store calculated velocity in settings for output
    settings['calculated_velocity_adjustment'] = velocity_adj
    
    # Calculate O (adjusted forecast) for future dates only
    market_adj = settings.get('market_adjustment', 0.05)
    velocity_weight = settings.get('velocity_weight', 0.15)
    adjustment = 1 + market_adj + (velocity_adj * velocity_weight)
    
    extended_O = []
    for i, week_end in enumerate(extended_dates):
        if week_end and week_end >= today:
            extended_O.append(extended_L[i] * adjustment)
        else:
            extended_O.append(0)
    
    # Calculate P (average of O and next O) for future dates
    extended_P = []
    for i in range(len(extended_O)):
        week_end = extended_dates[i] if i < len(extended_dates) else None
        if week_end and today <= week_end <= today + timedelta(days=365):
            current_O = extended_O[i]
            next_O = extended_O[i + 1] if i + 1 < len(extended_O) else current_O
            extended_P.append((current_O + next_O) / 2)
        else:
            extended_P.append(0)
    
    # Calculate lead time
    lead_time_days = (
        settings.get('amazon_doi_goal', 93) +
        settings.get('inbound_lead_time', 30) +
        settings.get('manufacture_lead_time', 7)
    )
    
    # Column AC: weekly_units_needed
    weekly_needed = calculate_weekly_units_needed(
        extended_P, extended_dates, today, lead_time_days
    )
    
    # Get inventory values
    total_inventory = settings.get('total_inventory', 0)
    fba_available = settings.get('fba_available', 0)
    
    # Column AE: units_to_make = MAX(0, SUM(AC) - inventory)
    units_to_make = calculate_units_to_make(weekly_needed, total_inventory)
    
    # Calculate DOI for total inventory
    doi_total = calculate_doi(extended_P, extended_dates, total_inventory, today)
    
    # Calculate DOI for FBA inventory
    doi_fba = calculate_doi(extended_P, extended_dates, fba_available, today)
    
    return {
        'units_to_make': units_to_make,
        'doi_total_days': doi_total['doi_days'],
        'doi_fba_days': doi_fba['doi_days'],
        'runout_date_total': doi_total['runout_date'],
        'runout_date_fba': doi_fba['runout_date'],
        'lead_time_days': lead_time_days,
        'total_units_needed': sum(weekly_needed),
        'sales_velocity_adjustment': velocity_adj,  # Dynamic or provided
        'adjustment_factor': adjustment,  # Final multiplier used
        'forecasts': [
            {
                'week_end': d.isoformat() if d else None,
                'forecast': f,
                'units_needed': w
            }
            for d, f, w in zip(extended_dates, extended_P, weekly_needed)
            if d and d >= today
        ][:52],  # Return first 52 weeks
        'settings': settings
    }


# =============================================================================
# 6-18 MONTH FORECAST ALGORITHM (forecast_6m-18m_V2 sheet)
# =============================================================================

def calculate_forecast_6_18m(
    units_data: List[Dict],
    seasonality_data: List[Dict],
    today: date = None,
    settings: Dict = None
) -> Dict:
    """
    Calculate 6-18 month forecast exactly as Excel does (forecast_6m-18m_V2 sheet).
    
    Excel formula chain:
    C = units_sold
    D = sv_smooth_env_97 (search volume from Keyword_Seasonality!I)
    E = C/D (conversion rate = sales / search volume)
    F = avg peak CVR (constant $F$3, average around max E)
    G = seasonality_index (from Keyword_Seasonality!J)
    H = F × (1 + 0.25 × (G - 1)) (25% weighted CVR)
    I = D × H (forecast = search volume × adjusted CVR)
    J = I for future weeks (forecast column)
    """
    if today is None:
        today = date.today()
    
    if settings is None:
        settings = DEFAULT_SETTINGS.copy()
    
    if not units_data:
        return {
            'units_to_make': 0,
            'doi_total_days': 0,
            'doi_fba_days': 0,
            'forecasts': [],
            'settings': settings
        }
    
    # Build seasonality lookups by week number
    # D = sv_smooth_env_97 (search volume), G = seasonality_index
    sv_smooth_97_lookup = {}  # Column D (sv_smooth_env_97)
    seasonality_idx_lookup = {}  # Column G (seasonality_index)
    
    for s in seasonality_data:
        week_num = s.get('week_of_year', s.get('week_number', 0))
        if week_num:
            # Use sv_smooth_env_97 directly, or calculate from sv_smooth_env
            sv_97 = s.get('sv_smooth_env_97', 0)
            if not sv_97:
                sv_97 = s.get('sv_smooth_env', 100) * 0.97
            sv_smooth_97_lookup[week_num] = sv_97
            seasonality_idx_lookup[week_num] = s.get('seasonality_index', 1.0)
    
    # Extract week dates and units
    week_dates = [parse_date(d.get('week_end')) for d in units_data]
    units = [d.get('units', 0) or 0 for d in units_data]
    
    # Column D: Get sv_smooth_env_97 for each week
    D_values = []
    for d in units_data:
        week_end = parse_date(d.get('week_end'))
        if week_end:
            week_of_year = week_end.isocalendar()[1]
            D_values.append(sv_smooth_97_lookup.get(week_of_year, 3000))
        else:
            D_values.append(3000)
    
    # Column E: Conversion rate = C / D (sales / search volume)
    E_values = []
    for c, d in zip(units, D_values):
        if d and d > 0 and c > 0:
            E_values.append(c / d)
        else:
            E_values.append(0)
    
    # Column F: Average peak conversion rate (constant)
    # Excel formula: =LET(maxVal, MAX(E:E), r, MATCH(maxVal, E:E, 0), AVERAGE(INDEX(E:E, r-2):INDEX(E:E, r+2)))
    # CRITICAL: Excel's AVERAGE includes zeros in the calculation!
    non_zero_E = [e for e in E_values if e > 0]
    if non_zero_E:
        max_E = max(E_values)  # Max from all E values
        max_idx = E_values.index(max_E)
        # 5-week window around peak (Excel includes zeros!)
        start_idx = max(0, max_idx - 2)
        end_idx = min(len(E_values), max_idx + 3)
        window = E_values[start_idx:end_idx]  # Include zeros like Excel
        F_constant = sum(window) / len(window) if window else max_E
    else:
        F_constant = 0.0050  # Default CVR if no data (0.5%)
    
    # Column G: Get seasonality index for each week
    G_values = []
    for d in units_data:
        week_end = parse_date(d.get('week_end'))
        if week_end:
            week_of_year = week_end.isocalendar()[1]
            G_values.append(seasonality_idx_lookup.get(week_of_year, 1.0))
        else:
            G_values.append(1.0)
    
    # Column H: 25% weighted CVR = F × (1 + 0.25 × (G - 1))
    H_values = []
    for g in G_values:
        H_values.append(F_constant * (1 + 0.25 * (g - 1)))
    
    # Column I: Forecast = D × H (search volume × adjusted CVR)
    I_values = []
    for d, h in zip(D_values, H_values):
        I_values.append(d * h)
    
    # Extend into future weeks (beyond data)
    last_date = week_dates[-1] if week_dates else today
    extended_dates = list(week_dates)
    extended_forecasts = list(I_values)
    
    # Add 104 weeks of future dates
    for i in range(1, 105):
        future_date = last_date + timedelta(days=7 * i)
        if future_date not in extended_dates:
            extended_dates.append(future_date)
            
            # Get values for future week
            week_of_year = future_date.isocalendar()[1]
            d_val = sv_smooth_97_lookup.get(week_of_year, 3000)
            g_val = seasonality_idx_lookup.get(week_of_year, 1.0)
            h_val = F_constant * (1 + 0.25 * (g_val - 1))
            i_val = d_val * h_val
            extended_forecasts.append(i_val)
    
    # Column J: Forecast for future weeks only
    J_values = []
    for week_end, forecast in zip(extended_dates, extended_forecasts):
        if week_end and week_end > today:
            J_values.append(forecast)
        else:
            J_values.append(0)
    
    # Calculate lead time
    lead_time_days = (
        settings.get('amazon_doi_goal', 93) +
        settings.get('inbound_lead_time', 30) +
        settings.get('manufacture_lead_time', 7)
    )
    
    # Weekly units needed
    weekly_needed = calculate_weekly_units_needed(
        J_values, extended_dates, today, lead_time_days
    )
    
    # Get inventory values
    total_inventory = settings.get('total_inventory', 0)
    fba_available = settings.get('fba_available', 0)
    
    # Units to make = MAX(0, SUM(weekly_needed) - inventory)
    units_to_make = calculate_units_to_make(weekly_needed, total_inventory)
    
    # Calculate DOI using J values (future forecasts)
    doi_total = calculate_doi(J_values, extended_dates, total_inventory, today)
    doi_fba = calculate_doi(J_values, extended_dates, fba_available, today)
    
    return {
        'units_to_make': units_to_make,
        'doi_total_days': doi_total['doi_days'],
        'doi_fba_days': doi_fba['doi_days'],
        'runout_date_total': doi_total['runout_date'],
        'runout_date_fba': doi_fba['runout_date'],
        'lead_time_days': lead_time_days,
        'total_units_needed': sum(weekly_needed),
        'F_constant': F_constant,  # Peak conversion rate for debugging
        'forecasts': [
            {
                'week_end': d.isoformat() if d else None,
                'forecast': f,
                'units_needed': w
            }
            for d, f, w in zip(extended_dates, J_values, weekly_needed)
            if d and d >= today
        ][:52],
        'settings': settings
    }


# =============================================================================
# 0-6 MONTH FORECAST ALGORITHM (forecast_0m-6m sheet)
# =============================================================================

def calculate_forecast_0_6m_exact(
    units_data: List[Dict],
    seasonality_data: List[Dict],
    vine_claims: List[Dict] = None,
    today: date = None,
    settings: Dict = None
) -> Dict:
    """
    Calculate 0-6 month forecast exactly as Excel does.
    
    Excel formula chain (forecast_0m-6m sheet):
    C = units_sold
    D = vine_units_claimed (for the week)
    E = MAX(0, C - D) (adjusted units)
    F = MAX(E:E) (peak adjusted units - constant)
    G = seasonality_index (from Keyword_Seasonality!J)
    H = peak_units × (G / current_seasonality)^0.65 (forecast with elasticity)
    
    Key insight: Uses peak sales scaled by seasonality with 0.65 elasticity
    """
    if today is None:
        today = date.today()
    
    if settings is None:
        settings = DEFAULT_SETTINGS.copy()
    
    if vine_claims is None:
        vine_claims = []
    
    if not units_data:
        return {
            'units_to_make': 0,
            'doi_total_days': 0,
            'doi_fba_days': 0,
            'forecasts': [],
            'settings': settings
        }
    
    # Build seasonality lookup (Column G)
    seasonality_idx_lookup = {}
    for s in seasonality_data:
        week_num = s.get('week_of_year', s.get('week_number', 0))
        if week_num:
            seasonality_idx_lookup[week_num] = s.get('seasonality_index', 1.0)
    
    # Build vine claims lookup by week
    vine_lookup = {}
    for vc in vine_claims:
        claim_date = parse_date(vc.get('claim_date'))
        if claim_date:
            # Group by week ending
            week_num = claim_date.isocalendar()[1]
            vine_lookup[week_num] = vine_lookup.get(week_num, 0) + (vc.get('units_claimed', 0) or 0)
    
    # Extract week dates and units
    week_dates = [parse_date(d.get('week_end')) for d in units_data]
    units = [d.get('units', 0) or 0 for d in units_data]
    
    # Column E: Adjusted units = units - vine_claims (minimum 0)
    E_values = []
    for i, d in enumerate(units_data):
        week_end = parse_date(d.get('week_end'))
        if week_end:
            week_of_year = week_end.isocalendar()[1]
            vine = vine_lookup.get(week_of_year, 0)
            adjusted = max(0, units[i] - vine)
            E_values.append(adjusted)
        else:
            E_values.append(units[i])
    
    # Column F: Peak adjusted units (constant)
    F_peak = max(E_values) if E_values else 0
    
    # Find the current (last historical) seasonality index
    # "lastRow" = last row before today
    last_historical_idx = None
    last_historical_seasonality = 1.0
    for i, week_end in enumerate(week_dates):
        if week_end and week_end < today:
            last_historical_idx = i
    
    if last_historical_idx is not None and week_dates[last_historical_idx]:
        last_week = week_dates[last_historical_idx]
        week_of_year = last_week.isocalendar()[1]
        last_historical_seasonality = seasonality_idx_lookup.get(week_of_year, 1.0)
    
    # Elasticity factor
    ELASTICITY = 0.65
    
    # Column H: Forecast = peak × (seasonality / current_seasonality)^elasticity
    # Only for future weeks within 12 months
    H_values = []
    twelve_months_out = today + timedelta(days=365)
    
    for i, week_end in enumerate(week_dates):
        if week_end:
            week_of_year = week_end.isocalendar()[1]
            G_seasonality = seasonality_idx_lookup.get(week_of_year, 1.0)
            
            # Only calculate for future weeks within 12 months
            if today <= week_end <= twelve_months_out:
                if last_historical_seasonality > 0:
                    ratio = G_seasonality / last_historical_seasonality
                    forecast = max(0, F_peak * (ratio ** ELASTICITY))
                else:
                    forecast = F_peak
                H_values.append(forecast)
            else:
                H_values.append(0)
        else:
            H_values.append(0)
    
    # Extend into future weeks
    last_date = week_dates[-1] if week_dates else today
    extended_dates = list(week_dates)
    extended_forecasts = list(H_values)
    
    for i in range(1, 60):  # Add up to 60 weeks
        future_date = last_date + timedelta(days=7 * i)
        if future_date not in extended_dates and future_date <= twelve_months_out:
            extended_dates.append(future_date)
            
            week_of_year = future_date.isocalendar()[1]
            G_seasonality = seasonality_idx_lookup.get(week_of_year, 1.0)
            
            if last_historical_seasonality > 0:
                ratio = G_seasonality / last_historical_seasonality
                forecast = max(0, F_peak * (ratio ** ELASTICITY))
            else:
                forecast = F_peak
            extended_forecasts.append(forecast)
    
    # Calculate lead time
    lead_time_days = (
        settings.get('amazon_doi_goal', 93) +
        settings.get('inbound_lead_time', 30) +
        settings.get('manufacture_lead_time', 7)
    )
    
    # Column U: Weekly units needed
    weekly_needed = calculate_weekly_units_needed(
        extended_forecasts, extended_dates, today, lead_time_days
    )
    
    # Get inventory values
    total_inventory = settings.get('total_inventory', 0)
    fba_available = settings.get('fba_available', 0)
    
    # Column W: Units to make = MAX(0, SUM(U) - Inventory)
    units_to_make = calculate_units_to_make(weekly_needed, total_inventory)
    
    # Calculate DOI
    doi_total = calculate_doi(extended_forecasts, extended_dates, total_inventory, today)
    doi_fba = calculate_doi(extended_forecasts, extended_dates, fba_available, today)
    
    return {
        'units_to_make': units_to_make,
        'doi_total_days': doi_total['doi_days'],
        'doi_fba_days': doi_fba['doi_days'],
        'runout_date_total': doi_total['runout_date'],
        'runout_date_fba': doi_fba['runout_date'],
        'lead_time_days': lead_time_days,
        'total_units_needed': sum(weekly_needed),
        'F_peak': F_peak,  # Peak units for debugging
        'last_seasonality': last_historical_seasonality,
        'elasticity': ELASTICITY,
        'forecasts': [
            {
                'week_end': d.isoformat() if d else None,
                'forecast': f,
                'units_needed': w
            }
            for d, f, w in zip(extended_dates, extended_forecasts, weekly_needed)
            if d and d >= today
        ][:52],
        'settings': settings
    }


# =============================================================================
# SIMPLIFIED WRAPPER FUNCTIONS
# =============================================================================

def generate_full_forecast(
    product_asin: str,
    units_sold_data: List[Dict],
    seasonality_data: List[Dict],
    inventory: Dict,
    settings: Dict = None,
    today: date = None,
    algorithm: str = '18m+',
    vine_claims: List[Dict] = None
) -> Dict:
    """
    Generate complete forecast using the exact Excel algorithms.
    
    This is the main entry point for forecast generation.
    
    Args:
        algorithm: Which algorithm to use as primary ('0-6m', '6-18m', or '18m+')
        vine_claims: Optional vine claim data for 0-6m algorithm
    """
    if today is None:
        today = date.today()
    
    if settings is None:
        settings = DEFAULT_SETTINGS.copy()
    
    if vine_claims is None:
        vine_claims = []
    
    # Add inventory to settings for the calculation
    settings['total_inventory'] = inventory.get('total_inventory', 0)
    settings['fba_available'] = inventory.get('fba_available', 0)
    
    # Calculate using all three algorithms
    result_18m = calculate_forecast_18m_plus(units_sold_data, today, settings)
    result_6_18m = calculate_forecast_6_18m(units_sold_data, seasonality_data, today, settings)
    result_0_6m = calculate_forecast_0_6m_exact(units_sold_data, seasonality_data, vine_claims, today, settings)
    
    # Select primary result based on algorithm choice
    if algorithm == '0-6m':
        primary = result_0_6m
    elif algorithm == '6-18m':
        primary = result_6_18m
    else:
        primary = result_18m
    
    return {
        'product_asin': product_asin,
        'generated_at': datetime.now().isoformat(),
        'calculation_date': today.isoformat(),
        'inventory': inventory,
        'active_algorithm': algorithm,
        'settings': {
            'amazon_doi_goal': settings.get('amazon_doi_goal', 93),
            'inbound_lead_time': settings.get('inbound_lead_time', 30),
            'manufacture_lead_time': settings.get('manufacture_lead_time', 7),
            'total_lead_time': primary['lead_time_days'],
            'market_adjustment': settings.get('market_adjustment', 0.05),
            'sales_velocity_adjustment': settings.get('sales_velocity_adjustment', 0.10),
            'velocity_weight': settings.get('velocity_weight', 0.15)
        },
        'algorithms': {
            '0-6m': {
                'name': '0-6 Month Algorithm',
                'units_to_make': result_0_6m['units_to_make'],
                'doi_total_days': result_0_6m['doi_total_days'],
                'doi_fba_days': result_0_6m['doi_fba_days'],
                'runout_date_total': result_0_6m['runout_date_total'].isoformat() if result_0_6m['runout_date_total'] else None,
                'runout_date_fba': result_0_6m['runout_date_fba'].isoformat() if result_0_6m['runout_date_fba'] else None,
                'total_units_needed': result_0_6m['total_units_needed']
            },
            '6-18m': {
                'name': '6-18 Month Algorithm',
                'units_to_make': result_6_18m['units_to_make'],
                'doi_total_days': result_6_18m['doi_total_days'],
                'doi_fba_days': result_6_18m['doi_fba_days'],
                'runout_date_total': result_6_18m['runout_date_total'].isoformat() if result_6_18m['runout_date_total'] else None,
                'runout_date_fba': result_6_18m['runout_date_fba'].isoformat() if result_6_18m['runout_date_fba'] else None,
                'total_units_needed': result_6_18m['total_units_needed']
            },
            '18m+': {
                'name': '18+ Month Algorithm',
                'units_to_make': result_18m['units_to_make'],
                'doi_total_days': result_18m['doi_total_days'],
                'doi_fba_days': result_18m['doi_fba_days'],
                'runout_date_total': result_18m['runout_date_total'].isoformat() if result_18m['runout_date_total'] else None,
                'runout_date_fba': result_18m['runout_date_fba'].isoformat() if result_18m['runout_date_fba'] else None,
                'total_units_needed': result_18m['total_units_needed']
            }
        },
        'forecasts': {
            '0-6m': result_0_6m['forecasts'],
            '6-18m': result_6_18m['forecasts'],
            '18m+': result_18m['forecasts']
        },
        'summary': {
            'total_inventory': inventory.get('total_inventory', 0),
            'fba_available': inventory.get('fba_available', 0),
            'primary_units_to_make': primary['units_to_make'],
            'primary_doi_total': primary['doi_total_days'],
            'primary_doi_fba': primary['doi_fba_days']
        }
    }


# =============================================================================
# LEGACY COMPATIBILITY FUNCTIONS
# =============================================================================

def calculate_doi_exact(
    forecasts: List[float],
    week_dates: List[date],
    total_inventory: int,
    fba_available: int,
    today: date
) -> Dict:
    """Legacy compatibility wrapper for DOI calculation."""
    doi_total = calculate_doi(forecasts, week_dates, total_inventory, today)
    doi_fba = calculate_doi(forecasts, week_dates, fba_available, today)
    
    return {
        'doi_total_days': doi_total['doi_days'],
        'doi_fba_days': doi_fba['doi_days'],
        'runout_date_total': doi_total['runout_date'],
        'runout_date_fba': doi_fba['runout_date']
    }


# =============================================================================
# SEASONALITY CALCULATIONS (for backward compatibility)
# =============================================================================

def calculate_seasonality(search_volumes: List[float]) -> List[Dict]:
    """
    Calculate seasonality indices from weekly search volume data.
    """
    n = len(search_volumes)
    if n == 0:
        return []
    
    # Peak envelope
    sv_peak_env = []
    for i in range(n):
        start = max(0, i - 2)
        end = min(n, i + 1)
        window = search_volumes[start:end]
        sv_peak_env.append(max(window) if window else search_volumes[i])
    
    # Peak envelope offset
    sv_peak_env_offset = []
    for i in range(n):
        if i < n - 1:
            sv_peak_env_offset.append((sv_peak_env[i] + sv_peak_env[i + 1]) / 2)
        else:
            sv_peak_env_offset.append(sv_peak_env[i])
    
    # Smooth envelope
    sv_smooth_env = []
    for i in range(n):
        start = max(0, i - 1)
        end = min(n, i + 2)
        window = sv_peak_env_offset[start:end]
        sv_smooth_env.append(sum(window) / len(window) if window else sv_peak_env_offset[i])
    
    # Final curve
    sv_final_curve = []
    for i in range(n):
        vals = [search_volumes[i], sv_peak_env_offset[i], sv_smooth_env[i]]
        sv_final_curve.append(sum(vals) / len(vals))
    
    # Smooth
    sv_smooth = []
    for i in range(n):
        start = max(0, i - 1)
        end = min(n, i + 2)
        sv_smooth.append(sum(sv_final_curve[start:end]) / len(sv_final_curve[start:end]))
    
    # Final smooth
    sv_smooth_final = []
    for i in range(n):
        if i < n - 1:
            sv_smooth_final.append((sv_smooth[i] + sv_smooth[i + 1]) / 2)
        else:
            sv_smooth_final.append(sv_smooth[i])
    
    max_h = max(sv_smooth_final) if sv_smooth_final else 1
    avg_h = sum(sv_smooth_final) / len(sv_smooth_final) if sv_smooth_final else 1
    
    results = []
    for i in range(n):
        results.append({
            'week_of_year': i + 1,
            'search_volume': search_volumes[i],
            'seasonality_index': sv_smooth_final[i] / max_h if max_h > 0 else 0,
            'seasonality_multiplier': sv_smooth_final[i] / avg_h if avg_h > 0 else 1
        })
    
    return results


# =============================================================================
# FORECAST 0-6 MONTH (backward compatibility)
# =============================================================================

def calculate_forecast_0_6m(
    units_data: List[Dict],
    seasonality: List[Dict],
    today: date = None,
    forecast_multiplier: float = 0.85
) -> Tuple[List[Dict], float]:
    """
    Calculate 0-6 month forecast using max week seasonality approach.
    """
    if today is None:
        today = date.today()
    
    if not units_data:
        return [], 0
    
    max_units = max(d.get('units', 0) or 0 for d in units_data)
    adjusted_max = max_units * forecast_multiplier
    
    seasonality_lookup = {s.get('week_of_year', 1): s.get('seasonality_index', 1.0) for s in seasonality}
    
    results = []
    for d in units_data:
        week_end = parse_date(d.get('week_end'))
        week_num = d.get('week_number', 1)
        week_of_year = week_num % 52 or 52
        season_idx = seasonality_lookup.get(week_of_year, 1.0)
        
        if week_end and week_end >= today:
            forecast = adjusted_max * season_idx
        else:
            forecast = d.get('units', 0) or 0
        
        results.append({
            **d,
            'week_end': week_end.isoformat() if week_end else None,
            'forecast_type': '0-6m',
            'seasonality_index': season_idx,
            'forecast_units': forecast
        })
    
    # Extend into future
    if units_data:
        last_week_end = parse_date(units_data[-1].get('week_end'))
        if last_week_end:
            for i in range(1, 53):
                future_week_end = last_week_end + timedelta(days=7 * i)
                week_of_year = future_week_end.isocalendar()[1]
                if week_of_year > 52:
                    week_of_year = 1
                season_idx = seasonality_lookup.get(week_of_year, 1.0)
                forecast = adjusted_max * season_idx
                
                results.append({
                    'week_end': future_week_end.isoformat(),
                    'forecast_type': '0-6m',
                    'seasonality_index': season_idx,
                    'forecast_units': forecast
                })
    
    return results, adjusted_max


# =============================================================================
# FORECAST 6-18 MONTH (backward compatibility - wrapper for new function)
# =============================================================================

def calculate_forecast_6_18m_legacy(
    units_data: List[Dict],
    seasonality: List[Dict],
    today: date = None,
    forecast_multiplier: float = 1.0
) -> Tuple[List[Dict], float]:
    """
    Legacy wrapper for backward compatibility.
    Use calculate_forecast_6_18m for new code.
    """
    if today is None:
        today = date.today()
    
    result = calculate_forecast_6_18m(units_data, seasonality, today, {})
    
    # Convert to legacy format
    results = []
    for f in result.get('forecasts', []):
        results.append({
            'week_end': f['week_end'],
            'forecast_type': '6-18m',
            'forecast_units': f['forecast'],
            'base_weekly_avg': result.get('F_constant', 0)
        })
    
    return results, result.get('F_constant', 0)