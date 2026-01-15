"""API routes for product forecasting application."""
from flask import Blueprint, jsonify, request
from app import db
from app.models import FBAInventory, AWDInventory, Product, UnitsSold, LabelInventory, VineClaims
from sqlalchemy import func

api_bp = Blueprint('api', __name__)


@api_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'Product Forecasting API'
    })


@api_bp.route('/stats', methods=['GET'])
def get_stats():
    """Get database statistics."""
    stats = {
        'fba_inventory_count': db.session.query(func.count(FBAInventory.id)).scalar(),
        'awd_inventory_count': db.session.query(func.count(AWDInventory.id)).scalar(),
        'products_count': db.session.query(func.count(Product.id)).scalar(),
        'units_sold_count': db.session.query(func.count(UnitsSold.id)).scalar(),
    }
    return jsonify(stats)


@api_bp.route('/products', methods=['GET'])
def get_products():
    """Get all products with pagination."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    pagination = Product.query.paginate(page=page, per_page=per_page, error_out=False)
    
    return jsonify({
        'products': [{
            'id': p.id,
            'asin': p.asin,
            'brand': p.brand,
            'product_name': p.product_name,
            'size': p.size
        } for p in pagination.items],
        'total': pagination.total,
        'pages': pagination.pages,
        'current_page': page
    })


@api_bp.route('/products/<asin>', methods=['GET'])
def get_product(asin):
    """Get product by ASIN."""
    product = Product.query.filter_by(asin=asin).first_or_404()
    return jsonify({
        'id': product.id,
        'asin': product.asin,
        'brand': product.brand,
        'product_name': product.product_name,
        'size': product.size
    })


@api_bp.route('/products/<asin>/sales', methods=['GET'])
def get_product_sales(asin):
    """Get sales history for a product."""
    sales = UnitsSold.query.filter_by(asin=asin).order_by(UnitsSold.week_date).all()
    
    return jsonify({
        'asin': asin,
        'sales': [{
            'week_date': s.week_date.isoformat(),
            'units': s.units
        } for s in sales],
        'total_units': sum(s.units for s in sales),
        'data_points': len(sales)
    })


@api_bp.route('/fba-inventory', methods=['GET'])
def get_fba_inventory():
    """Get FBA inventory with pagination."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    pagination = FBAInventory.query.paginate(page=page, per_page=per_page, error_out=False)
    
    return jsonify({
        'inventory': [{
            'id': inv.id,
            'sku': inv.sku,
            'asin': inv.asin,
            'product_name': inv.product_name,
            'available': inv.available,
            'days_of_supply': inv.days_of_supply,
            'units_shipped_t30': inv.units_shipped_t30,
            'snapshot_date': inv.snapshot_date.isoformat() if inv.snapshot_date else None
        } for inv in pagination.items],
        'total': pagination.total,
        'pages': pagination.pages,
        'current_page': page
    })


@api_bp.route('/fba-inventory/<asin>', methods=['GET'])
def get_fba_by_asin(asin):
    """Get FBA inventory for specific ASIN."""
    inventory = FBAInventory.query.filter_by(asin=asin).all()
    
    return jsonify({
        'asin': asin,
        'inventory': [{
            'id': inv.id,
            'sku': inv.sku,
            'available': inv.available,
            'days_of_supply': inv.days_of_supply,
            'units_shipped_t7': inv.units_shipped_t7,
            'units_shipped_t30': inv.units_shipped_t30,
            'units_shipped_t60': inv.units_shipped_t60,
            'units_shipped_t90': inv.units_shipped_t90,
            'inbound_quantity': inv.inbound_quantity,
            'supplier': inv.supplier,
            'snapshot_date': inv.snapshot_date.isoformat() if inv.snapshot_date else None
        } for inv in inventory],
        'count': len(inventory)
    })


@api_bp.route('/awd-inventory', methods=['GET'])
def get_awd_inventory():
    """Get AWD inventory with pagination."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    pagination = AWDInventory.query.paginate(page=page, per_page=per_page, error_out=False)
    
    return jsonify({
        'inventory': [{
            'id': inv.id,
            'sku': inv.sku,
            'asin': inv.asin,
            'product_name': inv.product_name,
            'available_in_awd_units': inv.available_in_awd_units,
            'available_in_fba_units': inv.available_in_fba_units,
            'days_of_supply': inv.days_of_supply
        } for inv in pagination.items],
        'total': pagination.total,
        'pages': pagination.pages,
        'current_page': page
    })


@api_bp.route('/awd-inventory/<asin>', methods=['GET'])
def get_awd_by_asin(asin):
    """Get AWD inventory for specific ASIN."""
    inventory = AWDInventory.query.filter_by(asin=asin).all()
    
    return jsonify({
        'asin': asin,
        'inventory': [{
            'id': inv.id,
            'sku': inv.sku,
            'available_in_awd_units': inv.available_in_awd_units,
            'available_in_awd_cases': inv.available_in_awd_cases,
            'inbound_to_awd_units': inv.inbound_to_awd_units,
            'outbound_to_fba_units': inv.outbound_to_fba_units,
            'available_in_fba_units': inv.available_in_fba_units,
            'days_of_supply': inv.days_of_supply
        } for inv in inventory],
        'count': len(inventory)
    })


# =====================================================
# STATIC FORECAST ROUTES (must be defined BEFORE dynamic routes)
# =====================================================

@api_bp.route('/forecast/all', methods=['GET'])
def get_all_forecasts():
    """
    Get forecast summary for ALL products - FAST with bulk loading + parallel processing.
    
    Returns: Brand, Product, Size, Units to Make for ALL products (no pagination).
    Sorted by DOI Total (ascending) - lowest inventory days first.
    
    Query params:
        - brand: Filter by brand name (optional)
        - sort: Sort field - 'doi' (default), 'units', 'product', 'fba'
        - order: 'asc' (default) or 'desc'
        - amazon_doi_goal: DOI goal in days (default: 93)
        - inbound_lead_time: Inbound lead time in days (default: 30)
        - manufacture_lead_time: Manufacturing lead time in days (default: 7)
        - market_adjustment: Market adjustment percentage (default: 0.05)
    """
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        calculate_forecast_6_18m as tps_6_18m,
        calculate_forecast_0_6m_exact as tps_0_6m,
        DEFAULT_SETTINGS
    )
    from app.models import Seasonality
    from datetime import date
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    start_time = time.time()
    
    # Load seasonality data once for 6-18m and 0-6m algorithms (include sv_smooth_env_97)
    seasonality_records = Seasonality.query.all()
    seasonality_data = [{
        'week_of_year': s.week_of_year, 
        'seasonality_index': s.seasonality_index,
        'sv_smooth_env_97': s.sv_smooth_env_97
    } for s in seasonality_records]
    
    # Query params
    brand_filter = request.args.get('brand', None)
    sort_by = request.args.get('sort', 'doi')
    order = request.args.get('order', 'asc')
    
    # DOI Settings - allow custom values from frontend
    amazon_doi_goal = request.args.get('amazon_doi_goal', type=int, default=93)
    inbound_lead_time = request.args.get('inbound_lead_time', type=int, default=30)
    manufacture_lead_time = request.args.get('manufacture_lead_time', type=int, default=7)
    market_adjustment = request.args.get('market_adjustment', type=float, default=0.05)
    
    # Build custom settings
    custom_settings = DEFAULT_SETTINGS.copy()
    custom_settings['amazon_doi_goal'] = amazon_doi_goal
    custom_settings['inbound_lead_time'] = inbound_lead_time
    custom_settings['manufacture_lead_time'] = manufacture_lead_time
    custom_settings['market_adjustment'] = market_adjustment
    
    # Calculate total required DOI
    total_required_doi = amazon_doi_goal + inbound_lead_time + manufacture_lead_time
    
    today = date.today()
    
    # === BULK LOAD ALL DATA UPFRONT (1 query each instead of N queries) ===
    
    # Get all products
    product_query = db.session.query(
        Product.asin, Product.brand, Product.product_name, Product.size
    )
    if brand_filter:
        product_query = product_query.filter(Product.brand.ilike(f'%{brand_filter}%'))
    products = {p.asin: p for p in product_query.all()}
    
    # Get first sale dates for all products (1 query)
    first_sales = dict(
        db.session.query(
            UnitsSold.asin,
            func.min(UnitsSold.week_date)
        ).filter(UnitsSold.units > 0).group_by(UnitsSold.asin).all()
    )
    
    # Get all sales data grouped by ASIN (1 query)
    all_sales = db.session.query(
        UnitsSold.asin, UnitsSold.week_date, UnitsSold.units
    ).order_by(UnitsSold.asin, UnitsSold.week_date).all()
    
    sales_by_asin = {}
    for sale in all_sales:
        if sale.asin not in sales_by_asin:
            sales_by_asin[sale.asin] = []
        sales_by_asin[sale.asin].append({'week_end': sale.week_date, 'units': sale.units})
    
    # Get FBA inventory totals (1 query)
    fba_totals = dict(
        db.session.query(
            FBAInventory.asin,
            func.coalesce(func.sum(FBAInventory.available), 0) + 
            func.coalesce(func.sum(FBAInventory.inbound_quantity), 0) +
            func.coalesce(func.sum(FBAInventory.total_reserved_quantity), 0)
        ).group_by(FBAInventory.asin).all()
    )
    
    fba_available = dict(
        db.session.query(
            FBAInventory.asin,
            func.coalesce(func.sum(FBAInventory.available), 0)
        ).group_by(FBAInventory.asin).all()
    )
    
    # Get AWD inventory totals (1 query)
    awd_totals = dict(
        db.session.query(
            AWDInventory.asin,
            func.coalesce(func.sum(AWDInventory.available_in_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.inbound_to_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.reserved_in_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.outbound_to_fba_units), 0)
        ).group_by(AWDInventory.asin).all()
    )
    
    # Get all vine claims (1 query)
    all_vine_claims = VineClaims.query.all()
    vine_claims_by_asin = {}
    for vc in all_vine_claims:
        if vc.asin not in vine_claims_by_asin:
            vine_claims_by_asin[vc.asin] = []
        vine_claims_by_asin[vc.asin].append({
            'claim_date': vc.claim_date,
            'units_claimed': vc.units_claimed
        })
    
    load_time = time.time() - start_time
    
    # === CALCULATE FORECASTS IN PARALLEL ===
    
    def calculate_single(asin):
        try:
            product = products.get(asin)
            if not product:
                return None
            
            first_sale = first_sales.get(asin)
            if not first_sale:
                return None
            
            units_data = sales_by_asin.get(asin, [])
            if len(units_data) < 4:
                return None
            
            # Get inventory
            total_inv = int(fba_totals.get(asin, 0) or 0) + int(awd_totals.get(asin, 0) or 0)
            fba_avail = int(fba_available.get(asin, 0) or 0)
            
            # Get vine claims for this ASIN
            vine_claims = vine_claims_by_asin.get(asin, [])
            
            # Settings - use custom settings from request params
            settings = custom_settings.copy()
            settings['total_inventory'] = total_inv
            settings['fba_available'] = fba_avail
            
            # Calculate age
            age_days = (today - first_sale).days
            age_months = age_days / 30.44
            algorithm = "18m+" if age_months >= 18 else ("6-18m" if age_months >= 6 else "0-6m")
            
            # Run appropriate algorithm based on product age
            if algorithm == "18m+":
                result = tps_18m(units_data, today, settings)
            elif algorithm == "6-18m":
                result = tps_6_18m(units_data, seasonality_data, today, settings, vine_claims)
            else:  # 0-6m
                result = tps_0_6m(units_data, seasonality_data, vine_claims, today, settings)
            
            return {
                'brand': product.brand or 'TPS Plant Foods',
                'product': product.product_name,
                'product_name': product.product_name,  # Alias for frontend compatibility
                'size': product.size,
                'asin': asin,
                'units_to_make': result['units_to_make'],
                'doi_total_days': round(result['doi_total_days'], 0),
                'doi_fba_days': round(result['doi_fba_days'], 0),
                'doi_total': round(result['doi_total_days'], 0),  # Alias
                'doi_fba': round(result['doi_fba_days'], 0),  # Alias
                'total_inventory': total_inv,
                'fba_available': fba_avail,
                'algorithm': algorithm,
                'age_months': round(age_months, 1)
            }
        except:
            return None
    
    # Run calculations in parallel (8 threads)
    forecasts = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(calculate_single, asin): asin for asin in products.keys()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                forecasts.append(result)
    
    calc_time = time.time() - start_time - load_time
    
    # Sort
    sort_key = {
        'doi': 'doi_total_days',
        'fba': 'doi_fba_days',
        'units': 'units_to_make',
        'product': 'product'
    }.get(sort_by, 'doi_total_days')
    
    reverse = (order == 'desc')
    forecasts.sort(key=lambda x: (x.get(sort_key) or 0) if sort_key != 'product' else (x.get(sort_key) or ''), reverse=reverse)
    
    total_time = time.time() - start_time
    
    return jsonify({
        'success': True,
        'products': forecasts,  # Frontend expects 'products' not 'forecasts'
        'forecasts': forecasts,  # Keep for backwards compatibility
        'count': len(forecasts),
        'total': len(forecasts),
        'sort': {'field': sort_by, 'order': order},
        'settings': {
            'amazon_doi_goal': amazon_doi_goal,
            'inbound_lead_time': inbound_lead_time,
            'manufacture_lead_time': manufacture_lead_time,
            'total_required_doi': total_required_doi,
            'market_adjustment': market_adjustment
        },
        'performance': {
            'data_load_seconds': round(load_time, 2),
            'calculation_seconds': round(calc_time, 2),
            'total_seconds': round(total_time, 2)
        }
    })


@api_bp.route('/forecast/refresh', methods=['POST'])
def refresh_forecast_cache():
    """
    Refresh the forecast cache for all products.
    
    This recalculates all forecasts and stores them in cache.
    Should be called periodically (e.g., daily) or after data updates.
    
    Note: This can take 1-2 minutes for 1000 products.
    """
    from app.services.cache_service import cache_service
    
    stats = cache_service.refresh_all_forecasts()
    
    return jsonify({
        'message': 'Cache refresh complete',
        'stats': stats
    })


# =====================================================
# DYNAMIC FORECAST ROUTES (with <asin> parameter)
# =====================================================

@api_bp.route('/forecast/<asin>', methods=['GET'])
def get_forecast_data(asin):
    """
    Get complete forecast for a product - matching Excel Settings page output.
    
    Query params (optional):
        - amazon_doi_goal: Days of inventory goal (default: 93)
        - inbound_lead_time: Shipping time in days (default: 30)
        - manufacture_lead_time: Production time in days (default: 7)
    
    Returns:
        - Product Info (ASIN, Product, Size)
        - Total Inventory
        - Production Forecast (Units to Make, DOI Total, DOI FBA Available)
        - Product Age (days, weeks, months, years)
        - Global Settings used
    """
    from app.services.forecast_service import forecast_service
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        calculate_forecast_6_18m as tps_6_18m,
        calculate_forecast_0_6m_exact as tps_0_6m,
        DEFAULT_SETTINGS
    )
    from datetime import date
    
    # Get product info
    product = Product.query.filter_by(asin=asin).first()
    if not product:
        return jsonify({'error': f'Product not found: {asin}'}), 404
    
    # Get product age
    first_sale = db.session.query(func.min(UnitsSold.week_date)).filter(
        UnitsSold.asin == asin, UnitsSold.units > 0
    ).scalar()
    
    if not first_sale:
        return jsonify({'error': 'No sales history for product'}), 404
    
    today = date.today()
    age_days = (today - first_sale).days
    age_weeks = age_days / 7
    age_months = age_days / 30.44
    age_years = age_days / 365.25
    
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
    
    # Get inventory levels (aggregated across SKUs)
    inventory = forecast_service.get_inventory_levels(asin)
    total_inventory = inventory.total_inventory
    fba_available = inventory.fba_available
    
    # Get custom DOI settings from query parameters (or use defaults)
    custom_amazon_doi_goal = request.args.get('amazon_doi_goal', type=int)
    custom_inbound_lead_time = request.args.get('inbound_lead_time', type=int)
    custom_manufacture_lead_time = request.args.get('manufacture_lead_time', type=int)
    
    # Settings
    settings = DEFAULT_SETTINGS.copy()
    settings['total_inventory'] = total_inventory
    settings['fba_available'] = fba_available
    
    # Apply custom DOI settings if provided
    if custom_amazon_doi_goal is not None:
        settings['amazon_doi_goal'] = custom_amazon_doi_goal
    if custom_inbound_lead_time is not None:
        settings['inbound_lead_time'] = custom_inbound_lead_time
    if custom_manufacture_lead_time is not None:
        settings['manufacture_lead_time'] = custom_manufacture_lead_time
    
    # Recalculate total_required_doi based on custom settings
    settings['total_required_doi'] = (
        settings['amazon_doi_goal'] +
        settings['inbound_lead_time'] +
        settings['manufacture_lead_time']
    )
    
    # Get vine claims for this ASIN
    vine_claims_records = VineClaims.query.filter_by(asin=asin).all()
    vine_claims = [{'claim_date': vc.claim_date, 'units_claimed': vc.units_claimed} for vc in vine_claims_records]
    
    # Run the appropriate algorithm
    if algorithm == "18m+":
        result = tps_18m(units_data, today, settings)
        units_to_make = result['units_to_make']
        doi_total = result['doi_total_days']
        doi_fba = result['doi_fba_days']
        velocity_adj = result.get('sales_velocity_adjustment', 0)
    elif algorithm == "6-18m":
        # Get seasonality data from database (include sv_smooth_env_97 for forecast calculation)
        from app.models import Seasonality
        seasonality = Seasonality.query.all()
        seasonality_data = [{
            'week_of_year': s.week_of_year, 
            'seasonality_index': s.seasonality_index,
            'sv_smooth_env_97': s.sv_smooth_env_97
        } for s in seasonality]
        result = tps_6_18m(units_data, seasonality_data, today, settings, vine_claims)
        units_to_make = result['units_to_make']
        doi_total = result['doi_total_days']
        doi_fba = result['doi_fba_days']
        velocity_adj = 0
    else:  # 0-6m
        from app.models import Seasonality
        seasonality = Seasonality.query.all()
        seasonality_data = [{'week_of_year': s.week_of_year, 'seasonality_index': s.seasonality_index} for s in seasonality]
        result = tps_0_6m(units_data, seasonality_data, vine_claims, today, settings)
        units_to_make = result['units_to_make']
        doi_total = result['doi_total_days']
        doi_fba = result['doi_fba_days']
        velocity_adj = 0
    
    # Build response matching Excel Settings page
    return jsonify({
        'product_info': {
            'child_asin': asin,
            'product': product.product_name,
            'size': product.size
        },
        'total_inventory': total_inventory,
        'production_forecast': {
            'algorithm': algorithm,
            'units_to_make': units_to_make,
            'doi_total_days': round(doi_total, 0),
            'doi_fba_available_days': round(doi_fba, 0)
        },
        'product_age': {
            'days': round(age_days, 0),
            'weeks': round(age_weeks, 0),
            'months': round(age_months, 1),
            'years': round(age_years, 1)
        },
        'global_settings': {
            'amazon_doi_goal': settings.get('amazon_doi_goal', 93),
            'inbound_lead_time': settings.get('inbound_lead_time', 30),
            'manufacture_lead_time': settings.get('manufacture_lead_time', 7),
            'total_lead_time_days': settings.get('inbound_lead_time', 30) + settings.get('manufacture_lead_time', 7),
            'total_doi_days_goal': settings.get('amazon_doi_goal', 93) + settings.get('inbound_lead_time', 30) + settings.get('manufacture_lead_time', 7),
            'total_doi_weeks_goal': round((settings.get('amazon_doi_goal', 93) + settings.get('inbound_lead_time', 30) + settings.get('manufacture_lead_time', 7)) / 7, 1)
        },
        'algorithm_settings': {
            'market_adjustment': f"{settings.get('market_adjustment', 0.05) * 100:.2f}%",
            'sales_velocity_adjustment': f"{velocity_adj * 100:.2f}%" if algorithm == "18m+" else "N/A",
            'sales_velocity_adjustment_weight': f"{settings.get('velocity_weight', 0.15) * 100:.0f}%" if algorithm == "18m+" else "N/A"
        }
    })


@api_bp.route('/forecast/<asin>/calculate', methods=['GET', 'POST'])
def calculate_forecast(asin):
    """
    Run the forecast algorithm for a product.
    
    Query params (GET) or JSON body (POST):
        - amazon_doi_goal: Days of inventory goal (default: 93)
        - inbound_lead_time: Shipping time in days (default: 30)
        - manufacture_lead_time: Production time in days (default: 7)
        - market_adjustment: Percentage adjustment (default: 0.05 = 5%)
        - sales_velocity_adj_weight: Weight of velocity adjustment (default: 0.15 = 15%)
        - force_algorithm: Force specific algorithm ('18m+', '6-18m', '0-6m')
    """
    from app.services.forecast_service import forecast_service
    from app.algorithms.forecast_18m_plus import ForecastSettings
    
    # Get parameters from request
    if request.method == 'POST':
        data = request.get_json() or {}
    else:
        data = request.args.to_dict()
    
    # Build settings
    settings = ForecastSettings(
        amazon_doi_goal=int(data.get('amazon_doi_goal', 93)),
        inbound_lead_time=int(data.get('inbound_lead_time', 30)),
        manufacture_lead_time=int(data.get('manufacture_lead_time', 7)),
        market_adjustment=float(data.get('market_adjustment', 0.05)),
        sales_velocity_adj_weight=float(data.get('sales_velocity_adj_weight', 0.15))
    )
    
    force_algorithm = data.get('force_algorithm')
    
    # Run forecast
    result = forecast_service.run_forecast(asin, settings, force_algorithm)
    
    return jsonify(result)


@api_bp.route('/forecast/<asin>/details', methods=['GET', 'POST'])
def get_forecast_details(asin):
    """
    Get detailed weekly forecast data for a product.
    
    Returns the full forecast dataframe for visualization/analysis.
    """
    from app.services.forecast_service import forecast_service
    from app.algorithms.forecast_18m_plus import ForecastSettings
    
    # Get parameters
    if request.method == 'POST':
        data = request.get_json() or {}
    else:
        data = request.args.to_dict()
    
    # Build settings
    settings = ForecastSettings(
        amazon_doi_goal=int(data.get('amazon_doi_goal', 93)),
        inbound_lead_time=int(data.get('inbound_lead_time', 30)),
        manufacture_lead_time=int(data.get('manufacture_lead_time', 7)),
        market_adjustment=float(data.get('market_adjustment', 0.05)),
        sales_velocity_adj_weight=float(data.get('sales_velocity_adj_weight', 0.15))
    )
    
    result = forecast_service.get_forecast_details(asin, settings)
    
    return jsonify(result)


@api_bp.route('/forecast/batch', methods=['POST'])
def batch_forecast():
    """
    Run forecast for multiple products at once.
    
    JSON body:
        {
            "asins": ["ASIN1", "ASIN2", ...],
            "settings": { ... optional settings ... }
        }
    """
    from app.services.forecast_service import forecast_service
    from app.algorithms.forecast_18m_plus import ForecastSettings
    
    data = request.get_json() or {}
    asins = data.get('asins', [])
    settings_data = data.get('settings', {})
    
    if not asins:
        return jsonify({'error': 'No ASINs provided'}), 400
    
    # Build settings
    settings = ForecastSettings(
        amazon_doi_goal=int(settings_data.get('amazon_doi_goal', 93)),
        inbound_lead_time=int(settings_data.get('inbound_lead_time', 30)),
        manufacture_lead_time=int(settings_data.get('manufacture_lead_time', 7)),
        market_adjustment=float(settings_data.get('market_adjustment', 0.05)),
        sales_velocity_adj_weight=float(settings_data.get('sales_velocity_adj_weight', 0.15))
    )
    
    results = []
    for asin in asins:
        result = forecast_service.run_forecast(asin, settings)
        results.append(result)
    
    return jsonify({
        'total': len(results),
        'results': results
    })


@api_bp.route('/forecast/<asin>/chart', methods=['GET'])
def get_forecast_chart(asin):
    """
    Get complete forecast data for chart visualization.
    
    Returns:
        - Product info (name, size, ASIN, brand)
        - FBA inventory breakdown (total, available, reserved, inbound)
        - AWD inventory breakdown (total, outbound, available, reserved)
        - Label inventory
        - DOI metrics (FBA days, Total days, goal date)
        - Units to Make
        - Historical data with smoothed values
        - Forecast data with adjusted values
    """
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        calculate_forecast_6_18m as tps_6_18m,
        calculate_forecast_0_6m_exact as tps_0_6m,
        DEFAULT_SETTINGS
    )
    from app.models import Seasonality, VineClaims
    from datetime import date, timedelta, datetime
    
    today = date.today()
    
    # Get product info
    product = Product.query.filter_by(asin=asin).first()
    if not product:
        return jsonify({'error': f'Product not found: {asin}'}), 404
    
    # Get product age
    first_sale = db.session.query(func.min(UnitsSold.week_date)).filter(
        UnitsSold.asin == asin, UnitsSold.units > 0
    ).scalar()
    
    if not first_sale:
        return jsonify({'error': 'No sales history for product'}), 404
    
    age_days = (today - first_sale).days
    age_months = age_days / 30.44
    
    # Determine algorithm
    if age_months >= 18:
        algorithm = "18m+"
    elif age_months >= 6:
        algorithm = "6-18m"
    else:
        algorithm = "0-6m"
    
    # Get seasonality data (for 6-18m and 0-6m)
    seasonality = Seasonality.query.all()
    seasonality_data = [{
        'week_of_year': s.week_of_year, 
        'seasonality_index': s.seasonality_index,
        'sv_smooth_env_97': s.sv_smooth_env_97
    } for s in seasonality]
    
    # Get vine claims
    vine_records = VineClaims.query.filter_by(asin=asin).all()
    vine_claims = [{'claim_date': v.claim_date, 'units_claimed': v.units_claimed} for v in vine_records]
    
    # Get sales data
    sales = UnitsSold.query.filter_by(asin=asin).order_by(UnitsSold.week_date).all()
    units_data = [{'week_end': s.week_date, 'units': s.units} for s in sales]
    
    # Get FBA inventory (detailed)
    fba_records = FBAInventory.query.filter_by(asin=asin).all()
    fba_total = sum(f.available or 0 for f in fba_records) + \
                sum(f.inbound_quantity or 0 for f in fba_records) + \
                sum(f.total_reserved_quantity or 0 for f in fba_records)
    fba_available = sum(f.available or 0 for f in fba_records)
    fba_reserved = sum(f.total_reserved_quantity or 0 for f in fba_records)
    fba_inbound = sum(f.inbound_quantity or 0 for f in fba_records)
    
    # Get AWD inventory (detailed)
    awd_records = AWDInventory.query.filter_by(asin=asin).all()
    awd_total = sum(a.available_in_awd_units or 0 for a in awd_records) + \
                sum(a.inbound_to_awd_units or 0 for a in awd_records) + \
                sum(a.reserved_in_awd_units or 0 for a in awd_records) + \
                sum(a.outbound_to_fba_units or 0 for a in awd_records)
    awd_available = sum(a.available_in_awd_units or 0 for a in awd_records)
    awd_outbound = sum(a.outbound_to_fba_units or 0 for a in awd_records)
    awd_reserved = sum(a.reserved_in_awd_units or 0 for a in awd_records)
    
    # Get label inventory (with error handling in case table doesn't exist)
    try:
        label = LabelInventory.query.filter_by(asin=asin).first()
        label_inventory = label.label_inventory if label else 0
        label_id = label.label_id if label else None
        label_status = label.label_status if label else None
    except Exception as e:
        print(f"Warning: Could not query label inventory: {e}")
        label_inventory = 0
        label_id = None
        label_status = None
    
    # Calculate total inventory
    total_inventory = fba_total + awd_total
    
    # Settings
    settings = DEFAULT_SETTINGS.copy()
    settings['total_inventory'] = total_inventory
    settings['fba_available'] = fba_available
    
    # Run the appropriate algorithm based on product age
    if algorithm == "18m+":
        result = tps_18m(units_data, today, settings)
        velocity_adj = result.get('sales_velocity_adjustment', 0)
    elif algorithm == "6-18m":
        result = tps_6_18m(units_data, seasonality_data, today, settings, vine_claims)
        velocity_adj = 0
    else:  # 0-6m
        result = tps_0_6m(units_data, seasonality_data, vine_claims, today, settings)
        velocity_adj = 0
    
    units_to_make = result['units_to_make']
    doi_total = result.get('doi_total_days', 0)
    doi_fba = result.get('doi_fba_days', 0)
    
    # Calculate DOI goal date
    doi_goal = settings.get('amazon_doi_goal', 93) + settings.get('inbound_lead_time', 30) + settings.get('manufacture_lead_time', 7)
    doi_goal_date = today + timedelta(days=doi_goal)
    
    # Build algorithm-specific chart data
    units_list = [s['units'] for s in units_data]
    algorithm_forecasts = result.get('forecasts', [])
    
    # Get weekly average for fallback
    weekly_forecast_avg = 0
    if algorithm_forecasts:
        forecast_values = [f.get('forecast', 0) for f in algorithm_forecasts if f.get('forecast', 0) > 0]
        weekly_forecast_avg = sum(forecast_values) / len(forecast_values) if forecast_values else 0
    elif len(units_list) > 0:
        recent_weeks = units_list[-13:] if len(units_list) >= 13 else units_list
        weekly_forecast_avg = sum(recent_weeks) / len(recent_weeks) if recent_weeks else 0
    
    market_adj = settings.get('market_adjustment', 0.05)
    
    # Build seasonality lookup for chart series
    seasonality_lookup = {s['week_of_year']: s['seasonality_index'] for s in seasonality_data}
    sv_smooth_lookup = {s['week_of_year']: s.get('sv_smooth_env_97', 3000) for s in seasonality_data}
    
    # Build vine claims lookup by (year, week)
    vine_lookup = {}
    for vc in vine_claims:
        claim_date = vc.get('claim_date')
        if claim_date:
            iso_cal = claim_date.isocalendar()
            key = (iso_cal[0], iso_cal[1])
            vine_lookup[key] = vine_lookup.get(key, 0) + (vc.get('units_claimed', 0) or 0)
    
    # Build historical data - different series based on algorithm
    historical = []
    
    if algorithm == "18m+":
        # 18m+: units_sold, units_sold_smoothed, forecast (historical portion), prior_year_smoothed
        for i, sale in enumerate(units_data):
            week_end = sale['week_end']
            week_end_str = week_end.isoformat() if hasattr(week_end, 'isoformat') else str(week_end)
            
            # 5-week smoothing
            start_idx = max(0, i - 2)
            end_idx = min(len(units_list), i + 3)
            window = units_list[start_idx:end_idx]
            units_smooth = sum(window) / len(window) if window else 0
            
            # Prior year (52 weeks back)
            prior_idx = i - 52
            prior_year_smooth = 0
            if prior_idx >= 0:
                py_start = max(0, prior_idx - 2)
                py_end = min(len(units_list), prior_idx + 3)
                py_window = units_list[py_start:py_end]
                prior_year_smooth = sum(py_window) / len(py_window) if py_window else 0
            
            historical.append({
                'week_end': week_end_str,
                'units_sold': sale['units'],
                'units_sold_smoothed': round(units_smooth, 1),
                'prior_year_smoothed': round(prior_year_smooth, 1)
            })
    
    elif algorithm == "6-18m":
        # 6-18m: units_sold, units_sold_potential, forecast
        # Calculate F_constant (peak CVR) from result
        F_constant = result.get('F_constant', 0.005)
        
        for i, sale in enumerate(units_data):
            week_end = sale['week_end']
            week_end_str = week_end.isoformat() if hasattr(week_end, 'isoformat') else str(week_end)
            week_of_year = week_end.isocalendar()[1] if hasattr(week_end, 'isocalendar') else 1
            
            # Adjusted units (vine claims subtracted)
            iso_cal = week_end.isocalendar() if hasattr(week_end, 'isocalendar') else (2025, 1, 1)
            vine_key = (iso_cal[0], iso_cal[1])
            vine = vine_lookup.get(vine_key, 0)
            adj_units = max(0, sale['units'] - vine)
            
            # Units sold potential (Column I in Excel: D × H)
            sv_97 = sv_smooth_lookup.get(week_of_year, 3000)
            seasonality_idx = seasonality_lookup.get(week_of_year, 1.0)
            H = F_constant * (1 + 0.25 * (seasonality_idx - 1))
            units_potential = sv_97 * H
            
            historical.append({
                'week_end': week_end_str,
                'units_sold': adj_units,
                'units_sold_potential': round(units_potential, 1),
                'forecast': round(units_potential, 1) if week_end > today else 0
            })
    
    else:  # 0-6m
        # 0-6m: adj_units_sold, max_week_seasonality_index_applied
        F_peak = result.get('F_peak', max(units_list) if units_list else 0)
        
        # Find current seasonality index (last historical week)
        last_week = units_data[-1]['week_end'] if units_data else today
        last_week_of_year = last_week.isocalendar()[1] if hasattr(last_week, 'isocalendar') else 1
        current_seasonality = seasonality_lookup.get(last_week_of_year, 1.0)
        
        for i, sale in enumerate(units_data):
            week_end = sale['week_end']
            week_end_str = week_end.isoformat() if hasattr(week_end, 'isoformat') else str(week_end)
            week_of_year = week_end.isocalendar()[1] if hasattr(week_end, 'isocalendar') else 1
            
            # Adjusted units (vine claims subtracted)
            iso_cal = week_end.isocalendar() if hasattr(week_end, 'isocalendar') else (2025, 1, 1)
            vine_key = (iso_cal[0], iso_cal[1])
            vine = vine_lookup.get(vine_key, 0)
            adj_units = max(0, sale['units'] - vine)
            
            # max_week_seasonality_index_applied (Column H in Excel)
            seasonality_idx = seasonality_lookup.get(week_of_year, 1.0)
            if current_seasonality > 0:
                max_week_seasonality = F_peak * pow(seasonality_idx / current_seasonality, 0.65)
            else:
                max_week_seasonality = F_peak
            
            historical.append({
                'week_end': week_end_str,
                'adj_units_sold': adj_units,
                'max_week_seasonality_index_applied': round(max_week_seasonality, 1)
            })
    
    # Build forecast data
    forecast_weeks = []
    if algorithm_forecasts:
        for forecast_item in algorithm_forecasts:
            week_end = forecast_item.get('week_end')
            forecast_val = forecast_item.get('forecast', 0)
            forecast_weeks.append({
                'week_end': week_end,
                'forecast': round(forecast_val, 1)
            })
    
    # Extend forecast if needed
    last_date = units_data[-1]['week_end'] if units_data else today
    last_forecast_date = last_date
    if algorithm_forecasts and len(algorithm_forecasts) > 0:
        last_week_end = algorithm_forecasts[-1].get('week_end')
        if last_week_end:
            if isinstance(last_week_end, str):
                try:
                    last_forecast_date = date.fromisoformat(last_week_end)
                except:
                    last_forecast_date = last_date
            elif hasattr(last_week_end, 'isoformat'):
                last_forecast_date = last_week_end
    
    base_forecast = weekly_forecast_avg * (1 + market_adj)
    existing_forecast_count = len(forecast_weeks)
    while len(forecast_weeks) < 104:
        weeks_to_add = len(forecast_weeks) - existing_forecast_count + 1
        next_date = last_forecast_date + timedelta(weeks=weeks_to_add)
        forecast_weeks.append({
            'week_end': next_date.isoformat(),
            'forecast': round(base_forecast, 1)
        })
    
    # Calculate labels needed
    labels_needed = max(0, units_to_make - label_inventory)
    labels_have_enough = label_inventory >= units_to_make
    
    # Define chart series based on algorithm
    if algorithm == "18m+":
        chart_series = {
            'historical': ['units_sold', 'units_sold_smoothed', 'prior_year_smoothed'],
            'forecast': ['forecast'],
            'legend': {
                'units_sold': 'Units Sold',
                'units_sold_smoothed': 'Units Sold Smoothed',
                'prior_year_smoothed': 'Prior Year Smoothed',
                'forecast': 'Forecast'
            }
        }
    elif algorithm == "6-18m":
        chart_series = {
            'historical': ['units_sold', 'units_sold_potential'],
            'forecast': ['forecast'],
            'legend': {
                'units_sold': 'Units Sold',
                'units_sold_potential': 'Units Sold Potential',
                'forecast': 'Forecast'
            }
        }
    else:  # 0-6m
        chart_series = {
            'historical': ['adj_units_sold', 'max_week_seasonality_index_applied'],
            'forecast': ['forecast'],
            'legend': {
                'adj_units_sold': 'Adj. Units Sold',
                'max_week_seasonality_index_applied': 'Max Week Seasonality Applied',
                'forecast': 'Forecast'
            }
        }
    
    return jsonify({
        'success': True,
        'asin': asin,
        'algorithm': algorithm,
        'chart_series': chart_series,
        'product': {
            'name': product.product_name,
            'size': product.size,
            'brand': product.brand or 'TPS Plant Foods'
        },
        'inventory': {
            'fba': {
                'total': fba_total,
                'available': fba_available,
                'reserved': fba_reserved,
                'inbound': fba_inbound
            },
            'awd': {
                'total': awd_total,
                'outbound': awd_outbound,
                'available': awd_available,
                'reserved': awd_reserved
            }
        },
        'labels': {
            'inventory': label_inventory,
            'needed': labels_needed,
            'have_enough': labels_have_enough,
            'label_id': label_id,
            'status': label_status
        },
        'add_units': units_to_make,  # Units to add to production
        'doi': {
            'fba_days': round(doi_fba, 0),
            'total_days': round(doi_total, 0),
            'goal_days': doi_goal,
            'goal_date': doi_goal_date.isoformat()
        },
        'units_to_make': units_to_make,
        'historical': historical,
        'forecast': forecast_weeks,
        'metadata': {
            'today': today.isoformat(),
            'weeks_historical': len(historical),
            'weeks_forecast': len(forecast_weeks),
            'weekly_avg_forecast': round(weekly_forecast_avg, 1),
            'velocity_adjustment': round(velocity_adj, 4),
            'market_adjustment': market_adj
        }
    })


@api_bp.route('/forecast/<asin>/tps', methods=['GET', 'POST'])
def calculate_forecast_tps(asin):
    """
    Run the TPS algorithm (exact Excel replication) for a product.
    
    This is the proven algorithm that matches Excel formulas cell-for-cell.
    
    Query params (GET) or JSON body (POST):
        - amazon_doi_goal: Days of inventory goal (default: 93)
        - inbound_lead_time: Shipping time in days (default: 30)
        - manufacture_lead_time: Production time in days (default: 7)
        - market_adjustment: Percentage as decimal (default: 0.05)
        - sales_velocity_adjustment: Percentage as decimal (default: 0.10)
        - velocity_weight: Weight as decimal (default: 0.15)
    """
    from app.services.forecast_service import forecast_service
    
    # Get parameters from request
    if request.method == 'POST':
        data = request.get_json() or {}
    else:
        data = request.args.to_dict()
    
    # Build custom settings if provided
    custom_settings = {}
    if 'amazon_doi_goal' in data:
        custom_settings['amazon_doi_goal'] = int(data['amazon_doi_goal'])
    if 'inbound_lead_time' in data:
        custom_settings['inbound_lead_time'] = int(data['inbound_lead_time'])
    if 'manufacture_lead_time' in data:
        custom_settings['manufacture_lead_time'] = int(data['manufacture_lead_time'])
    if 'market_adjustment' in data:
        custom_settings['market_adjustment'] = float(data['market_adjustment'])
    if 'sales_velocity_adjustment' in data:
        custom_settings['sales_velocity_adjustment'] = float(data['sales_velocity_adjustment'])
    if 'velocity_weight' in data:
        custom_settings['velocity_weight'] = float(data['velocity_weight'])
    
    # Run TPS forecast
    result = forecast_service.run_forecast_tps(asin, custom_settings if custom_settings else None)
    
    return jsonify(result)


# =====================================================
# LABEL INVENTORY ROUTES
# =====================================================

@api_bp.route('/labels', methods=['GET'])
def get_label_inventory():
    """
    Get label inventory for all products.
    
    Query params:
        - sort: Sort field - 'inventory' (default), 'product', 'needed'
        - order: 'asc' (default) or 'desc'
    """
    try:
        sort_by = request.args.get('sort', 'inventory')
        order = request.args.get('order', 'asc')
        
        labels = LabelInventory.query.all()
        
        results = [{
            'asin': l.asin,
            'product_name': l.product_name,
            'size': l.size,
            'label_id': l.label_id,
            'label_status': l.label_status,
            'label_inventory': l.label_inventory
        } for l in labels]
    except Exception as e:
        return jsonify({
            'error': f'Label inventory table not available: {str(e)}',
            'labels': [],
            'total': 0,
            'total_labels_in_stock': 0
        }), 200  # Return 200 with empty data instead of 500
    
    # Sort
    sort_key = {
        'inventory': 'label_inventory',
        'product': 'product_name'
    }.get(sort_by, 'label_inventory')
    
    reverse = (order == 'desc')
    results.sort(key=lambda x: (x.get(sort_key) or 0) if sort_key != 'product_name' else (x.get(sort_key) or ''), reverse=reverse)
    
    return jsonify({
        'labels': results,
        'total': len(results),
        'total_labels_in_stock': sum(l.label_inventory for l in labels)
    })


@api_bp.route('/labels/needed', methods=['GET'])
def get_labels_needed():
    """
    Get labels needed based on forecast - combines forecast with label inventory.
    
    For each product:
    - labels_needed = units_to_make - label_inventory
    - If negative, means we have enough labels
    
    Query params:
        - sort: Sort field - 'needed' (default), 'product', 'inventory'
        - order: 'desc' (default) or 'asc'
    """
    from app.services.forecast_service import forecast_service
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        DEFAULT_SETTINGS
    )
    from datetime import date
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    start_time = time.time()
    
    sort_by = request.args.get('sort', 'needed')
    order = request.args.get('order', 'desc')
    
    today = date.today()
    
    # Get all label inventory
    labels = {l.asin: l for l in LabelInventory.query.all()}
    
    # Get all products that have labels
    products = {p.asin: p for p in Product.query.filter(Product.asin.in_(labels.keys())).all()}
    
    # Bulk load forecast data (same as /forecast/all)
    first_sales = dict(
        db.session.query(
            UnitsSold.asin,
            func.min(UnitsSold.week_date)
        ).filter(UnitsSold.units > 0).group_by(UnitsSold.asin).all()
    )
    
    all_sales = db.session.query(
        UnitsSold.asin, UnitsSold.week_date, UnitsSold.units
    ).order_by(UnitsSold.asin, UnitsSold.week_date).all()
    
    sales_by_asin = {}
    for sale in all_sales:
        if sale.asin not in sales_by_asin:
            sales_by_asin[sale.asin] = []
        sales_by_asin[sale.asin].append({'week_end': sale.week_date, 'units': sale.units})
    
    # Inventory totals
    fba_totals = dict(
        db.session.query(
            FBAInventory.asin,
            func.coalesce(func.sum(FBAInventory.available), 0) + 
            func.coalesce(func.sum(FBAInventory.inbound_quantity), 0) +
            func.coalesce(func.sum(FBAInventory.total_reserved_quantity), 0)
        ).group_by(FBAInventory.asin).all()
    )
    
    fba_available = dict(
        db.session.query(
            FBAInventory.asin,
            func.coalesce(func.sum(FBAInventory.available), 0)
        ).group_by(FBAInventory.asin).all()
    )
    
    awd_totals = dict(
        db.session.query(
            AWDInventory.asin,
            func.coalesce(func.sum(AWDInventory.available_in_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.inbound_to_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.reserved_in_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.outbound_to_fba_units), 0)
        ).group_by(AWDInventory.asin).all()
    )
    
    def calculate_single(asin):
        try:
            label = labels.get(asin)
            product = products.get(asin)
            
            if not label or not product:
                return None
            
            first_sale = first_sales.get(asin)
            if not first_sale:
                return {
                    'asin': asin,
                    'product_name': label.product_name,
                    'size': label.size,
                    'label_id': label.label_id,
                    'label_inventory': label.label_inventory,
                    'units_to_make': 0,
                    'labels_needed': 0,
                    'status': 'No forecast data'
                }
            
            units_data = sales_by_asin.get(asin, [])
            if len(units_data) < 4:
                return {
                    'asin': asin,
                    'product_name': label.product_name,
                    'size': label.size,
                    'label_id': label.label_id,
                    'label_inventory': label.label_inventory,
                    'units_to_make': 0,
                    'labels_needed': 0,
                    'status': 'Insufficient sales data'
                }
            
            # Get inventory
            total_inv = int(fba_totals.get(asin, 0) or 0) + int(awd_totals.get(asin, 0) or 0)
            fba_avail = int(fba_available.get(asin, 0) or 0)
            
            # Run forecast
            settings = DEFAULT_SETTINGS.copy()
            settings['total_inventory'] = total_inv
            settings['fba_available'] = fba_avail
            
            result = tps_18m(units_data, today, settings)
            units_to_make = result['units_to_make']
            
            # Calculate labels needed
            labels_needed = max(0, units_to_make - label.label_inventory)
            
            return {
                'asin': asin,
                'product_name': label.product_name,
                'size': label.size,
                'label_id': label.label_id,
                'label_inventory': label.label_inventory,
                'units_to_make': units_to_make,
                'labels_needed': labels_needed,
                'status': 'Need labels' if labels_needed > 0 else 'Have enough'
            }
        except:
            return None
    
    # Parallel calculation
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(calculate_single, asin): asin for asin in labels.keys()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    
    # Sort
    sort_key = {
        'needed': 'labels_needed',
        'inventory': 'label_inventory',
        'product': 'product_name',
        'units': 'units_to_make'
    }.get(sort_by, 'labels_needed')
    
    reverse = (order == 'desc')
    results.sort(key=lambda x: (x.get(sort_key) or 0) if sort_key != 'product_name' else (x.get(sort_key) or ''), reverse=reverse)
    
    total_time = time.time() - start_time
    
    # Summary stats
    total_labels_needed = sum(r['labels_needed'] for r in results)
    products_needing_labels = len([r for r in results if r['labels_needed'] > 0])
    
    return jsonify({
        'labels': results,
        'total_products': len(results),
        'summary': {
            'total_labels_needed': total_labels_needed,
            'products_needing_labels': products_needing_labels,
            'products_with_enough': len(results) - products_needing_labels
        },
        'performance': {
            'total_seconds': round(total_time, 2)
        }
    })


@api_bp.route('/labels/schedule', methods=['GET'])
def get_labels_schedule():
    """
    Get label production schedule - grouped by label_id with DOI-based timing.
    
    This shows:
    - Labels grouped by label_id (one label design may be used for multiple products)
    - When labels are needed based on DOI (days of inventory)
    - Aggregated quantities per label design
    
    Response format:
    LABEL STATUS | BRAND | PRODUCT | SIZE | ADD | QTY | DOI | NEEDED BY
    """
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        DEFAULT_SETTINGS
    )
    from datetime import date, timedelta
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    start_time = time.time()
    today = date.today()
    
    # Get all label inventory
    labels_list = LabelInventory.query.all()
    labels = {l.asin: l for l in labels_list}
    
    # Get all products that have labels
    products = {p.asin: p for p in Product.query.filter(Product.asin.in_(labels.keys())).all()}
    
    # Bulk load data
    first_sales = dict(
        db.session.query(
            UnitsSold.asin,
            func.min(UnitsSold.week_date)
        ).filter(UnitsSold.units > 0).group_by(UnitsSold.asin).all()
    )
    
    all_sales = db.session.query(
        UnitsSold.asin, UnitsSold.week_date, UnitsSold.units
    ).order_by(UnitsSold.asin, UnitsSold.week_date).all()
    
    sales_by_asin = {}
    for sale in all_sales:
        if sale.asin not in sales_by_asin:
            sales_by_asin[sale.asin] = []
        sales_by_asin[sale.asin].append({'week_end': sale.week_date, 'units': sale.units})
    
    fba_totals = dict(
        db.session.query(
            FBAInventory.asin,
            func.coalesce(func.sum(FBAInventory.available), 0) + 
            func.coalesce(func.sum(FBAInventory.inbound_quantity), 0) +
            func.coalesce(func.sum(FBAInventory.total_reserved_quantity), 0)
        ).group_by(FBAInventory.asin).all()
    )
    
    fba_available = dict(
        db.session.query(
            FBAInventory.asin,
            func.coalesce(func.sum(FBAInventory.available), 0)
        ).group_by(FBAInventory.asin).all()
    )
    
    awd_totals = dict(
        db.session.query(
            AWDInventory.asin,
            func.coalesce(func.sum(AWDInventory.available_in_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.inbound_to_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.reserved_in_awd_units), 0) +
            func.coalesce(func.sum(AWDInventory.outbound_to_fba_units), 0)
        ).group_by(AWDInventory.asin).all()
    )
    
    def calculate_single_with_doi(asin):
        """Calculate forecast with DOI for timing."""
        try:
            label = labels.get(asin)
            product = products.get(asin)
            
            if not label or not product:
                return None
            
            first_sale = first_sales.get(asin)
            units_data = sales_by_asin.get(asin, [])
            
            units_to_make = 0
            doi_total = 0
            doi_fba = 0
            
            if first_sale and len(units_data) >= 4:
                total_inv = int(fba_totals.get(asin, 0) or 0) + int(awd_totals.get(asin, 0) or 0)
                fba_avail = int(fba_available.get(asin, 0) or 0)
                
                settings = DEFAULT_SETTINGS.copy()
                settings['total_inventory'] = total_inv
                settings['fba_available'] = fba_avail
                
                result = tps_18m(units_data, today, settings)
                units_to_make = result['units_to_make']
                doi_total = result.get('doi_total_days', 0)
                doi_fba = result.get('doi_fba_days', 0)
            
            # Calculate when labels are needed based on DOI
            # Labels needed by = today + DOI - lead_time (37 days default)
            lead_time = 37  # manufacture + inbound lead time
            stockout_date = today + timedelta(days=int(doi_total)) if doi_total > 0 else today
            labels_needed_by = stockout_date - timedelta(days=lead_time)
            if labels_needed_by < today:
                labels_needed_by = today  # Already overdue
            
            return {
                'asin': asin,
                'label_id': label.label_id,
                'label_status': label.label_status,
                'brand': product.brand or 'TPS Plant Foods',
                'product_name': label.product_name,
                'size': label.size,
                'label_inventory': label.label_inventory,
                'units_to_make': units_to_make,
                'doi_total': round(doi_total, 0),
                'doi_fba': round(doi_fba, 0),
                'stockout_date': stockout_date.isoformat(),
                'labels_needed_by': labels_needed_by.isoformat()
            }
        except Exception as e:
            return None
    
    # Parallel calculation
    product_results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(calculate_single_with_doi, asin): asin for asin in labels.keys()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                product_results.append(result)
    
    # Group by label_id and aggregate
    label_groups = defaultdict(lambda: {
        'label_id': '',
        'label_status': '',
        'brands': set(),
        'products': [],
        'sizes': set(),
        'total_label_inventory': 0,
        'total_units_to_make': 0,
        'min_doi': float('inf'),
        'earliest_needed_by': None
    })
    
    for pr in product_results:
        lid = pr['label_id']
        group = label_groups[lid]
        
        group['label_id'] = lid
        group['label_status'] = pr['label_status']
        group['brands'].add(pr['brand'])
        group['products'].append({
            'asin': pr['asin'],
            'name': pr['product_name'],
            'size': pr['size'],
            'units_to_make': pr['units_to_make'],
            'doi': pr['doi_total'],
            'needed_by': pr['labels_needed_by']
        })
        group['sizes'].add(pr['size'])
        group['total_label_inventory'] += pr['label_inventory']
        group['total_units_to_make'] += pr['units_to_make']
        
        # Track minimum DOI (most urgent)
        if pr['doi_total'] < group['min_doi']:
            group['min_doi'] = pr['doi_total']
        
        # Track earliest needed_by date
        needed_by = date.fromisoformat(pr['labels_needed_by'])
        if group['earliest_needed_by'] is None or needed_by < group['earliest_needed_by']:
            group['earliest_needed_by'] = needed_by
    
    # Convert to list
    results = []
    for lid, group in label_groups.items():
        labels_needed = max(0, group['total_units_to_make'] - group['total_label_inventory'])
        min_doi = group['min_doi'] if group['min_doi'] != float('inf') else 0
        
        results.append({
            'label_id': lid,
            'label_status': group['label_status'],
            'brand': ', '.join(sorted(group['brands'])) if group['brands'] else 'TPS Plant Foods',
            'products': group['products'],
            'product_names': ', '.join([p['name'] for p in group['products'][:3]]) + ('...' if len(group['products']) > 3 else ''),
            'sizes': ', '.join(sorted(group['sizes'])),
            'add': 0,  # Placeholder for UI "ADD" column
            'qty': group['total_units_to_make'],
            'label_inventory': group['total_label_inventory'],
            'labels_needed': labels_needed,
            'doi': int(min_doi),
            'needed_by': group['earliest_needed_by'].isoformat() if group['earliest_needed_by'] else None
        })
    
    # Sort by DOI ascending (most urgent first - lowest DOI)
    results.sort(key=lambda x: (x['doi'], -x['labels_needed']))
    
    total_time = time.time() - start_time
    
    return jsonify({
        'labels': results,
        'total_label_designs': len(results),
        'total_products': len(product_results),
        'summary': {
            'total_labels_needed': sum(r['labels_needed'] for r in results),
            'labels_needing_production': len([r for r in results if r['labels_needed'] > 0]),
            'total_label_inventory': sum(r['label_inventory'] for r in results),
            'urgent_count': len([r for r in results if r['doi'] < 30 and r['labels_needed'] > 0])
        },
        'performance': {
            'total_seconds': round(total_time, 2)
        }
    })
