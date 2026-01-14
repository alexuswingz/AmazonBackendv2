"""API routes for product forecasting application."""
from flask import Blueprint, jsonify, request
from app import db
from app.models import FBAInventory, AWDInventory, Product, UnitsSold
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
    """
    from app.algorithms.algorithms_tps import (
        calculate_forecast_18m_plus as tps_18m,
        DEFAULT_SETTINGS
    )
    from datetime import date
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    start_time = time.time()
    
    brand_filter = request.args.get('brand', None)
    sort_by = request.args.get('sort', 'doi')
    order = request.args.get('order', 'asc')
    
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
            
            # Settings
            settings = DEFAULT_SETTINGS.copy()
            settings['total_inventory'] = total_inv
            settings['fba_available'] = fba_avail
            
            # Calculate age
            age_days = (today - first_sale).days
            age_months = age_days / 30.44
            algorithm = "18m+" if age_months >= 18 else ("6-18m" if age_months >= 6 else "0-6m")
            
            # Run algorithm
            result = tps_18m(units_data, today, settings)
            
            return {
                'brand': product.brand or 'TPS Plant Foods',
                'product': product.product_name,
                'size': product.size,
                'asin': asin,
                'units_to_make': result['units_to_make'],
                'doi_total_days': round(result['doi_total_days'], 0),
                'doi_fba_days': round(result['doi_fba_days'], 0),
                'algorithm': algorithm
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
        'forecasts': forecasts,
        'total': len(forecasts),
        'sort': {'field': sort_by, 'order': order},
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
    
    # Settings
    settings = DEFAULT_SETTINGS.copy()
    settings['total_inventory'] = total_inventory
    settings['fba_available'] = fba_available
    
    # Run the appropriate algorithm
    if algorithm == "18m+":
        result = tps_18m(units_data, today, settings)
        units_to_make = result['units_to_make']
        doi_total = result['doi_total_days']
        doi_fba = result['doi_fba_days']
        velocity_adj = result.get('sales_velocity_adjustment', 0)
    elif algorithm == "6-18m":
        # Get seasonality data from database
        from app.models import Seasonality
        seasonality = Seasonality.query.all()
        seasonality_data = [{'week_of_year': s.week_of_year, 'seasonality_index': s.seasonality_index} for s in seasonality]
        result = tps_6_18m(units_data, today, settings, seasonality_data)
        units_to_make = result['units_to_make']
        doi_total = result['doi_total_days']
        doi_fba = result['doi_fba_days']
        velocity_adj = 0
    else:  # 0-6m
        from app.models import Seasonality
        seasonality = Seasonality.query.all()
        seasonality_data = [{'week_of_year': s.week_of_year, 'seasonality_index': s.seasonality_index} for s in seasonality]
        result = tps_0_6m(units_data, today, settings, seasonality_data)
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
