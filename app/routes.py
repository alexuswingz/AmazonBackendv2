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


@api_bp.route('/forecast/<asin>', methods=['GET'])
def get_forecast_data(asin):
    """
    Get combined data for forecasting analysis.
    Returns sales history, current inventory levels.
    """
    # Get product info
    product = Product.query.filter_by(asin=asin).first()
    
    # Get sales data
    sales = UnitsSold.query.filter_by(asin=asin).order_by(UnitsSold.week_date).all()
    
    # Get current FBA inventory
    fba_inv = FBAInventory.query.filter_by(asin=asin).first()
    
    # Get current AWD inventory
    awd_inv = AWDInventory.query.filter_by(asin=asin).first()
    
    return jsonify({
        'product': {
            'asin': asin,
            'brand': product.brand if product else None,
            'name': product.product_name if product else None,
            'size': product.size if product else None
        },
        'sales_history': [{
            'week': s.week_date.isoformat(),
            'units': s.units
        } for s in sales],
        'fba_inventory': {
            'available': fba_inv.available if fba_inv else 0,
            'days_of_supply': fba_inv.days_of_supply if fba_inv else 0,
            'inbound': fba_inv.inbound_quantity if fba_inv else 0,
        } if fba_inv else None,
        'awd_inventory': {
            'available_units': awd_inv.available_in_awd_units if awd_inv else 0,
            'available_cases': awd_inv.available_in_awd_cases if awd_inv else 0,
        } if awd_inv else None,
        'metrics': {
            'total_weeks': len(sales),
            'total_units_sold': sum(s.units for s in sales),
            'avg_weekly_units': round(sum(s.units for s in sales) / len(sales), 2) if sales else 0
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
