"""Find products with 18+ months of sales history"""
from app import create_app, db
from app.models import UnitsSold
from sqlalchemy import func
from datetime import date, timedelta

app = create_app('development')
with app.app_context():
    # Find products with 18+ months of data (78+ weeks)
    cutoff_date = date(2026, 1, 14) - timedelta(days=18*30)  # ~18 months ago
    
    # Get products with earliest sale date before cutoff
    results = db.session.query(
        UnitsSold.asin,
        func.min(UnitsSold.week_date).label('first_sale'),
        func.max(UnitsSold.week_date).label('last_sale'),
        func.count(UnitsSold.id).label('weeks'),
        func.sum(UnitsSold.units).label('total_units')
    ).group_by(UnitsSold.asin).having(
        func.min(UnitsSold.week_date) < cutoff_date
    ).having(
        func.sum(UnitsSold.units) > 1000  # Has decent sales volume
    ).order_by(func.sum(UnitsSold.units).desc()).limit(15).all()
    
    print('Products with 18+ months history and good sales volume:')
    print('=' * 80)
    print(f"{'ASIN':<15} {'First Sale':<12} {'Weeks':<8} {'Total Units':<12} {'Age (months)'}")
    print('-' * 80)
    for r in results:
        age_days = (date(2026, 1, 14) - r.first_sale).days
        age_months = age_days / 30
        print(f"{r.asin:<15} {str(r.first_sale):<12} {r.weeks:<8} {r.total_units:<12} {age_months:.1f}")
