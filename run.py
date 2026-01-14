"""
Product Forecasting Flask Application
Entry point for local development.
"""
import os
from app import create_app, db

# Create Flask app
app = create_app(os.getenv('FLASK_ENV', 'development'))


@app.cli.command('init-db')
def init_db():
    """Initialize the database (create tables)."""
    with app.app_context():
        db.create_all()
        print("[OK] Database tables created successfully!")


@app.cli.command('seed')
def seed_db():
    """Seed the database from Excel file."""
    from app.seeder import seed_database
    from pathlib import Path
    
    # Default Excel path - adjust as needed
    excel_path = Path(__file__).parent.parent / 'V2.2 AutoForecast 1000 Bananas 2026.1.7 (3).xlsx'
    
    if not excel_path.exists():
        print(f"Error: Excel file not found at {excel_path}")
        return
    
    print(f"Seeding from: {excel_path}")
    stats = seed_database(app, str(excel_path))
    print("\nSeeding Statistics:")
    for table, info in stats.items():
        if isinstance(info, dict):
            print(f"  - {table}: {info['rows']:,} rows ({info['time']})")
        else:
            print(f"  - {table}: {info}")


@app.cli.command('drop-db')
def drop_db():
    """Drop all database tables."""
    with app.app_context():
        db.drop_all()
        print("[OK] All database tables dropped!")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
