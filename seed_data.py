"""
Standalone seeding script - Run directly without Flask CLI.
Usage: python seed_data.py
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from app import create_app, db
from app.seeder import DataSeeder


def main():
    """Run database seeding."""
    # Excel file path
    excel_path = Path(__file__).parent.parent / 'V2.2 AutoForecast 1000 Bananas 2026.1.7 (3).xlsx'
    
    if not excel_path.exists():
        print(f"[ERROR] Excel file not found at {excel_path}")
        sys.exit(1)
    
    print(f"[SOURCE] {excel_path.name}")
    print(f"[DATABASE] forecast.db")
    
    # Create app and seed
    app = create_app('development')
    
    with app.app_context():
        # Create all tables
        db.create_all()
        print("[OK] Database tables created\n")
        
        # Run seeder
        seeder = DataSeeder(str(excel_path), db.engine)
        stats = seeder.seed_all(drop_existing=True)
    
    print("\n[STATS] Final Statistics:")
    for table, info in stats.items():
        if isinstance(info, dict) and 'rows' in info:
            print(f"   - {table}: {info['rows']:,} rows ({info['time']})")
        elif isinstance(info, dict):
            print(f"   - {table}: {info.get('time', info)}")
        else:
            print(f"   - {table}: {info}")
    
    print("\n[DONE] Seeding complete! Run 'python run.py' to start the server.")


if __name__ == '__main__':
    main()
