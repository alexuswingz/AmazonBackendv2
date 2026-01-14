"""
Flask Application Factory

Initializes the Flask app with:
- SQLAlchemy database
- SQLite performance optimizations
- API blueprints
"""
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import config

db = SQLAlchemy()


def create_app(config_name='default'):
    """Application factory pattern."""
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize database
    db.init_app(app)
    
    # Apply SQLite performance optimizations
    with app.app_context():
        from app.db_utils import apply_sqlite_optimizations
        apply_sqlite_optimizations(app)
    
    # Register blueprints
    from app.routes import api_bp
    app.register_blueprint(api_bp, url_prefix='/api')
    
    # Register CLI commands
    register_cli_commands(app)
    
    return app


def register_cli_commands(app):
    """Register custom CLI commands."""
    
    @app.cli.command('db-stats')
    def db_stats():
        """Show database statistics."""
        from app.db_utils import get_table_stats, get_index_stats
        get_table_stats()
        get_index_stats()
    
    @app.cli.command('db-optimize')
    def db_optimize():
        """Run database optimization (ANALYZE + VACUUM)."""
        from app.db_utils import analyze_tables, vacuum_database
        analyze_tables()
        vacuum_database()
    
    @app.cli.command('db-analyze')
    def db_analyze():
        """Update query planner statistics."""
        from app.db_utils import analyze_tables
        analyze_tables()
