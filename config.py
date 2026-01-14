"""
Application Configuration

Supports both SQLite (development) and PostgreSQL (production/Railway).
Optimized for fast queries with connection pooling.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def get_sqlite_uri(db_name: str) -> str:
    """Generate SQLite URI."""
    db_path = BASE_DIR / db_name
    return f"sqlite:///{db_path}"


def get_database_url() -> str:
    """Get database URL, fixing Railway's postgres:// to postgresql://"""
    url = os.getenv('DATABASE_URL', '')
    # Railway uses postgres:// but SQLAlchemy needs postgresql://
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    return url or get_sqlite_uri('forecast.db')


class Config:
    """Base configuration."""
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # SQLAlchemy engine options for better performance
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,  # Verify connections before use
        'pool_recycle': 300,    # Recycle connections every 5 min
    }


class DevelopmentConfig(Config):
    """Development configuration with SQLite."""
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = get_sqlite_uri('forecast.db')
    SQLALCHEMY_ECHO = False
    
    SQLALCHEMY_ENGINE_OPTIONS = {
        **Config.SQLALCHEMY_ENGINE_OPTIONS,
        'connect_args': {
            'check_same_thread': False,
            'timeout': 30,
        },
    }


class ProductionConfig(Config):
    """Production configuration for PostgreSQL (Railway)."""
    DEBUG = False
    SQLALCHEMY_DATABASE_URI = get_database_url()
    
    # PostgreSQL optimized connection pool
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,
        'pool_recycle': 300,
        'pool_size': 10,          # Number of connections to keep
        'max_overflow': 20,       # Extra connections when pool is full
        'pool_timeout': 30,       # Wait time for connection
    }


class TestingConfig(Config):
    """Testing configuration."""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
