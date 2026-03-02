import os
from datetime import timedelta

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    DATABASE_PATH = os.environ.get('DATABASE_PATH') or 'proerp.db'
    BACKUP_DIR = os.environ.get('BACKUP_DIR') or 'backups'
    LOG_DIR = os.environ.get('LOG_DIR') or 'logs'
    
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024
    
    SESSION_COOKIE_SECURE = os.environ.get('FLASK_ENV') == 'production'
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(days=7)
    
    RATELIMIT_ENABLED = os.environ.get('FLASK_ENV') == 'production'
    RATELIMIT_STORAGE_URL = 'memory://'
    RATELIMIT_DEFAULT = '100 per minute'
    RATELIMIT_LOG_ENABLED = False
    
    JSON_SORT_KEYS = False
    
    @classmethod
    def init_app(cls, app):
        if os.environ.get('FLASK_ENV') == 'production':
            import logging
            logging.getLogger('werkzeug').setLevel(logging.WARNING)

class DevelopmentConfig(Config):
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    DEBUG = False
    TESTING = False
    
    RATELIMIT_ENABLED = True
    RATELIMIT_DEFAULT = '200 per minute'

class TestingConfig(Config):
    TESTING = True
    DEBUG = True

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
