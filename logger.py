import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'proerp.log')
ERROR_FILE = os.path.join(LOG_DIR, 'error.log')
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 5

def setup_logger(name='proerp'):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    error_handler = RotatingFileHandler(
        ERROR_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

def log_request(endpoint, method, user_id=None):
    logger.info(f"REQUEST | {method} {endpoint} | user={user_id or 'anonymous'}")

def log_error(error, context=''):
    logger.error(f"ERROR | {context} | {type(error).__name__}: {str(error)}", exc_info=True)

def log_db_operation(operation, table, record_id=None):
    logger.debug(f"DB OP | {operation} | {table} | id={record_id}")
