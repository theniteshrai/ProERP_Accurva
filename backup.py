import os
import sqlite3
import json
import shutil
from datetime import datetime
from logger import logger

BACKUP_DIR = 'backups'
MAX_BACKUPS = 10

def get_backup_dir():
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    return BACKUP_DIR

def create_backup(db_path='proerp.db', backup_name=None):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
    if backup_name is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"backup_{timestamp}"
    
    backup_file = os.path.join(get_backup_dir(), f"{backup_name}.db")
    
    try:
        shutil.copy2(db_path, backup_file)
        
        metadata = {
            'backup_name': backup_name,
            'created_at': datetime.now().isoformat(),
            'original_db': db_path,
            'size_bytes': os.path.getsize(backup_file)
        }
        
        meta_file = os.path.join(get_backup_dir(), f"{backup_name}_meta.json")
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        cleanup_old_backups()
        
        logger.info(f"Backup created: {backup_name}")
        return {'success': True, 'backup_name': backup_name, 'file': backup_file}
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        return {'success': False, 'error': str(e)}

def restore_backup(backup_name, target_db='proerp.db'):
    backup_file = os.path.join(get_backup_dir(), f"{backup_name}.db")
    
    if not os.path.exists(backup_file):
        return {'success': False, 'error': 'Backup file not found'}
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    pre_restore_backup = os.path.join(get_backup_dir(), f"prerestore_{timestamp}.db")
    
    try:
        if os.path.exists(target_db):
            shutil.copy2(target_db, pre_restore_backup)
            logger.info(f"Pre-restore backup created: prerestore_{timestamp}")
        
        shutil.copy2(backup_file, target_db)
        logger.info(f"Database restored from: {backup_name}")
        
        return {'success': True, 'backup_name': backup_name}
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        return {'success': False, 'error': str(e)}

def list_backups():
    backup_dir = get_backup_dir()
    backups = []
    
    for f in sorted(os.listdir(backup_dir), reverse=True):
        if f.endswith('.db'):
            backup_name = f[:-3]
            meta_file = os.path.join(backup_dir, f"{backup_name}_meta.json")
            
            if os.path.exists(meta_file):
                with open(meta_file, 'r') as mf:
                    meta = json.load(mf)
            else:
                stat = os.stat(os.path.join(backup_dir, f))
                meta = {
                    'backup_name': backup_name,
                    'created_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'size_bytes': stat.st_size
                }
            
            backups.append(meta)
    
    return backups

def cleanup_old_backups():
    backups = list_backups()
    if len(backups) > MAX_BACKUPS:
        for backup in backups[MAX_BACKUPS:]:
            backup_file = os.path.join(get_backup_dir(), f"{backup['backup_name']}.db")
            meta_file = os.path.join(get_backup_dir(), f"{backup['backup_name']}_meta.json")
            
            if os.path.exists(backup_file):
                os.remove(backup_file)
            if os.path.exists(meta_file):
                os.remove(meta_file)
            
            logger.info(f"Old backup cleaned up: {backup['backup_name']}")

def delete_backup(backup_name):
    backup_file = os.path.join(get_backup_dir(), f"{backup_name}.db")
    meta_file = os.path.join(get_backup_dir(), f"{backup_name}_meta.json")
    
    try:
        if os.path.exists(backup_file):
            os.remove(backup_file)
        if os.path.exists(meta_file):
            os.remove(meta_file)
        
        logger.info(f"Backup deleted: {backup_name}")
        return {'success': True}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def export_json(db_path='proerp.db'):
    if not os.path.exists(db_path):
        return {'success': False, 'error': 'Database not found'}
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    def get_all(table):
        c.execute(f'SELECT * FROM {table}')
        return [dict(row) for row in c.fetchall()]
    
    tables = ['organisations', 'users', 'module_access', 'parties', 'items', 
              'invoices', 'invoice_items', 'transactions', 'expenses', 
              'purchase_orders', 'purchase_order_items', 'quotations', 
              'quotation_items', 'settings']
    
    data = {'exported_at': datetime.now().isoformat()}
    for table in tables:
        try:
            data[table] = get_all(table)
        except Exception as e:
            logger.warning(f"Could not export {table}: {e}")
            data[table] = []
    
    conn.close()
    return data
