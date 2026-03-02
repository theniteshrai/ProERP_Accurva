# ProERP - Production Deployment Guide

## Tech Stack (Open Source)

| Component | Technology |
|-----------|------------|
| Backend | Flask + Python |
| Database | SQLite (built-in) / PostgreSQL (optional) |
| Server | Gunicorn |
| Rate Limiting | Flask-Limiter |
| Logging | Built-in Python logging |

## Quick Start

### Development
```bash
pip install -r requirements.txt
python app.py
```

### Production

#### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 2. Configure Environment
Edit `.env` file:
```env
FLASK_ENV=production
SECRET_KEY=your-secure-random-secret-key-here
DATABASE_PATH=proerp.db
LOG_LEVEL=INFO
```

**Important**: Generate a secure secret key:
```python
import secrets
secrets.token_hex(32)
```

#### 3. Start Server

**Windows:**
```bash
start_prod.bat
```

**Linux/Mac:**
```bash
chmod +x start_prod.sh
./start_prod.sh
```

Or manually:
```bash
gunicorn -w 4 -b 127.0.0.1:3000 --timeout 120 app:app
```

## Production Features

- ✅ Rate Limiting (10 req/min on login)
- ✅ Security Headers (XSS, Frame options, etc.)
- ✅ Error Handlers (400, 401, 403, 404, 429, 500)
- ✅ Input Validation
- ✅ Secure Cookies (production mode)
- ✅ Request Logging
- ✅ Gunicorn (4 workers)

## File Structure
```
ProERP/
├── app.py              # Main application
├── config.py           # Configuration
├── logger.py           # Logging
├── backup.py           # Backup system
├── requirements.txt    # Dependencies
├── .env               # Environment variables
├── .gitignore         # Git ignore
├── start_prod.bat      # Windows startup
├── start_prod.sh      # Linux startup
├── public/            # Frontend
├── backups/           # Database backups
└── logs/              # Log files
```

## Security Notes

1. **Change SECRET_KEY** in production
2. **Use HTTPS** with reverse proxy (nginx)
3. **Keep backups** in secure location
4. **Monitor logs** regularly

## Optional: PostgreSQL

For high concurrency, migrate to PostgreSQL:

1. Install psycopg2: `pip install psycopg2-binary`
2. Update DATABASE_URL in .env
3. Update get_db() in app.py to use psycopg2
