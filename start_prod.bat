@echo off
REM Production Start Script for Windows
REM Usage: start_prod.bat

echo Starting ProERP in production mode...

set FLASK_ENV=production
set SECRET_KEY=%SECRET_KEY%

echo Installing dependencies if needed...
pip install -q -r requirements.txt

echo Starting server with Gunicorn...
gunicorn -w 4 -b 127.0.0.1:3000 --timeout 120 --access-logfile logs/access.log --error-logfile logs/error.log app:app

pause
