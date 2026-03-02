#!/bin/bash
# Production Start Script for Linux/Mac
# Usage: ./start_prod.sh

set -e

echo "Starting ProERP in production mode..."

export FLASK_ENV=production

# Create logs directory
mkdir -p logs backups

# Install dependencies
pip install -q -r requirements.txt

# Use PORT from environment (Render sets this)
PORT=${PORT:-5000}

# Start with Gunicorn (4 workers)
gunicorn \
    --workers 4 \
    --bind 0.0.0.0:$PORT \
    --timeout 120 \
    --access-logfile logs/access.log \
    --error-logfile logs/error.log \
    --log-level info \
    --capture-output \
    app:app
