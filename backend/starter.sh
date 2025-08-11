#!/bin/bash

# Activate Python virtual environment (if using)
# source venv/bin/activate
# Add this before starting the server
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Create images directory if not exists
mkdir -p images

# Run database migrations (if any)
python -c """
import psycopg
from psycopg import sql

# Connect to PostgreSQL using psycopg v3
conn = psycopg.connect('${DATABASE_URL}')
conn.autocommit = True  # Enable autocommit for DDL statements

# Create table if not exists
with conn.cursor() as cur:
    cur.execute(
        sql.SQL('''
        CREATE TABLE IF NOT EXISTS operations (
            id SERIAL PRIMARY KEY,
            device_id TEXT NOT NULL,
            before_path TEXT NOT NULL,
            after_path TEXT NOT NULL,
            gas_level FLOAT,
            location TEXT,
            timestamp TIMESTAMP DEFAULT NOW()
        )
        ''')
    )
conn.close()
"""

# Start FastAPI server
uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --timeout-keep-alive 300
