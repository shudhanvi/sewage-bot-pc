from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import Optional
import os
import random
import time
import datetime
import json
import logging

# --- Configure Backend Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
# --- END Configure Backend Logging ---

# --- Async Database Setup ---
from databases import Database
import sqlalchemy

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL and not DATABASE_URL.startswith("postgresql+asyncpg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

database = Database(DATABASE_URL)

# Define your table creation SQL query - Ensures all columns are present and correct types
CREATE_OPERATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS operations (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    before_path VARCHAR(500) NOT NULL,
    after_path VARCHAR(500) NOT NULL,
    gas_data JSONB,
    gas_data_raw TEXT,
    location VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    district VARCHAR(255),
    division VARCHAR(255),
    area VARCHAR(255),
    -- NEW COLUMNS FOR OPERATION TIMES
    operation_time_minutes NUMERIC,
    operation_start_time TIMESTAMP WITH TIME ZONE,
    operation_end_time TIMESTAMP WITH TIME ZONE
);
"""
# SQL to drop the table (for controlled resets)
DROP_OPERATIONS_TABLE_SQL = "DROP TABLE IF EXISTS operations;"

app = FastAPI()

# Mount static files for serving uploads
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


# --- CORS Configuration ---
origins = [
    "https://sewage-bot-poc-1.onrender.com",
    "https://sewage-bot-poc.onrender.com",
    "https://shudh.anvi.co",
    "https://shudh-anvi.onrender.com",
    "https://www.shudh.anvi.co",
    "https://project-shudh.onrender.com",
    "https://shudh-dashboard.onrender.com",
    "https://shudh-anvi-main.onrender.com/",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.on_event("startup")
async def startup():
    logger.info("Starting up application...")
    await database.connect()
    logger.info("Database connected.")

    # --- NEW: Conditional Database Reset ---
    reset_db = os.getenv("RESET_DB_ON_STARTUP", "false").lower() == "true"
    if reset_db:
        logger.warning("RESET_DB_ON_STARTUP is set to true. Dropping existing 'operations' table...")
        await database.execute(DROP_OPERATIONS_TABLE_SQL)
        logger.warning("'operations' table dropped.")
    # --- END NEW ---

    await database.execute(CREATE_OPERATIONS_TABLE_SQL)
    logger.info("'operations' table ensured (created or verified).")
    logger.info("Application startup complete.")

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down application...")
    await database.disconnect()
    logger.info("Database disconnected.")
    logger.info("Application shutdown complete.")

@app.post("/api/upload")
async def upload_data(
    device_id: str = Form(...),
    before: UploadFile = File(...),
    after: UploadFile = File(...),
    gas_data_raw: Optional[str] = Form(None), # This is the incoming JSON string
    location: Optional[str] = Form(None),
    district: Optional[str] = Form(None),
    division: Optional[str] = Form(None),
    area: Optional[str] = Form(None),
    # NEW PARAMETERS
    operation_time: Optional[str] = Form(None),
    start_time: Optional[str] = Form(None),
    end_time: Optional[str] = Form(None)
):
    try:
        # Save 'before' image
        before_filename = f"{device_id}_before_{int(time.time())}_{random.randint(1000, 9999)}.jpg"
        before_path = os.path.join(UPLOAD_DIR, before_filename)
        with open(before_path, "wb") as buffer:
            buffer.write(await before.read())
        logger.info(f"Saved before image: {before_filename}")

        # Save 'after' image
        after_filename = f"{device_id}_after_{int(time.time())}_{random.randint(1000, 9999)}.jpg"
        after_path = os.path.join(UPLOAD_DIR, after_filename)
        with open(after_path, "wb") as buffer:
            buffer.write(await after.read())
        logger.info(f"Saved after image: {after_filename}")

        # Parse gas_data_raw (JSON string) into a Python dict for gas_data JSONB column
        gas_data_parsed = None
        if gas_data_raw:
            try:
                gas_data_parsed = json.loads(gas_data_raw)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse gas_data_raw JSON: {gas_data_raw}. Storing as NULL for gas_data column.")
                # gas_data_parsed remains None, which will result in NULL for gas_data JSONB

        # Parse operation_start_time and operation_end_time from ISO format strings
        parsed_start_time = None
        if start_time:
            try:
                # Assuming ISO format with timezone (e.g., "2025-08-14T17:35:26.000000+05:30")
                parsed_start_time = datetime.datetime.fromisoformat(start_time)
            except ValueError:
                logger.error(f"Failed to parse start_time: {start_time}. Storing as NULL.")

        parsed_end_time = None
        if end_time:
            try:
                parsed_end_time = datetime.datetime.fromisoformat(end_time)
            except ValueError:
                logger.error(f"Failed to parse end_time: {end_time}. Storing as NULL.")
        
        parsed_operation_time = None
        if operation_time:
            try:
                parsed_operation_time = float(operation_time)
            except ValueError:
                logger.error(f"Failed to parse operation_time: {operation_time}. Storing as NULL.")

        # Use UTC for consistency with TIMESTAMP WITH TIME ZONE
        current_timestamp = datetime.datetime.now(datetime.timezone.utc)

        # Prepare values for database insertion
        values = {
            "device_id": device_id,
            "before_path": f"uploads/{before_filename}",
            "after_path": f"uploads/{after_filename}",
            "gas_data": json.dumps(gas_data_parsed) if gas_data_parsed else None, # This is the Python dict (or None) for JSONB column
            "gas_data_raw": gas_data_raw, # This is the original JSON string (or None) for TEXT column
            "location": location,
            "timestamp": current_timestamp,
            "district": district,
            "division": division,
            "area": area,
            "operation_time_minutes": parsed_operation_time,
            "operation_start_time": parsed_start_time,
            "operation_end_time": parsed_end_time
        }

        query = """
        INSERT INTO operations (
            device_id, before_path, after_path, gas_data, gas_data_raw, location, timestamp, district, division, area,
            operation_time_minutes, operation_start_time, operation_end_time
        ) VALUES (
            :device_id, :before_path, :after_path, :gas_data, :gas_data_raw, :location, :timestamp, :district, :division, :area,
            :operation_time_minutes, :operation_start_time, :operation_end_time
        );
        """
        await database.execute(query=query, values=values)
        logger.info(f"Data successfully inserted for device: {device_id}")

        return JSONResponse(status_code=200, content={"message": "Upload successful", "device_id": device_id})

    except Exception as e:
        logger.error(f"Error during upload: {e}", exc_info=True) # exc_info=True for full traceback
        raise HTTPException(status_code=500, detail=f"Failed to insert into database: {e}")

@app.get("/api/data")
async def get_data(limit: int = 100):
    """Fetch recent operations with optional limit asynchronously"""
    query = "SELECT * FROM operations ORDER BY timestamp DESC LIMIT :limit;"
    results = await database.fetch_all(query, values={"limit": limit})
    return [dict(r) for r in results]

@app.get("/api/data/device/{device_id_filter}")
async def get_data_by_device(device_id_filter: str):
    """Fetch all operations for a specific device_id"""
    query = "SELECT * FROM operations WHERE device_id = :device_id_filter ORDER BY timestamp DESC;"
    results = await database.fetch_all(query, values={"device_id_filter": device_id_filter})
    return [dict(r) for r in results]

@app.get("/uploads/{filename}")
async def get_uploaded_file(filename: str):
    """Serve uploaded image files."""
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)
