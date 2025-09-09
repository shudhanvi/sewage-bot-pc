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

# Define your table creation SQL query
CREATE_OPERATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS operations (
    id SERIAL PRIMARY KEY,
    operation_id VARCHAR(255) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    before_path VARCHAR(500) NOT NULL,
    after_path VARCHAR(500) NOT NULL,
    gas_data_raw TEXT,
    gas_status VARCHAR(20),
    location JSONB,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    district VARCHAR(255),
    division VARCHAR(255),
    area VARCHAR(255)
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
    "https://shudh-anvi-main.onrender.com",
    "https://shudh.anvirobotics.com",
    "http://localhost:3000",
    "https://shudh-anvi-main-l6pz.onrender.com",  #New Frontend Render Link.
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

    # --- Conditional Database Reset ---
    reset_db = os.getenv("RESET_DB_ON_STARTUP", "false").lower() == "true"
    if reset_db:
        logger.warning("RESET_DB_ON_STARTUP is set to true. Dropping existing 'operations' table...")
        await database.execute(DROP_OPERATIONS_TABLE_SQL)
        logger.warning("'operations' table dropped.")
    # --- END ---

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
    gas_data_raw: Optional[str] = Form(None),
    gas_status: Optional[str] = Form(None),
    location: Optional[str] = Form(None),
    district: Optional[str] = Form(None),
    division: Optional[str] = Form(None),
    area: Optional[str] = Form(None),
    operation_id: Optional[str] = Form(None),
    before_image_url: Optional[str] = Form(None),  # Azure URL
    after_image_url: Optional[str] = Form(None)    # Azure URL
):
    try:
        # Generate operation_id if not provided
        if not operation_id:
            operation_id = f"{device_id}_{int(time.time())}_{random.randint(1000, 9999)}"
        
        # Parse location if provided
        location_data = {}
        if location:
            try:
                location_data = json.loads(location)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse location JSON: {location}")

        # Use UTC for consistency with TIMESTAMP WITH TIME ZONE
        current_timestamp = datetime.datetime.now(datetime.timezone.utc)

        # Prepare values for database insertion
        values = {
            "operation_id": operation_id,
            "device_id": device_id,
            "before_path": before_image_url,  # Store Azure URL
            "after_path": after_image_url,    # Store Azure URL
            "gas_data_raw": gas_data_raw,
            "gas_status": gas_status,
            "location": json.dumps(location_data) if location_data else None,
            "timestamp": current_timestamp,
            "district": district,
            "division": division,
            "area": area
        }

        query = """
        INSERT INTO operations (
            operation_id, device_id, before_path, after_path, gas_data_raw, gas_status,
            location, timestamp, district, division, area
        ) VALUES (
            :operation_id, :device_id, :before_path, :after_path, :gas_data_raw, :gas_status,
            :location, :timestamp, :district, :division, :area
        );
        """
        await database.execute(query=query, values=values)
        logger.info(f"Data successfully inserted for device: {device_id}, operation: {operation_id}")

        return JSONResponse(status_code=200, content={
            "message": "Upload successful", 
            "device_id": device_id, 
            "operation_id": operation_id
        })

    except Exception as e:
        logger.error(f"Error during upload: {e}", exc_info=True)
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

@app.get("/api/data/location/{district_filter}")
async def get_data_by_district(district_filter: str):
    """Fetch all operations for a specific district"""
    query = "SELECT * FROM operations WHERE district = :district_filter ORDER BY timestamp DESC;"
    results = await database.fetch_all(query, values={"district_filter": district_filter})
    return [dict(r) for r in results]

@app.get("/uploads/{filename}")
async def get_uploaded_file(filename: str):
    """Serve uploaded image files."""
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)
