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

# Define the correct operations table schema
CREATE_OPERATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS operations (
    id SERIAL PRIMARY KEY,
    operation_id VARCHAR(255) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    before_image_url VARCHAR(500) NOT NULL,
    after_image_url VARCHAR(500) NOT NULL,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    duration_seconds INTEGER,
    area VARCHAR(255),
    division VARCHAR(255),
    district VARCHAR(255),
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    operation_status VARCHAR(50) DEFAULT 'completed',
    gas_status VARCHAR(20) DEFAULT 'normal'
);
"""

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
    "http://localhost:5173",
    "https://shudh-anvi-main-l6pz.onrender.com",
    "*"
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
    try:
        await database.connect()
        logger.info("✅ Database connected successfully.")
        
        # Test database connection
        result = await database.fetch_one("SELECT 1")
        logger.info(f"✅ Database test query successful: {result}")
        
        # Create operations table if it doesn't exist
        logger.info("🔄 Creating operations table if it doesn't exist...")
        await database.execute(CREATE_OPERATIONS_TABLE_SQL)
        logger.info("✅ 'operations' table ensured.")
        
        # Verify table was created and check structure
        try:
            tables = await database.fetch_all("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            logger.info("📊 Available tables:")
            for table in tables:
                logger.info(f"   - {table['table_name']}")
                
            # Check operations table columns
            columns = await database.fetch_all("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'operations'
                ORDER BY ordinal_position
            """)
            logger.info("📋 Operations table columns:")
            for col in columns:
                logger.info(f"   - {col['column_name']}: {col['data_type']}")
                
        except Exception as e:
            logger.warning(f"Could not inspect database structure: {e}")
        
        # Check if table has data
        count_result = await database.fetch_one("SELECT COUNT(*) as count FROM operations")
        logger.info(f"✅ Operations table has {count_result['count']} records")
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise e
    
    logger.info("✅ Application startup complete.")

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down application...")
    await database.disconnect()
    logger.info("✅ Database disconnected.")
    logger.info("✅ Application shutdown complete.")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "healthy", "message": "SHUDH Backend API is running"}

@app.get("/api/health")
async def health_check():
    """Detailed health check"""
    try:
        # Test database connection
        db_status = "healthy"
        try:
            await database.fetch_one("SELECT 1")
            
            # Test operations table
            table_exists = await database.fetch_one("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'operations'
                );
            """)
            db_status = f"healthy, operations table: {table_exists['exists']}"
        except Exception as e:
            db_status = f"unhealthy: {str(e)}"
        
        return {
            "status": "healthy",
            "timestamp": datetime.datetime.now().isoformat(),
            "database": db_status,
            "environment": os.getenv("RENDER", "development")
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}

@app.post("/api/upload")
async def upload_data(
    device_id: str = Form(...),
    operation_id: str = Form(...),
    before_image_url: str = Form(...),
    after_image_url: str = Form(...),
    location: str = Form(...),
    district: str = Form(...),
    division: str = Form(...),
    area: str = Form(...),
    duration_seconds: int = Form(...),
    start_time: str = Form(None),
    end_time: str = Form(None)
):
    try:
        logger.info(f"📥 Received upload request for device: {device_id}, operation: {operation_id}")
        
        # Parse location data
        location_data = {}
        latitude = 0.0
        longitude = 0.0
        try:
            location_data = json.loads(location)
            latitude = float(location_data.get("latitude", 0))
            longitude = float(location_data.get("longitude", 0))
            logger.info(f"📍 Location data - Lat: {latitude}, Lon: {longitude}")
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse location JSON: {location}, error: {e}")

        # Parse timestamps
        start_timestamp = None
        end_timestamp = None
        try:
            if start_time:
                start_timestamp = datetime.datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            if end_time:
                end_timestamp = datetime.datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        except Exception as e:
            logger.warning(f"Failed to parse timestamps: {e}")

        # Prepare values for database insertion
        values = {
            "operation_id": operation_id,
            "device_id": device_id,
            "before_image_url": before_image_url,
            "after_image_url": after_image_url,
            "latitude": latitude,
            "longitude": longitude,
            "duration_seconds": duration_seconds,
            "area": area,
            "division": division,
            "district": district,
            "start_time": start_timestamp,
            "end_time": end_timestamp,
            "operation_status": "completed",
            "gas_status": "normal"
        }

        logger.info(f"💾 Inserting into operations table: {operation_id}")

        query = """
        INSERT INTO operations (
            operation_id, device_id, before_image_url, after_image_url, 
            latitude, longitude, duration_seconds, area, division, district,
            start_time, end_time, operation_status, gas_status
        ) VALUES (
            :operation_id, :device_id, :before_image_url, :after_image_url,
            :latitude, :longitude, :duration_seconds, :area, :division, :district,
            :start_time, :end_time, :operation_status, :gas_status
        );
        """
        
        await database.execute(query=query, values=values)
        logger.info(f"✅ Data successfully inserted for operation: {operation_id}")

        return JSONResponse(status_code=200, content={
            "message": "Upload successful", 
            "device_id": device_id, 
            "operation_id": operation_id,
            "timestamp": datetime.datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"❌ Error during upload: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to insert into database: {e}")

@app.get("/api/data")
async def get_data(limit: int = 100):
    """Fetch recent operations with optional limit asynchronously"""
    try:
        logger.info(f"📤 Fetching data with limit: {limit}")
        
        query = """
        SELECT 
            id,
            operation_id,
            device_id,
            before_image_url,
            after_image_url,
            latitude,
            longitude,
            duration_seconds,
            area,
            division,
            district,
            start_time,
            end_time,
            created_at,
            operation_status,
            gas_status
        FROM operations 
        ORDER BY created_at DESC 
        LIMIT :limit;
        """
        
        results = await database.fetch_all(query, values={"limit": limit})
        
        # Convert to list of dicts
        data = [dict(r) for r in results]
        logger.info(f"✅ Retrieved {len(data)} records")
        return data
        
    except Exception as e:
        logger.error(f"❌ Error fetching data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")

@app.get("/api/data/device/{device_id_filter}")
async def get_data_by_device(device_id_filter: str):
    """Fetch all operations for a specific device_id"""
    try:
        logger.info(f"📤 Fetching data for device: {device_id_filter}")
        
        query = """
        SELECT 
            id,
            operation_id,
            device_id,
            before_image_url,
            after_image_url,
            latitude,
            longitude,
            duration_seconds,
            area,
            division,
            district,
            start_time,
            end_time,
            created_at,
            operation_status,
            gas_status
        FROM operations 
        WHERE device_id = :device_id_filter 
        ORDER BY created_at DESC;
        """
        
        results = await database.fetch_all(query, values={"device_id_filter": device_id_filter})
        data = [dict(r) for r in results]
        
        logger.info(f"✅ Retrieved {len(data)} records for device {device_id_filter}")
        return data
        
    except Exception as e:
        logger.error(f"❌ Error fetching device data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch device data: {e}")

@app.get("/api/data/location/{district_filter}")
async def get_data_by_district(district_filter: str):
    """Fetch all operations for a specific district"""
    try:
        logger.info(f"📤 Fetching data for district: {district_filter}")
        
        query = """
        SELECT 
            id,
            operation_id,
            device_id,
            before_image_url,
            after_image_url,
            latitude,
            longitude,
            duration_seconds,
            area,
            division,
            district,
            start_time,
            end_time,
            created_at,
            operation_status,
            gas_status
        FROM operations 
        WHERE district = :district_filter 
        ORDER BY created_at DESC;
        """
        
        results = await database.fetch_all(query, values={"district_filter": district_filter})
        data = [dict(r) for r in results]
        
        logger.info(f"✅ Retrieved {len(data)} records for district {district_filter}")
        return data
        
    except Exception as e:
        logger.error(f"❌ Error fetching district data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch district data: {e}")

@app.get("/uploads/{filename}")
async def get_uploaded_file(filename: str):
    """Serve uploaded image files."""
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)

@app.get("/api/test")
async def test_endpoint():
    """Test endpoint to verify API functionality"""
    return {
        "message": "API is working!",
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "success"
    }
