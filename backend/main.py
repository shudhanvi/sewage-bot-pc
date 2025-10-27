from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import datetime
import json
import logging
from databases import Database
import ssl

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Database Setup with YOUR SPECIFIC URL ---
# Your database URL from Render.com
DATABASE_URL = "postgresql://shudhdb_latest_user:a0eikB9mSFIGbZWvHaAg8X8hLGxd4gqM@dpg-d3vhqsili9vc73crj52g-a.singapore-postgres.render.com/shudhdb_latest"

logger.info("🔧 Configuring database connection for your specific Render.com database...")

# Transform the database URL for SSL and asyncpg
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Add SSL parameters for Render.com
if "?" in DATABASE_URL:
    DATABASE_URL += "&ssl=require"
else:
    DATABASE_URL += "?ssl=require"

logger.info(f"✅ Database URL configured with SSL")
logger.info(f"📊 Using database: shudhdb_latest")

# Create SSL context for Render.com PostgreSQL
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Initialize database with SSL
database = Database(DATABASE_URL, ssl=ssl_context)

# --- FastAPI App ---
app = FastAPI(title="SHUDH Backend API", version="1.0")

# --- CORS Configuration ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Database Table Schema ---
CREATE_OPERATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS operations (
    id SERIAL PRIMARY KEY,
    operation_id VARCHAR(255) NOT NULL UNIQUE,
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

# --- Application Events ---
@app.on_event("startup")
async def startup():
    """Initialize database connection and create tables"""
    logger.info("🚀 Starting SHUDH Backend API with your Render.com database...")
    
    try:
        # Connect to database
        await database.connect()
        logger.info("✅ Database connected successfully with SSL")
        
        # Create operations table
        await database.execute(CREATE_OPERATIONS_TABLE_SQL)
        logger.info("✅ Operations table created/verified")
        
        # Test the connection
        test_result = await database.fetch_one("SELECT 1 as connection_test")
        logger.info(f"✅ Database test query successful")
        
        # Check table count
        count_result = await database.fetch_one("SELECT COUNT(*) as count FROM operations")
        logger.info(f"📊 Operations table has {count_result['count']} records")
        
        logger.info("🎉 Application startup completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise e

@app.on_event("shutdown")
async def shutdown():
    """Clean up database connections"""
    logger.info("🛑 Shutting down application...")
    await database.disconnect()
    logger.info("✅ Database disconnected")

# --- API Routes ---
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "SHUDH Backend API",
        "database": "shudhdb_latest (Render.com)",
        "ssl": "enabled",
        "timestamp": datetime.datetime.now().isoformat()
    }

@app.get("/api/health")
async def health_check():
    """Comprehensive health check"""
    try:
        # Test database connection
        await database.fetch_one("SELECT 1")
        
        # Get table info
        table_info = await database.fetch_one("SELECT COUNT(*) as record_count FROM operations")
        
        return {
            "status": "healthy",
            "database": "shudhdb_latest",
            "ssl": "enabled",
            "records_count": table_info["record_count"],
            "timestamp": datetime.datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }

@app.post("/api/upload")
async def upload_operation_data(
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
    """
    Upload operation data from Camera 2
    """
    try:
        logger.info(f"📥 Received upload request - Operation: {operation_id}")
        
        # Parse location data
        location_data = json.loads(location)
        latitude = float(location_data.get("latitude", 0.0))
        longitude = float(location_data.get("longitude", 0.0))
        
        # Parse timestamps
        start_timestamp = None
        end_timestamp = None
        
        if start_time:
            try:
                start_timestamp = datetime.datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except Exception:
                pass
        
        if end_time:
            try:
                end_timestamp = datetime.datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except Exception:
                pass
        
        # Prepare database values
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
            "end_time": end_timestamp
        }
        
        logger.info(f"💾 Inserting into shudhdb_latest database...")
        
        # Database insert query
        query = """
        INSERT INTO operations (
            operation_id, device_id, before_image_url, after_image_url, 
            latitude, longitude, duration_seconds, area, division, district,
            start_time, end_time
        ) VALUES (
            :operation_id, :device_id, :before_image_url, :after_image_url,
            :latitude, :longitude, :duration_seconds, :area, :division, :district,
            :start_time, :end_time
        );
        """
        
        # Execute the insert
        await database.execute(query=query, values=values)
        
        logger.info(f"✅ Successfully uploaded to shudhdb_latest: {operation_id}")
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Data uploaded to shudhdb_latest successfully",
                "operation_id": operation_id,
                "database": "shudhdb_latest (Render.com)",
                "ssl": "enabled",
                "timestamp": datetime.datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Upload failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload data: {str(e)}"
        )

@app.get("/api/data")
async def get_operations_data(limit: int = 100):
    """Get recent operations data"""
    try:
        query = """
        SELECT * FROM operations 
        ORDER BY created_at DESC 
        LIMIT :limit;
        """
        
        results = await database.fetch_all(query=query, values={"limit": limit})
        operations = [dict(record) for record in results]
        
        logger.info(f"✅ Retrieved {len(operations)} records from shudhdb_latest")
        
        return operations
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch data: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
