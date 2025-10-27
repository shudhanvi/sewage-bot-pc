from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import datetime
import json
import logging
import asyncpg
import ssl

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Database Configuration ---
DATABASE_URL = "postgresql://shudhdb_latest_user:a0eikB9mSFIGbZWvHaAg8X8hLGxd4gqM@dpg-d3vhqsili9vc73crj52g-a.singapore-postgres.render.com/shudhdb_latest"

# Parse database URL
def parse_db_url(db_url):
    """Parse database URL into connection parameters"""
    # Remove postgresql:// prefix
    db_url = db_url.replace('postgresql://', '')
    
    # Split user:password and host:port/database
    user_pass, host_db = db_url.split('@')
    user, password = user_pass.split(':')
    
    # Split host:port and database
    if ':' in host_db:
        host_port, database = host_db.split('/')
        host, port = host_port.split(':')
    else:
        host, database = host_db.split('/')
        port = '5432'
    
    return {
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'database': database
    }

# Parse the database URL
db_params = parse_db_url(DATABASE_URL)
logger.info(f"🔧 Database: {db_params['host']}:{db_params['port']}/{db_params['database']}")

# Create SSL context for Render.com
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Global database connection
db_connection = None

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

# --- Database Functions ---
async def get_db_connection():
    """Get database connection with SSL"""
    global db_connection
    if db_connection is None or db_connection.is_closed():
        try:
            db_connection = await asyncpg.connect(
                user=db_params['user'],
                password=db_params['password'],
                database=db_params['database'],
                host=db_params['host'],
                port=db_params['port'],
                ssl=ssl_context
            )
            logger.info("✅ Database connected with SSL")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise e
    return db_connection

async def execute_query(query, *args):
    """Execute a database query"""
    conn = await get_db_connection()
    try:
        result = await conn.execute(query, *args)
        return result
    except Exception as e:
        logger.error(f"❌ Query execution failed: {e}")
        raise e

async def fetch_query(query, *args):
    """Fetch data from database"""
    conn = await get_db_connection()
    try:
        result = await conn.fetch(query, *args)
        return result
    except Exception as e:
        logger.error(f"❌ Fetch query failed: {e}")
        raise e

async def fetch_one(query, *args):
    """Fetch one row from database"""
    conn = await get_db_connection()
    try:
        result = await conn.fetchrow(query, *args)
        return result
    except Exception as e:
        logger.error(f"❌ Fetch one failed: {e}")
        raise e

# --- Application Events ---
@app.on_event("startup")
async def startup():
    """Initialize database connection and create tables"""
    logger.info("🚀 Starting SHUDH Backend API with SSL...")
    
    try:
        # Connect to database
        conn = await get_db_connection()
        logger.info("✅ Database connection established")
        
        # Create operations table
        await execute_query(CREATE_OPERATIONS_TABLE_SQL)
        logger.info("✅ Operations table created/verified")
        
        # Test the connection
        test_result = await fetch_one("SELECT 1 as test_value, NOW() as current_time")
        logger.info(f"✅ Database test successful: {test_result['test_value']}")
        
        # Check table count
        count_result = await fetch_one("SELECT COUNT(*) as count FROM operations")
        logger.info(f"📊 Operations table has {count_result['count']} records")
        
        logger.info("🎉 Application startup completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise e

@app.on_event("shutdown")
async def shutdown():
    """Clean up database connections"""
    logger.info("🛑 Shutting down application...")
    global db_connection
    if db_connection and not db_connection.is_closed():
        await db_connection.close()
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
        db_test = await fetch_one("SELECT 1 as test_value, NOW() as current_time")
        
        # Get table info
        table_info = await fetch_one("SELECT COUNT(*) as record_count FROM operations")
        
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
        
        logger.info(f"📍 Location - Lat: {latitude:.6f}, Lon: {longitude:.6f}")
        
        # Parse timestamps
        start_timestamp = None
        end_timestamp = None
        
        if start_time:
            try:
                start_timestamp = datetime.datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Could not parse start_time: {e}")
        
        if end_time:
            try:
                end_timestamp = datetime.datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Could not parse end_time: {e}")
        
        # Database insert query
        query = """
        INSERT INTO operations (
            operation_id, device_id, before_image_url, after_image_url, 
            latitude, longitude, duration_seconds, area, division, district,
            start_time, end_time, operation_status, gas_status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'completed', 'normal')
        """
        
        # Execute the insert with parameters
        await execute_query(
            query,
            operation_id, device_id, before_image_url, after_image_url,
            latitude, longitude, duration_seconds, area, division, district,
            start_timestamp, end_timestamp
        )
        
        logger.info(f"✅ Successfully uploaded to shudhdb_latest: {operation_id}")
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Operation data uploaded successfully",
                "operation_id": operation_id,
                "device_id": device_id,
                "database": "shudhdb_latest (Render.com)",
                "ssl": "enabled",
                "timestamp": datetime.datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Upload failed for operation {operation_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload operation data: {str(e)}"
        )

@app.get("/api/data")
async def get_operations_data(limit: int = 100):
    """Get recent operations data"""
    try:
        query = "SELECT * FROM operations ORDER BY created_at DESC LIMIT $1"
        results = await fetch_query(query, limit)
        
        operations = []
        for record in results:
            operations.append(dict(record))
        
        logger.info(f"✅ Retrieved {len(operations)} records from shudhdb_latest")
        
        return operations
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch operations data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch operations data: {str(e)}"
        )

@app.get("/api/data/device/{device_id}")
async def get_operations_by_device(device_id: str):
    """Get operations for a specific device"""
    try:
        query = "SELECT * FROM operations WHERE device_id = $1 ORDER BY created_at DESC"
        results = await fetch_query(query, device_id)
        
        operations = []
        for record in results:
            operations.append(dict(record))
        
        logger.info(f"✅ Retrieved {len(operations)} operations for device {device_id}")
        
        return operations
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch device operations: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch device operations: {str(e)}"
        )

# --- Main Execution ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
