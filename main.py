from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from contextlib import asynccontextmanager
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global engine variable
engine = None

# HuggingFace dataset repo
REPO_API = "https://huggingface.co/api/datasets/bwzheng2010/yahoo-finance-data/tree/main/data"
BASE_URL = "https://huggingface.co/datasets/bwzheng2010/yahoo-finance-data/resolve/main/data/"

# Configuration for chunked processing
CHUNK_SIZE = 10000  # Number of rows per chunk
MAX_MEMORY_MB = 500  # Maximum memory usage per chunk in MB
DOWNLOAD_CHUNK_SIZE = 8192  # Download chunk size in bytes
PROGRESS_UPDATE_INTERVAL = 1024 * 1024  # Update progress every 1MB
MAX_WORKERS = 3  # Number of parallel workers for processing

# Thread-safe progress tracking
progress_lock = threading.Lock()
global_progress = {
    "total_files": 0,
    "completed_files": 0,
    "current_file": "",
    "file_progress": 0.0
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global engine
    # Use environment variable for database URL, fallback to localhost for development
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:1234@localhost:5432/yahoo")
    logger.info(f"Connecting to database: {DATABASE_URL.split('@')[1]}")
    
    try:
        engine = create_engine(DATABASE_URL)
        logger.info("Database engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise
    
    yield
    
    # Shutdown
    if engine:
        engine.dispose()
        logger.info("Database engine disposed")


app = FastAPI(lifespan=lifespan)


def update_global_progress(current_file, file_progress):
    """Update global progress tracking"""
    with progress_lock:
        global_progress["current_file"] = current_file
        global_progress["file_progress"] = file_progress
        
        # Calculate overall progress
        if global_progress["total_files"] > 0:
            overall_progress = (global_progress["completed_files"] + file_progress) / global_progress["total_files"] * 100
            logger.info(f"🔄 OVERALL PROGRESS: {overall_progress:.1f}% | File: {current_file} | File Progress: {file_progress:.1f}%")


def get_parquet_files():
    """Fetch parquet files list dynamically from HuggingFace API"""
    logger.info("Fetching parquet files list from HuggingFace API")
    try:
        response = requests.get(REPO_API)
        response.raise_for_status()
        files = response.json()
        logger.info(f"Successfully fetched {len(files)} files from API")
        
        parquet_files = [f["path"].split("/")[-1] for f in files if f["path"].endswith(".parquet")]
        logger.info(f"Found {len(parquet_files)} parquet files: {parquet_files}")
        return parquet_files
    except requests.RequestException as e:
        logger.error(f"Failed to fetch files from HuggingFace API: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while fetching files: {e}")
        raise


def get_file_size_mb(file_path):
    """Get file size in MB"""
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    return size_mb


def download_file_in_chunks(url, local_file_path, file_name):
    """Download large file in chunks with progress tracking"""
    logger.info(f"📥 Starting chunked download: {file_name}")
    
    try:
        # Get file size first
        head_response = requests.head(url, allow_redirects=True)
        total_size = int(head_response.headers.get('content-length', 0))
        
        if total_size > 0:
            total_size_mb = total_size / (1024 * 1024)
            logger.info(f"📊 File size: {total_size_mb:.2f} MB")
        else:
            logger.info(" File size unknown, downloading without progress tracking")
        
        # Download file in chunks
        downloaded_bytes = 0
        last_progress_update = 0
        
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            
            with open(local_file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if chunk:  # Filter out keep-alive chunks
                        file.write(chunk)
                        downloaded_bytes += len(chunk)
                        
                        # Update progress every PROGRESS_UPDATE_INTERVAL bytes
                        if downloaded_bytes - last_progress_update >= PROGRESS_UPDATE_INTERVAL:
                            if total_size > 0:
                                progress_percent = (downloaded_bytes / total_size) * 100
                                downloaded_mb = downloaded_bytes / (1024 * 1024)
                                logger.info(f" Download progress: {progress_percent:.1f}% ({downloaded_mb:.1f} MB)")
                                update_global_progress(file_name, progress_percent * 0.3)  # Download is 30% of total process
                            else:
                                downloaded_mb = downloaded_bytes / (1024 * 1024)
                                logger.info(f"📥 Downloaded: {downloaded_mb:.1f} MB")
                            
                            last_progress_update = downloaded_bytes
        
        # Final progress update
        final_size_mb = downloaded_bytes / (1024 * 1024)
        logger.info(f"✅ Download completed: {final_size_mb:.2f} MB")
        update_global_progress(file_name, 30.0)  # Download complete = 30%
        
        return {
            "status": "success",
            "downloaded_bytes": downloaded_bytes,
            "downloaded_mb": final_size_mb
        }
        
    except Exception as e:
        logger.error(f"❌ Error downloading file: {e}")
        # Clean up partial download
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            logger.info(f" Cleaned up partial download: {local_file_path}")
        
        return {
            "status": "error",
            "error": str(e)
        }


def process_single_chunk(chunk_data, table_name, chunk_idx, total_chunks, file_name):
    """Process a single chunk of data"""
    start_row, end_row, file_path = chunk_data
    
    try:
        logger.info(f" Processing chunk {chunk_idx + 1}/{total_chunks}: rows {start_row:,} to {end_row:,}")
        
        # Read chunk from parquet file
        chunk_df = pd.read_parquet(file_path)
        chunk_df = chunk_df.iloc[start_row:end_row]
        
        chunk_rows = len(chunk_df)
        logger.info(f"📊 Loaded chunk with {chunk_rows:,} rows")
        
        # Insert chunk into database
        if chunk_idx == 0:
            # First chunk: create table
            chunk_df.to_sql(table_name, engine, if_exists="replace", index=False)
            logger.info(f"🗄️ Created table {table_name} with {chunk_rows:,} rows")
        else:
            # Subsequent chunks: append to existing table
            chunk_df.to_sql(table_name, engine, if_exists="append", index=False)
            logger.info(f"➕ Appended {chunk_rows:,} rows to table {table_name}")
        
        # Update progress (processing is 70% of total process, starting from 30%)
        chunk_progress = 30.0 + (chunk_idx + 1) / total_chunks * 70.0
        update_global_progress(file_name, chunk_progress)
        
        # Memory cleanup
        del chunk_df
        
        return {
            "chunk": chunk_idx + 1,
            "rows": chunk_rows,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"❌ Error processing chunk {chunk_idx + 1}: {e}")
        return {
            "chunk": chunk_idx + 1,
            "rows": 0,
            "status": "error",
            "error": str(e)
        }


def process_parquet_in_chunks_parallel(file_path, table_name, file_name):
    """Process large parquet file in chunks with parallel processing"""
    logger.info(f"🔄 Starting parallel processing: {file_name}")
    
    # Get file size
    file_size_mb = get_file_size_mb(file_path)
    logger.info(f"📊 File size: {file_size_mb:.2f} MB")
    
    try:
        # Get total number of rows without loading all data
        parquet_file = pd.read_parquet(file_path)
        total_rows = len(parquet_file)
        total_columns = len(parquet_file.columns)
        
        logger.info(f" Total rows: {total_rows:,}, Total columns: {total_columns}")
        
        # Calculate optimal chunk size based on file size and memory constraints
        if file_size_mb > MAX_MEMORY_MB:
            # For large files, use smaller chunks
            optimal_chunk_size = max(1000, min(CHUNK_SIZE, int(MAX_MEMORY_MB * 1000000 / (total_columns * 8))))
            logger.info(f"🔧 Large file detected, using chunk size: {optimal_chunk_size}")
        else:
            optimal_chunk_size = CHUNK_SIZE
            logger.info(f"🔧 Using default chunk size: {optimal_chunk_size}")
        
        # Calculate number of chunks needed
        num_chunks = math.ceil(total_rows / optimal_chunk_size)
        logger.info(f"🔧 Will process in {num_chunks} chunks of ~{optimal_chunk_size} rows each")
        
        # Prepare chunk data for parallel processing
        chunk_data_list = []
        for chunk_idx in range(num_chunks):
            start_row = chunk_idx * optimal_chunk_size
            end_row = min((chunk_idx + 1) * optimal_chunk_size, total_rows)
            chunk_data_list.append((start_row, end_row, file_path))
        
        # Process chunks in parallel
        total_inserted = 0
        chunk_results = []
        
        logger.info(f"🚀 Starting parallel processing with {MAX_WORKERS} workers")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all chunks for processing
            future_to_chunk = {
                executor.submit(process_single_chunk, chunk_data, table_name, chunk_idx, num_chunks, file_name): chunk_idx
                for chunk_idx, chunk_data in enumerate(chunk_data_list)
            }
            
            # Collect results as chunks complete
            for future in as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    result = future.result()
                    chunk_results.append(result)
                    total_inserted += result["rows"]
                    
                    if result["status"] == "success":
                        logger.info(f"✅ Chunk {chunk_idx + 1}/{num_chunks} completed: {result['rows']:,} rows")
                    else:
                        logger.error(f"❌ Chunk {chunk_idx + 1}/{num_chunks} failed: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    logger.error(f"❌ Exception in chunk {chunk_idx + 1}: {e}")
                    chunk_results.append({
                        "chunk": chunk_idx + 1,
                        "rows": 0,
                        "status": "error",
                        "error": str(e)
                    })
        
        # Sort results by chunk number
        chunk_results.sort(key=lambda x: x["chunk"])
        
        logger.info(f"✅ Completed parallel processing: {total_inserted:,} total rows inserted")
        update_global_progress(file_name, 100.0)  # Processing complete = 100%
        
        return {
            "status": "success",
            "total_rows": total_rows,
            "total_inserted": total_inserted,
            "chunks_processed": num_chunks,
            "chunk_results": chunk_results,
            "file_size_mb": file_size_mb
        }
        
    except Exception as e:
        logger.error(f"❌ Error processing parquet file {file_path}: {e}")
        return {
            "status": "error",
            "error": str(e),
            "file_size_mb": file_size_mb
        }


@app.get("/load_parquet")
def load_parquet_to_postgres():
    if not engine:
        logger.error("Database engine not initialized")
        return {"status": "error", "error": "Database not connected"}
    
    start_time = datetime.now()
    logger.info("🚀 Starting chunked parquet loading process with parallel processing")
    
    try:
        parquet_files = get_parquet_files()
        
        # Initialize global progress tracking
        with progress_lock:
            global_progress["total_files"] = len(parquet_files)
            global_progress["completed_files"] = 0
            global_progress["current_file"] = ""
            global_progress["file_progress"] = 0.0
        
        results = {}
        
        logger.info(f"📊 Processing {len(parquet_files)} files with chunked download and parallel processing")

        for i, file in enumerate(parquet_files, 1):
            logger.info(f" Processing file {i}/{len(parquet_files)}: {file}")
            
            url = BASE_URL + file
            local_file = f"/tmp/{file}"

            try:
                # Download file in chunks
                download_result = download_file_in_chunks(url, local_file, file)
                
                if download_result["status"] != "success":
                    results[file] = {
                        "status": "error",
                        "message": f"Download failed: {download_result['error']}"
                    }
                    # Update progress for failed file
                    with progress_lock:
                        global_progress["completed_files"] += 1
                    continue
                
                file_size_mb = download_result["downloaded_mb"]
                logger.info(f"✅ Successfully downloaded {file} ({file_size_mb:.2f} MB)")

                # Table name = file name without extension
                table_name = os.path.splitext(file)[0]
                logger.info(f"️ Processing table: {table_name}")

                # Process parquet file in chunks with parallel processing
                process_result = process_parquet_in_chunks_parallel(local_file, table_name, file)
                
                if process_result["status"] == "success":
                    results[file] = {
                        "status": "success",
                        "message": f"Inserted {process_result['total_inserted']:,} rows into {table_name}",
                        "total_rows": process_result["total_rows"],
                        "chunks_processed": process_result["chunks_processed"],
                        "file_size_mb": process_result["file_size_mb"],
                        "download_size_mb": file_size_mb
                    }
                else:
                    results[file] = {
                        "status": "error",
                        "message": f"Error processing {file}: {process_result['error']}",
                        "file_size_mb": process_result.get("file_size_mb", 0),
                        "download_size_mb": file_size_mb
                    }

                # Cleanup
                os.remove(local_file)
                logger.info(f"🧹 Cleaned up temporary file: {local_file}")
                
                # Update completed files count
                with progress_lock:
                    global_progress["completed_files"] += 1

            except Exception as e:
                logger.error(f"❌ Error processing file {file}: {e}")
                results[file] = {
                    "status": "error",
                    "message": f"Error: {str(e)}"
                }
                # Clean up temp file if it exists
                if os.path.exists(local_file):
                    os.remove(local_file)
                    logger.info(f"🧹 Cleaned up failed download: {local_file}")
                
                # Update completed files count
                with progress_lock:
                    global_progress["completed_files"] += 1

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Calculate summary statistics
        successful_files = [r for r in results.values() if r.get("status") == "success"]
        total_rows_processed = sum(r.get("total_rows", 0) for r in successful_files)
        total_chunks_processed = sum(r.get("chunks_processed", 0) for r in successful_files)
        total_downloaded_mb = sum(r.get("download_size_mb", 0) for r in successful_files)
        
        logger.info(f"🎉 Chunked parquet loading process completed in {duration:.2f} seconds")
        logger.info(f"✅ Successfully processed {len(successful_files)} files")
        logger.info(f"📊 Total rows processed: {total_rows_processed:,}")
        logger.info(f"🔄 Total chunks processed: {total_chunks_processed}")
        logger.info(f"📥 Total data downloaded: {total_downloaded_mb:.2f} MB")
        
        return {
            "status": "success",
            "tables_created": len(results),
            "successful_files": len(successful_files),
            "total_rows_processed": total_rows_processed,
            "total_chunks_processed": total_chunks_processed,
            "total_downloaded_mb": total_downloaded_mb,
            "duration_seconds": duration,
            "details": results,
            "timestamp": end_time.isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Fatal error in load_parquet_to_postgres: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/progress")
def get_progress():
    """Get current processing progress"""
    with progress_lock:
        if global_progress["total_files"] > 0:
            overall_progress = (global_progress["completed_files"] + global_progress["file_progress"] / 100) / global_progress["total_files"] * 100
        else:
            overall_progress = 0
        
        return {
            "overall_progress_percent": overall_progress,
            "total_files": global_progress["total_files"],
            "completed_files": global_progress["completed_files"],
            "current_file": global_progress["current_file"],
            "current_file_progress_percent": global_progress["file_progress"],
            "timestamp": datetime.now().isoformat()
        }


@app.get("/load_parquet_config")
def get_processing_config():
    """Get current chunked processing configuration"""
    return {
        "chunk_size": CHUNK_SIZE,
        "max_memory_mb": MAX_MEMORY_MB,
        "download_chunk_size": DOWNLOAD_CHUNK_SIZE,
        "progress_update_interval": PROGRESS_UPDATE_INTERVAL,
        "max_workers": MAX_WORKERS,
        "description": "Configuration for chunked download and parallel processing of large parquet files"
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    logger.info("Health check requested")
    if not engine:
        logger.error("Health check failed - database engine not initialized")
        return {"status": "unhealthy", "database": "not_initialized", "timestamp": datetime.now().isoformat()}
    
    try:
        # Test database connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("Health check passed - database connection OK")
        return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "database": "disconnected", "error": str(e), "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    logger.info("Starting FastAPI application")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
