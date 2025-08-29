import logging
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI

from config import get_config, LOG_FORMAT, LOG_FILE
from database import db_manager
from checkpoint import checkpoint_manager
from progress import progress_tracker
from file_operations import file_ops
from data_processing import data_processor
from api_service import api_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Starting application")
    
    if not db_manager.connect():
        raise Exception("Failed to connect to database")
    
    yield
    
    # Shutdown
    db_manager.disconnect()
    logger.info("🛑 Application shutdown complete")

app = FastAPI(lifespan=lifespan)

@app.get("/load_parquet")
def load_parquet_to_postgres(resume: bool = True, session_id: str = None):
    """
    Load parquet files with checkpoint support
    Args:
        resume: Whether to resume from previous checkpoint (default: True)
        session_id: Specific session ID to resume (optional)
    """
    if not db_manager.is_connected():
        logger.error("Database not connected")
        return {"status": "error", "error": "Database not connected"}
    
    start_time = datetime.now()
    
    # Create or use session ID
    if session_id:
        current_session_id = session_id
        logger.info(f"🔄 Using provided session ID: {current_session_id}")
    else:
        current_session_id = checkpoint_manager.create_session_id()
        logger.info(f"🆕 Created new session ID: {current_session_id}")
    
    # Load checkpoint if resuming
    completed_files = []
    if resume:
        checkpoint_data = checkpoint_manager.load_checkpoint(session_id)
        if checkpoint_data:
            completed_files = checkpoint_data.get("completed_files", [])
            logger.info(f"📂 Resuming from checkpoint: {len(completed_files)} files already completed")
        else:
            logger.info("📂 No checkpoint found, starting fresh")
    
    logger.info("🚀 Starting chunked parquet loading process with checkpoint support")
    
    try:
        parquet_files = api_service.get_parquet_files()
        
        # Filter out already completed files
        remaining_files = []
        for file in parquet_files:
            if not checkpoint_manager.is_file_processed(file, completed_files):
                remaining_files.append(file)
            else:
                logger.info(f"⏭️ Skipping already processed file: {file}")
        
        # Initialize progress tracking
        progress_tracker.initialize(len(parquet_files), current_session_id)
        progress_tracker.update_completed_files(len(completed_files))
        
        results = {}
        
        logger.info(f"📊 Processing {len(remaining_files)} remaining files (out of {len(parquet_files)} total)")
        logger.info(f"✅ Already completed: {len(completed_files)} files")

        for i, file in enumerate(remaining_files, 1):
            logger.info(f" Processing file {i}/{len(remaining_files)}: {file}")
            
            from config import BASE_URL
            url = BASE_URL + file
            local_file = f"/tmp/{file}"

            try:
                # Download file in chunks
                download_result = file_ops.download_file_in_chunks(url, local_file, file)
                
                if download_result["status"] != "success":
                    results[file] = {
                        "status": "error",
                        "message": f"Download failed: {download_result['error']}"
                    }
                    # Save checkpoint
                    checkpoint_manager.save_checkpoint(current_session_id, completed_files, file, 0.0, len(parquet_files))
                    continue
                
                file_size_mb = download_result["downloaded_mb"]
                logger.info(f"✅ Successfully downloaded {file} ({file_size_mb:.2f} MB)")

                # Table name = file name without extension
                table_name = file.split('.')[0]
                logger.info(f"️ Processing table: {table_name}")

                # Process parquet file in chunks with parallel processing
                process_result = data_processor.process_parquet_in_chunks_parallel(local_file, table_name, file)
                
                if process_result["status"] == "success":
                    file_result = {
                        "status": "success",
                        "message": f"Inserted {process_result['total_inserted']:,} rows into {table_name}",
                        "total_rows": process_result["total_rows"],
                        "chunks_processed": process_result["chunks_processed"],
                        "file_size_mb": process_result["file_size_mb"],
                        "download_size_mb": file_size_mb,
                        "processed_at": datetime.now().isoformat()
                    }
                    results[file] = file_result
                    
                    # Add to completed files
                    completed_files.append({
                        "file": file,
                        "table_name": table_name,
                        "total_rows": process_result["total_rows"],
                        "file_size_mb": file_size_mb,
                        "processed_at": file_result["processed_at"]
                    })
                else:
                    results[file] = {
                        "status": "error",
                        "message": f"Error processing {file}: {process_result['error']}",
                        "file_size_mb": process_result.get("file_size_mb", 0),
                        "download_size_mb": file_size_mb
                    }

                # Cleanup
                file_ops.cleanup_file(local_file)
                
                # Update completed files count and save checkpoint
                progress_tracker.update_completed_files(len(completed_files))
                checkpoint_manager.save_checkpoint(current_session_id, completed_files, None, 100.0, len(parquet_files))

            except Exception as e:
                logger.error(f"❌ Error processing file {file}: {e}")
                results[file] = {
                    "status": "error",
                    "message": f"Error: {str(e)}"
                }
                # Clean up temp file if it exists
                file_ops.cleanup_file(local_file)
                
                # Save checkpoint even on error
                checkpoint_manager.save_checkpoint(current_session_id, completed_files, file, 0.0, len(parquet_files))

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Calculate summary statistics
        successful_files = [r for r in results.values() if r.get("status") == "success"]
        total_rows_processed = sum(r.get("total_rows", 0) for r in successful_files)
        total_chunks_processed = sum(r.get("chunks_processed", 0) for r in successful_files)
        total_downloaded_mb = sum(r.get("download_size_mb", 0) for r in successful_files)
        
        logger.info(f"🎉 Chunked parquet loading process completed in {duration:.2f} seconds")
        logger.info(f"✅ Successfully processed {len(successful_files)} files in this session")
        logger.info(f"✅ Total completed files: {len(completed_files)}")
        logger.info(f"📊 Total rows processed: {total_rows_processed:,}")
        logger.info(f"🔄 Total chunks processed: {total_chunks_processed}")
        logger.info(f"📥 Total data downloaded: {total_downloaded_mb:.2f} MB")
        
        return {
            "status": "success",
            "session_id": current_session_id,
            "tables_created": len(results),
            "successful_files": len(successful_files),
            "total_completed_files": len(completed_files),
            "total_rows_processed": total_rows_processed,
            "total_chunks_processed": total_chunks_processed,
            "total_downloaded_mb": total_downloaded_mb,
            "duration_seconds": duration,
            "details": results,
            "completed_files": completed_files,
            "timestamp": end_time.isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Fatal error in load_parquet_to_postgres: {e}")
        return {
            "status": "error",
            "error": str(e),
            "session_id": current_session_id,
            "timestamp": datetime.now().isoformat()
        }

@app.get("/checkpoints")
def list_checkpoints():
    """List all available checkpoints"""
    checkpoints = checkpoint_manager.list_checkpoints()
    return {
        "checkpoints": checkpoints,
        "total_checkpoints": len(checkpoints)
    }

@app.get("/clear_checkpoints")
def clear_checkpoints():
    """Clear all checkpoints"""
    deleted_count = checkpoint_manager.clear_checkpoints()
    return {
        "status": "success",
        "deleted_checkpoints": deleted_count,
        "message": f"Cleared {deleted_count} checkpoints"
    }

@app.get("/progress")
def get_progress():
    """Get current processing progress"""
    return progress_tracker.get_progress()

@app.get("/load_parquet_config")
def get_processing_config():
    """Get current chunked processing configuration"""
    return get_config()

@app.get("/health")
def health_check():
    """Health check endpoint"""
    logger.info("Health check requested")
    if not db_manager.is_connected():
        logger.error("Health check failed - database not connected")
        return {"status": "unhealthy", "database": "not_connected", "timestamp": datetime.now().isoformat()}
    
    if db_manager.test_connection():
        logger.info("Health check passed - database connection OK")
        return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
    else:
        logger.error("Health check failed - database connection test failed")
        return {"status": "unhealthy", "database": "disconnected", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    logger.info("Starting FastAPI application")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
