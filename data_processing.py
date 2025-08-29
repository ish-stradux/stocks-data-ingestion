import pandas as pd
import math
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple
from config import CHUNK_SIZE, MAX_MEMORY_MB, MAX_WORKERS
from database import db_manager
from progress import progress_tracker

logger = logging.getLogger(__name__)

class DataProcessor:
    """Handles data processing operations"""
    
    def __init__(self):
        self.chunk_size = CHUNK_SIZE
        self.max_memory_mb = MAX_MEMORY_MB
        self.max_workers = MAX_WORKERS
    
    def process_single_chunk(self, chunk_data: Tuple, table_name: str, chunk_idx: int, 
                           total_chunks: int, file_name: str) -> Dict[str, Any]:
        """Process a single chunk of data"""
        start_row, end_row, file_path = chunk_data
        
        try:
            logger.info(f"�� Processing chunk {chunk_idx + 1}/{total_chunks}: rows {start_row:,} to {end_row:,}")
            
            # Read chunk from parquet file
            chunk_df = pd.read_parquet(file_path)
            chunk_df = chunk_df.iloc[start_row:end_row]
            
            chunk_rows = len(chunk_df)
            logger.info(f"📊 Loaded chunk with {chunk_rows:,} rows")
            
            # Insert chunk into database
            if chunk_idx == 0:
                # First chunk: create table
                chunk_df.to_sql(table_name, db_manager.engine, if_exists="replace", index=False)
                logger.info(f"🗄️ Created table {table_name} with {chunk_rows:,} rows")
            else:
                # Subsequent chunks: append to existing table
                chunk_df.to_sql(table_name, db_manager.engine, if_exists="append", index=False)
                logger.info(f"➕ Appended {chunk_rows:,} rows to table {table_name}")
            
            # Update progress (processing is 70% of total process, starting from 30%)
            chunk_progress = 30.0 + (chunk_idx + 1) / total_chunks * 70.0
            progress_tracker.update_file_progress(file_name, chunk_progress)
            
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
    
    def process_parquet_in_chunks_parallel(self, file_path: str, table_name: str, file_name: str) -> Dict[str, Any]:
        """Process large parquet file in chunks with parallel processing"""
        logger.info(f"🔄 Starting parallel processing: {file_name}")
        
        # Get file size
        from file_operations import file_ops
        file_size_mb = file_ops.get_file_size_mb(file_path)
        logger.info(f"📊 File size: {file_size_mb:.2f} MB")
        
        try:
            # Get total number of rows without loading all data
            parquet_file = pd.read_parquet(file_path)
            total_rows = len(parquet_file)
            total_columns = len(parquet_file.columns)
            
            logger.info(f"�� Total rows: {total_rows:,}, Total columns: {total_columns}")
            
            # Calculate optimal chunk size based on file size and memory constraints
            if file_size_mb > self.max_memory_mb:
                # For large files, use smaller chunks
                optimal_chunk_size = max(1000, min(self.chunk_size, 
                                                 int(self.max_memory_mb * 1000000 / (total_columns * 8))))
                logger.info(f"🔧 Large file detected, using chunk size: {optimal_chunk_size}")
            else:
                optimal_chunk_size = self.chunk_size
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
            
            logger.info(f"🚀 Starting parallel processing with {self.max_workers} workers")
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all chunks for processing
                future_to_chunk = {
                    executor.submit(self.process_single_chunk, chunk_data, table_name, chunk_idx, num_chunks, file_name): chunk_idx
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
            progress_tracker.update_file_progress(file_name, 100.0)  # Processing complete = 100%
            
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

# Global data processor instance
data_processor = DataProcessor()
