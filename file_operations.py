import os
import requests
import logging
from typing import Dict, Any
from config import BASE_URL, DOWNLOAD_CHUNK_SIZE, PROGRESS_UPDATE_INTERVAL
from progress import progress_tracker

logger = logging.getLogger(__name__)

class FileOperations:
    """Handles file download and operations"""
    
    def __init__(self):
        self.base_url = BASE_URL
        self.download_chunk_size = DOWNLOAD_CHUNK_SIZE
        self.progress_update_interval = PROGRESS_UPDATE_INTERVAL
    
    def get_file_size_mb(self, file_path: str) -> float:
        """Get file size in MB"""
        size_bytes = os.path.getsize(file_path)
        size_mb = size_bytes / (1024 * 1024)
        return size_mb
    
    def download_file_in_chunks(self, url: str, local_file_path: str, file_name: str) -> Dict[str, Any]:
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
                logger.info("�� File size unknown, downloading without progress tracking")
            
            # Download file in chunks
            downloaded_bytes = 0
            last_progress_update = 0
            
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                
                with open(local_file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=self.download_chunk_size):
                        if chunk:  # Filter out keep-alive chunks
                            file.write(chunk)
                            downloaded_bytes += len(chunk)
                            
                            # Update progress every PROGRESS_UPDATE_INTERVAL bytes
                            if downloaded_bytes - last_progress_update >= self.progress_update_interval:
                                if total_size > 0:
                                    progress_percent = (downloaded_bytes / total_size) * 100
                                    downloaded_mb = downloaded_bytes / (1024 * 1024)
                                    logger.info(f"�� Download progress: {progress_percent:.1f}% ({downloaded_mb:.1f} MB)")
                                    progress_tracker.update_file_progress(file_name, progress_percent * 0.3)  # Download is 30% of total process
                                else:
                                    downloaded_mb = downloaded_bytes / (1024 * 1024)
                                    logger.info(f"📥 Downloaded: {downloaded_mb:.1f} MB")
                                
                                last_progress_update = downloaded_bytes
            
            # Final progress update
            final_size_mb = downloaded_bytes / (1024 * 1024)
            logger.info(f"✅ Download completed: {final_size_mb:.2f} MB")
            progress_tracker.update_file_progress(file_name, 30.0)  # Download complete = 30%
            
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
                logger.info(f"�� Cleaned up partial download: {local_file_path}")
            
            return {
                "status": "error",
                "error": str(e)
            }
    
    def cleanup_file(self, file_path: str):
        """Clean up temporary file"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"🧹 Cleaned up temporary file: {file_path}")
        except Exception as e:
            logger.error(f"❌ Error cleaning up file {file_path}: {e}")

# Global file operations instance
file_ops = FileOperations()
