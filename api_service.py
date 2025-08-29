import requests
import logging
from typing import List
from config import REPO_API

logger = logging.getLogger(__name__)

class APIService:
    """Handles API interactions with external services"""
    
    def __init__(self):
        self.repo_api = REPO_API
    
    def get_parquet_files(self) -> List[str]:
        """Fetch parquet files list dynamically from HuggingFace API"""
        logger.info("Fetching parquet files list from HuggingFace API")
        try:
            response = requests.get(self.repo_api)
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

# Global API service instance
api_service = APIService()
