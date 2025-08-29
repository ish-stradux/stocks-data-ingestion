import os
from typing import Dict, Any

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:1234@localhost:5432/yahoo")

# HuggingFace Configuration
REPO_API = "https://huggingface.co/api/datasets/bwzheng2010/yahoo-finance-data/tree/main/data"
BASE_URL = "https://huggingface.co/datasets/bwzheng2010/yahoo-finance-data/resolve/main/data/"

# Processing Configuration
CHUNK_SIZE = 10000  # Number of rows per chunk
MAX_MEMORY_MB = 500  # Maximum memory usage per chunk in MB
DOWNLOAD_CHUNK_SIZE = 8192  # Download chunk size in bytes
PROGRESS_UPDATE_INTERVAL = 1024 * 1024  # Update progress every 1MB
MAX_WORKERS = 3  # Number of parallel workers for processing

# Checkpoint Configuration
CHECKPOINT_FILE = "processing_checkpoint.json"
CHECKPOINT_DIR = "/tmp/checkpoints"

# Logging Configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = 'app.log'

def get_config() -> Dict[str, Any]:
    """Get all configuration settings"""
    return {
        "database_url": DATABASE_URL,
        "repo_api": REPO_API,
        "base_url": BASE_URL,
        "chunk_size": CHUNK_SIZE,
        "max_memory_mb": MAX_MEMORY_MB,
        "download_chunk_size": DOWNLOAD_CHUNK_SIZE,
        "progress_update_interval": PROGRESS_UPDATE_INTERVAL,
        "max_workers": MAX_WORKERS,
        "checkpoint_file": CHECKPOINT_FILE,
        "checkpoint_dir": CHECKPOINT_DIR,
        "log_format": LOG_FORMAT,
        "log_file": LOG_FILE
    }
