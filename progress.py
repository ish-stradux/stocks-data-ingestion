import threading
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Thread-safe progress tracking for processing operations"""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.progress = {
            "total_files": 0,
            "completed_files": 0,
            "current_file": "",
            "file_progress": 0.0,
            "session_id": None
        }
    
    def initialize(self, total_files: int, session_id: str):
        """Initialize progress tracking"""
        with self.lock:
            self.progress["total_files"] = total_files
            self.progress["completed_files"] = 0
            self.progress["current_file"] = ""
            self.progress["file_progress"] = 0.0
            self.progress["session_id"] = session_id
    
    def update_file_progress(self, current_file: str, file_progress: float):
        """Update progress for current file"""
        with self.lock:
            self.progress["current_file"] = current_file
            self.progress["file_progress"] = file_progress
            
            # Calculate overall progress
            if self.progress["total_files"] > 0:
                overall_progress = (self.progress["completed_files"] + file_progress) / self.progress["total_files"] * 100
                logger.info(f"🔄 OVERALL PROGRESS: {overall_progress:.1f}% | File: {current_file} | File Progress: {file_progress:.1f}%")
    
    def update_completed_files(self, completed_count: int):
        """Update completed files count"""
        with self.lock:
            self.progress["completed_files"] = completed_count
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current progress information"""
        with self.lock:
            if self.progress["total_files"] > 0:
                overall_progress = (self.progress["completed_files"] + self.progress["file_progress"] / 100) / self.progress["total_files"] * 100
            else:
                overall_progress = 0
            
            return {
                "session_id": self.progress["session_id"],
                "overall_progress_percent": overall_progress,
                "total_files": self.progress["total_files"],
                "completed_files": self.progress["completed_files"],
                "current_file": self.progress["current_file"],
                "current_file_progress_percent": self.progress["file_progress"],
                "timestamp": datetime.now().isoformat()
            }

# Global progress tracker instance
progress_tracker = ProgressTracker()
