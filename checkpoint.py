import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from config import CHECKPOINT_DIR

logger = logging.getLogger(__name__)

class CheckpointManager:
    """Manages processing checkpoints for resumable operations"""
    
    def __init__(self):
        self.checkpoint_dir = CHECKPOINT_DIR
        self.ensure_checkpoint_dir()
    
    def ensure_checkpoint_dir(self):
        """Ensure checkpoint directory exists"""
        if not os.path.exists(self.checkpoint_dir):
            os.makedirs(self.checkpoint_dir)
            logger.info(f"Created checkpoint directory: {self.checkpoint_dir}")
    
    def create_session_id(self) -> str:
        """Create a unique session ID for this processing run"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"session_{timestamp}"
    
    def save_checkpoint(self, session_id: str, completed_files: List[Dict], 
                       current_file: str = None, file_progress: float = 0.0, 
                       total_files: int = 0) -> bool:
        """Save processing checkpoint"""
        checkpoint_data = {
            "session_id": session_id,
            "timestamp": datetime.now().isoformat(),
            "completed_files": completed_files,
            "current_file": current_file,
            "file_progress": file_progress,
            "total_files": total_files
        }
        
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{session_id}_checkpoint.json")
        
        try:
            with open(checkpoint_path, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            logger.info(f"💾 Checkpoint saved: {len(completed_files)} files completed")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")
            return False
    
    def load_checkpoint(self, session_id: str = None) -> Optional[Dict]:
        """Load processing checkpoint"""
        if session_id:
            checkpoint_path = os.path.join(self.checkpoint_dir, f"{session_id}_checkpoint.json")
        else:
            # Find the most recent checkpoint
            checkpoint_files = [f for f in os.listdir(self.checkpoint_dir) 
                              if f.endswith('_checkpoint.json')]
            if not checkpoint_files:
                return None
            
            # Sort by modification time and get the most recent
            checkpoint_files.sort(key=lambda x: os.path.getmtime(
                os.path.join(self.checkpoint_dir, x)), reverse=True)
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_files[0])
        
        try:
            if os.path.exists(checkpoint_path):
                with open(checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                logger.info(f"�� Checkpoint loaded: {len(checkpoint_data.get('completed_files', []))} files completed")
                return checkpoint_data
        except Exception as e:
            logger.error(f"❌ Failed to load checkpoint: {e}")
        
        return None
    
    def list_checkpoints(self) -> List[Dict]:
        """List all available checkpoints"""
        checkpoint_files = [f for f in os.listdir(self.checkpoint_dir) 
                          if f.endswith('_checkpoint.json')]
        checkpoints = []
        
        for checkpoint_file in checkpoint_files:
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_file)
            try:
                with open(checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                
                # Get file modification time
                mod_time = os.path.getmtime(checkpoint_path)
                
                checkpoints.append({
                    "session_id": checkpoint_data.get("session_id"),
                    "timestamp": checkpoint_data.get("timestamp"),
                    "completed_files": len(checkpoint_data.get("completed_files", [])),
                    "total_files": checkpoint_data.get("total_files", 0),
                    "current_file": checkpoint_data.get("current_file"),
                    "file_progress": checkpoint_data.get("file_progress", 0),
                    "modified_time": datetime.fromtimestamp(mod_time).isoformat()
                })
            except Exception as e:
                logger.error(f"Error reading checkpoint {checkpoint_file}: {e}")
        
        # Sort by modification time (most recent first)
        checkpoints.sort(key=lambda x: x["modified_time"], reverse=True)
        return checkpoints
    
    def clear_checkpoints(self) -> int:
        """Clear all checkpoints"""
        checkpoint_files = [f for f in os.listdir(self.checkpoint_dir) 
                          if f.endswith('_checkpoint.json')]
        
        deleted_count = 0
        for checkpoint_file in checkpoint_files:
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_file)
            try:
                os.remove(checkpoint_path)
                logger.info(f"Deleted checkpoint: {checkpoint_file}")
                deleted_count += 1
            except Exception as e:
                logger.error(f"Error deleting checkpoint {checkpoint_file}: {e}")
        
        return deleted_count
    
    def is_file_processed(self, file_name: str, completed_files: List[Dict]) -> bool:
        """Check if file is already processed"""
        for completed_file in completed_files:
            if completed_file.get("file") == file_name:
                return True
        return False

# Global checkpoint manager instance
checkpoint_manager = CheckpointManager()
