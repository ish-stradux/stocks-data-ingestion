from sqlalchemy import create_engine
import logging
from typing import Optional
from config import DATABASE_URL

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.engine: Optional[object] = None
    
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.engine = create_engine(DATABASE_URL)
            logger.info(f"Connected to database: {DATABASE_URL.split('@')[1]}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
    
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self.engine is not None
    
    def test_connection(self) -> bool:
        """Test database connection"""
        if not self.engine:
            return False
        
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

# Global database manager instance
db_manager = DatabaseManager()
