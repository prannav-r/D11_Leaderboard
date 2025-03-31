import sqlite3
import shutil
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from contextlib import contextmanager
import logging
from config import Config

# Set up logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = sqlite3.connect(Config.DB_PATH, timeout=Config.DB_TIMEOUT)
        conn.row_factory = sqlite3.Row  # Enable dictionary-like access to rows
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise DatabaseError(f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

def backup_database() -> str:
    """Create a backup of the database"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{Config.DB_BACKUP_PATH}/dream11_backup_{timestamp}.db"
        shutil.copy2(Config.DB_PATH, backup_file)
        logger.info(f"Database backup created: {backup_file}")
        return backup_file
    except Exception as e:
        logger.error(f"Failed to create database backup: {str(e)}")
        raise DatabaseError(f"Backup failed: {str(e)}")

def init_db() -> None:
    """Initialize database with required tables and indexes"""
    with get_db_connection() as conn:
        c = conn.cursor()
        
        # Create points table with constraints
        c.execute('''CREATE TABLE IF NOT EXISTS points
                     (user TEXT PRIMARY KEY,
                      points INTEGER DEFAULT 0 CHECK (points >= 0))''')
        
        # Create history table with constraints
        c.execute('''CREATE TABLE IF NOT EXISTS history
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user TEXT NOT NULL,
                      points_change INTEGER NOT NULL,
                      timestamp TEXT NOT NULL,
                      match_number INTEGER,
                      recorded_by TEXT NOT NULL,
                      FOREIGN KEY (match_number) REFERENCES match_results(match_number))''')
        
        # Create match_results table with constraints
        c.execute('''CREATE TABLE IF NOT EXISTS match_results
                     (match_number INTEGER PRIMARY KEY,
                      winner TEXT NOT NULL,
                      timestamp TEXT NOT NULL,
                      recorded_by TEXT NOT NULL)''')
        
        # Create indexes for better performance
        c.execute('CREATE INDEX IF NOT EXISTS idx_history_user ON history(user)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_history_timestamp ON history(timestamp)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_history_match ON history(match_number)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_match_results_timestamp ON match_results(timestamp)')
        
        conn.commit()
        logger.info("Database initialized successfully")

def get_points() -> Dict[str, int]:
    """Get all user points"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT user, points FROM points')
            return {row['user']: row['points'] for row in c.fetchall()}
    except Exception as e:
        logger.error(f"Error getting points: {str(e)}")
        raise DatabaseError(f"Failed to get points: {str(e)}")

def update_points(user: str, points_change: int, match_number: Optional[int] = None, recorded_by: str = 'System') -> None:
    """Update points for a user"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            
            # Start transaction
            c.execute('BEGIN TRANSACTION')
            
            try:
                # Update points
                c.execute('''INSERT INTO points (user, points)
                            VALUES (?, ?)
                            ON CONFLICT(user) DO UPDATE SET
                            points = points + ?''',
                         (user, points_change, points_change))
                
                # Record history
                c.execute('''INSERT INTO history (user, points_change, timestamp, match_number, recorded_by)
                            VALUES (?, ?, ?, ?, ?)''',
                         (user, points_change, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                          match_number, recorded_by))
                
                # If this is a match win, record it
                if match_number is not None:
                    c.execute('''INSERT INTO match_results (match_number, winner, timestamp, recorded_by)
                                VALUES (?, ?, ?, ?)''',
                             (match_number, user, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              recorded_by))
                
                conn.commit()
                logger.info(f"Updated points for {user}: {points_change}")
                
            except Exception as e:
                conn.rollback()
                raise e
                
    except Exception as e:
        logger.error(f"Error updating points: {str(e)}")
        raise DatabaseError(f"Failed to update points: {str(e)}")

def clear_points() -> None:
    """Clear all points and history"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM points')
            c.execute('DELETE FROM history')
            c.execute('DELETE FROM match_results')
            conn.commit()
            logger.info("All points cleared successfully")
    except Exception as e:
        logger.error(f"Error clearing points: {str(e)}")
        raise DatabaseError(f"Failed to clear points: {str(e)}")

def undo_last_point() -> Tuple[bool, str]:
    """Undo the last point change"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            
            # Start transaction
            c.execute('BEGIN TRANSACTION')
            
            try:
                # Get the last history entry
                c.execute('''SELECT id, user, points_change, match_number
                            FROM history
                            ORDER BY id DESC LIMIT 1''')
                last_entry = c.fetchone()
                
                if not last_entry:
                    return False, "No points to undo"
                
                # Revert points
                c.execute('''UPDATE points
                            SET points = points - ?
                            WHERE user = ?''',
                         (last_entry['points_change'], last_entry['user']))
                
                # If this was a match win, remove it from match_results
                if last_entry['match_number'] is not None:
                    c.execute('DELETE FROM match_results WHERE match_number = ?',
                             (last_entry['match_number'],))
                
                # Remove the history entry
                c.execute('DELETE FROM history WHERE id = ?', (last_entry['id'],))
                
                conn.commit()
                logger.info(f"Undid {last_entry['points_change']} point(s) for {last_entry['user']}")
                return True, f"Undid {last_entry['points_change']} point(s) for {last_entry['user']}"
                
            except Exception as e:
                conn.rollback()
                raise e
                
    except Exception as e:
        logger.error(f"Error undoing last point: {str(e)}")
        raise DatabaseError(f"Failed to undo last point: {str(e)}")

def get_match_results() -> List[Tuple[int, str, str]]:
    """Get all match results"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT match_number, winner, timestamp FROM match_results')
            return [(row['match_number'], row['winner'], row['timestamp']) for row in c.fetchall()]
    except Exception as e:
        logger.error(f"Error getting match results: {str(e)}")
        raise DatabaseError(f"Failed to get match results: {str(e)}") 