import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from contextlib import contextmanager
from config import Config

class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = sqlite3.connect(Config.DB_PATH)
        conn.row_factory = sqlite3.Row  # Enable dictionary-like access to rows
        yield conn
    except sqlite3.Error as e:
        raise DatabaseError(f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

def init_db() -> None:
    """Initialize database with required tables"""
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
        
        conn.commit()

def get_points() -> Dict[str, int]:
    """Get all points from database"""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user, points FROM points')
        return dict(c.fetchall())

def update_points(user: str, points_change: int, match_number: Optional[int] = None, recorded_by: str = None) -> None:
    """Update points for a user with validation"""
    if abs(points_change) > Config.MAX_POINTS_PER_UPDATE:
        raise ValueError(f"Points change exceeds maximum allowed value of {Config.MAX_POINTS_PER_UPDATE}")
    
    with get_db_connection() as conn:
        c = conn.cursor()
        
        # Start transaction
        c.execute('BEGIN TRANSACTION')
        
        try:
            # Update or insert points
            c.execute('''INSERT INTO points (user, points)
                        VALUES (?, ?)
                        ON CONFLICT(user) DO UPDATE
                        SET points = points + ?
                        WHERE points + ? >= 0''',
                     (user, points_change, points_change, points_change))
            
            if c.rowcount == 0:
                raise ValueError("Points cannot be negative")
            
            # Record history
            c.execute('''INSERT INTO history (user, points_change, timestamp, match_number, recorded_by)
                        VALUES (?, ?, ?, ?, ?)''',
                     (user, points_change, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                      match_number, recorded_by))
            
            # If this is a match win, add to match results
            if match_number is not None:
                if match_number > Config.MAX_MATCH_NUMBER:
                    raise ValueError(f"Match number exceeds maximum allowed value of {Config.MAX_MATCH_NUMBER}")
                
                c.execute('''INSERT INTO match_results (match_number, winner, timestamp, recorded_by)
                            VALUES (?, ?, ?, ?)''',
                         (match_number, user, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                          recorded_by))
            
            conn.commit()
            
        except sqlite3.Error as e:
            conn.rollback()
            raise DatabaseError(f"Failed to update points: {str(e)}")

def clear_points() -> None:
    """Clear all points and match results"""
    with get_db_connection() as conn:
        c = conn.cursor()
        try:
            c.execute('BEGIN TRANSACTION')
            c.execute('DELETE FROM points')
            c.execute('DELETE FROM history')
            c.execute('DELETE FROM match_results')
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            raise DatabaseError(f"Failed to clear points: {str(e)}")

def undo_last_point() -> Tuple[bool, str]:
    """Undo the last point change"""
    with get_db_connection() as conn:
        c = conn.cursor()
        try:
            c.execute('BEGIN TRANSACTION')
            
            # Get last history entry
            c.execute('''SELECT user, points_change, match_number 
                        FROM history 
                        ORDER BY id DESC LIMIT 1''')
            last_change = c.fetchone()
            
            if not last_change:
                return False, "No points to undo"
            
            user, points_change, match_number = last_change
            
            # Revert points
            c.execute('UPDATE points SET points = points - ? WHERE user = ?',
                     (points_change, user))
            
            # Remove last history entry
            c.execute('DELETE FROM history WHERE id = (SELECT MAX(id) FROM history)')
            
            # If this was a match win, remove it from match_results
            if match_number is not None:
                c.execute('DELETE FROM match_results WHERE match_number = ? AND winner = ?',
                         (match_number, user))
            
            conn.commit()
            return True, f"Undid {points_change} point(s) for {user}"
            
        except sqlite3.Error as e:
            conn.rollback()
            raise DatabaseError(f"Failed to undo last point: {str(e)}")

def get_leaderboard() -> List[Tuple[str, int]]:
    """Get leaderboard data"""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user, points FROM points ORDER BY points DESC')
        return c.fetchall()

def get_match_results() -> List[Tuple[int, str, str]]:
    """Get match results data"""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT match_number, winner, timestamp FROM match_results ORDER BY match_number')
        return c.fetchall() 