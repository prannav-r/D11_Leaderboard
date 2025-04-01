from supabase import create_client, Client
from datetime import datetime, timezone
import logging
from typing import Dict, List, Tuple, Optional
from config import Config

# Set up logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Supabase client
supabase: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass

def init_db() -> None:
    """Initialize database tables and indexes"""
    try:
        # Create points table
        supabase.table('points').insert({
            'username': 'test',
            'points': 0
        }).execute()
        supabase.table('points').delete().eq('username', 'test').execute()
        
        # Create history table
        supabase.table('history').insert({
            'username': 'test',
            'points': 0,
            'match_number': 1,
            'updated_by': 'test',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }).execute()
        supabase.table('history').delete().eq('username', 'test').execute()
        
        # Create match_results table
        supabase.table('match_results').insert({
            'match_number': 1,
            'winner': 'test',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }).execute()
        supabase.table('match_results').delete().eq('match_number', 1).execute()
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise DatabaseError(f"Failed to initialize database: {str(e)}")

def get_points() -> Dict[str, int]:
    """Get current points for all users"""
    try:
        response = supabase.table('points').select('username, points').execute()
        return {row['username']: row['points'] for row in response.data}
    except Exception as e:
        logger.error(f"Error getting points: {str(e)}")
        raise DatabaseError(f"Failed to get points: {str(e)}")

def update_points(username: str, points: int, match_number: int, updated_by: str) -> None:
    """Update points for a user and record in history"""
    try:
        # Start transaction
        supabase.rpc('begin_transaction').execute()
        
        # Update points
        current_points = supabase.table('points').select('points').eq('username', username).execute()
        if not current_points.data:
            supabase.table('points').insert({
                'username': username,
                'points': points
            }).execute()
        else:
            new_points = current_points.data[0]['points'] + points
            supabase.table('points').update({'points': new_points}).eq('username', username).execute()
        
        # Record in history
        supabase.table('history').insert({
            'username': username,
            'points': points,
            'match_number': match_number,
            'updated_by': updated_by,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }).execute()
        
        # Record match result
        supabase.table('match_results').insert({
            'match_number': match_number,
            'winner': username,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }).execute()
        
        # Commit transaction
        supabase.rpc('commit_transaction').execute()
        
    except Exception as e:
        # Rollback transaction
        supabase.rpc('rollback_transaction').execute()
        logger.error(f"Error updating points: {str(e)}")
        raise DatabaseError(f"Failed to update points: {str(e)}")

def clear_points() -> None:
    """Clear all points and history"""
    try:
        # Start transaction
        supabase.rpc('begin_transaction').execute()
        
        # Clear all tables
        supabase.table('points').delete().neq('username', '').execute()
        supabase.table('history').delete().neq('username', '').execute()
        supabase.table('match_results').delete().neq('match_number', 0).execute()
        
        # Commit transaction
        supabase.rpc('commit_transaction').execute()
        
    except Exception as e:
        # Rollback transaction
        supabase.rpc('rollback_transaction').execute()
        logger.error(f"Error clearing points: {str(e)}")
        raise DatabaseError(f"Failed to clear points: {str(e)}")

def undo_last_point() -> Tuple[bool, str]:
    """Undo the last point change"""
    try:
        # Start transaction
        supabase.rpc('begin_transaction').execute()
        
        # Get last history entry
        last_entry = supabase.table('history').select('*').order('timestamp', desc=True).limit(1).execute()
        
        if not last_entry.data:
            return False, "No points to undo"
        
        entry = last_entry.data[0]
        
        # Update points
        current_points = supabase.table('points').select('points').eq('username', entry['username']).execute()
        if current_points.data:
            new_points = current_points.data[0]['points'] - entry['points']
            supabase.table('points').update({'points': new_points}).eq('username', entry['username']).execute()
        
        # Delete history entry
        supabase.table('history').delete().eq('id', entry['id']).execute()
        
        # Delete match result
        supabase.table('match_results').delete().eq('match_number', entry['match_number']).execute()
        
        # Commit transaction
        supabase.rpc('commit_transaction').execute()
        
        return True, f"Undid {entry['points']} point(s) for {entry['username']}"
        
    except Exception as e:
        # Rollback transaction
        supabase.rpc('rollback_transaction').execute()
        logger.error(f"Error undoing last point: {str(e)}")
        raise DatabaseError(f"Failed to undo last point: {str(e)}")

def get_match_results() -> List[Tuple[int, str, str]]:
    """Get all match results"""
    try:
        response = supabase.table('match_results').select('*').order('match_number').execute()
        return [(row['match_number'], row['winner'], row['timestamp']) for row in response.data]
    except Exception as e:
        logger.error(f"Error getting match results: {str(e)}")
        raise DatabaseError(f"Failed to get match results: {str(e)}") 