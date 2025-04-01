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
        # Test database connection and table existence
        tables = ['points', 'history', 'match_results']
        for table in tables:
            try:
                response = supabase.table(table).select('count').execute()
                logger.info(f"Successfully connected to {table} table")
            except Exception as e:
                logger.error(f"Error accessing {table} table: {str(e)}")
                raise DatabaseError(f"Failed to access {table} table: {str(e)}")
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise DatabaseError(f"Failed to initialize database: {str(e)}")

def get_points() -> Dict[str, int]:
    """Get current points for all users"""
    try:
        response = supabase.table('points').select('username, points').execute()
        if not response.data:
            logger.info("No points found in database")
            return {}
        return {row['username']: row['points'] for row in response.data}
    except Exception as e:
        logger.error(f"Error getting points: {str(e)}")
        raise DatabaseError(f"Failed to get points: {str(e)}")

def update_points(username: str, points: int, match_number: int, updated_by: str) -> None:
    """Update points for a user and record in history"""
    try:
        # Get current points
        current_points = supabase.table('points').select('points').eq('username', username).execute()
        
        # Update or insert points
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
        
        # Record match result (use upsert to handle duplicates)
        supabase.table('match_results').upsert({
            'match_number': match_number,
            'winner': username,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }).execute()
        
    except Exception as e:
        logger.error(f"Error updating points: {str(e)}")
        raise DatabaseError(f"Failed to update points: {str(e)}")

def clear_points() -> None:
    """Clear all points and history"""
    try:
        # Clear all tables
        supabase.table('points').delete().neq('username', '').execute()
        supabase.table('history').delete().neq('username', '').execute()
        supabase.table('match_results').delete().neq('match_number', 0).execute()
        
    except Exception as e:
        logger.error(f"Error clearing points: {str(e)}")
        raise DatabaseError(f"Failed to clear points: {str(e)}")

def undo_last_point() -> Tuple[bool, str]:
    """Undo the last point change"""
    try:
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
        
        return True, f"Undid {entry['points']} point(s) for {entry['username']}"
        
    except Exception as e:
        logger.error(f"Error undoing last point: {str(e)}")
        raise DatabaseError(f"Failed to undo last point: {str(e)}")

def get_match_results() -> List[Tuple[int, str, str, str]]:
    """Get all match results with admin who recorded them"""
    try:
        # First check if we have any match results
        check_response = supabase.table('match_results').select('count').execute()
        if not check_response.data or check_response.data[0]['count'] == 0:
            logger.info("No match results found in database")
            return []

        # Join match_results with history to get the admin who recorded the win
        response = supabase.table('match_results').select(
            'match_number, winner, timestamp, history!inner(updated_by)'
        ).order('match_number').execute()
        
        if not response.data:
            logger.info("No match results found after join")
            return []
            
        # Process the response data
        results = []
        for row in response.data:
            try:
                # Extract the admin from the nested history data
                admin = row['history']['updated_by'] if row.get('history') else 'Unknown'
                results.append((
                    row['match_number'],
                    row['winner'],
                    row['timestamp'],
                    admin
                ))
            except Exception as e:
                logger.error(f"Error processing row {row}: {str(e)}")
                continue
        
        return results
        
    except Exception as e:
        logger.error(f"Error getting match results: {str(e)}")
        raise DatabaseError(f"Failed to get match results: {str(e)}") 