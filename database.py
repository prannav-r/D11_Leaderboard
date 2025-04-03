from supabase import create_client, Client
from datetime import datetime, timezone
import logging
from typing import Dict, List, Tuple, Optional, Union
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
        tables = ['points', 'history', 'match_results', 'user_alerts']
        for table in tables:
            try:
                # Try to select from the table to verify it exists
                response = supabase.table(table).select('count').execute()
                logger.info(f"Successfully connected to {table} table")
                
                # For user_alerts table, verify structure
                if table == 'user_alerts':
                    # Try to insert a test record
                    test_user_id = 123456789  # Test user ID
                    try:
                        supabase.table('user_alerts').insert({
                            'user_id': test_user_id,
                            'enabled': False
                        }).execute()
                        logger.info("Successfully tested user_alerts table insert")
                        
                        # Try to update the test record
                        supabase.table('user_alerts').update({
                            'enabled': True
                        }).eq('user_id', test_user_id).execute()
                        logger.info("Successfully tested user_alerts table update")
                        
                        # Clean up test record
                        supabase.table('user_alerts').delete().eq('user_id', test_user_id).execute()
                        logger.info("Successfully cleaned up test record")
                    except Exception as e:
                        logger.error(f"Error testing user_alerts table: {str(e)}")
                        raise DatabaseError(f"Failed to test user_alerts table: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error accessing {table} table: {str(e)}")
                raise DatabaseError(f"Failed to access {table} table: {str(e)}")
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise DatabaseError(f"Failed to initialize database: {str(e)}")

def get_points(user_id: Optional[int] = None) -> Union[Dict[str, int], int]:
    """Get points for all users or a specific user"""
    try:
        # Get points for specific user
        if user_id:
            response = supabase.table('points').select('user_points').eq('username', str(user_id)).execute()
            if not response.data:
                return 0
            return response.data[0]['user_points']
        
        # Get points for all users
        response = supabase.table('points').select('username,user_points').execute()
        return {item['username']: item['user_points'] for item in response.data}
    except Exception as e:
        logger.error(f"Error getting points: {str(e)}")
        raise DatabaseError(f"Failed to get points: {str(e)}")

def update_points(username: str, points: int, match_number: int, updated_by: str) -> None:
    """Update points for a user and record in history"""
    try:
        # Get current points
        current_points = supabase.table('points').select('user_points').eq('username', username).execute()
        
        # Update or insert points
        if not current_points.data:
            supabase.table('points').insert({
                'username': username,
                'user_points': points
            }).execute()
        else:
            new_points = current_points.data[0]['user_points'] + points
            supabase.table('points').update({'user_points': new_points}).eq('username', username).execute()
        
        # Record in history
        supabase.table('history').insert({
            'username': username,
            'points': points,
            'match_number': match_number,
            'updated_by': updated_by,
            'timestamp': datetime.now().isoformat()
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
        current_points = supabase.table('points').select('user_points').eq('username', entry['username']).execute()
        if current_points.data:
            new_points = current_points.data[0]['user_points'] - entry['user_points']
            supabase.table('points').update({'user_points': new_points}).eq('username', entry['username']).execute()
        
        # Delete history entry
        supabase.table('history').delete().eq('id', entry['id']).execute()
        
        # Delete match result
        supabase.table('match_results').delete().eq('match_number', entry['match_number']).execute()
        
        return True, f"Undid {entry['user_points']} point(s) for {entry['username']}"
        
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

        # Get match results
        match_response = supabase.table('match_results').select(
            'match_number, winner, timestamp'
        ).order('match_number').execute()
        
        if not match_response.data:
            logger.info("No match results found")
            return []
            
        # Get corresponding history entries for each match
        results = []
        for match in match_response.data:
            try:
                # Get the history entry for this match
                history_response = supabase.table('history').select(
                    'updated_by'
                ).eq('match_number', match['match_number']).limit(1).execute()
                
                # Get the admin who recorded the win
                admin = history_response.data[0]['updated_by'] if history_response.data else 'Unknown'
                
                results.append((
                    match['match_number'],
                    match['winner'],
                    match['timestamp'],
                    admin
                ))
            except Exception as e:
                logger.error(f"Error processing match {match['match_number']}: {str(e)}")
                continue
        
        return results
        
    except Exception as e:
        logger.error(f"Error getting match results: {str(e)}")
        raise DatabaseError(f"Failed to get match results: {str(e)}")

def get_user_alert_preference(user_id: int) -> bool:
    """Get user's alert preference"""
    try:
        response = supabase.table('user_alerts').select('enabled').eq('user_id', user_id).execute()
        if not response.data:
            return False
        return response.data[0]['enabled']
    except Exception as e:
        logger.error(f"Error getting user alert preference: {str(e)}")
        raise DatabaseError(f"Failed to get user alert preference: {str(e)}")

def set_user_alert_preference(user_id: int, enabled: bool) -> None:
    """Set user's alert preference"""
    try:
        # First check if user preference exists
        check_response = supabase.table('user_alerts').select('enabled').eq('user_id', user_id).execute()
        
        if check_response.data:
            # Update existing preference
            response = supabase.table('user_alerts').update({
                'enabled': enabled,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }).eq('user_id', user_id).execute()
            
            if not response.data:
                logger.error(f"Failed to update alert preference for user {user_id}")
                raise DatabaseError("Failed to update alert preference")
        else:
            # Insert new preference
            response = supabase.table('user_alerts').insert({
                'user_id': user_id,
                'enabled': enabled,
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }).execute()
            
            if not response.data:
                logger.error(f"Failed to insert alert preference for user {user_id}")
                raise DatabaseError("Failed to insert alert preference")
            
    except Exception as e:
        logger.error(f"Error setting user alert preference: {str(e)}")
        raise DatabaseError(f"Failed to set user alert preference: {str(e)}")

def get_users_with_alerts() -> List[int]:
    """Get list of user IDs who have alerts enabled"""
    try:
        response = supabase.table('user_alerts').select('user_id').eq('enabled', True).execute()
        return [row['user_id'] for row in response.data]
    except Exception as e:
        logger.error(f"Error getting users with alerts: {str(e)}")
        raise DatabaseError(f"Failed to get users with alerts: {str(e)}")

def get_user_match_wins(user_id: int) -> List[Tuple[int, str, str, str]]:
    """Get all matches won by a specific user"""
    try:
        # Get match results for this user
        response = supabase.table('match_results').select(
            'match_number, winner, timestamp'
        ).eq('winner', str(user_id)).order('match_number').execute()
        
        if not response.data:
            return []
            
        # Get corresponding history entries for each match
        results = []
        for match in response.data:
            try:
                # Get the history entry for this match
                history_response = supabase.table('history').select(
                    'updated_by'
                ).eq('match_number', match['match_number']).limit(1).execute()
                
                # Get the admin who recorded the win
                admin = history_response.data[0]['updated_by'] if history_response.data else 'Unknown'
                
                results.append((
                    match['match_number'],
                    match['winner'],
                    match['timestamp'],
                    admin
                ))
            except Exception as e:
                logger.error(f"Error processing match {match['match_number']}: {str(e)}")
                continue
        
        return results
        
    except Exception as e:
        logger.error(f"Error getting user match wins: {str(e)}")
        raise DatabaseError(f"Failed to get user match wins: {str(e)}")

def get_user_stats(user_id: int) -> List[Tuple[int, bool]]:
    """Get user stats including points and alert status"""
    try:
        # Format username as <@user_id> for points query
        username = f"<@{user_id}>"
        
        # Get points for the user using their username
        points_response = supabase.table('points').select('user_points').eq('username', username).execute()
        points = points_response.data[0]['user_points'] if points_response.data else 0
        
        # Get alert status
        alert_response = supabase.table('user_alerts').select('enabled').eq('user_id', user_id).execute()
        alert_enabled = alert_response.data[0]['enabled'] if alert_response.data else False
        
        return [(points, alert_enabled)]
    except Exception as e:
        logger.error(f"Error getting user stats: {str(e)}")
        raise DatabaseError(f"Failed to get user stats: {str(e)}") 