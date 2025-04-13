from supabase import create_client, Client
from datetime import datetime, timezone
import logging
from typing import Dict, List, Tuple, Optional, Union, Any
from config import Config
from utils import retry_on_error, structured_logger

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

class TransactionError(DatabaseError):
    """Exception for transaction-related errors"""
    pass

@retry_on_error(max_retries=3, delay=1)
async def execute_in_transaction(operations: List[Dict[str, Any]]) -> None:
    """Execute a list of database operations in a transaction"""
    try:
        # Start transaction
        structured_logger.info("Starting database transaction", {"operations": len(operations)})
        
        # Execute each operation
        for op in operations:
            table = op.get('table')
            action = op.get('action')
            data = op.get('data', {})
            conditions = op.get('conditions', {})
            
            try:
                if action == 'insert':
                    response = supabase.table(table).insert(data).execute()
                    if not response.data:
                        raise TransactionError(f"Failed to insert into {table}")
                        
                elif action == 'update':
                    query = supabase.table(table).update(data)
                    for key, value in conditions.items():
                        query = query.eq(key, value)
                    response = query.execute()
                    if not response.data:
                        raise TransactionError(f"Failed to update {table}")
                        
                elif action == 'upsert':
                    response = supabase.table(table).upsert(data).execute()
                    if not response.data:
                        raise TransactionError(f"Failed to upsert into {table}")
                        
                elif action == 'delete':
                    query = supabase.table(table).delete()
                    for key, value in conditions.items():
                        query = query.eq(key, value)
                    response = query.execute()
                    
                else:
                    raise ValueError(f"Invalid action: {action}")
                    
            except Exception as e:
                structured_logger.error(
                    "Error executing database operation",
                    {
                        "table": table,
                        "action": action,
                        "error": str(e)
                    }
                )
                raise TransactionError(f"Failed to execute {action} on {table}: {str(e)}")
                
        structured_logger.info("Transaction completed successfully")
        
    except Exception as e:
        structured_logger.error("Transaction failed", {"error": str(e)})
        raise TransactionError(f"Transaction failed: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
def init_db() -> None:
    """Initialize database tables"""
    try:
        # List of tables to check/initialize
        tables = ['points', 'history']
        
        # Check each table
        for table in tables:
            try:
                # Test table access
                response = supabase.table(table).select('*').limit(1).execute()
                structured_logger.info(f"Successfully connected to {table} table")
                
            except Exception as e:
                structured_logger.error(f"Error accessing {table} table", {"error": str(e)})
                raise DatabaseError(f"Failed to access {table} table: {str(e)}")
                
        structured_logger.info("Database initialization completed successfully")
        
    except Exception as e:
        structured_logger.error("Database initialization failed", {"error": str(e)})
        raise DatabaseError(f"Failed to initialize database: {str(e)}")

def get_points(user_id: Optional[int] = None) -> Union[Dict[str, int], int]:
    """Get points for all users or a specific user"""
    try:
        # Get points for specific user
        if user_id:
            response = supabase.table('points').select('user_points').eq('username', f'<@{user_id}>').execute()
            if not response.data:
                return 0
            return response.data[0]['user_points']
        
        # Get points for all users
        response = supabase.table('points').select('username,user_points').execute()
        return {item['username']: item['user_points'] for item in response.data}
    except Exception as e:
        logger.error(f"Error getting points: {str(e)}")
        raise DatabaseError(f"Failed to get points: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def update_points(username: str, points: int, match_number: int, updated_by: str) -> None:
    """Update points for a user and record in history"""
    try:
        # Get current points
        current_points = supabase.table('points').select('user_points').eq('username', username).execute()
        
        # Prepare transaction operations
        operations = []
        
        # Update or insert points
        if not current_points.data:
            operations.append({
                'table': 'points',
                'action': 'insert',
                'data': {
                    'username': username,
                    'user_points': points
                }
            })
        else:
            new_points = current_points.data[0]['user_points'] + points
            operations.append({
                'table': 'points',
                'action': 'update',
                'data': {'user_points': new_points},
                'conditions': {'username': username}
            })
        
        # Record in history
        operations.append({
            'table': 'history',
            'action': 'insert',
            'data': {
                'username': username,
                'points': points,
                'match_number': match_number,
                'updated_by': updated_by,
                'timestamp': get_ist_time().isoformat()
            }
        })
        
        # Execute all operations in a transaction
        await execute_in_transaction(operations)
        
        structured_logger.info(
            "Points updated successfully",
            {
                "username": username,
                "points": points,
                "match_number": match_number,
                "updated_by": updated_by
            }
        )
        
    except Exception as e:
        structured_logger.error(
            "Error updating points",
            {
                "username": username,
                "points": points,
                "match_number": match_number,
                "error": str(e)
            }
        )
        raise DatabaseError(f"Failed to update points: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
def clear_points() -> None:
    """Clear all points and history"""
    try:
        # Clear points table
        supabase.table('points').delete().neq('username', '').execute()
        
        # Clear history table
        supabase.table('history').delete().neq('username', '').execute()
    except Exception as e:
        structured_logger.error("Error clearing points", {"error": str(e)})
        raise DatabaseError(f"Failed to clear points: {str(e)}")

def undo_last_points_update() -> Tuple[bool, str]:
    """Undo the last points update"""
    try:
        # Get last history entry
        last_entry = supabase.table('history').select('*').order('timestamp', desc=True).limit(1).execute()
        
        if not last_entry.data:
            return False, "No points to undo"
        
        entry = last_entry.data[0]
        
        # Update points
        current_points = supabase.table('points').select('user_points').eq('username', entry['username']).execute()
        if current_points.data:
            new_points = current_points.data[0]['user_points'] - entry['points']
            supabase.table('points').update({'user_points': new_points}).eq('username', entry['username']).execute()
        
        # Delete the history entry
        supabase.table('history').delete().eq('id', entry['id']).execute()
        
        return True, f"Undid {entry['points']} point(s) for {entry['username']}"
    except Exception as e:
        logger.error(f"Error undoing points update: {str(e)}")
        raise DatabaseError(f"Failed to undo points update: {str(e)}")

def get_match_results() -> List[Tuple[int, str, str, str]]:
    """Get all match results with admin who recorded them"""
    try:
        # Get match results from history table
        response = supabase.table('history').select(
            'match_number, username, timestamp, updated_by'
        ).order('match_number').execute()
        
        if not response.data:
            logger.info("No match results found")
            return []
            
        # Process results
        results = []
        for entry in response.data:
            results.append((
                entry['match_number'],
                entry['username'],
                entry['timestamp'],
                entry['updated_by']
            ))
        
        return results
        
    except Exception as e:
        logger.error(f"Error getting match results: {str(e)}")
        raise DatabaseError(f"Failed to get match results: {str(e)}")

def get_user_match_wins(user_id: int) -> List[Tuple[int, str, str, str]]:
    """Get all matches won by a specific user"""
    try:
        # Get match results from history table
        response = supabase.table('history').select(
            'match_number, username, timestamp, updated_by'
        ).eq('username', str(user_id)).order('match_number').execute()
        
        if not response.data:
            return []
            
        # Process results
        results = []
        for entry in response.data:
            results.append((
                entry['match_number'],
                entry['username'],
                entry['timestamp'],
                entry['updated_by']
            ))
        
        return results
        
    except Exception as e:
        logger.error(f"Error getting user match wins: {str(e)}")
        raise DatabaseError(f"Failed to get user match wins: {str(e)}")

def get_user_stats(user_id: int) -> List[Tuple[int, bool, List[Tuple[int, str, str]]]]:
    """Get user stats including points, alert status, and recent match wins"""
    try:
        # Get points
        response = supabase.table('points').select('user_points').eq('username', f'<@{user_id}>').execute()
        points = response.data[0]['user_points'] if response.data else 0
        
        # Get alert status
        alert = get_user_alert_preference(user_id)
        
        # Get recent match wins (last 2)
        wins_response = supabase.table('history').select(
            'match_number, username, timestamp'
        ).eq('username', f'<@{user_id}>').order('timestamp', desc=True).limit(2).execute()
        
        recent_wins = []
        for win in wins_response.data:
            recent_wins.append((
                win['match_number'],
                win['username'],
                win['timestamp']
            ))
        
        return [(points, alert, recent_wins)]
        
    except Exception as e:
        structured_logger.error("Error getting user stats", {"error": str(e)})
        raise DatabaseError(f"Failed to get user stats: {str(e)}")

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

def set_user_alert_preference(user_id: int, enabled: bool) -> bool:
    """Set or update user's alert preference"""
    try:
        # Upsert alert preference
        response = supabase.table('user_alerts').upsert({
            'user_id': user_id,
            'enabled': enabled,
            'updated_at': datetime.now(timezone.utc).isoformat()
        }).execute()
        
        return True
    except Exception as e:
        logger.error(f"Error setting user alert preference: {str(e)}")
        raise DatabaseError(f"Failed to set user alert preference: {str(e)}")

def get_users_with_alerts() -> List[int]:
    """Get all users with alerts enabled"""
    try:
        response = supabase.table('user_alerts').select('user_id').eq('enabled', True).execute()
        return [item['user_id'] for item in response.data]
    except Exception as e:
        logger.error(f"Error getting users with alerts: {str(e)}")
        raise DatabaseError(f"Failed to get users with alerts: {str(e)}")

def has_used_win_today(match_number: int) -> bool:
    """Check if a record already exists for this match in history"""
    try:
        # Check history for any entries with this match number
        response = supabase.table('history').select('match_number').eq('match_number', match_number).execute()
        
        # If any records exist for this match number, return True
        return len(response.data) > 0
        
    except Exception as e:
        logger.error(f"Error checking match history: {str(e)}")
        raise DatabaseError(f"Failed to check match history: {str(e)}")

def is_match_today(match_number: int, schedule: dict) -> bool:
    """Check if a match is scheduled for today"""
    try:
        # Get today's date in IST
        today = get_ist_time().date()
        
        # Check if match is in today's schedule
        for match_no, match_info in schedule.items():
            if match_no == match_number and match_info['date'].date() == today:
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"Error checking match schedule: {str(e)}")
        raise DatabaseError(f"Failed to check match schedule: {str(e)}") 