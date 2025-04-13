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
async def init_db() -> None:
    """Initialize database tables"""
    try:
        # List of tables to check/initialize
        tables = ['points', 'history']
        
        # Check each table
        for table in tables:
            try:
                # Test table access
                response = await supabase.table(table).select('*').limit(1).execute()
                structured_logger.info(f"Successfully connected to {table} table")
                
            except Exception as e:
                structured_logger.error(f"Error accessing {table} table", {"error": str(e)})
                raise DatabaseError(f"Failed to access {table} table: {str(e)}")
                
        structured_logger.info("Database initialization completed successfully")
        
    except Exception as e:
        structured_logger.error("Database initialization failed", {"error": str(e)})
        raise DatabaseError(f"Failed to initialize database: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def get_points(user_id: Optional[int] = None) -> Union[Dict[str, int], int]:
    """Get points for all users or a specific user"""
    try:
        # Get points for specific user
        if user_id:
            response = await supabase.table('points').select('user_points').eq('username', f'<@{user_id}>').execute()
            if not response.data:
                return 0
            return response.data[0]['user_points']
        
        # Get points for all users
        response = await supabase.table('points').select('username,user_points').execute()
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
async def clear_points() -> None:
    """Clear all points and history"""
    try:
        # Clear points table
        await supabase.table('points').delete().neq('username', '').execute()
        
        # Clear history table
        await supabase.table('history').delete().neq('username', '').execute()
    except Exception as e:
        structured_logger.error("Error clearing points", {"error": str(e)})
        raise DatabaseError(f"Failed to clear points: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def undo_last_points_update() -> Tuple[bool, str]:
    """Undo the last points update"""
    try:
        # Get last history entry
        last_entry = await supabase.table('history').select('*').order('timestamp', desc=True).limit(1).execute()
        
        if not last_entry.data:
            return False, "No points to undo"
        
        entry = last_entry.data[0]
        
        # Update points
        current_points = await supabase.table('points').select('user_points').eq('username', entry['username']).execute()
        if current_points.data:
            new_points = current_points.data[0]['user_points'] - entry['points']
            await supabase.table('points').update({'user_points': new_points}).eq('username', entry['username']).execute()
        
        # Delete the history entry
        await supabase.table('history').delete().eq('id', entry['id']).execute()
        
        return True, f"Undid {entry['points']} point(s) for {entry['username']}"
    except Exception as e:
        logger.error(f"Error undoing points update: {str(e)}")
        raise DatabaseError(f"Failed to undo points update: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def get_match_results() -> List[Tuple[int, str, str, str]]:
    """Get all match results with admin who recorded them"""
    try:
        # Get match results from history table
        response = await supabase.table('history').select(
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

@retry_on_error(max_retries=3, delay=1)
async def get_user_match_wins(user_id: int) -> List[Tuple[int, str, str, str]]:
    """Get match wins for a specific user"""
    try:
        # Get match wins from history table
        response = await supabase.table('history').select(
            'match_number, username, timestamp, updated_by'
        ).eq('username', f'<@{user_id}>').order('match_number').execute()
        
        if not response.data:
            logger.info(f"No match wins found for user {user_id}")
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

@retry_on_error(max_retries=3, delay=1)
async def get_user_stats(user_id: int) -> List[Tuple[int, bool, List[Tuple[int, str, str]]]]:
    """Get stats for a specific user"""
    try:
        # Get points
        points = await get_points(user_id)
        
        # Get alert preference
        alert_preference = await get_user_alert_preference(user_id)
        
        # Get recent match wins
        match_wins = await get_user_match_wins(user_id)
        
        return [(points, alert_preference, match_wins)]
    except Exception as e:
        logger.error(f"Error getting user stats: {str(e)}")
        raise DatabaseError(f"Failed to get user stats: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def get_user_alert_preference(user_id: int) -> bool:
    """Get alert preference for a user"""
    try:
        response = await supabase.table('points').select('alert_enabled').eq('username', f'<@{user_id}>').execute()
        if not response.data:
            return False
        return response.data[0]['alert_enabled']
    except Exception as e:
        logger.error(f"Error getting user alert preference: {str(e)}")
        raise DatabaseError(f"Failed to get user alert preference: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def set_user_alert_preference(user_id: int, enabled: bool) -> bool:
    """Set alert preference for a user"""
    try:
        response = await supabase.table('points').upsert({
            'username': f'<@{user_id}>',
            'alert_enabled': enabled
        }).execute()
        return bool(response.data)
    except Exception as e:
        logger.error(f"Error setting user alert preference: {str(e)}")
        raise DatabaseError(f"Failed to set user alert preference: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def get_users_with_alerts() -> List[int]:
    """Get list of users with alerts enabled"""
    try:
        response = await supabase.table('points').select('username').eq('alert_enabled', True).execute()
        if not response.data:
            return []
        return [int(user['username'].strip('<>@')) for user in response.data]
    except Exception as e:
        logger.error(f"Error getting users with alerts: {str(e)}")
        raise DatabaseError(f"Failed to get users with alerts: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def has_used_win_today(match_number: int) -> bool:
    """Check if a user has already used win for a match today"""
    try:
        # Get current date in IST
        current_time = get_ist_time()
        current_date = current_time.date()
        
        # Check history for today's entries
        response = await supabase.table('history').select('*').eq('match_number', match_number).execute()
        if not response.data:
            return False
            
        # Check if any entry is from today
        for entry in response.data:
            entry_date = datetime.fromisoformat(entry['timestamp']).date()
            if entry_date == current_date:
                return True
                
        return False
    except Exception as e:
        logger.error(f"Error checking win usage: {str(e)}")
        raise DatabaseError(f"Failed to check win usage: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
async def is_match_today(match_number: int, schedule: dict) -> bool:
    """Check if a match is scheduled for today"""
    try:
        # Get current date in IST
        current_time = get_ist_time()
        current_date = current_time.date()
        
        # Check if match is in schedule and scheduled for today
        match_info = schedule.get(match_number)
        if not match_info:
            return False
            
        return match_info['date'].date() == current_date
    except Exception as e:
        logger.error(f"Error checking match schedule: {str(e)}")
        raise DatabaseError(f"Failed to check match schedule: {str(e)}") 