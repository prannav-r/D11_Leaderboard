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

def execute_in_transaction(operations: List[Dict[str, Any]]) -> None:
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
                    supabase.table(table).insert(data).execute()
                elif action == 'update':
                    query = supabase.table(table).update(data)
                    for key, value in conditions.items():
                        query = query.eq(key, value)
                    query.execute()
                elif action == 'delete':
                    query = supabase.table(table).delete()
                    for key, value in conditions.items():
                        query = query.eq(key, value)
                    query.execute()
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
        tables = ['points', 'history', 'match_results']
        
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
            response = supabase.table('points').select('points').eq('username', f'<@{user_id}>').execute()
            if not response.data:
                return 0
            return response.data[0]['points']
        
        # Get points for all users
        response = supabase.table('points').select('username,points').execute()
        return {item['username']: item['points'] for item in response.data}
    except Exception as e:
        logger.error(f"Error getting points: {str(e)}")
        raise DatabaseError(f"Failed to get points: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
def update_points(username: str, points: int, match_number: int, updated_by: str) -> None:
    """Update points for a user and record in history"""
    try:
        # Get current points
        current_points = supabase.table('points').select('points').eq('username', username).execute()
        
        # Prepare transaction operations
        operations = []
        
        # Update or insert points
        if not current_points.data:
            operations.append({
                'table': 'points',
                'action': 'insert',
                'data': {
                    'username': username,
                    'points': points
                }
            })
        else:
            new_points = current_points.data[0]['points'] + points
            operations.append({
                'table': 'points',
                'action': 'update',
                'data': {'points': new_points},
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
                'timestamp': datetime.now().isoformat()
            }
        })
        
        # Record match result
        operations.append({
            'table': 'match_results',
            'action': 'upsert',
            'data': {
                'match_number': match_number,
                'winner': username,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        })
        
        # Execute all operations in a transaction
        execute_in_transaction(operations)
        
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
        current_points = supabase.table('points').select('points').eq('username', entry['username']).execute()
        if current_points.data:
            new_points = current_points.data[0]['points'] - entry['points']
            supabase.table('points').update({'points': new_points}).eq('username', entry['username']).execute()
        
        # Delete the history entry
        supabase.table('history').delete().eq('id', entry['id']).execute()
        
        return True, f"Undid {entry['points']} point(s) for {entry['username']}"
    except Exception as e:
        logger.error(f"Error undoing points update: {str(e)}")
        raise DatabaseError(f"Failed to undo points update: {str(e)}")

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

def get_user_stats(user_id: int) -> List[Tuple[int]]:
    """Get user stats including points"""
    try:
        # Get points
        response = supabase.table('points').select('points').eq('username', f'<@{user_id}>').execute()
        points = response.data[0]['points'] if response.data else 0
        
        return [(points,)]
        
    except Exception as e:
        structured_logger.error("Error getting user stats", {"error": str(e)})
        raise DatabaseError(f"Failed to get user stats: {str(e)}") 