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
    """Initialize database tables and indexes"""
    try:
        # Test database connection and table existence
        tables = ['points', 'history', 'match_results', 'user_alerts']
        for table in tables:
            try:
                # Try to select from the table to verify it exists
                response = supabase.table(table).select('count').execute()
                structured_logger.info(f"Successfully connected to {table} table")
                
                # For user_alerts table, verify structure
                if table == 'user_alerts':
                    # Try to insert a test record
                    test_user_id = 123456789  # Test user ID
                    try:
                        operations = [
                            {
                                'table': 'user_alerts',
                                'action': 'insert',
                                'data': {
                                    'user_id': test_user_id,
                                    'enabled': False
                                }
                            },
                            {
                                'table': 'user_alerts',
                                'action': 'update',
                                'data': {'enabled': True},
                                'conditions': {'user_id': test_user_id}
                            },
                            {
                                'table': 'user_alerts',
                                'action': 'delete',
                                'conditions': {'user_id': test_user_id}
                            }
                        ]
                        execute_in_transaction(operations)
                        structured_logger.info("Successfully tested user_alerts table operations")
                        
                    except Exception as e:
                        structured_logger.error("Error testing user_alerts table", {"error": str(e)})
                        raise DatabaseError(f"Failed to test user_alerts table: {str(e)}")
                        
            except Exception as e:
                structured_logger.error(f"Error accessing {table} table", {"error": str(e)})
                raise DatabaseError(f"Failed to access {table} table: {str(e)}")
        
        structured_logger.info("Database initialized successfully")
        
    except Exception as e:
        structured_logger.error("Error initializing database", {"error": str(e)})
        raise DatabaseError(f"Failed to initialize database: {str(e)}")

def get_points(user_id: Optional[int] = None) -> Union[Dict[str, int], int]:
    """Get points for all users or a specific user"""
    try:
        # Get points for specific user
        if user_id:
            username = f"<@{user_id}>"
            response = supabase.table('points').select('user_points').eq('username', username).execute()
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
def update_points(username: str, points: int, match_number: int, updated_by: str) -> None:
    """Update points for a user and record in history"""
    try:
        # Ensure username is in correct format
        if not username.startswith('<@') or not username.endswith('>'):
            username = f"<@{username}>"
        
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
        # Prepare transaction operations
        operations = [
            {
                'table': 'points',
                'action': 'delete',
                'conditions': {'username': {'neq': ''}}
            },
            {
                'table': 'history',
                'action': 'delete',
                'conditions': {'username': {'neq': ''}}
            }
        ]
        
        # Execute all operations in a transaction
        execute_in_transaction(operations)
        
        structured_logger.info("Successfully cleared all points and history")
        
    except Exception as e:
        structured_logger.error("Error clearing points", {"error": str(e)})
        raise DatabaseError(f"Failed to clear points: {str(e)}")

@retry_on_error(max_retries=3, delay=1)
def undo_last_points_update() -> Tuple[bool, str]:
    """Undo the last points update"""
    try:
        # Get the last history entry
        response = supabase.table('history').select('*').order('timestamp', desc=True).limit(1).execute()
        
        if not response.data:
            return False, "No points to undo"
        
        entry = response.data[0]
        
        # Prepare transaction operations
        operations = []
        
        # Update points
        current_points = supabase.table('points').select('user_points').eq('username', entry['username']).execute()
        if current_points.data:
            new_points = current_points.data[0]['user_points'] - entry['points']
            operations.append({
                'table': 'points',
                'action': 'update',
                'data': {'user_points': new_points},
                'conditions': {'username': entry['username']}
            })
        
        # Delete the history entry
        operations.append({
            'table': 'history',
            'action': 'delete',
            'conditions': {'id': entry['id']}
        })
        
        # Execute all operations in a transaction
        execute_in_transaction(operations)
        
        structured_logger.info(
            "Successfully undid points update",
            {
                "username": entry['username'],
                "points": entry['points']
            }
        )
        
        return True, f"Undid {entry['points']} point(s) for {entry['username']}"
        
    except Exception as e:
        structured_logger.error("Error undoing points update", {"error": str(e)})
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
        # Format username as <@user_id> for query
        username = f"<@{user_id}>"
        
        # Get match results for this user
        response = supabase.table('match_results').select(
            'match_number, winner, timestamp'
        ).eq('winner', username).order('match_number').execute()
        
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