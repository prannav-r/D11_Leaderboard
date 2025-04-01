import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Any
from config import Config
import re
import os

# Set up logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rate limiting
command_counts = defaultdict(lambda: {"count": 0, "reset_time": datetime.now()})
command_cooldowns = {}

def setup_logging():
    """Set up logging configuration"""
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Create a file handler
    log_file = os.path.join('logs', f'dream11_bot_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Get the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # Add the handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def is_admin(user) -> bool:
    """Check if the user is an admin"""
    return user.id in Config.ADMIN_USER_IDS

def is_mention(text: str) -> bool:
    """Check if text is a Discord mention"""
    return bool(re.match(r'<@!?\d+>', text)) or text.startswith('@')

def format_username(username: str) -> str:
    """Format username for display"""
    if is_mention(username):
        if username.startswith('@'):
            return username
        return username
    return f"@{username}"

def validate_input(username: str, match_number: int) -> tuple[bool, str]:
    """Validate input parameters"""
    # Check if it's a mention or @username
    if is_mention(username):
        return True, ""
    
    # Regular username validation
    if not username or len(username) > 32 or not username.isalnum():
        return False, "Invalid username format. Use only letters and numbers or mention a user."
    
    if not isinstance(match_number, int) or match_number < 1 or match_number > Config.MAX_MATCH_NUMBER:
        return False, f"Invalid match number. Must be between 1 and {Config.MAX_MATCH_NUMBER}."
    
    return True, ""

def format_points(points: Dict[str, int]) -> str:
    """Format points for display"""
    if not points:
        return "No points recorded yet!"
    
    try:
        sorted_users = sorted(points.items(), key=lambda x: x[1], reverse=True)
        leaderboard = "🏆 Dream11 Leaderboard 🏆\n\n"
        
        for rank, (user, points) in enumerate(sorted_users, 1):
            leaderboard += f"{rank}. {format_username(user)}: {points} point(s)\n"
        
        return leaderboard
    except Exception as e:
        logger.error(f"Error formatting points: {str(e)}")
        return "Error formatting leaderboard. Please try again later."

def get_command_cooldown(user_id: int, command: str) -> bool:
    """Check if command is on cooldown for user"""
    now = datetime.now()
    cooldown_key = f"{user_id}_{command}"
    
    if cooldown_key in command_cooldowns:
        if now < command_cooldowns[cooldown_key]:
            return False
    
    command_cooldowns[cooldown_key] = now + timedelta(seconds=Config.COMMAND_COOLDOWN)
    return True

def check_rate_limit(user_id: int) -> bool:
    """Check if user has exceeded rate limit"""
    now = datetime.now()
    user_data = command_counts[user_id]
    
    # Reset count if time has passed
    if now > user_data["reset_time"]:
        user_data["count"] = 0
        user_data["reset_time"] = now + timedelta(minutes=1)
    
    # Check if user has exceeded limit
    if user_data["count"] >= Config.MAX_COMMANDS_PER_MINUTE:
        return False
    
    user_data["count"] += 1
    return True 