import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Any
from config import Config
import re

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
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def is_admin(user) -> bool:
    """Check if the user is an admin"""
    return user.id in Config.ADMIN_USER_IDS

def extract_mention_id(mention: str) -> str:
    """Extract user ID from a Discord mention"""
    match = re.match(r'<@!?(\d+)>', mention)
    if match:
        return match.group(1)
    return mention

def validate_input(username: str, match_number: int) -> tuple[bool, str]:
    """Validate input parameters"""
    # Check if it's a mention
    if re.match(r'<@!?\d+>', username):
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
    
    sorted_users = sorted(points.items(), key=lambda x: x[1], reverse=True)
    leaderboard = "🏆 Dream11 Leaderboard 🏆\n\n"
    
    for rank, (user, points) in enumerate(sorted_users, 1):
        # Check if the user is a mention
        if re.match(r'<@!?\d+>', user):
            leaderboard += f"{rank}. {user}: {points} point(s)\n"
        else:
            leaderboard += f"{rank}. @{user}: {points} point(s)\n"
    
    return leaderboard

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