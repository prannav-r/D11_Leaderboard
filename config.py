import os
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables
load_dotenv()

class Config:
    # Discord Configuration
    DISCORD_TOKEN: str = os.getenv('DISCORD_TOKEN', '')
    ADMIN_USER_IDS: list[int] = [int(id) for id in os.getenv('ADMIN_USER_IDS', '').split(',') if id]
    
    # Database Configuration
    DB_PATH: str = os.getenv('DB_PATH', 'dream11.db')
    
    # Security Settings
    MAX_POINTS_PER_UPDATE: int = int(os.getenv('MAX_POINTS_PER_UPDATE', '100'))
    MAX_MATCH_NUMBER: int = int(os.getenv('MAX_MATCH_NUMBER', '74'))
    
    @classmethod
    def validate(cls) -> Dict[str, Any]:
        """Validate configuration and return any errors"""
        errors = {}
        
        if not cls.DISCORD_TOKEN:
            errors['DISCORD_TOKEN'] = "Discord token is required"
            
        if not cls.ADMIN_USER_IDS:
            errors['ADMIN_USER_IDS'] = "At least one admin user ID is required"
            
        return errors 