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
    DB_TIMEOUT: int = int(os.getenv('DB_TIMEOUT', '30'))  # seconds
    DB_BACKUP_PATH: str = os.getenv('DB_BACKUP_PATH', 'backups')
    
    # Security Settings
    MAX_POINTS_PER_UPDATE: int = int(os.getenv('MAX_POINTS_PER_UPDATE', '100'))
    MAX_MATCH_NUMBER: int = int(os.getenv('MAX_MATCH_NUMBER', '74'))
    COMMAND_COOLDOWN: int = int(os.getenv('COMMAND_COOLDOWN', '5'))  # seconds
    MAX_COMMANDS_PER_MINUTE: int = int(os.getenv('MAX_COMMANDS_PER_MINUTE', '30'))
    
    # Development Settings
    DEBUG: bool = os.getenv('DEBUG', 'false').lower() == 'true'
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls) -> Dict[str, Any]:
        """Validate configuration and return any errors"""
        errors = {}
        
        # Discord Configuration Validation
        if not cls.DISCORD_TOKEN:
            errors['DISCORD_TOKEN'] = "Discord token is required"
            
        if not cls.ADMIN_USER_IDS:
            errors['ADMIN_USER_IDS'] = "At least one admin user ID is required"
            
        # Database Configuration Validation
        if cls.DB_TIMEOUT < 1:
            errors['DB_TIMEOUT'] = "Database timeout must be at least 1 second"
            
        # Security Settings Validation
        if cls.MAX_POINTS_PER_UPDATE < 1:
            errors['MAX_POINTS_PER_UPDATE'] = "Maximum points per update must be at least 1"
            
        if cls.MAX_MATCH_NUMBER < 1:
            errors['MAX_MATCH_NUMBER'] = "Maximum match number must be at least 1"
            
        if cls.COMMAND_COOLDOWN < 0:
            errors['COMMAND_COOLDOWN'] = "Command cooldown cannot be negative"
            
        if cls.MAX_COMMANDS_PER_MINUTE < 1:
            errors['MAX_COMMANDS_PER_MINUTE'] = "Maximum commands per minute must be at least 1"
            
        # Create backup directory if it doesn't exist
        if not os.path.exists(cls.DB_BACKUP_PATH):
            os.makedirs(cls.DB_BACKUP_PATH)
            
        return errors 