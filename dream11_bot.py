import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Callable, Awaitable
import re
import csv
from config import Config
from database import (
    init_db,
    get_points,
    update_points,
    clear_points,
    get_user_alert_preference,
    set_user_alert_preference,
    get_users_with_alerts,
    get_user_match_wins,
    get_user_stats,
    undo_last_points_update
)
from utils import (
    setup_logging,
    is_admin,
    validate_input,
    format_points,
    get_command_cooldown,
    check_rate_limit,
    is_mention,
    format_username,
    structured_logger
)
import time

# Set up logging
logger = setup_logging()
logger.info("Starting Dream11 Bot initialization...")

# Initialize client
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.guild_messages = True
intents.dm_messages = True
intents.dm_reactions = True
client = discord.Client(intents=intents)
logger.info("Discord client initialized with required intents")

# Initialize database
try:
    init_db()
    logger.info("Database initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize database: {e}")
    raise

# Initialize command cooldown tracking
last_command_time = 0
logger.info("Command cooldown tracking initialized")

# Load IPL 2025 Schedule
def load_schedule():
    try:
        schedule = {}
        with open('IPL_2025_SEASON_SCHEDULE.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                match_no = int(row['Match No'])
                # Convert time format from "7:30 PM" to "19:30"
                time_str = row['Start']
                try:
                    # Parse the time with AM/PM format
                    time_obj = datetime.strptime(time_str, '%I:%M %p')
                    # Convert to 24-hour format
                    time_24h = time_obj.strftime('%H:%M')
                except ValueError as e:
                    logger.error(f"Error parsing time '{time_str}' for match {match_no}: {e}")
                    time_24h = time_str  # Keep original if parsing fails
                
                schedule[match_no] = {
                    'date': datetime.strptime(row['Date'], '%Y-%m-%d'),
                    'day': row['Day'],
                    'start': time_24h,
                    'home': row['Home'],
                    'away': row['Away'],
                    'venue': row['Venue'],
                    'alert': row.get('Alert', 'false').lower() == 'true'  # Read alert column, default to false
                }
        logger.info(f"Successfully loaded schedule with {len(schedule)} matches")
        return schedule
    except Exception as e:
        logger.error(f"Failed to load schedule: {e}")
        raise

# Load schedule at startup
try:
    IPL_2025_SCHEDULE = load_schedule()
    logger.info("IPL schedule loaded successfully")
except Exception as e:
    logger.error(f"Failed to load IPL schedule: {e}")
    raise

# Team name to acronym mapping
TEAM_ACRONYMS = {
    "Kolkata Knight Riders": "KKR",
    "Royal Challengers Bengaluru": "RCB",
    "Sunrisers Hyderabad": "SRH",
    "Rajasthan Royals": "RR",
    "Chennai Super Kings": "CSK",
    "Mumbai Indians": "MI",
    "Delhi Capitals": "DC",
    "Lucknow Super Giants": "LSG",
    "Gujarat Titans": "GT",
    "Punjab Kings": "PBKS"
}

# Add alert checking task
async def check_match_alerts():
    """Check for upcoming matches and send alerts 30 minutes before each match"""
    logger.info("Alert checking task started")
    
    while True:
        try:
            # Get current time in UTC
            current_time = datetime.now(timezone.utc)
            logger.info(f"Checking alerts at {current_time}")
            
            # Get users with alerts enabled
            users_with_alerts = get_users_with_alerts()
            if not users_with_alerts:
                logger.info("No users with alerts enabled, sleeping for 5 minutes")
                await asyncio.sleep(300)  # Sleep for 5 minutes
                continue
            
            # Check each match in the schedule
            for match_no, match_info in IPL_2025_SCHEDULE.items():
                try:
                    # Skip if alert is disabled for this match
                    if not match_info.get('alert', False):
                        continue
                    
                    # Parse match start time
                    match_date = match_info['date']
                    start_time = datetime.strptime(match_info['start'], '%H:%M').time()
                    match_datetime = datetime.combine(match_date, start_time)
                    match_datetime = match_datetime.replace(tzinfo=timezone.utc)
                    
                    # Calculate time until match
                    time_until_match = match_datetime - current_time
                    
                    # Only send alerts for matches within the next 30 minutes
                    if time_until_match.total_seconds() > 1800:  # 30 minutes = 1800 seconds
                        continue
                    
                    # Skip if we've already sent an alert for this match
                    if hasattr(check_match_alerts, 'last_alerted_matches'):
                        if match_no in check_match_alerts.last_alerted_matches:
                            continue
                    else:
                        check_match_alerts.last_alerted_matches = set()
                    
                    # Get team acronyms
                    home_team = TEAM_ACRONYMS.get(match_info['home'].strip(), match_info['home'].strip())
                    away_team = TEAM_ACRONYMS.get(match_info['away'].strip(), match_info['away'].strip())
                    
                    # Create alert message
                    alert_message = (
                        f"🔔 Match Alert!\n"
                        f"Match {match_no}: {home_team} vs {away_team}\n"
                        f"Starting in {int(time_until_match.total_seconds() / 60)} minutes!\n"
                        f"Venue: {match_info['venue']}"
                    )
                    
                    # Send alert to each user
                    successful_sends = 0
                    for user_id in users_with_alerts:
                        try:
                            user = await client.fetch_user(user_id)
                            if user:
                                await user.send(alert_message)
                                successful_sends += 1
                                logger.info(f"Sent alert to user {user_id} for Match {match_no}")
                        except discord.Forbidden:
                            logger.warning(f"Could not send DM to user {user_id} - DMs disabled")
                        except Exception as e:
                            logger.error(f"Error sending alert to user {user_id}: {str(e)}")
                    
                    # Mark this match as alerted if we sent at least one successful alert
                    if successful_sends > 0:
                        check_match_alerts.last_alerted_matches.add(match_no)
                        logger.info(f"Successfully sent alerts for Match {match_no} to {successful_sends} users")
                    
                except Exception as e:
                    logger.error(f"Error processing match {match_no}: {str(e)}")
                    continue
            
            # Sleep for 1 minute before next check
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Error in alert checking task: {str(e)}")
            await asyncio.sleep(60)  # Wait a minute before retrying

@client.event
async def on_ready():
    logger.info(f"Dream11 Bot has logged in as {client.user}")
    logger.info(f"Bot is in {len(client.guilds)} guilds")
    
    # Check DM permissions
    # try:
    #     i=0;
    #     for i in range (0,3):# Try to send a DM to the bot owner
    #         owner = await client.fetch_user(Config.ADMIN_USER_IDS[0])  # Get first admin as owner
    #         await owner.send("✅ Bot has successfully started and has DM permissions!")
    #         logger.info("DM permissions verified successfully")
    #         i=i+1
    # except discord.Forbidden:
    #     logger.error("❌ Bot does not have permission to send DMs. Please enable DMs in Discord settings.")
    # except Exception as e:
    #     logger.error(f"Error checking DM permissions: {e}")
    
    # Start the alert checking task
    client.loop.create_task(check_match_alerts())
    logger.info("Alert checking task started")

# Command router
class CommandRouter:
    def __init__(self):
        self.commands: Dict[str, Callable[[discord.Message], Awaitable[None]]] = {}
        self.middlewares: List[Callable[[discord.Message], Awaitable[bool]]] = []
        
    def command(self, name: str):
        def decorator(func: Callable[[discord.Message], Awaitable[None]]):
            self.commands[name] = func
            return func
        return decorator
        
    def add_middleware(self, func: Callable[[discord.Message], Awaitable[bool]]):
        self.middlewares.append(func)
        return func
        
    async def process(self, message: discord.Message) -> None:
        if message.author == client.user:
            return
            
        # Run middleware
        for middleware in self.middlewares:
            if not await middleware(message):
                return
                
        # Find and execute command
        for prefix, command in self.commands.items():
            if message.content.startswith(prefix):
                try:
                    await command(message)
                except Exception as e:
                    structured_logger.error(
                        "Error executing command",
                        {
                            "command": prefix,
                            "user": message.author.name,
                            "error": str(e)
                        }
                    )
                    await message.channel.send("❌ An error occurred while processing your command. Please try again later.")
                break

# Initialize command router
router = CommandRouter()

# Command middleware
@router.add_middleware
async def check_rate_limit_middleware(message: discord.Message) -> bool:
    if not check_rate_limit(message.author.id):
        await message.channel.send("⚠️ You're using commands too quickly. Please wait a moment.")
        return False
    return True

@router.add_middleware
async def check_command_cooldown_middleware(message: discord.Message) -> bool:
    global last_command_time
    current_time = time.time()
    if current_time - last_command_time < Config.COMMAND_COOLDOWN:
        return False
    last_command_time = current_time
    return True

# Command handlers
@router.command("!win")
async def handle_win(message: discord.Message):
    # Check command cooldown
    if not get_command_cooldown(message.author.id, "win"):
        await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
        return

    # Extract username and match number from command
    parts = message.content[len("!win "):].strip().split()
    if len(parts) < 2:
        await message.channel.send("❌ Please specify both username and match number: `!win <username> <match_number>`")
        return
        
    username = parts[0]
    try:
        match_number = int(parts[1])
    except ValueError:
        await message.channel.send("❌ Please provide a valid match number.")
        return

    # Validate input
    is_valid, error_message = validate_input(username, match_number)
    if not is_valid:
        await message.channel.send(f"❌ {error_message}")
        return
    
    # Get current date
    current_date = datetime.now().date()
    
    # Check if user is admin
    if not is_admin(message.author):
        # For regular users, check if match is scheduled for today
        match_schedule = IPL_2025_SCHEDULE.get(match_number)
        if not match_schedule:
            await message.channel.send(f"❌ Match {match_number} not found in schedule.")
            return
        
        match_date = match_schedule['date'].date()
        if match_date != current_date:
            # Format the match date for better readability
            formatted_date = match_schedule['date'].strftime('%B %d, %Y')
            await message.channel.send(
                f"❌ You can only record points for matches scheduled for today.\n"
                f"Match {match_number} is scheduled for {formatted_date}.\n"
                f"Only admins can record points for matches on other dates."
            )
            logger.info(f"User {message.author.name} attempted to record points for Match {match_number} scheduled for {formatted_date}")
            return
    
    # Update points
    try:
        update_points(username, 1, match_number, message.author.name)
        success_message = f"✅ Added 1 point to {format_username(username)} for winning Match {match_number}"
        if is_admin(message.author):
            success_message += " (Admin override)"
        await message.channel.send(success_message)
        logger.info(f"Points updated: Match {match_number} - Winner: {username} - Recorded by: {message.author.name}")
    except Exception as e:
        logger.error(f"Error updating points: {str(e)}")
        await message.channel.send("❌ Error updating points. Please try again later.")

@router.command("!undo")
async def handle_undo(message: discord.Message):
    # Check command cooldown
    if not get_command_cooldown(message.author.id, "undo"):
        await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
        return

    # Check if user is admin
    if not is_admin(message.author):
        await message.channel.send("❌ This command is restricted to admin users only.")
        return
        
    success, message_text = undo_last_points_update()
    if success:
        await message.channel.send(f"✅ {message_text}")
    else:
        await message.channel.send(f"❌ {message_text}")

@router.command("!clearpoints")
async def handle_clearpoints(message: discord.Message):
    # Check command cooldown
    if not get_command_cooldown(message.author.id, "clearpoints"):
        await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
        return

    # Check if user is admin
    if not is_admin(message.author):
        await message.channel.send("❌ This command is restricted to admin users only.")
        return

    clear_points()
    await message.channel.send("✅ All Dream11 points have been cleared successfully.")

@router.command("!mystats")
async def handle_mystats(message: discord.Message):
    try:
        logger.info(f"Processing mystats command for user {message.author.name}")
        
        # Get user stats (points and alert status)
        stats = get_user_stats(message.author.id)
        if not stats:
            points = 0
            alert_enabled = False
        else:
            points, alert_enabled = stats[0]
            
        logger.info(f"Stats for user {message.author.name}: Points={points}, Alerts={alert_enabled}")
        
        # Create embed for stats
        embed = discord.Embed(
            title=f"Stats for {message.author.name}",
            description="Your Dream11 contest statistics",
            color=discord.Color.blue()
        )
        
        # Add alert status with bell emoji
        embed.add_field(
            name="🔔 Match Alerts",
            value="✅Enabled" if alert_enabled else "❌ Disabled",
            inline=True
        )
        
        # Add points with trophy emoji
        embed.add_field(
            name="🏆 Points Won",
            value=str(points),
            inline=True
        )
        
        # Add footer with hint about !alert command
        embed.set_footer(text="Use !alert to toggle match alerts")
        
        await message.channel.send(embed=embed)
        
    except Exception as e:
        logger.error(f"Error processing mystats command: {str(e)}")
        await message.channel.send("❌ Error fetching your stats. Please try again later.")

@client.event
async def on_message(message: discord.Message):
    await router.process(message)

# Run the bot
try:
    logger.info("Attempting to start bot with Discord token...")
    client.run(Config.DISCORD_TOKEN)
except Exception as e:
    logger.error(f"Failed to start bot: {e}")
    raise