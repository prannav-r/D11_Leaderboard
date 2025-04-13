import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
import re
import csv
from config import Config
from database import (
    init_db,
    get_points,
    update_points,
    clear_points,
    undo_last_points_update,
    get_match_results,
    get_user_alert_preference,
    set_user_alert_preference,
    get_users_with_alerts,
    get_user_match_wins,
    get_user_stats,
    has_used_win_today,
    is_match_today
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
    get_ist_time,
    convert_to_ist
)
import time
import pytz

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
    """Check for upcoming matches and send alerts at 3 PM and 7 PM IST"""
    while True:
        try:
            # Get current time in IST
            current_time = get_ist_time()
            current_hour = current_time.hour
            
            # Only check at 3 PM and 7 PM IST
            if current_hour not in [15, 19]:  # 15 = 3 PM, 19 = 7 PM IST
                # Sleep until next check time
                if current_hour < 15:
                    sleep_until = current_time.replace(hour=15, minute=0, second=0, microsecond=0)
                elif current_hour < 19:
                    sleep_until = current_time.replace(hour=19, minute=0, second=0, microsecond=0)
                else:
                    # If past 7 PM, sleep until 3 PM next day
                    sleep_until = (current_time + timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
                
                sleep_seconds = (sleep_until - current_time).total_seconds()
                logger.info(f"Sleeping until {sleep_until} ({sleep_seconds} seconds)")
                await asyncio.sleep(sleep_seconds)
                continue
            
            logger.info(f"Checking alerts at {current_time}")
            
            # Get users with alerts enabled
            users_with_alerts = get_users_with_alerts()
            if not users_with_alerts:
                await asyncio.sleep(3600)  # Sleep for 1 hour if no alerts
                continue
            
            # Check each match in the schedule
            for match_no, match_info in IPL_2025_SCHEDULE.items():
                try:
                    # Check if alert is enabled for this match
                    if not match_info.get('alert', False):
                        continue
                        
                    # Parse match start time (already in IST)
                    match_date = match_info['date']
                    start_time = datetime.strptime(match_info['start'], '%H:%M').time()
                    match_datetime = datetime.combine(match_date, start_time)
                    
                    # Only send alerts for matches today
                    if match_date.date() != current_time.date():
                        continue
                    
                    # Get team acronyms
                    home_team = TEAM_ACRONYMS.get(match_info['home'].strip(), match_info['home'].strip())
                    away_team = TEAM_ACRONYMS.get(match_info['away'].strip(), match_info['away'].strip())
                    
                    # Create alert message
                    alert_message = (
                        f"🔔 Match Alert!\n"
                        f"Match {match_no}: {home_team} vs {away_team}\n"
                        f"Starting at {match_info['start']} IST!\n"
                        f"Venue: {match_info['venue']}"
                    )
                    
                    # Send alert to each user
                    for user_id in users_with_alerts:
                        try:
                            user = await client.fetch_user(user_id)
                            if user:
                                await user.send(alert_message)
                                logger.info(f"Sent alert to user {user_id} for Match {match_no}")
                        except Exception as e:
                            logger.error(f"Error sending alert to user {user_id}: {str(e)}")
                            
                except Exception as e:
                    logger.error(f"Error processing match {match_no}: {str(e)}")
                    continue
            
            # Sleep for 1 hour before next check
            await asyncio.sleep(3600)
            
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

async def handle_win_command(message, match_number: int, username: str) -> None:
    """Handle the !win command"""
    # Check if user has already used win today
    if has_used_win_today(match_number):
        await message.channel.send("❌ You have already used !win today. Try again tomorrow!")
        return

    # Check if match is today
    if not is_match_today(match_number, IPL_2025_SCHEDULE):
        await message.channel.send("❌ You can only use !win for matches scheduled today!")
        return

    # Update points
    try:
        await update_points(username, 1, match_number, f"<@{message.author.id}>")
        await message.channel.send(f"✅ {format_username(username)} has won 1 point for Match {match_number}!")
    except Exception as e:
        logger.error(f"Error updating points: {str(e)}")
        await message.channel.send("❌ Error updating points. Please try again later.")

async def handle_undo_command(message) -> None:
    """Handle the !undo command"""
    if not is_admin(message.author):
        await message.channel.send("❌ Only admins can use this command!")
        return

    success, message_text = undo_last_points_update()
    await message.channel.send(message_text)

async def handle_clear_command(message) -> None:
    """Handle the !clear command"""
    if not is_admin(message.author):
        await message.channel.send("❌ Only admins can use this command!")
        return

    clear_points()
    await message.channel.send("✅ All points and history have been cleared!")

async def handle_points_command(message, username: Optional[str] = None) -> None:
    """Handle the !points command"""
    if username:
        # Get points for specific user
        points = get_points(int(username.strip('@<>')))
        await message.channel.send(f"{format_username(username)} has {points} point(s)!")
    else:
        # Get points for all users
        points = get_points()
        await message.channel.send(format_points(points))

async def handle_alert_command(message, enabled: bool) -> None:
    """Handle the !alert command"""
    success = set_user_alert_preference(message.author.id, enabled)
    if success:
        status = "enabled" if enabled else "disabled"
        await message.channel.send(f"✅ Match alerts have been {status}!")
    else:
        await message.channel.send("❌ Failed to update alert preference. Please try again later.")

async def handle_tdy_command(message) -> None:
    """Handle the !tdy command"""
    # Get current date in IST
    current_time = get_ist_time()
    current_date = current_time.date()
    
    # Find matches scheduled for today
    today_matches = []
    for match_no, match_info in IPL_2025_SCHEDULE.items():
        if match_info['date'].date() == current_date:
            # Get team acronyms
            home_team = match_info['home'].strip()
            away_team = match_info['away'].strip()
            home_acronym = TEAM_ACRONYMS.get(home_team, home_team)
            away_acronym = TEAM_ACRONYMS.get(away_team, away_team)
            
            today_matches.append({
                'match_no': match_no,
                'home': home_acronym,
                'away': away_acronym,
                'start': match_info['start']
            })
    
    if not today_matches:
        await message.channel.send("No matches scheduled for today.")
        return
        
    # Create output message
    output = "🏏 Today's Matches 🏏\n\n"
    output += "Match #" + " " * 5 + "Teams" + " " * 20 + "Start Time\n"
    output += "-" * 50 + "\n"
    
    # Sort matches by match number
    today_matches.sort(key=lambda x: x['match_no'])
    
    for match in today_matches:
        output += f"Match {match['match_no']:<5} {match['home']} vs {match['away']:<15} {match['start']} IST\n"
    
    await message.channel.send(output)

async def handle_help_command(message) -> None:
    """Handle the !help command"""
    help_message = (
        "🤖 Dream11 Bot Commands 🤖\n\n"
        "!win <match_number> <@username> - Award 1 point to a user for a match\n"
        "!points [@username] - Show points for all users or a specific user\n"
        "!undo - Undo the last points update (Admin only)\n"
        "!clear - Clear all points and history (Admin only)\n"
        "!alert on/off - Enable/disable match alerts\n"
        "!tdy - Show today's matches\n"
        "!help - Show this help message\n\n"
        "Note: All times are in IST"
    )
    await message.channel.send(help_message)

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    # Check rate limit
    if not check_rate_limit(message.author.id):
        await message.channel.send("⚠️ You're using commands too quickly. Please wait a moment.")
        return

    # Check for command cooldown
    global last_command_time
    current_time = time.time()
    if current_time - last_command_time < Config.COMMAND_COOLDOWN:
        return
    last_command_time = current_time

    try:
        if message.content.startswith("!win"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "win"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            # Parse command
            parts = message.content.split()
            if len(parts) != 4:
                await message.channel.send("❌ Invalid format. Use: !win <match_number> <@username>")
                return

            try:
                match_number = int(parts[1])
                username = parts[2] + " " + parts[3]  # Handle usernames with spaces
            except (ValueError, IndexError):
                await message.channel.send("❌ Invalid format. Use: !win <match_number> <@username>")
                return

            # Validate input
            is_valid, error_msg = validate_input(username, match_number)
            if not is_valid:
                await message.channel.send(f"❌ {error_msg}")
                return

            await handle_win_command(message, match_number, username)

        elif message.content.startswith("!undo"):
            if not get_command_cooldown(message.author.id, "undo"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return
            await handle_undo_command(message)

        elif message.content.startswith("!clear"):
            if not get_command_cooldown(message.author.id, "clear"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return
            await handle_clear_command(message)

        elif message.content.startswith("!points"):
            if not get_command_cooldown(message.author.id, "points"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            parts = message.content.split()
            username = parts[1] if len(parts) > 1 else None
            await handle_points_command(message, username)

        elif message.content.startswith("!alert"):
            if not get_command_cooldown(message.author.id, "alert"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            parts = message.content.split()
            if len(parts) != 2 or parts[1] not in ["on", "off"]:
                await message.channel.send("❌ Invalid format. Use: !alert on/off")
                return

            await handle_alert_command(message, parts[1] == "on")

        elif message.content.startswith("!tdy"):
            if not get_command_cooldown(message.author.id, "tdy"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return
            await handle_tdy_command(message)

        elif message.content.startswith("!help"):
            if not get_command_cooldown(message.author.id, "help"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return
            await handle_help_command(message)

    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        await message.channel.send("❌ An error occurred. Please try again later.")

# Run the bot
try:
    logger.info("Attempting to start bot with Discord token...")
    client.run(Config.DISCORD_TOKEN)
except Exception as e:
    logger.error(f"Failed to start bot: {e}")
    raise