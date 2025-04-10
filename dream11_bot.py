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
    format_username
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
    """Check for upcoming matches and send alerts at 3 PM and 7 PM"""
    while True:
        try:
            # Get current time in UTC
            current_time = datetime.now(timezone.utc)
            current_hour = current_time.hour
            
            # Only check at 3 PM and 7 PM
            if current_hour not in [15, 19]:  # 15 = 3 PM, 19 = 7 PM
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
                        
                    # Parse match start time
                    match_date = match_info['date']
                    start_time = datetime.strptime(match_info['start'], '%H:%M').time()
                    match_datetime = datetime.combine(match_date, start_time)
                    match_datetime = match_datetime.replace(tzinfo=timezone.utc)
                    
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
                        f"Starting at {match_info['start']}!\n"
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
            if len(parts) != 3:
                await message.channel.send("❌ Invalid command format. Use: !win @username match_number")
                return

            # Extract username and match number
            username = parts[1]
            try:
                match_number = int(parts[2])
            except ValueError:
                await message.channel.send("❌ Invalid match number. Please provide a valid number.")
                return

            # Validate username format
            if not username.startswith('<@') or not username.endswith('>'):
                await message.channel.send("❌ Invalid username format. Please mention the user using @.")
                return

            # Check if user is admin
            if not is_admin(message.author):
                # For non-admin users:
                # 1. Check if they've already used the command today
                if has_used_win_today(message.author.id):
                    await message.channel.send("❌ You can only use the !win command once per day.")
                    return

                # 2. Check if the match is scheduled for today
                if not is_match_today(match_number):
                    await message.channel.send("❌ You can only record wins for matches scheduled for today.")
                    return

            # Update points
            await update_points(username, 1, match_number, message.author.name)
            await message.channel.send(f"✅ Added 1 point to {username} for Match {match_number}")

        elif message.content.startswith("!d11"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "d11"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            try:
                # Get points and match results
                points = get_points()
                match_results = get_match_results()
                
                # Format leaderboard
                leaderboard = "🏆 Dream11 Leaderboard 🏆\n\n"
                if points:
                    sorted_users = sorted(points.items(), key=lambda x: x[1], reverse=True)
                    for rank, (user, points) in enumerate(sorted_users, 1):
                        leaderboard += f"{rank}. {format_username(user)}: {points} point(s)\n"
                else:
                    leaderboard += "No points recorded yet!\n"
                
                # Send leaderboard first
                await message.channel.send(leaderboard)
                
                # Add recent match results section if there are results
                if match_results:
                    # Sort match results by match number in descending order and take last 5
                    sorted_matches = sorted(match_results, key=lambda x: x[0], reverse=True)[:5]
                    
                    # Create header for recent matches
                    match_log = "🏆 Recent Match Winners 🏆\n\n"
                    match_log += "Match #     Match Details                    Winner\n"
                    match_log += "-" * 70 + "\n"
                    
                    # Add matches
                    for match_no, winner, _, _ in sorted_matches:
                        # Get match details from schedule
                        match_info = IPL_2025_SCHEDULE.get(match_no, {})
                        if match_info:
                            home_team = TEAM_ACRONYMS.get(match_info['home'].strip(), match_info['home'].strip())
                            away_team = TEAM_ACRONYMS.get(match_info['away'].strip(), match_info['away'].strip())
                            match_details = f"{home_team} vs {away_team}"
                        else:
                            match_details = "Unknown Teams"
                        
                        # Format the line with proper spacing
                        match_log += f"Match {match_no:<5} {match_details:<30} {format_username(winner)}\n"
                    
                    # Send the recent matches
                    await message.channel.send(match_log)
                
            except Exception as e:
                logger.error(f"Error displaying leaderboard: {str(e)}")
                error_message = "❌ Error displaying leaderboard. "
                if "Failed to access" in str(e):
                    error_message += "Database connection error. Please check your Supabase configuration."
                elif "Failed to get points" in str(e):
                    error_message += "Unable to fetch points data."
                elif "Failed to get match results" in str(e):
                    error_message += "Unable to fetch match results."
                else:
                    error_message += "Please try again later."
                await message.channel.send(error_message)

        elif message.content.startswith("!undo"):
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

        elif message.content.startswith("!clearpoints"):
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

        elif message.content.startswith("!adminlog"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "adminlog"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            # Check if user is admin
            if not is_admin(message.author):
                await message.channel.send("❌ This command is restricted to admin users only.")
                return
                
            try:
                match_results = get_match_results()
                if not match_results:
                    await message.channel.send("No match results recorded yet!")
                else:
                    # Sort match results by match number
                    sorted_matches = sorted(match_results, key=lambda x: x[0])
                    
                    # Split matches into chunks of 10 for better readability
                    chunk_size = 10
                    for i in range(0, len(sorted_matches), chunk_size):
                        chunk = sorted_matches[i:i + chunk_size]
                        
                        # Create header for this chunk
                        output = "📊 Detailed Match Results Log:\n\n"
                        if i > 0:
                            output = f"📊 Detailed Match Results Log (Continued):\n\n"
                        
                        # Add matches for this chunk
                        for match_no, winner, timestamp, admin in chunk:
                            # Get match details from schedule
                            match_info = IPL_2025_SCHEDULE.get(match_no, {})
                            if match_info:
                                home_team = TEAM_ACRONYMS.get(match_info['home'].strip(), match_info['home'].strip())
                                away_team = TEAM_ACRONYMS.get(match_info['away'].strip(), match_info['away'].strip())
                                match_details = f"{home_team} vs {away_team}"
                            else:
                                match_details = "Unknown Teams"
                            
                            output += f"Match: {match_no}\n"
                            output += f"Teams: {match_details}\n"
                            output += f"Winner: {format_username(winner)}\n"
                            output += f"Recorded by: {admin}\n"
                            output += f"Timestamp: {timestamp}\n"
                            output += "-" * 30 + "\n"
                        
                        # Send the chunk
                        await message.channel.send(output)
                        
            except Exception as e:
                logger.error(f"Error reading match results: {str(e)}")
                error_message = "❌ Error reading match results. "
                if "Failed to access" in str(e):
                    error_message += "Database connection error. Please check your Supabase configuration."
                elif "Failed to get match results" in str(e):
                    error_message += "Unable to fetch match results data."
                else:
                    error_message += "Please try again later."
                await message.channel.send(error_message)

        elif message.content.startswith("!tdy"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "tdy"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            # Get current date
            current_date = datetime.now().date()
            
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
                output += f"Match {match['match_no']:<5} {match['home']} vs {match['away']:<15} {match['start']}\n"
            
            await message.channel.send(output)

        elif message.content.startswith("!about"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "about"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            # Create an embed message
            embed = discord.Embed(
                title="📋 Dream11 Bot Commands",
                description="Here is the list of Dream11 commands you can use:",
                color=discord.Color.blue()
            )
            
            # Add fields for regular commands
            embed.add_field(
                name="Regular Commands",
                value="These commands are available to all users:",
                inline=False
            )
            embed.add_field(
                name="1. `!win <username> <match_number>`",
                value="Add 1 point to a user for winning a match\nYou can use @mentions or regular usernames",
                inline=False
            )
            embed.add_field(
                name="2. `!d11`",
                value="Show Dream11 leaderboard and match winners log",
                inline=False
            )
            embed.add_field(
                name="3. `!tdy`",
                value="Show today's scheduled matches",
                inline=False
            )
            embed.add_field(
                name="4. `!alert`",
                value="Toggle match alerts (30 minutes before each match)",
                inline=False
            )
            embed.add_field(
                name="5. `!mystats`",
                value="Show your personal stats (points and alert status)",
                inline=False
            )
            embed.add_field(
                name="6. `!about`",
                value="Show this help message",
                inline=False
            )
            
            # Add separator
            embed.add_field(
                name="\u200b",  # Zero-width space for visual separation
                value="\u200b",
                inline=False
            )
            
            # Add fields for admin commands
            embed.add_field(
                name="Admin Commands",
                value="These commands are restricted to admin users only:",
                inline=False
            )
            embed.add_field(
                name="1. `!undo`",
                value="Undo last point change",
                inline=False
            )
            embed.add_field(
                name="2. `!clearpoints`",
                value="Clear all points",
                inline=False
            )
            embed.add_field(
                name="3. `!adminlog`",
                value="Show detailed match results log",
                inline=False
            )

            # Footer with developer credit
            embed.set_footer(text="Developed by Pr😉")

            # Send the embed message
            await message.channel.send(embed=embed)

        elif message.content.startswith("!alert"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "alert"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            try:
                # Get current preference
                current_preference = get_user_alert_preference(message.author.id)
                logger.info(f"Current alert preference for user {message.author.id}: {current_preference}")
                
                # Toggle the preference
                new_preference = not current_preference
                set_user_alert_preference(message.author.id, new_preference)
                logger.info(f"Updated alert preference for user {message.author.id} to: {new_preference}")
                
                # Send confirmation message
                if new_preference:
                    await message.channel.send(
                        "✅ Match alerts enabled! You will receive a DM 30 minutes before each match starts.\n"
                        "Use `!alert` again to disable alerts."
                    )
                else:
                    await message.channel.send(
                        "✅ Match alerts disabled! You will no longer receive match alerts.\n"
                        "Use `!alert` again to enable alerts."
                    )
                    
            except DatabaseError as e:
                logger.error(f"Database error in alert command: {str(e)}")
                await message.channel.send(
                    "❌ Error updating alert preference. Database error occurred.\n"
                    "Please try again later or contact an admin if the issue persists."
                )
            except Exception as e:
                logger.error(f"Unexpected error in alert command: {str(e)}")
                await message.channel.send(
                    "❌ An unexpected error occurred while updating alert preference.\n"
                    "Please try again later or contact an admin if the issue persists."
                )

        elif message.content.startswith("!mystats"):
            try:
                logger.info(f"Processing mystats command for user {message.author.name}")
                
                # Get user stats
                stats = get_user_stats(message.author.id)
                points = stats[0][0]  # Get points
                alert_status = stats[0][1]  # Get alert status
                
                # Get match wins
                wins = get_user_match_wins(message.author.id)
                
                # Create embed
                embed = discord.Embed(
                    title=f"{message.author.name}'s Stats",
                    color=discord.Color.blue()
                )
                
                # Add points
                embed.add_field(
                    name="Points",
                    value=str(points),
                    inline=True
                )
                
                # Add alert status
                alert_text = "✅ Enabled" if alert_status else "❌ Disabled"
                embed.add_field(
                    name="Match Alerts",
                    value=alert_text,
                    inline=True
                )
                
                # Add match wins if any
                if wins:
                    wins_text = "\n".join([f"Match {win[0]} - {win[2]}" for win in wins])
                    embed.add_field(
                        name="Match Wins",
                        value=wins_text,
                        inline=False
                    )
                
                await message.channel.send(embed=embed)
                
            except Exception as e:
                logger.error(f"Error processing mystats command: {str(e)}")
                await message.channel.send("❌ Failed to get your stats. Please try again later.")

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        await message.channel.send("❌ An unexpected error occurred. Please try again later.")

# Run the bot
try:
    logger.info("Attempting to start bot with Discord token...")
    client.run(Config.DISCORD_TOKEN)
except Exception as e:
    logger.error(f"Failed to start bot: {e}")
    raise