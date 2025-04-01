import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List
import re
import csv
from config import Config
from database import (
    init_db,
    get_points,
    update_points,
    clear_points,
    undo_last_point,
    get_match_results
)
from utils import (
    setup_logging,
    is_admin,
    validate_input,
    format_points,
    get_command_cooldown,
    check_rate_limit,
    extract_mention_id
)

# Set up logging
logger = setup_logging()

# Initialize client
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

# Initialize database
init_db()

# Load IPL 2025 Schedule
def load_schedule():
    schedule = {}
    with open('IPL_2025_SEASON_SCHEDULE.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            match_no = int(row['Match No'])
            schedule[match_no] = {
                'date': datetime.strptime(row['Date'], '%Y-%m-%d'),
                'day': row['Day'],
                'start': row['Start'],
                'home': row['Home'],
                'away': row['Away'],
                'venue': row['Venue']
            }
    return schedule

# Load schedule at startup
IPL_2025_SCHEDULE = load_schedule()

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

@client.event
async def on_ready():
    logger.info(f"Dream11 Bot has logged in as {client.user}")

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    # Check rate limit
    if not check_rate_limit(message.author.id):
        await message.channel.send("⚠️ You're using commands too quickly. Please wait a moment.")
        return

    try:
        if message.content.startswith("!win"):
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
                if not match_schedule or match_schedule['date'].date() != current_date:
                    await message.channel.send("❌ You can only record points for matches scheduled for today. Admins can record points for any match.")
                    return
            
            # Update points
            update_points(username, 1, match_number, message.author.name)
            await message.channel.send(f"✅ Added 1 point to {username} for winning Match {match_number}")
            
        elif message.content.startswith("!d11"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "d11"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            points = get_points()
            leaderboard = format_points(points)
            await message.channel.send(leaderboard)

        elif message.content.startswith("!undo"):
            # Check command cooldown
            if not get_command_cooldown(message.author.id, "undo"):
                await message.channel.send(f"⏳ Please wait {Config.COMMAND_COOLDOWN} seconds before using this command again.")
                return

            # Check if user is admin
            if not is_admin(message.author):
                await message.channel.send("❌ This command is restricted to admin users only.")
                return
                
            success, message_text = undo_last_point()
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
                    output = "Match Results Log:\n\n"
                    for match_no, winner, timestamp in match_results:
                        output += f"Match: {match_no}\n"
                        output += f"Winner: {winner}\n"
                        output += f"Timestamp: {timestamp}\n"
                        output += "-" * 30 + "\n"
                    await message.channel.send(output)
            except Exception as e:
                logger.error(f"Error reading match results: {str(e)}")
                await message.channel.send("❌ Error reading match results. Please try again later.")

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
                name="4. `!about`",
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

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        await message.channel.send("❌ An unexpected error occurred. Please try again later.")

# Run the bot
client.run(Config.DISCORD_TOKEN)