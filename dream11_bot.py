import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
import csv
from dotenv import load_dotenv
import os
from typing import Dict, Optional
import asyncio
from config import Config
from database import (
    init_db, update_points, clear_points, undo_last_point,
    get_leaderboard, get_match_results, DatabaseError
)

# Load environment variables
load_dotenv()

# Initialize bot with command prefix and intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Command cooldown decorator
def admin_command():
    """Decorator for admin-only commands"""
    async def predicate(ctx):
        if ctx.author.id not in Config.ADMIN_USER_IDS:
            await ctx.send("❌ This command is restricted to administrators only.")
            return False
        return True
    return commands.check(predicate)

# Error handler
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, DatabaseError):
        await ctx.send(f"❌ Database error: {str(error)}")
    elif isinstance(error, ValueError):
        await ctx.send(f"❌ {str(error)}")
    else:
        await ctx.send(f"❌ An error occurred: {str(error)}")

# Load IPL 2025 Schedule
def load_schedule() -> Dict:
    schedule = {}
    try:
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
    except FileNotFoundError:
        print("Warning: IPL schedule file not found")
    except Exception as e:
        print(f"Error loading schedule: {str(e)}")
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

@bot.event
async def on_ready():
    print(f"Dream11 Bot has logged in as {bot.user}")
    init_db()

@bot.command(name='win')
@admin_command()
async def record_win(ctx, username: str, match_number: int):
    """Record a match win for a user"""
    try:
        update_points(username, 10, match_number, str(ctx.author))
        await ctx.send(f"✅ Recorded win for {username} in match {match_number}")
    except ValueError as e:
        await ctx.send(f"❌ {str(e)}")

@bot.command(name='points')
@admin_command()
async def update_user_points(ctx, username: str, points: int):
    """Update points for a user"""
    try:
        update_points(username, points, recorded_by=str(ctx.author))
        await ctx.send(f"✅ Updated points for {username}: {points:+d}")
    except ValueError as e:
        await ctx.send(f"❌ {str(e)}")

@bot.command(name='leaderboard')
async def show_leaderboard(ctx):
    """Display the current leaderboard"""
    try:
        points_data = get_leaderboard()
        if not points_data:
            await ctx.send("No points recorded yet!")
            return

        leaderboard = "🏆 Dream11 Leaderboard 🏆\n\n"
        for rank, (user, points) in enumerate(points_data, 1):
            leaderboard += f"{rank}. {user}: {points} point(s)\n"

        match_results = get_match_results()
        if match_results:
            leaderboard += "\n\n🏆 Dream11 Contest Match Winners Log 🏆\n\n"
            leaderboard += "Match #" + " " * 5 + "Match Details" + " " * 20 + "Winner\n"
            leaderboard += "-" * 70 + "\n"

            for match_no, winner, _ in match_results:
                schedule_info = IPL_2025_SCHEDULE.get(match_no, {})
                if schedule_info:
                    home_team = schedule_info['home'].strip()
                    away_team = schedule_info['away'].strip()
                    home_acronym = TEAM_ACRONYMS.get(home_team, home_team)
                    away_acronym = TEAM_ACRONYMS.get(away_team, away_team)
                    match_details = f"{home_acronym} vs {away_acronym}"
                else:
                    match_details = "Unknown Match"

                leaderboard += f"Match {match_no:<5} {match_details:<30} {winner:<15}\n"

        await ctx.send(leaderboard)
    except DatabaseError as e:
        await ctx.send(f"❌ {str(e)}")

@bot.command(name='undo')
@admin_command()
async def undo_points(ctx):
    """Undo the last point change"""
    try:
        success, message = undo_last_point()
        await ctx.send(message)
    except DatabaseError as e:
        await ctx.send(f"❌ {str(e)}")

@bot.command(name='clear')
@admin_command()
async def clear_all_points(ctx):
    """Clear all points and match results"""
    try:
        clear_points()
        await ctx.send("✅ All points and match results have been cleared successfully.")
    except DatabaseError as e:
        await ctx.send(f"❌ {str(e)}")

def main():
    """Main entry point with configuration validation"""
    errors = Config.validate()
    if errors:
        print("Configuration errors:")
        for key, error in errors.items():
            print(f"- {key}: {error}")
        return

    try:
        bot.run(Config.DISCORD_TOKEN)
    except discord.LoginFailure:
        print("Failed to log in: Invalid token")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 