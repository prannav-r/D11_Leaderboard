import discord
import sqlite3
from datetime import datetime
import csv
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize client
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

# Database initialization
def init_db():
    conn = sqlite3.connect('dream11.db')
    c = conn.cursor()
    
    # Create points table
    c.execute('''CREATE TABLE IF NOT EXISTS points
                 (user TEXT PRIMARY KEY, points INTEGER DEFAULT 0)''')
    
    # Create history table
    c.execute('''CREATE TABLE IF NOT EXISTS history
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user TEXT,
                  points_change INTEGER,
                  timestamp TEXT,
                  match_number INTEGER,
                  recorded_by TEXT)''')
    
    # Create match_results table
    c.execute('''CREATE TABLE IF NOT EXISTS match_results
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  match_number INTEGER,
                  winner TEXT,
                  timestamp TEXT,
                  recorded_by TEXT)''')
    
    conn.commit()
    conn.close()

# Initialize database at startup
init_db()

# Admin user ID (replace with your Discord user ID)
ADMIN_USER_ID = 796665468664021012

def get_dream11_points():
    """Get all Dream11 points from database"""
    conn = sqlite3.connect('dream11.db')
    c = conn.cursor()
    c.execute('SELECT user, points FROM points')
    points = dict(c.fetchall())
    conn.close()
    return points

def update_dream11_points(user, points_change, match_number=None, recorded_by=None):
    """Update Dream11 points for a user in database"""
    conn = sqlite3.connect('dream11.db')
    c = conn.cursor()
    
    # Update or insert points
    c.execute('INSERT INTO points (user, points) VALUES (?, ?) ON CONFLICT(user) DO UPDATE SET points = points + ?',
              (user, points_change, points_change))
    
    # Record history
    c.execute('INSERT INTO history (user, points_change, timestamp, match_number, recorded_by) VALUES (?, ?, ?, ?, ?)',
              (user, points_change, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), match_number, recorded_by))
    
    # If this is a match win, add to match results
    if match_number is not None:
        c.execute('INSERT INTO match_results (match_number, winner, timestamp, recorded_by) VALUES (?, ?, ?, ?)',
                  (match_number, user, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), recorded_by))
    
    conn.commit()
    conn.close()

def clear_all_points():
    """Clear all Dream11 points and match results from database"""
    try:
        conn = sqlite3.connect('dream11.db')
        c = conn.cursor()
        
        # Clear all tables
        c.execute('DELETE FROM points')
        c.execute('DELETE FROM history')
        c.execute('DELETE FROM match_results')
        
        conn.commit()
        conn.close()
        return "✅ All Dream11 points, history, and match results have been cleared successfully."
    except Exception as e:
        return f"❌ Error clearing points: {str(e)}"

def display_dream11_leaderboard():
    """Display Dream11 leaderboard and match winners log from database"""
    conn = sqlite3.connect('dream11.db')
    c = conn.cursor()
    
    # Get points
    c.execute('SELECT user, points FROM points ORDER BY points DESC')
    points_data = c.fetchall()
    
    if not points_data:
        conn.close()
        return "No points recorded yet!"
    
    # Create leaderboard message
    leaderboard = "🏆 Dream11 Leaderboard 🏆\n\n"
    for rank, (user, points) in enumerate(points_data, 1):
        leaderboard += f"{rank}. {user}: {points} point(s)\n"
    
    # Get match results
    c.execute('SELECT match_number, winner, timestamp FROM match_results ORDER BY match_number')
    match_results = c.fetchall()
    
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
    
    conn.close()
    return leaderboard

def undo_last_dream11_point():
    """Undo the last Dream11 point change from database"""
    conn = sqlite3.connect('dream11.db')
    c = conn.cursor()
    
    # Get last history entry
    c.execute('SELECT user, points_change, match_number FROM history ORDER BY id DESC LIMIT 1')
    last_change = c.fetchone()
    
    if not last_change:
        conn.close()
        return False, "No points to undo"
    
    user, points_change, match_number = last_change
    
    # Revert points
    c.execute('UPDATE points SET points = points - ? WHERE user = ?', (points_change, user))
    
    # Remove last history entry
    c.execute('DELETE FROM history WHERE id = (SELECT MAX(id) FROM history)')
    
    # If this was a match win, remove it from match_results
    if match_number is not None:
        c.execute('DELETE FROM match_results WHERE match_number = ? AND winner = ?', (match_number, user))
    
    conn.commit()
    conn.close()
    
    return True, f"Undid {points_change} point(s) for {user}"

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

def is_admin(user):
    """Check if the user is an admin"""
    return user.id == ADMIN_USER_ID

# Discord bot events
@client.event
async def on_ready():
    print(f"Dream11 Bot has logged in as {client.user}")

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith("!win"):
        try:
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
            
            # Check if match has already been recorded
            conn = sqlite3.connect('dream11.db')
            c = conn.cursor()
            c.execute('SELECT match_number FROM match_results WHERE match_number = ?', (match_number,))
            result = c.fetchone()
            conn.close()
            if result:
                await message.channel.send(f"❌ Match {match_number} has already been recorded. Winner: {result[0]}")
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
            update_dream11_points(username, 1, match_number, message.author.name)
            await message.channel.send(f"✅ Added 1 point to {username} for winning Match {match_number}")
            
        except Exception as e:
            await message.channel.send(f"❌ Error updating points: {str(e)}")

    elif message.content.startswith("!d11"):
        leaderboard = display_dream11_leaderboard()
        await message.channel.send(leaderboard)

    elif message.content.startswith("!undo"):
        # Check if user is admin
        if not is_admin(message.author):
            await message.channel.send("❌ This command is restricted to admin users only.")
            return
            
        success, message_text = undo_last_dream11_point()
        if success:
            await message.channel.send(f"✅ {message_text}")
        else:
            await message.channel.send(f"❌ {message_text}")

    elif message.content.startswith("!clearpoints"):
        # Check if user is admin
        if not is_admin(message.author):
            await message.channel.send("❌ This command is restricted to admin users only.")
            return
        await message.channel.send(clear_all_points())

    elif message.content.startswith("!adminlog"):
        # Check if user is admin
        if not is_admin(message.author):
            await message.channel.send("❌ This command is restricted to admin users only.")
            return
            
        try:
            conn = sqlite3.connect('dream11.db')
            c = conn.cursor()
            c.execute('SELECT match_number, winner, timestamp, recorded_by FROM match_results ORDER BY match_number')
            results = c.fetchall()
            conn.close()
            if not results:
                await message.channel.send("No match results recorded yet!")
            else:
                output = "Match Results File Contents:\n\n"
                for match_no, winner, timestamp, recorded_by in results:
                    output += f"Match: {match_no}\n"
                    output += f"Winner: {winner}\n"
                    output += f"Recorded By: {recorded_by}\n"
                    output += f"Timestamp: {timestamp}\n"
                    output += "-" * 30 + "\n"
                await message.channel.send(output)
        except Exception as e:
            await message.channel.send(f"Error reading match results: {str(e)}")

    elif message.content.startswith("!tdy"):
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
            value="Add 1 point to a user for winning a match",
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

# Run the bot
client.run(os.getenv('DREAM11_BOT_TOKEN')) 