# Dream11 Leaderboard Bot

A Discord bot for tracking Dream11 contest points and displaying leaderboards.

## Features

- Track points for users
- Display leaderboard
- Record match winners
- Undo last point change
- Clear all points
- Admin-only commands

## Setup Instructions

### Local Development

1. Clone this repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in your values:
   ```bash
   cp .env.example .env
   ```
5. Edit `.env` with your Discord bot token and admin user ID
6. Run the bot:
   ```bash
   python dream11_bot.py
   ```

### Railway Deployment

1. Create a new project on Railway.app
2. Connect your GitHub repository
3. Add the following environment variables in Railway:
   - `DISCORD_TOKEN`: Your Discord bot token
   - `ADMIN_USER_ID`: Your Discord user ID
4. Deploy the project

## Commands

- `!win <username> <match_number>`: Record a match win (Admin only)
- `!points <username> <points>`: Add/remove points (Admin only)
- `!leaderboard`: Display the current leaderboard
- `!undo`: Undo the last point change (Admin only)
- `!clear`: Clear all points (Admin only)

## Database

The bot uses SQLite for data storage. The database file (`dream11.db`) will be automatically created when the bot starts.

## Requirements

- Python 3.8+
- discord.py
- python-dotenv
