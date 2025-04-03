# Dream11 Leaderboard Bot

A Discord bot for tracking Dream11 contest points and match results.

## Features

- Track points for Dream11 contest winners
- View leaderboard and match results
- Admin commands for managing points
- Rate limiting and command cooldowns
- Match schedule integration

## Deployment on Railway

1. Fork this repository
2. Create a new project on [Railway](https://railway.app/)
3. Connect your GitHub repository
4. Add the following environment variables in Railway:
   ```
   DISCORD_TOKEN=your_discord_bot_token
   ADMIN_USER_IDS=123456789,987654321  # Comma-separated list of admin Discord user IDs
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_KEY=your_supabase_anon_key
   MAX_POINTS_PER_UPDATE=100
   MAX_MATCH_NUMBER=74
   COMMAND_COOLDOWN=5
   MAX_COMMANDS_PER_MINUTE=30
   DEBUG=false
   LOG_LEVEL=INFO
   ```

## Local Development

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create a `.env` file with the required environment variables
5. Run the bot:
   ```bash
   python dream11_bot.py
   ```

## Commands

### Regular Commands

- `!win <username> <match_number>` - Add 1 point to a user for winning a match
- `!d11` - Show Dream11 leaderboard and recent match winners
- `!tdy` - Show today's scheduled matches
- `!mystats` - Show your personal stats (points and alert status)
- `!about` - Show help message

### Admin Commands

- `!undo` - Undo last point change
- `!clearpoints` - Clear all points
- `!adminlog` - Show detailed match results log

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
