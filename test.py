import pandas as pd

# Read CSV file
df = pd.read_csv("IPL_2025_SEASON_SCHEDULE.csv")

# Display first few rows
print(df.head())

# Access a specific column
print(df["Match Day"])

# Access a specific row
print(df.iloc[0])  # First row