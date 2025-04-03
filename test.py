import pandas as pd

# Load CSV file
df = pd.read_csv("IPL_2025_SEASON_SCHEDULE.csv")

# Convert "Date" and "Start" to datetime format
df["Start Time"] = pd.to_datetime(df["Date"] + " " + df["Start"])

# Subtract 30 minutes for alert time
df["Alert Time"] = df["Start Time"] - pd.Timedelta(minutes=30)

# Convert "Alert Time" to string format if needed
df["Alert Time"] = df["Alert Time"].dt.strftime("%Y-%m-%d %I:%M%p")

# Drop "Start Time" column (optional)
df.drop(columns=["Start Time"], inplace=True)

# Save the updated CSV
df.to_csv("updated_data.csv", index=False)

print(df.head())