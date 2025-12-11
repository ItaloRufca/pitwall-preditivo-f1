
import pandas as pd
import json
import os

# Paths
BUCKET = os.environ.get('S3_BUCKET_NAME', 'f1-data-lake-raw-zone') # Fallback if env not set, though we usually load from main
# Actually we can just load the local gold path if we know where S3 maps or if we use s3fs
# But in this environment I might need to read from s3 using the configured credentials if available, or just assume the user has the data locally if they ran previous steps.
# The user's previous context shows loading from s3://{BUCKET}/gold/...
# I will use the same method as update_features.py or main.py

import s3fs

def calculate_rain_aptitude():
    try:
        # Load Data (Assuming same S3 paths as main.py)
        # We need to find the bucket name from env or hardcode/guess based on previous context if env is missing in this shell
        # I'll check env first.
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except:
            pass
            
        bucket = os.environ.get('S3_BUCKET_NAME')
        if not bucket:
            print("Error: S3_BUCKET_NAME not found.")
            return

        print(f"Loading data from {bucket}...")
        race_path = f"s3://{bucket}/gold/gold_race_widetable/"
        
        df = pd.read_parquet(race_path)
        
        # 1. Identify Wet Races
        # rain_flag should be present
        if 'rain_flag' not in df.columns:
            print("Error: rain_flag column not found.")
            return

        wet_races = df[df['rain_flag'] == 1]
        dry_races = df[df['rain_flag'] == 0]
        
        print(f"Found {len(wet_races['meeting_key'].unique())} wet races and {len(dry_races['meeting_key'].unique())} dry races.")
        
        # 2. Calculate "Rain Aptitude"
        # Metric: We want to know if a driver performs *better* or *worse* in rain relative to their usual self.
        # But pace is hard to compare because wet tracks are just slower.
        # Relative metric: (Driver Position in Wet) - (Driver Median Position in Dry)
        # Or: (Driver Avg Lap / Field Avg Lap) in Wet vs Dry.
        
        # Let's use Position Gain/Loss relative to their average Dry Grid position? 
        # Or simpler: Relative Pace. how much slower are they vs the winner in rain?
        
        # Approach: "Gap to Leader in Wet" vs "Gap to Leader in Dry"
        # Calculate 'Ratio to Winner' for every race.
        # Best Lap Ratio = Driver Best Lap / Winner Best Lap (lower is better, 1.0 is winner)
        
        # Group by meeting to find winner's pace
        meeting_bests = df.groupby('meeting_key')['best_lap_time'].min().reset_index()
        meeting_bests.rename(columns={'best_lap_time': 'winner_lap_time'}, inplace=True)
        
        df = pd.merge(df, meeting_bests, on='meeting_key', how='left')
        df['pace_deficit'] = df['best_lap_time'] / df['winner_lap_time']
        
        # Exclude DNFs or junk times (pace_deficit > 1.25 is likely slow/crash/pit issue, keep reasonable)
        df_clean = df[(df['pace_deficit'] >= 1.0) & (df['pace_deficit'] < 1.30)].copy()
        
        # Aggregates
        driver_stats = df_clean.groupby(['driver_number', 'rain_flag'])['pace_deficit'].mean().unstack()
        
        # driver_stats columns: 0 (Dry), 1 (Wet)
        # Rename
        driver_stats.columns = ['dry_pace', 'wet_pace']
        
        # Filter for drivers who have both wet and dry data
        driver_stats = driver_stats.dropna()
        
        # Rain Aptitude Ratio: 
        # Ideally, everyone is slower in rain? No, this is "Ratio to Winner".
        # If I am 1.02 (+2%) behind Max in Dry, but 1.05 (+5%) behind Max in Wet, I am WORSE in rain.
        # Rain Factor = Wet Pace Deficit / Dry Pace Deficit
        # Higher > 1.0 means "Gets worse in rain relative to leader"
        # Lower < 1.0 means "Gets closer to leader in rain" (Rain Master)
        
        driver_stats['rain_aptitude'] = driver_stats['wet_pace'] / driver_stats['dry_pace']
        
        # Normalize/Center
        # We want a multiplier for the penalty.
        # 0.9 = Good in rain (suffer less penalty)
        # 1.1 = Bad in rain (suffer more penalty)
        
        # Let's just export this dictionary
        factor_map = driver_stats['rain_aptitude'].to_dict()
        
        # Fill defaults for missing drivers (1.0)
        # We might have drivers not in this historic set
        
        print("Calculated Rain Factors (Sample):")
        print(driver_stats.sort_values('rain_aptitude').head(10))
        
        # Save to notebooks directory where model lives
        output_path = 'notebooks/driver_rain_factors.json'
        with open(output_path, 'w') as f:
            json.dump(factor_map, f, indent=4)
            
        print(f"Factors saved to {output_path}")
        
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    calculate_rain_aptitude()
