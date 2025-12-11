
import os
import pandas as pd
import s3fs
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.environ.get('S3_BUCKET_NAME')

try:
    print(f"Inspecting s3://{BUCKET}/gold/gold_race_wt/")
    df = pd.read_parquet(f"s3://{BUCKET}/gold/gold_race_wt/")
    print(f"Columns: {df.columns.tolist()}")
    
    weather_cols = [c for c in df.columns if 'rain' in c.lower() or 'temp' in c.lower() or 'humid' in c.lower() or 'wind' in c.lower()]
    print(f"Weather Columns Found: {weather_cols}")
    
    for c in weather_cols:
        print(f"Column: {c}, Type: {df[c].dtype}")
        print(df[c].value_counts().head())
        print("-" * 20)

except Exception as e:
    print(f"Error: {e}")
