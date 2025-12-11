
import os
import pandas as pd
import s3fs
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.environ.get('S3_BUCKET_NAME')

try:
    df = pd.read_parquet(f"s3://{BUCKET}/gold/train_data.parquet")
    print(f"Shape: {df.shape}")
    print("Dtypes:")
    print(df.dtypes.to_string())
    
    # Check for object columns that might be passed to XGBoost
    obj_cols = df.select_dtypes(include=['object']).columns.tolist()
    print(f"\nObject columns: {obj_cols}")
    
    # Check for potential mixed types or infinite values
    num_df = df.select_dtypes(include=['number'])
    print(f"\nNumeric columns: {num_df.columns.tolist()}")
    print(f"Infinite values: {np.isinf(num_df).sum().sum()}")
    
except Exception as e:
    print(e)
