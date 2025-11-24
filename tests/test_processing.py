import os
import pytest
import pandas as pd
import json
from src.processing.process_silver import process_laps, process_weather, extract_metadata_from_path, process_file

def test_extract_metadata():
    path = "data/bronze/year=2023/meeting_key=99/session_key=999/laps.json"
    dataset, metadata = extract_metadata_from_path(path)
    
    assert dataset == "laps"
    assert metadata["year"] == 2023
    assert metadata["meeting_key"] == 99
    assert metadata["session_key"] == 999

def test_process_laps():
    data = {
        "lap_duration": ["80.5", "invalid", None, "81.0"],
        "sector_1_duration": ["20.1", "20.2", "20.3", "20.4"]
    }
    df = pd.DataFrame(data)
    
    processed_df = process_laps(df)
    
    # Should drop 'invalid' and None in lap_duration
    assert len(processed_df) == 2
    assert pd.api.types.is_float_dtype(processed_df["lap_duration"])
    assert processed_df.iloc[0]["lap_duration"] == 80.5

def test_process_weather():
    data = {
        "date": ["2023-01-01 10:00:00", "invalid"],
        "air_temperature": ["25.5", "26.0"]
    }
    df = pd.DataFrame(data)
    
    processed_df = process_weather(df)
    
    assert pd.api.types.is_datetime64_any_dtype(processed_df["date"])
    assert pd.api.types.is_float_dtype(processed_df["air_temperature"])
    # Invalid date becomes NaT, but row is kept unless we drop it (logic doesn't drop in weather)
    assert len(processed_df) == 2

def test_process_file_integration(tmp_path):
    # Setup fake file structure
    year_dir = tmp_path / "year=2023" / "meeting_key=10" / "session_key=100"
    year_dir.mkdir(parents=True)
    
    file_path = year_dir / "laps.json"
    
    fake_data = [{"lap_number": 1, "lap_duration": 90.0}]
    with open(file_path, "w") as f:
        json.dump(fake_data, f)
        
    # Test processing
    dataset_name, df = process_file(str(file_path))
    
    assert dataset_name == "laps"
    assert not df.empty
    assert "year" in df.columns
    assert df.iloc[0]["year"] == 2023
    assert df.iloc[0]["lap_duration"] == 90.0
