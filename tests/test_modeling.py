import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from src.modeling.predictor import F1Predictor
from src.modeling.feature_engineering import prepare_training_data

# Mock Data
MOCK_LAPS = pd.DataFrame({
    'session_key': [100, 100, 101],
    'meeting_key': [10, 10, 11],
    'driver_number': [1, 2, 1],
    'lap_duration': [80.0, 81.0, 90.0]
})

MOCK_WEATHER = pd.DataFrame({
    'session_key': [100, 101],
    'meeting_key': [10, 11],
    'rainfall': [0.0, 1.0], # Dry, Rain
    'air_temperature': [25, 20],
    'track_temperature': [30, 25]
})

@pytest.fixture
def mock_silver_loader():
    with patch("src.modeling.feature_engineering.load_silver_table") as mock:
        def side_effect(table):
            if table == "laps":
                return MOCK_LAPS
            if table == "weather":
                return MOCK_WEATHER
            return pd.DataFrame()
        mock.side_effect = side_effect
        yield mock

def test_prepare_training_data(mock_silver_loader):
    df = prepare_training_data()
    
    assert not df.empty
    assert len(df) == 3 # 3 laps joined
    assert 'weather_category' in df.columns
    # Session 100 is Dry (rainfall 0), Session 101 is Rain (rainfall 1)
    assert df[df['session_key'] == 100]['weather_category'].iloc[0] == 'dry'
    assert df[df['session_key'] == 101]['weather_category'].iloc[0] == 'rain'

def test_predictor_flow(mock_silver_loader):
    predictor = F1Predictor()
    
    # Train
    predictor.train()
    assert predictor.is_trained
    assert len(predictor.driver_list) > 0
    
    # Predict
    # Meeting 10, Dry
    grid = predictor.predict_grid(meeting_key=10, weather_category='dry')
    
    assert grid is not None
    assert not grid.empty
    assert 'position' in grid.columns
    assert 'predicted_lap_time' in grid.columns
    
    # Check if sorted
    assert grid['predicted_lap_time'].is_monotonic_increasing

def test_predictor_invalid_weather(mock_silver_loader):
    predictor = F1Predictor()
    predictor.train()
    
    grid = predictor.predict_grid(meeting_key=10, weather_category='snow')
    assert grid is None
