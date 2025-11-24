import os
import pytest
from unittest.mock import MagicMock, patch
from src.ingestion.ingest_bronze import run_ingestion, fetch_data, upload_to_s3

# Mock Data
MOCK_SESSIONS = [
    {"meeting_key": 100, "session_key": 1000, "year": 2023},
    {"meeting_key": 100, "session_key": 1001, "year": 2023}
]

MOCK_LAPS = [
    {"lap_number": 1, "lap_duration": 80.5},
    {"lap_number": 2, "lap_duration": 81.2}
]

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

@pytest.fixture
def mock_s3():
    with patch("boto3.client") as mock:
        yield mock

@pytest.fixture
def mock_requests():
    with patch("requests.get") as mock:
        yield mock

def test_fetch_data_success(mock_requests):
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_SESSIONS
    mock_response.raise_for_status.return_value = None
    mock_requests.return_value = mock_response

    data = fetch_data("sessions", params={"year": 2023})
    assert data == MOCK_SESSIONS
    mock_requests.assert_called_once()

def test_upload_to_s3(mock_s3):
    import pandas as pd
    df = pd.DataFrame(MOCK_LAPS)
    
    s3_client_mock = MagicMock()
    mock_s3.return_value = s3_client_mock
    
    upload_to_s3(df, "test-bucket", "path/to/file.csv")
    
    s3_client_mock.put_object.assert_called_once()
    call_args = s3_client_mock.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    assert call_args["Key"] == "path/to/file.csv"
    # Body is string from StringIO.getvalue()
    assert "lap_duration" in call_args["Body"]

def test_run_ingestion_flow(mock_env, mock_requests, mock_s3):
    # Setup Mocks
    
    with patch("src.ingestion.ingest_bronze.fetch_data") as mock_fetch:
        # Scenario: 
        # Only return sessions for 2023, empty for others
        # For sessions in 2023, return MOCK_LAPS for endpoints
        
        def side_effect(endpoint, params=None):
            if endpoint == "sessions":
                if params.get("year") == 2023:
                    return MOCK_SESSIONS
                return []
            return MOCK_LAPS

        mock_fetch.side_effect = side_effect
        
        run_ingestion()
        
        # Verifications
        assert mock_fetch.call_count > 1 
        
        # Check if upload was called
        s3_client_mock = mock_s3.return_value
        assert s3_client_mock.put_object.called
        # We have 2 sessions * 6 endpoints = 12 uploads expected
        assert s3_client_mock.put_object.call_count == 12
