import pytest
from unittest.mock import patch
from pymongo.errors import ServerSelectionTimeoutError
from src.db_helper import connect_to_mongodb  

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv('MONGODB_CONNECTION_STRING', 'mongodb://username:password@localhost:27107')

def test_successful_connection(mock_env):
    db = connect_to_mongodb('my_database')
    assert db is not None

def test_unsuccessful_connection():
    with patch('os.getenv') as mock_getenv:
        mock_getenv.return_value = 'invalid_connection_string'
        with pytest.raises(ValueError):
            connect_to_mongodb('my_database')

def test_no_connection_string():
    with patch('os.getenv') as mock_getenv:
        mock_getenv.return_value = None
        with pytest.raises(ValueError):
            connect_to_mongodb('my_database')
            

#I will wrtie the rest of test after !