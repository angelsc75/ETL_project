import pytest
from unittest.mock import Mock, patch, call
from src.loading.sqldbloader import SQLloader

@pytest.fixture
def mock_psycopg2():
    with patch('src.loading.sqlloader.psycopg2') as mock:
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock.connect.return_value = mock_connection
        yield mock

@pytest.fixture
def sql_loader(mock_psycopg2):
    loader = SQLloader(
        host="test_host",
        database="test_db",
        user="test_user",
        password="test_pass",
        port="5432"
    )
    return loader

def test_connection_success(sql_loader, mock_psycopg2):
    """Test successful database connection"""
    sql_loader.connect()
    
    mock_psycopg2.connect.assert_called_once_with(
        host="test_host",
        database="test_db",
        user="test_user",
        password="test_pass",
        port="5432"
    )

def test_connection_failure(sql_loader, mock_psycopg2):
    """Test database connection failure"""
    mock_psycopg2.connect.side_effect = Exception("Connection failed")
    
    with pytest.raises(Exception, match="Connection failed"):
        sql_loader.connect()

def test_load_to_sql(sql_loader):
    """Test data loading to SQL tables"""
    test_data = [{
        "name": "John",
        "last_name": "Doe",
        "fullname": "John Doe",
        "sex": "M",
        "telfnumber": "1234567890",
        "passport": "ABC123",
        "email": "john@test.com",
        "city": "TestCity",
        "address": "Test Address",
        "company": "TestCo",
        "company_address": "Company Address",
        "company_telfnumber": "0987654321",
        "company_email": "info@testco.com",
        "job": "Developer",
        "IBAN": "DE123456789",
        "salary": "50000",
        "currency": "EUR",
        "IPv4": "192.168.1.1"
    }]
    
    sql_loader.load_to_sql(test_data)
    
    # Verify that cursor executed queries for all data types
    assert sql_loader.cursor.executemany.call_count >= 5

def test_close_connections(sql_loader):
    """Test proper closing of connections"""
    sql_loader.connect()
    sql_loader.close()
    
    sql_loader.cursor.close.assert_called_once()
    sql_loader.connection.close.assert_called_once()