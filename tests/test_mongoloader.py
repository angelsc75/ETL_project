import pytest
from unittest.mock import Mock, patch
from src.loading.mongodbloader import MongoDBLoader

@pytest.fixture
def mock_mongodb():
    with patch('src.loading.mongodbloader.MongoClient') as mock_client:
        mock_collection = Mock()
        mock_client.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
        yield mock_collection

@pytest.fixture
def mongo_loader(mock_mongodb):
    loader = MongoDBLoader("mongodb://test", "test_db", "test_collection")
    return loader

def test_mongodb_connection(mongo_loader):
    """Test that MongoDB connection is established correctly"""
    assert mongo_loader.client is not None
    assert mongo_loader.db is not None
    assert mongo_loader.collection is not None

def test_load_to_mongodb_success(mongo_loader, mock_mongodb):
    """Test successful data insertion"""
    test_data = [{"test": "data"}]
    mock_mongodb.insert_many.return_value = Mock(inserted_ids=["test_id"])
    
    mongo_loader.load_to_mongodb(test_data)
    
    mock_mongodb.insert_many.assert_called_once_with(test_data)

def test_load_to_mongodb_failure(mongo_loader, mock_mongodb):
    """Test handling of insertion failure"""
    test_data = [{"test": "data"}]
    mock_mongodb.insert_many.side_effect = Exception("Test error")
    
    with pytest.raises(Exception, match="Test error"):
        mongo_loader.load_to_mongodb(test_data)

def test_close_connection(mongo_loader):
    """Test that connection is closed properly"""
    mongo_loader.close()
    mongo_loader.client.close.assert_called_once()

