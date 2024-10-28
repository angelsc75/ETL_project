import pytest
import json
import zlib
import time
import sys
import os
from unittest.mock import Mock, patch, call

# Ajustar el path para incluir el directorio src
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.loading.redisloader import RedisLoader

@pytest.fixture
def mock_redis():
    """Fixture para crear un mock de Redis"""
    with patch('src.loading.redisloader.redis.Redis') as mock_redis:
        mock_instance = mock_redis.return_value
        mock_instance.ping.return_value = True
        # Configurar el mock para pipeline
        mock_pipeline = Mock()
        mock_pipeline.execute.return_value = [True, True, True, True]
        mock_instance.pipeline.return_value.__enter__.return_value = mock_pipeline
        yield mock_instance


@pytest.fixture
def redis_loader(mock_redis):
    """Fixture para crear una instancia de RedisLoader con Redis mockeado"""
    
    with patch('src.loading.redisloader.redis.ConnectionPool'):
        loader = RedisLoader(buffer_size=1000)
        return loader

def test_redis_loader_initialization(redis_loader):
    """Test la inicialización correcta del loader"""
    assert redis_loader.buffer_size == 1000
    assert redis_loader.compression_enabled == True
    assert redis_loader.processing_list == "processing_queue"

def test_redis_loader_initialization_error():
    """Test el manejo de errores durante la inicialización"""
   
    with patch('src.loading.redisloader.redis.Redis') as mock_redis:
        mock_redis.return_value.ping.side_effect = Exception('Connection error')
        with pytest.raises(Exception) as exc_info:
            RedisLoader()
        assert str(exc_info.value) == 'Connection error'

def test_compress_decompress_data(redis_loader):
    """Test la compresión y descompresión de datos"""
    test_data = {'test': 'data'}
    compressed = redis_loader._compress_data(test_data)
    decompressed = redis_loader._decompress_data(compressed)
    assert decompressed == test_data

def test_add_to_buffer(redis_loader, mock_redis):
    """Test añadir datos al buffer"""
    test_data = {'test': 'data'}
    mock_pipeline = Mock()
    mock_pipeline.execute.return_value = [1, 1, '1']
    mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
    
    with patch('builtins.print'):
        result = redis_loader.add_to_buffer(test_data)
    
    # Verificar que el resultado es False cuando el buffer no está lleno
    assert not result
    assert mock_redis.pipeline.called

def test_get_buffer_batch(redis_loader, mock_redis):
    """Test obtener un batch del buffer"""
    # Resetear los mocks
    mock_redis.pipeline.reset_mock()
    mock_redis.get.reset_mock()
    
    test_data = {'test': 'data'}
    compressed_data = redis_loader._compress_data(test_data)
    
    mock_redis.get.return_value = '1'
    mock_pipeline = Mock()
    mock_pipeline.execute.return_value = [
        [compressed_data],  # lrange result
        1,  # delete result
        1,  # set buffer_count result
        1   # set last_flush result
    ]
    mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
    
    with patch('builtins.print'):
        result = redis_loader.get_buffer_batch()
    
    assert result == [test_data]
    assert mock_redis.pipeline.called

def test_should_flush_size(redis_loader, mock_redis):
    """Test la lógica de flush por tamaño"""
    mock_redis.get.return_value = str(time.time())
    
    # Probar cuando el buffer está lleno
    assert redis_loader._should_flush(1000)
    
    # Probar cuando el buffer no está lleno
    assert not redis_loader._should_flush(500)

def test_should_flush_time(redis_loader, mock_redis):
    """Test la lógica de flush por tiempo"""
    # Simular último flush hace más de max_flush_interval segundos
    mock_redis.get.return_value = str(time.time() - redis_loader.max_flush_interval - 1)
    
    assert redis_loader._should_flush(1)

def test_close(redis_loader, mock_redis):
    """Test el cierre correcto del loader"""
    # Resetear los mocks
    mock_redis.pipeline.reset_mock()
    mock_redis.get.reset_mock()
    
    mock_redis.get.return_value = '0'
    mock_pipeline = Mock()
    mock_pipeline.execute.return_value = [True, True, True, True]
    mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
    
    with patch('builtins.print'):
        redis_loader.close()
    
    assert mock_redis.pipeline.called
    assert mock_pipeline.delete.call_count == 4

def test_update_buffer_stats(redis_loader, mock_redis):
    """Test la actualización de estadísticas del buffer"""
    # Resetear el mock antes de la prueba
    mock_redis.set.reset_mock()
    
    with patch('builtins.print'):
        redis_loader._update_buffer_stats(500, "test")
    
    # Verificar que set fue llamado con los argumentos correctos
    assert mock_redis.set.call_count == 1
    stats_data = json.loads(mock_redis.set.call_args[0][1])
    assert stats_data['current_size'] == 500
    assert stats_data['max_size'] == redis_loader.buffer_size
    assert stats_data['operation'] == "test"

def test_error_handling_in_compress(redis_loader):
    """Test el manejo de errores durante la compresión"""
    test_data = {'test': 'data'}
    
    with patch('json.dumps', side_effect=Exception('Compression error')):
        with pytest.raises(Exception):
            redis_loader._compress_data(test_data)