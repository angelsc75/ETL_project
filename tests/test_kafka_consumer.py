import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock, call
from confluent_kafka import KafkaError

# Ajustar el path para incluir el directorio src

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.extraction.kafkaconsumer import KafkaConsumer


@pytest.fixture
def mock_dependencies():
    """Fixture para crear todos los mocks necesarios"""
    mock_redis_loader = Mock()
    mock_mongo_loader = Mock()
    mock_sql_loader = Mock()
    mock_consumer = Mock()
    mock_admin_client = Mock()
    
    return {
        'redis_loader': mock_redis_loader,
        'mongo_loader': mock_mongo_loader,
        'sql_loader': mock_sql_loader,
        'consumer': mock_consumer,
        'admin_client': mock_admin_client
    }

@pytest.fixture
def kafka_consumer(mock_dependencies):
    """Fixture para crear una instancia de KafkaConsumer con dependencias mockeadas"""
    with patch('kafkaconsumer.Consumer', return_value=mock_dependencies['consumer']), \
         patch('kafkaconsumer.AdminClient', return_value=mock_dependencies['admin_client']):
        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            group_id='test-group',
            redis_loader=mock_dependencies['redis_loader'],
            mongo_loader=mock_dependencies['mongo_loader'],
            sql_loader=mock_dependencies['sql_loader']
        )
        return consumer

def test_kafka_consumer_initialization(kafka_consumer, mock_dependencies):
    """Test la inicialización correcta del consumidor"""
    assert kafka_consumer.consumer == mock_dependencies['consumer']
    assert kafka_consumer.redis_loader == mock_dependencies['redis_loader']
    assert kafka_consumer.mongo_loader == mock_dependencies['mongo_loader']
    assert kafka_consumer.sql_loader == mock_dependencies['sql_loader']

def test_kafka_consumer_initialization_error():
    """Test el manejo de errores durante la inicialización"""
    with patch('kafkaconsumer.Consumer', side_effect=Exception('Connection error')), \
         pytest.raises(Exception) as exc_info:
        KafkaConsumer(
            bootstrap_servers='invalid:9092',
            group_id='test-group',
            redis_loader=Mock(),
            mongo_loader=Mock(),
            sql_loader=Mock()
        )
    assert str(exc_info.value) == 'Connection error'

@patch('kafkaconsumer.process_and_group_data')
def test_message_processing_success(mock_process_data, kafka_consumer, mock_dependencies):
    """Test el procesamiento exitoso de mensajes"""
    # Configurar el comportamiento del mock
    mock_message = Mock()
    mock_message.error.return_value = None
    mock_message.value.return_value = b'test message'
    
    mock_process_data.return_value = {'processed': 'data'}
    mock_dependencies['redis_loader'].add_to_buffer.return_value = False
    
    # Simular un mensaje y luego EOF
    mock_dependencies['consumer'].poll.side_effect = [
        mock_message,
        None,  # Simular timeout
        Mock(error=lambda: Mock(code=lambda: KafkaError._PARTITION_EOF))  # Simular EOF
    ]
    
    # Configurar el AdminClient mock
    metadata_mock = Mock()
    metadata_mock.topics = {'test-topic': None}
    mock_dependencies['admin_client'].list_topics.return_value = metadata_mock
    
    # Ejecutar
    with patch('builtins.print'):  # Suprimir prints durante el test
        kafka_consumer.start_consuming()
    
    # Verificar
    mock_process_data.assert_called_once_with('test message')
    mock_dependencies['redis_loader'].add_to_buffer.assert_called_once_with({'processed': 'data'})

def test_process_batch(kafka_consumer, mock_dependencies):
    """Test el procesamiento de un batch completo"""
    # Preparar datos de prueba
    test_batch = [{'id': 1}, {'id': 2}]
    mock_dependencies['redis_loader'].get_buffer_batch.return_value = test_batch
    
    # Ejecutar
    with patch('builtins.print'):  # Suprimir prints durante el test
        kafka_consumer.process_batch()
    
    # Verificar
    mock_dependencies['mongo_loader'].load_to_mongodb.assert_called_once_with(test_batch)
    mock_dependencies['sql_loader'].load_to_sql.assert_called_once_with(test_batch)

def test_save_messages_mongo(kafka_consumer, mock_dependencies):
    """Test el guardado de mensajes en MongoDB"""
    test_messages = [{'id': 1}, {'id': 2}]
    mock_dependencies['mongo_loader'].load_to_mongodb.return_value = 2
    
    with patch('builtins.print'):  # Suprimir prints durante el test
        kafka_consumer.save_messages_mongo(test_messages)
    
    mock_dependencies['mongo_loader'].load_to_mongodb.assert_called_once_with(test_messages)

def test_save_messages_sql(kafka_consumer, mock_dependencies):
    """Test el guardado de mensajes en PostgreSQL"""
    test_messages = [{'id': 1}, {'id': 2}]
    
    with patch('builtins.print'):  # Suprimir prints durante el test
        kafka_consumer.save_messages_sql(test_messages)
    
    mock_dependencies['sql_loader'].load_to_sql.assert_called_once_with(test_messages)

def test_error_handling_in_message_processing(kafka_consumer, mock_dependencies):
    """Test el manejo de errores durante el procesamiento de mensajes"""
    mock_message = Mock()
    mock_message.error.return_value = None
    mock_message.value.return_value = b'invalid message'
    
    mock_dependencies['consumer'].poll.side_effect = [
        mock_message,
        KeyboardInterrupt  # Simular interrupción para terminar el loop
    ]
    
    metadata_mock = Mock()
    metadata_mock.topics = {'test-topic': None}
    mock_dependencies['admin_client'].list_topics.return_value = metadata_mock
    
    with patch('kafkaconsumer.process_and_group_data', side_effect=Exception('Processing error')), \
         patch('builtins.print'):
        kafka_consumer.start_consuming()
    
    # Verificar que se llamó al close en todas las dependencias
    mock_dependencies['consumer'].close.assert_called_once()
    mock_dependencies['redis_loader'].close.assert_called_once()
    mock_dependencies['mongo_loader'].close.assert_called_once()