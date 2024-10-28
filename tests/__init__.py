import pytest
from src.transformation.datatransformer import (
    standardize_phone, separate_salary_currency, validate_email,
    normalize_gender, validate_generic_passport_format, process_and_group_data
)
import json

def test_standardize_phone():
    """Test phone number standardization"""
    assert standardize_phone("+1234567890") is not None
    assert standardize_phone("invalid") is None
    assert standardize_phone(None) is None

def test_separate_salary_currency():
    """Test salary and currency separation"""
    amount, currency = separate_salary_currency("50000USD")
    assert amount == 50000
    assert currency == "USD"
    
    amount, currency = separate_salary_currency("1,000.50 EUR")
    assert amount == 1000.50
    assert currency == "EUR"
    
    amount, currency = separate_salary_currency(None)
    assert amount is None
    assert currency is None

def test_validate_email():
    """Test email validation"""
    assert validate_email("test@example.com") == "test@example.com"
    assert validate_email("invalid-email") is None
    assert validate_email(None) is None

def test_normalize_gender():
    """Test gender normalization"""
    assert normalize_gender("M") == "M"
    assert normalize_gender("f") == "F"
    assert normalize_gender("other") == "ND"
    assert normalize_gender(None) == "ND"
    assert normalize_gender(["M"]) == "M"

def test_validate_passport():
    """Test passport validation"""
    assert validate_generic_passport_format("ABC123456") == "ABC123456"
    assert validate_generic_passport_format("12") is None
    assert validate_generic_passport_format(None) is None

def test_process_and_group_data():
    """Test data processing and grouping"""
    # Test personal data
    personal_json = json.dumps({
        "name": "John",
        "last_name": "Doe",
        "telfnumber": "+1234567890",
        "passport": "ABC123456",
        "email": "john@test.com",
        "sex": "M"
    })
    
    result = process_and_group_data(personal_json)
    assert "name" in result
    assert result["fullname"] == "John Doe"
    
    # Test location data
    location_json = json.dumps({
        "fullname": "John Doe",
        "address": "Test St",
        "city": "TestCity",
        "country": "TestCountry"
    })
    
    result = process_and_group_data(location_json)
    assert "city" in result
    assert result["address"] == "Test St"
    
    # Test invalid JSON
    with pytest.raises(json.JSONDecodeError):
        process_and_group_data("invalid json")