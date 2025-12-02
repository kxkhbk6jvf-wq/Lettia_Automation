"""
Utility functions and helpers for Lettia automation.
Common functions used across multiple services.
"""

from typing import Optional, Dict, Any
from datetime import datetime, date
from pathlib import Path
import json


def format_phone_number(phone: str) -> str:
    """
    Format phone number to international format.
    
    Args:
        phone: Phone number in various formats
        
    Returns:
        Phone number in international format (e.g., +351XXXXXXXXX)
        
    Planned behavior:
        - Remove spaces and special characters
        - Add country code if missing
        - Return standardized format
    """
    # TODO: Implement phone number formatting
    pass


def format_date(date_obj: date, format_string: str = "%Y-%m-%d") -> str:
    """
    Format a date object to string.
    
    Args:
        date_obj: Date object to format
        format_string: Format string (default: YYYY-MM-DD)
        
    Returns:
        Formatted date string
    """
    if isinstance(date_obj, datetime):
        return date_obj.strftime(format_string)
    elif isinstance(date_obj, date):
        return date_obj.strftime(format_string)
    return str(date_obj)


def parse_date(date_string: str, format_string: str = "%Y-%m-%d") -> date:
    """
    Parse a date string to date object.
    
    Args:
        date_string: Date string to parse
        format_string: Expected format string (default: YYYY-MM-DD)
        
    Returns:
        Date object
        
    Planned behavior:
        - Parse date string according to format
        - Handle various date formats
        - Raise ValueError for invalid formats
    """
    # TODO: Implement date parsing with error handling
    pass


def clean_string(text: str) -> str:
    """
    Clean and normalize a string.
    
    Args:
        text: String to clean
        
    Returns:
        Cleaned string
        
    Planned behavior:
        - Remove extra whitespace
        - Normalize unicode characters
        - Handle special characters
    """
    if not text:
        return ""
    return " ".join(text.split())


def safe_json_loads(json_string: str) -> Optional[Dict[str, Any]]:
    """
    Safely parse JSON string, returning None on error.
    
    Args:
        json_string: JSON string to parse
        
    Returns:
        Parsed dictionary or None if parsing fails
    """
    try:
        return json.loads(json_string)
    except (json.JSONDecodeError, TypeError):
        return None


def ensure_directory(path: Path) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to ensure exists
        
    Returns:
        Path object of the directory
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def calculate_date_range(start_date: date, end_date: date) -> int:
    """
    Calculate number of nights between two dates.
    
    Args:
        start_date: Check-in date
        end_date: Check-out date
        
    Returns:
        Number of nights
    """
    delta = end_date - start_date
    return delta.days

