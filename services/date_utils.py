"""
Date Utilities for SEF Automation.
Provides robust date parsing and normalization across different formats.
"""

from datetime import datetime, timedelta
from typing import Union


def normalize_date(value: Union[str, int, float]) -> str:
    """
    Accepts various date formats and returns a normalized date string in format YYYY-MM-DD.
    
    Supported formats:
    - YYYY-MM-DD (e.g., "2025-12-08")
    - YYYY/MM/DD (e.g., "2025/12/08")
    - Google Sheets serial numbers (integers or floats, e.g., 45212)
    
    Args:
        value: Date value as string, integer, or float
        
    Returns:
        Normalized date string in YYYY-MM-DD format
        
    Raises:
        ValueError: If the date cannot be parsed or is invalid
        
    Examples:
        >>> normalize_date("2025-12-08")
        '2025-12-08'
        >>> normalize_date("2025/12/08")
        '2025-12-08'
        >>> normalize_date("45212")
        '2023-10-02'
        >>> normalize_date(45212.0)
        '2023-10-02'
    """
    if value is None:
        raise ValueError("Date value cannot be None")
    
    # Convert to string if it's a number (Google Sheets serial date)
    if isinstance(value, (int, float)):
        try:
            # Google Sheets serial date: days since December 30, 1899
            # Excel/Google Sheets epoch: 1899-12-30
            epoch = datetime(1899, 12, 30)
            days = int(value)
            date_obj = epoch + timedelta(days=days)
            return date_obj.strftime('%Y-%m-%d')
        except (OverflowError, OSError) as e:
            raise ValueError(
                f"Invalid serial date number: {value}. "
                f"Cannot convert to date. Error: {str(e)}"
            ) from e
    
    # Convert to string and strip whitespace
    date_str = str(value).strip()
    
    if not date_str:
        raise ValueError("Date value is empty or whitespace only")
    
    # Try YYYY-MM-DD format
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        pass
    
    # Try YYYY/MM/DD format
    try:
        date_obj = datetime.strptime(date_str, '%Y/%m/%d')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        pass
    
    # Try MM/DD/YYYY format (US format) - try before DD/MM/YYYY for common cases
    try:
        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        pass
    
    # Try DD/MM/YYYY format (common alternative)
    try:
        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        pass
    
    # If all parsing attempts fail, raise an error
    raise ValueError(
        f"Cannot parse date: '{date_str}'. "
        f"Expected formats: YYYY-MM-DD, YYYY/MM/DD, DD/MM/YYYY, MM/DD/YYYY, or Google Sheets serial number."
    )


def normalize_date_safe(value: Union[str, int, float, None]) -> str:
    """
    Safe version of normalize_date that returns empty string on error instead of raising.
    
    Args:
        value: Date value as string, integer, float, or None
        
    Returns:
        Normalized date string in YYYY-MM-DD format, or empty string if parsing fails
        
    Examples:
        >>> normalize_date_safe("2025-12-08")
        '2025-12-08'
        >>> normalize_date_safe("invalid")
        ''
        >>> normalize_date_safe(None)
        ''
    """
    if value is None:
        return ""
    
    try:
        return normalize_date(value)
    except (ValueError, TypeError):
        return ""

