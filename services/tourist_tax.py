"""
Tourist Tax Calculation Service.
Implements official Portuguese tourist tax rules for SEF automation.
"""

from datetime import datetime, timedelta
from math import floor
from typing import Optional, Union

from services.date_utils import normalize_date


def calculate_tourist_tax(check_in: Union[str, int, float], check_out: Union[str, int, float], dob: Union[str, int, float]) -> int:
    """
    Calculate tourist tax based on official rules.
    
    Rules:
    - Base price: 2 EUR per guest per night
    - Maximum chargeable nights per guest: 7
    - Guests under 16 years old do NOT pay
    - If guest turns 16 during stay, they only pay nights when already 16
    - Age is computed per-night
    
    Args:
        check_in: Check-in date (accepts YYYY-MM-DD, YYYY/MM/DD, or Google Sheets serial number)
        check_out: Check-out date (accepts YYYY-MM-DD, YYYY/MM/DD, or Google Sheets serial number)
        dob: Date of birth (accepts YYYY-MM-DD, YYYY/MM/DD, or Google Sheets serial number)
    
    Returns:
        Total tourist tax amount in EUR (integer)
    
    Raises:
        ValueError: If date formats are invalid or dates are logically invalid
        
    Examples:
        >>> calculate_tourist_tax("2025-06-01", "2025-06-05", "2000-01-01")
        8  # 4 nights * 2 EUR = 8 EUR
        
        >>> calculate_tourist_tax("2025/06/01", "2025/06/10", "2000-01-01")
        14  # 7 nights max * 2 EUR = 14 EUR
        
        >>> calculate_tourist_tax("2025-06-01", "2025-06-05", "2010-06-01")
        0  # Guest is 15 years old, no tax
    """
    try:
        # Normalize dates to YYYY-MM-DD format
        check_in_normalized = normalize_date(check_in)
        check_out_normalized = normalize_date(check_out)
        dob_normalized = normalize_date(dob)
        
        # Parse normalized dates
        check_in_date = datetime.strptime(check_in_normalized, '%Y-%m-%d').date()
        check_out_date = datetime.strptime(check_out_normalized, '%Y-%m-%d').date()
        dob_date = datetime.strptime(dob_normalized, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(
            f"Invalid date format. Received: check_in='{check_in}', check_out='{check_out}', dob='{dob}'. "
            f"Error: {str(e)}"
        ) from e
    
    # Validate date logic
    if check_out_date <= check_in_date:
        # Invalid stay duration, return 0
        return 0
    
    if dob_date > datetime.now().date():
        # Date of birth in the future, return 0
        return 0
    
    # Calculate total nights
    total_nights = (check_out_date - check_in_date).days
    
    if total_nights <= 0:
        return 0
    
    # Calculate chargeable nights per the rules
    chargeable_nights = 0
    
    for i in range(total_nights):
        # Calculate date for this night
        night_date = check_in_date + timedelta(days=i)
        
        # Calculate age at this specific night
        # Using 365.25 to account for leap years
        days_old = (night_date - dob_date).days
        age_at_night = floor(days_old / 365.25)
        
        # If guest is 16 or older on this night, it's chargeable
        if age_at_night >= 16:
            chargeable_nights += 1
    
    # Apply maximum of 7 nights rule
    paid_nights = min(chargeable_nights, 7)
    
    # Calculate tax: 2 EUR per payable night
    tax_amount = paid_nights * 2
    
    return tax_amount


def calculate_tourist_tax_detailed(check_in: Union[str, int, float], check_out: Union[str, int, float], dob: Union[str, int, float]) -> dict:
    """
    Calculate tourist tax with detailed breakdown.
    
    Args:
        check_in: Check-in date in YYYY-MM-DD format
        check_out: Check-out date in YYYY-MM-DD format
        dob: Date of birth in YYYY-MM-DD format
    
    Returns:
        Dictionary with breakdown:
        - 'total_nights': Total nights in stay
        - 'chargeable_nights': Nights where guest was 16+ (before 7-night cap)
        - 'paid_nights': Final payable nights (capped at 7)
        - 'tax_amount': Total tax in EUR
        - 'per_night_rate': EUR per night (currently 2)
    
    Raises:
        ValueError: If date formats are invalid
    """
    try:
        # Normalize dates to YYYY-MM-DD format
        check_in_normalized = normalize_date(check_in)
        check_out_normalized = normalize_date(check_out)
        dob_normalized = normalize_date(dob)
        
        # Parse normalized dates
        check_in_date = datetime.strptime(check_in_normalized, '%Y-%m-%d').date()
        check_out_date = datetime.strptime(check_out_normalized, '%Y-%m-%d').date()
        dob_date = datetime.strptime(dob_normalized, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(
            f"Invalid date format. Received: check_in='{check_in}', check_out='{check_out}', dob='{dob}'. "
            f"Error: {str(e)}"
        ) from e
    
    if check_out_date <= check_in_date:
        return {
            'total_nights': 0,
            'chargeable_nights': 0,
            'paid_nights': 0,
            'tax_amount': 0,
            'per_night_rate': 2
        }
    
    if dob_date > datetime.now().date():
        return {
            'total_nights': (check_out_date - check_in_date).days,
            'chargeable_nights': 0,
            'paid_nights': 0,
            'tax_amount': 0,
            'per_night_rate': 2
        }
    
    total_nights = (check_out_date - check_in_date).days
    chargeable_nights = 0
    
    for i in range(total_nights):
        night_date = check_in_date + timedelta(days=i)
        days_old = (night_date - dob_date).days
        age_at_night = floor(days_old / 365.25)
        
        if age_at_night >= 16:
            chargeable_nights += 1
    
    paid_nights = min(chargeable_nights, 7)
    tax_amount = paid_nights * 2
    
    return {
        'total_nights': total_nights,
        'chargeable_nights': chargeable_nights,
        'paid_nights': paid_nights,
        'tax_amount': tax_amount,
        'per_night_rate': 2
    }

