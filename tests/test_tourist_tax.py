"""
Tests for tourist tax calculation service.
"""

import pytest
from services.tourist_tax import calculate_tourist_tax, calculate_tourist_tax_detailed


def test_basic_tax_calculation():
    """Test basic tourist tax calculation for adult guest."""
    # 4 nights, guest is 25 years old (well above 16)
    check_in = "2025-06-01"
    check_out = "2025-06-05"
    dob = "2000-01-01"
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    # 4 nights * 2 EUR = 8 EUR
    assert tax == 8


def test_tax_calculation_with_slash_format():
    """Test tourist tax calculation with YYYY/MM/DD format."""
    check_in = "2025/06/01"
    check_out = "2025/06/05"
    dob = "2000/01/01"
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    # 4 nights * 2 EUR = 8 EUR
    assert tax == 8


def test_tax_calculation_with_google_sheets_serial():
    """Test tourist tax calculation with Google Sheets serial numbers."""
    # Serial dates for June 1-5, 2025
    check_in = 45478  # 2025-06-01
    check_out = 45482  # 2025-06-05
    dob = 36526  # 2000-01-01
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    # 4 nights * 2 EUR = 8 EUR
    assert tax == 8


def test_max_nights_cap():
    """Test that maximum 7 nights are charged even for longer stays."""
    # 10 nights, but max is 7 nights
    check_in = "2025-06-01"
    check_out = "2025-06-11"  # 10 nights
    dob = "2000-01-01"
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    # Max 7 nights * 2 EUR = 14 EUR (not 20 EUR)
    assert tax == 14


def test_minor_guest_no_tax():
    """Test that guests under 16 pay no tax."""
    # Guest is 15 years old
    check_in = "2025-06-01"
    check_out = "2025-06-05"
    dob = "2010-06-01"  # 15 years old on check-in
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    # Guest is under 16, should pay 0
    assert tax == 0


def test_guest_turning_16_during_stay():
    """Test that guest turning 16 during stay only pays for nights when 16+."""
    # Guest turns 16 during the stay
    check_in = "2025-06-01"
    check_out = "2025-06-05"  # 4 nights
    dob = "2009-06-03"  # Turns 16 on June 3, 2025
    
    # Night 0 (June 1): 15 years old - no tax
    # Night 1 (June 2): 15 years old - no tax
    # Night 2 (June 3): 16 years old - tax
    # Night 3 (June 4): 16 years old - tax
    # Total chargeable: 2 nights
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    # 2 nights * 2 EUR = 4 EUR
    assert tax == 4


def test_invalid_date_format():
    """Test that invalid date formats raise ValueError."""
    # Test with truly invalid date format that cannot be parsed
    with pytest.raises(ValueError) as exc_info:
        calculate_tourist_tax("invalid-date", "2025-06-05", "2000-01-01")
    
    assert "Invalid date format" in str(exc_info.value) or "Cannot parse date" in str(exc_info.value)


def test_check_out_before_check_in():
    """Test that invalid date range (check_out <= check_in) returns 0."""
    check_in = "2025-06-05"
    check_out = "2025-06-01"  # Before check-in
    dob = "2000-01-01"
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    assert tax == 0


def test_same_day_check_in_out():
    """Test that same day check-in/out returns 0."""
    check_in = "2025-06-01"
    check_out = "2025-06-01"  # Same day
    dob = "2000-01-01"
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    assert tax == 0


def test_future_dob():
    """Test that future date of birth returns 0."""
    check_in = "2025-06-01"
    check_out = "2025-06-05"
    dob = "2030-01-01"  # Future date
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    assert tax == 0


def test_exactly_16_years_old():
    """Test guest exactly 16 years old on check-in."""
    check_in = "2025-06-01"
    check_out = "2025-06-05"
    dob = "2009-06-01"  # Exactly 16 on check-in
    
    tax = calculate_tourist_tax(check_in, check_out, dob)
    
    # All 4 nights chargeable
    assert tax == 8


def test_detailed_breakdown():
    """Test detailed tax calculation breakdown."""
    check_in = "2025-06-01"
    check_out = "2025-06-05"
    dob = "2000-01-01"
    
    breakdown = calculate_tourist_tax_detailed(check_in, check_out, dob)
    
    assert breakdown['total_nights'] == 4
    assert breakdown['chargeable_nights'] == 4
    assert breakdown['paid_nights'] == 4
    assert breakdown['tax_amount'] == 8
    assert breakdown['per_night_rate'] == 2

