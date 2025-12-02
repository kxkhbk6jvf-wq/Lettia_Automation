"""
Finance service for handling financial calculations and operations.
Calculates fees, VAT, and processes payment information.
"""

import logging
from datetime import datetime
from typing import Dict, Optional, Any

from config.settings import (
    get_vat_rate,
    get_airbnb_fee_percent,
    get_lodgify_fee_percent,
    get_stripe_fee_table
)

# Configure logging
logger = logging.getLogger(__name__)


def calculate_financials(normalized_reservation: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate all financial fields for a normalized reservation.
    
    This function populates the following financial fields:
    - airbnb_fee: Based on origin and AIRBNB_FEE_PERCENT
    - lodgify_fee: Based on origin and LODGIFY_FEE_PERCENT
    - stripe_fee: Based on origin, country, and STRIPE_FEE_TABLE
    - vat_amount: Based on total_price and VAT_RATE
    - net_revenue: total_price - all fees - vat_amount
    - anticipation_days: Days between reservation_date and check_in
    - price_per_night: total_price / nights
    - price_per_guest_per_night: total_price / (nights * guests_count)
    
    Args:
        normalized_reservation: Dictionary containing normalized reservation data with:
            - origin: Booking channel (e.g., "airbnb", "lodgify", "stripe", etc.)
            - total_price: Total booking amount (float)
            - country: Country code (e.g., "PT", "FR", "UK", "US")
            - reservation_date: Date when reservation was made (YYYY-MM-DD or datetime)
            - check_in: Check-in date (YYYY-MM-DD or datetime)
            - nights: Number of nights (int or float)
            - guests_count: Number of guests (int)
            - currency: Currency code (optional)
        
        config: Configuration dictionary containing:
            - VAT_RATE: VAT rate as float (e.g., 0.06 for 6%)
            - AIRBNB_FEE_PERCENT: Airbnb fee percentage (e.g., 0.15 for 15%)
            - LODGIFY_FEE_PERCENT: Lodgify fee percentage (e.g., 0.03 for 3%)
            - STRIPE_FEE_TABLE: Dictionary mapping country codes to fee percentages
              (e.g., {"PT": 0.014, "FR": 0.016, "UK": 0.019, "US": 0.025})
    
    Returns:
        Dictionary containing only the financial fields:
        {
            "airbnb_fee": float,
            "lodgify_fee": float,
            "stripe_fee": float,
            "dynamic_fee": float,
            "total_fees": float,
            "vat_amount": float,
            "net_revenue": float,
            "payout_expected": float,
            "anticipation_days": int,
            "price_per_night": float,
            "price_per_guest_per_night": float
        }
    """
    financials: Dict[str, Any] = {}
    
    try:
        # Extract values from reservation (with defaults)
        origin = str(normalized_reservation.get("origin", "")).lower().strip()
        total_price = float(normalized_reservation.get("total_price", 0.0))
        country = str(normalized_reservation.get("country", "")).strip().upper()
        reservation_date = normalized_reservation.get("reservation_date", "")
        check_in = normalized_reservation.get("check_in", "")
        nights = float(normalized_reservation.get("nights", 0.0))
        guests_count = float(normalized_reservation.get("guests_count", 1.0))
        
        # Extract config values
        vat_rate = float(config.get("VAT_RATE", 0.0))
        airbnb_fee_percent = float(config.get("AIRBNB_FEE_PERCENT", 0.0))
        lodgify_fee_percent = float(config.get("LODGIFY_FEE_PERCENT", 0.0))
        stripe_fee_table = config.get("STRIPE_FEE_TABLE", {})
        
        # Helper to safely convert to float
        def _to_float(value: Any) -> float:
            """Convert value to float, returning 0.0 if invalid."""
            if value is None:
                return 0.0
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                value = value.strip()
                if not value or value in ["", "None", "nan", "null"]:
                    return 0.0
                # Replace comma with dot for decimal separator
                value = value.replace(",", ".")
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return 0.0
            return 0.0
        
        # Calculate airbnb_fee (this is recalculated from config)
        if origin == "airbnb":
            financials["airbnb_fee"] = round(total_price * airbnb_fee_percent, 2)
        else:
            financials["airbnb_fee"] = 0.0
        
        # Preserve lodgify_fee, stripe_fee, and dynamic_fee from mapper (already calculated)
        # If not present, use defaults
        financials["lodgify_fee"] = _to_float(normalized_reservation.get("lodgify_fee", 0.0))
        financials["stripe_fee"] = _to_float(normalized_reservation.get("stripe_fee", 0.0))
        financials["dynamic_fee"] = _to_float(normalized_reservation.get("dynamic_fee", 0.0))
        
        # Calculate vat_amount
        financials["vat_amount"] = round(total_price * vat_rate, 2)
        
        # Recalculate total_fees with all fees (including dynamic_fee)
        total_fees = (
            financials["airbnb_fee"] +
            financials["lodgify_fee"] +
            financials["stripe_fee"] +
            financials["dynamic_fee"]
        )
        financials["total_fees"] = round(total_fees, 2)
        
        # Calculate net_revenue (total_price - total_fees - vat_amount)
        financials["net_revenue"] = round(total_price - total_fees - financials["vat_amount"], 2)
        
        # Calculate payout_expected (total_price - airbnb_fee - lodgify_fee - stripe_fee)
        # Note: dynamic_fee is excluded from payout_expected
        payout_expected = total_price - financials["airbnb_fee"] - financials["lodgify_fee"] - financials["stripe_fee"]
        financials["payout_expected"] = round(payout_expected, 2)
        
        # Calculate anticipation_days
        anticipation_days = None
        try:
            if reservation_date and check_in:
                # Parse dates (handle both string and datetime objects)
                if isinstance(reservation_date, str):
                    res_date = datetime.strptime(reservation_date.split()[0], "%Y-%m-%d").date()
                elif isinstance(reservation_date, datetime):
                    res_date = reservation_date.date()
                else:
                    res_date = None
                
                if isinstance(check_in, str):
                    checkin_date = datetime.strptime(check_in.split()[0], "%Y-%m-%d").date()
                elif isinstance(check_in, datetime):
                    checkin_date = check_in.date()
                else:
                    checkin_date = None
                
                if res_date and checkin_date:
                    delta = (checkin_date - res_date).days
                    anticipation_days = max(0, delta)  # Ensure non-negative
        except (ValueError, AttributeError, TypeError) as e:
            logger.warning(f"Could not calculate anticipation_days: {e}")
        
        financials["anticipation_days"] = anticipation_days if anticipation_days is not None else ""
        
        # Calculate price_per_night
        if nights > 0:
            financials["price_per_night"] = round(total_price / nights, 2)
        else:
            financials["price_per_night"] = 0.0
        
        # Calculate price_per_guest_per_night
        if nights > 0 and guests_count > 0:
            financials["price_per_guest_per_night"] = round(total_price / (nights * guests_count), 2)
        else:
            financials["price_per_guest_per_night"] = 0.0
        
        logger.debug(f"Calculated financials for reservation: {financials}")
        return financials
        
    except Exception as e:
        logger.error(f"Error calculating financials: {str(e)}", exc_info=True)
        # Return defaults on error
        return {
            "airbnb_fee": 0.0,
            "lodgify_fee": 0.0,
            "stripe_fee": 0.0,
            "dynamic_fee": 0.0,
            "total_fees": 0.0,
            "vat_amount": 0.0,
            "net_revenue": 0.0,
            "payout_expected": 0.0,
            "anticipation_days": "",
            "price_per_night": 0.0,
            "price_per_guest_per_night": 0.0
        }


def generate_financial_notes(normalized_reservation: Dict[str, Any], financials: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, str]:
    """
    Generate cell notes explaining how each financial field was calculated.
    
    Args:
        normalized_reservation: Dictionary containing reservation data
        financials: Dictionary containing calculated financial fields
        config: Configuration dictionary with rates and percentages
        
    Returns:
        Dictionary mapping field names to note text
    """
    notes = {}
    
    try:
        total_price = float(normalized_reservation.get("total_price", 0.0))
        origin = str(normalized_reservation.get("origin", "")).lower().strip()
        country = str(normalized_reservation.get("country", "")).strip()
        reservation_date = normalized_reservation.get("reservation_date", "")
        nights = float(normalized_reservation.get("nights", 0.0))
        guests_count = float(normalized_reservation.get("guests_count", 1.0))
        currency = str(normalized_reservation.get("currency", "EUR")).strip().upper()
        
        # Get config values
        vat_rate = float(config.get("VAT_RATE", 0.0))
        airbnb_fee_percent = float(config.get("AIRBNB_FEE_PERCENT", 0.0))
        lodgify_fee_percent = float(config.get("LODGIFY_FEE_PERCENT", 0.0))
        
        # Format currency symbol
        currency_symbol = currency if currency else "€"
        
        # VAT note
        vat_amount = financials.get("vat_amount", 0.0)
        if vat_amount:
            vat_rate_percent = vat_rate * 100
            notes["vat_amount"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"VAT rate: {vat_rate_percent:.1f}%\n"
                f"Formula used:\n"
                f"{total_price:.2f} * {vat_rate_percent:.1f}% = {vat_amount:.2f} {currency_symbol}"
            )
        
        # Airbnb fee note
        airbnb_fee = financials.get("airbnb_fee", 0.0)
        if airbnb_fee:
            airbnb_rate_percent = airbnb_fee_percent * 100
            notes["airbnb_fee"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"Booking origin: {origin.upper()}\n"
                f"Airbnb fee rate: {airbnb_rate_percent:.1f}%\n"
                f"Formula used:\n"
                f"{total_price:.2f} * {airbnb_rate_percent:.1f}% = {airbnb_fee:.2f} {currency_symbol}"
            )
        
        # Lodgify fee note
        lodgify_fee = financials.get("lodgify_fee", 0.0)
        if lodgify_fee:
            notes["lodgify_fee"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"Booking origin: {origin.upper()}\n"
                f"Lodgify fee rate: 1.0%\n"
                f"Formula used:\n"
                f"{total_price:.2f} * 1.0% = {lodgify_fee:.2f} {currency_symbol}"
            )
        
        # Stripe fee note
        stripe_fee = financials.get("stripe_fee", 0.0)
        if stripe_fee:
            # Determine Stripe fee rate from country
            if country == "United Kingdom":
                stripe_rate = 2.5
                fixed = 0.50
            elif country in ["Portugal", "France", "Germany", "Italy", "Spain", "Netherlands",
                            "Belgium", "Ireland", "Austria", "Denmark", "Sweden", "Finland",
                            "Luxembourg", "Greece", "Poland", "Czech Republic", "Hungary",
                            "Romania", "Bulgaria", "Croatia", "Slovenia", "Slovakia",
                            "Lithuania", "Latvia", "Estonia"]:
                stripe_rate = 1.5
                fixed = 0.50
            else:
                stripe_rate = 3.25
                fixed = 0.50
            
            notes["stripe_fee"] = (
                f"Guest origin: {country}\n"
                f"Stripe fee policy: {stripe_rate}% + {fixed:.2f} {currency_symbol}\n"
                f"Formula used:\n"
                f"{total_price:.2f} * {stripe_rate:.1f}% + {fixed:.2f} = {stripe_fee:.2f} {currency_symbol}"
            )
        
        # Dynamic fee note
        dynamic_fee = financials.get("dynamic_fee", 0.0)
        if dynamic_fee:
            notes["dynamic_fee"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"Reservation date: {reservation_date}\n"
                f"Dynamic fee rate: 0.8%\n"
                f"Applied from: 2024-11-12\n"
                f"Formula used:\n"
                f"{total_price:.2f} * 0.8% = {dynamic_fee:.2f} {currency_symbol}"
            )
        
        # Total fees note
        total_fees = financials.get("total_fees", 0.0)
        if total_fees:
            airbnb_fee = financials.get("airbnb_fee", 0.0)
            lodgify_fee = financials.get("lodgify_fee", 0.0)
            stripe_fee = financials.get("stripe_fee", 0.0)
            dynamic_fee = financials.get("dynamic_fee", 0.0)
            
            parts = []
            if airbnb_fee:
                parts.append(f"{airbnb_fee:.2f}")
            if lodgify_fee:
                parts.append(f"{lodgify_fee:.2f}")
            if stripe_fee:
                parts.append(f"{stripe_fee:.2f}")
            if dynamic_fee:
                parts.append(f"{dynamic_fee:.2f}")
            
            formula = " + ".join(parts) if parts else "0.00"
            
            notes["total_fees"] = (
                f"Sum of all platform fees:\n"
                f"Formula used:\n"
                f"{formula} = {total_fees:.2f} {currency_symbol}"
            )
        
        # Net revenue note
        net_revenue = financials.get("net_revenue", 0.0)
        if net_revenue:
            total_fees = financials.get("total_fees", 0.0)
            vat_amount = financials.get("vat_amount", 0.0)
            notes["net_revenue"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"Total fees: {total_fees:.2f} {currency_symbol}\n"
                f"VAT amount: {vat_amount:.2f} {currency_symbol}\n"
                f"Formula used:\n"
                f"{total_price:.2f} - {total_fees:.2f} - {vat_amount:.2f} = {net_revenue:.2f} {currency_symbol}"
            )
        
        # Price per night note
        price_per_night = financials.get("price_per_night", 0.0)
        if price_per_night and nights > 0:
            notes["price_per_night"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"Number of nights: {int(nights)}\n"
                f"Formula used:\n"
                f"{total_price:.2f} / {int(nights)} = {price_per_night:.2f} {currency_symbol}"
            )
        
        # Price per guest per night note
        price_per_guest_per_night = financials.get("price_per_guest_per_night", 0.0)
        if price_per_guest_per_night and nights > 0 and guests_count > 0:
            notes["price_per_guest_per_night"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"Number of nights: {int(nights)}\n"
                f"Number of guests: {int(guests_count)}\n"
                f"Formula used:\n"
                f"{total_price:.2f} / ({int(nights)} * {int(guests_count)}) = {price_per_guest_per_night:.2f} {currency_symbol}"
            )
        
        # Payout expected note
        payout_expected = financials.get("payout_expected", 0.0)
        if payout_expected:
            airbnb_fee = financials.get("airbnb_fee", 0.0)
            lodgify_fee = financials.get("lodgify_fee", 0.0)
            stripe_fee = financials.get("stripe_fee", 0.0)
            
            parts = []
            if airbnb_fee:
                parts.append(f"{airbnb_fee:.2f}")
            if lodgify_fee:
                parts.append(f"{lodgify_fee:.2f}")
            if stripe_fee:
                parts.append(f"{stripe_fee:.2f}")
            
            formula_parts = " + ".join(parts) if parts else "0.00"
            
            notes["payout_expected"] = (
                f"Reservation total price: {total_price:.2f} {currency_symbol}\n"
                f"Platform fees (excluding dynamic fee): {formula_parts}\n"
                f"Formula used:\n"
                f"{total_price:.2f} - ({formula_parts}) = {payout_expected:.2f} {currency_symbol}"
            )
        
        return notes
        
    except Exception as e:
        logger.error(f"Error generating financial notes: {str(e)}")
        return {}


class FinanceService:
    """Service class for financial calculations and fee processing."""
    
    def __init__(self):
        """Initialize finance service with fee rates from settings."""
        self.vat_rate = get_vat_rate()
        self.airbnb_fee_percent = get_airbnb_fee_percent()
        self.lodgify_fee_percent = get_lodgify_fee_percent()
        self.stripe_fee_table = get_stripe_fee_table()
    
    def calculate_vat(self, amount: float) -> float:
        """
        Calculate VAT amount from gross amount.
        
        Args:
            amount: Gross amount including VAT
            
        Returns:
            VAT amount
        """
        vat_amount = amount * (self.vat_rate / (1 + self.vat_rate))
        return round(vat_amount, 2)
    
    def calculate_net_amount(self, gross_amount: float) -> float:
        """
        Calculate net amount (excluding VAT) from gross amount.
        
        Args:
            gross_amount: Gross amount including VAT
            
        Returns:
            Net amount excluding VAT
        """
        net_amount = gross_amount / (1 + self.vat_rate)
        return round(net_amount, 2)
    
    def calculate_airbnb_fee(self, amount: float) -> float:
        """
        Calculate Airbnb platform fee.
        
        Args:
            amount: Booking amount
            
        Returns:
            Airbnb fee amount
        """
        fee = amount * self.airbnb_fee_percent
        return round(fee, 2)
    
    def calculate_lodgify_fee(self, amount: float) -> float:
        """
        Calculate Lodgify platform fee.
        
        Args:
            amount: Booking amount
            
        Returns:
            Lodgify fee amount
        """
        fee = amount * self.lodgify_fee_percent
        return round(fee, 2)
    
    def calculate_stripe_fee(self, amount: float, country: str = "PT") -> float:
        """
        Calculate Stripe payment processing fee based on country.
        
        Args:
            amount: Payment amount
            country: Country code for fee lookup
            
        Returns:
            Stripe fee amount
        """
        country_fee_percent = self.stripe_fee_table.get(country.upper(), 0.0)
        if isinstance(country_fee_percent, (int, float)):
            fee = amount * float(country_fee_percent)
            return round(fee, 2)
        return 0.0
    
    def calculate_net_revenue(self, gross_amount: float, channel: str = "direct") -> Dict[str, float]:
        """
        Calculate net revenue after all fees and VAT.
        
        Args:
            gross_amount: Gross booking amount
            channel: Booking channel (airbnb, lodgify, direct, etc.)
            
        Returns:
            Dictionary with breakdown: gross, vat, fees, net_revenue
        """
        channel = channel.lower().strip()
        
        # Calculate channel-specific fees
        airbnb_fee = self.calculate_airbnb_fee(gross_amount) if channel == "airbnb" else 0.0
        lodgify_fee = self.calculate_lodgify_fee(gross_amount) if channel == "lodgify" else 0.0
        
        # Calculate VAT
        vat_amount = self.calculate_vat(gross_amount)
        
        # Calculate total fees
        total_fees = airbnb_fee + lodgify_fee + vat_amount
        
        # Calculate net revenue
        net_revenue = round(gross_amount - total_fees, 2)
        
        return {
            "gross": gross_amount,
            "vat": vat_amount,
            "fees": total_fees,
            "net_revenue": net_revenue
        }
