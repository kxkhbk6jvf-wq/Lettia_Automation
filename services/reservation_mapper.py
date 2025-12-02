"""
Reservation Mapper Service.
Maps Lodgify CSV row data to internal normalized reservation schema.
"""

import logging
import re
from datetime import datetime
from typing import Dict, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)

# Complete phone prefix to country name mapping
PHONE_COUNTRY_MAP = {
    "+1": "United States",
    "+12": "United States",
    "+30": "Greece",
    "+31": "Netherlands",
    "+32": "Belgium",
    "+33": "France",
    "+34": "Spain",
    "+351": "Portugal",
    "+352": "Luxembourg",
    "+353": "Ireland",
    "+354": "Iceland",
    "+355": "Albania",
    "+356": "Malta",
    "+357": "Cyprus",
    "+358": "Finland",
    "+359": "Bulgaria",
    "+36": "Hungary",
    "+39": "Italy",
    "+40": "Romania",
    "+41": "Switzerland",
    "+420": "Czech Republic",
    "+421": "Slovakia",
    "+43": "Austria",
    "+44": "United Kingdom",
    "+45": "Denmark",
    "+46": "Sweden",
    "+47": "Norway",
    "+48": "Poland",
    "+49": "Germany",
    "+52": "Mexico",
    "+55": "Brazil",
    "+60": "Malaysia",
    "+61": "Australia",
    "+62": "Indonesia",
    "+63": "Philippines",
    "+64": "New Zealand",
    "+65": "Singapore",
    "+66": "Thailand",
    "+81": "Japan",
    "+82": "South Korea",
    "+84": "Vietnam",
    "+86": "China",
    "+852": "Hong Kong",
    "+853": "Macau",
    "+886": "Taiwan",
    "+90": "Turkey",
    "+91": "India",
    "+92": "Pakistan",
    "+94": "Sri Lanka",
    "+95": "Myanmar",
    "+98": "Iran"
}


class ReservationMapper:
    """
    Service for mapping CSV row data to normalized reservation format.
    
    Converts Lodgify CSV export format to internal Google Sheets schema.
    """
    
    def __init__(self):
        """Initialize reservation mapper."""
        logger.debug("ReservationMapper initialized")
    
    def _normalize_date(self, date_value: Any) -> str:
        """
        Normalize date to ISO 8601 format (YYYY-MM-DD).
        
        Handles various date formats from CSV.
        
        Args:
            date_value: Date value (string, datetime, or other)
            
        Returns:
            ISO 8601 formatted date string (YYYY-MM-DD) or empty string if invalid
        """
        if not date_value or date_value == "":
            return ""
        
        # If already a datetime object
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%d")
        
        # Convert to string
        date_str = str(date_value).strip()
        
        if not date_str or date_str.lower() in ["nan", "none", "null"]:
            return ""
        
        # Try parsing various date formats
        date_formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%d/%m/%Y %H:%M",
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue
        
        logger.warning(f"Could not parse date: {date_value}")
        return ""
    
    def _normalize_phone(self, phone_raw: str) -> str:
        """
        Normalize phone number by adding '+' prefix if not present.
        Always returns string with leading single quote to force text format in Google Sheets.
        
        Rules:
        - Remove spaces, hyphens, commas
        - If phone contains multiple numbers, take the first valid one
        - Convert numbers starting with "00" → "+"
        - If no prefix but starts with country code digits, prefix "+"
        - ALWAYS return with leading single quote to force text format
        
        Args:
            phone_raw: Phone number string (may contain multiple numbers)
            
        Returns:
            Normalized phone number with '+' prefix and leading quote, or empty string if input is empty
        """
        if not phone_raw:
            return ""
        
        # Split multiple numbers (commas, semicolons, spaces)
        parts = re.split(r"[;, ]+", str(phone_raw).strip())
        first = parts[0] if parts else ""
        
        if not first:
            return ""
        
        # Remove formatting (spaces, hyphens, commas)
        phone = first.replace(" ", "").replace("-", "").replace(",", "").replace("(", "").replace(")", "").replace(".", "")
        
        if not phone:
            return ""
        
        # Already in international format
        if phone.startswith("+"):
            return "'" + phone
        
        # Convert 00xx → +xx
        if phone.startswith("00"):
            return "'" + "+" + phone[2:]
        
        # If starts with digits and length >= 8, assume missing "+"
        if phone.isdigit() and len(phone) >= 8:
            return "'" + "+" + phone
        
        # Otherwise try to add "+" if it looks like a number
        if phone.isdigit():
            return "'" + "+" + phone
        
        # Return as-is with quote prefix
        return "'" + phone
    
    def _calculate_anticipation_days(self, check_in: str, reservation_date: str) -> str:
        """
        Calculate days between reservation_date and check_in.
        
        Args:
            check_in: Check-in date (ISO format: YYYY-MM-DD)
            reservation_date: Reservation creation date (ISO format: YYYY-MM-DD)
            
        Returns:
            Number of days as string, or empty string if calculation fails
        """
        if not check_in or not reservation_date:
            return ""
        
        try:
            check_in_date = datetime.strptime(check_in, "%Y-%m-%d").date()
            res_date = datetime.strptime(reservation_date, "%Y-%m-%d").date()
            
            delta = (check_in_date - res_date).days
            
            # Ensure non-negative
            return str(max(0, delta))
        except (ValueError, TypeError, AttributeError) as e:
            logger.debug(f"Could not calculate anticipation_days: {e}")
            return ""
    
    def _country_from_phone(self, normalized_phone: str) -> str:
        """
        Extract country name from normalized phone number based on prefix.
        
        Phone is expected to be normalized with leading "+" (may have leading quote).
        Uses longest prefix match from PHONE_COUNTRY_MAP.
        Returns empty string if country cannot be determined.
        
        Args:
            normalized_phone: Normalized phone number (e.g., "'+44762322410" or "+351912345678")
            
        Returns:
            Country name as string, or empty string if unknown
        """
        if not normalized_phone:
            return ""
        
        # Remove leading quote if present (from _normalize_phone)
        phone = normalized_phone.lstrip("'")
        
        if not phone:
            return ""
        
        # Ensure it starts with "+" for matching
        if not phone.startswith("+"):
            # If it starts with digits, try adding "+"
            if phone.isdigit() and len(phone) >= 8:
                phone = "+" + phone
            else:
                return ""
        
        # Longest prefix wins - sort by length descending
        for prefix in sorted(PHONE_COUNTRY_MAP.keys(), key=len, reverse=True):
            if phone.startswith(prefix):
                return PHONE_COUNTRY_MAP[prefix]
        
        # Unknown country
        return ""
    
    def _calculate_stripe_fee(self, source: str, total_price: float, phone: str) -> float:
        """
        Calculate Stripe fee based on origin and country from phone number.
        
        Stripe fee applies only for website bookings.
        Fee calculation:
        - United Kingdom: total_price * 0.025 + 0.50
        - EU countries: total_price * 0.015 + 0.50
        - Other countries: total_price * 0.0325 + 0.50
        
        Args:
            source: Booking source (e.g., "Website", "Airbnb")
            total_price: Total booking price
            phone: Phone number (may contain country code)
            
        Returns:
            Stripe fee as float, or 0.0 if not applicable
        """
        # Stripe fee only applies to website bookings
        if source != "Website":
            return 0.0
        
        # Extract country from phone number
        import re
        phone_clean = re.sub(r'[\s\-\(\)\.]', '', str(phone).strip())
        
        # Remove leading + or 00
        if phone_clean.startswith("+"):
            phone_clean = phone_clean[1:]
        elif phone_clean.startswith("00"):
            phone_clean = phone_clean[2:]
        
        # Country code to country name mapping
        country_map = {
            "44": "United Kingdom",
            "351": "Portugal",
            "33": "France",
            "49": "Germany",
            "39": "Italy",
            "34": "Spain",
            "31": "Netherlands",
            "32": "Belgium",
            "353": "Ireland",
            "43": "Austria",
            "45": "Denmark",
            "46": "Sweden",
            "47": "Norway",
            "358": "Finland",
            "41": "Switzerland",
            "352": "Luxembourg",
            "30": "Greece",
            "351": "Portugal",
            "48": "Poland",
            "420": "Czech Republic",
            "36": "Hungary",
            "40": "Romania",
            "359": "Bulgaria",
            "385": "Croatia",
            "386": "Slovenia",
            "421": "Slovakia",
            "370": "Lithuania",
            "371": "Latvia",
            "372": "Estonia",
        }
        
        # EU countries list
        EU_COUNTRIES = {
            "Portugal", "France", "Germany", "Italy", "Spain", "Netherlands",
            "Belgium", "Ireland", "Austria", "Denmark", "Sweden", "Finland",
            "Luxembourg", "Greece", "Poland", "Czech Republic", "Hungary",
            "Romania", "Bulgaria", "Croatia", "Slovenia", "Slovakia",
            "Lithuania", "Latvia", "Estonia"
        }
        
        # Extract country code (try 1-3 digits)
        country = "Other"
        for code_len in [3, 2, 1]:
            if len(phone_clean) >= code_len:
                code = phone_clean[:code_len]
                if code in country_map:
                    country = country_map[code]
                    break
        
        # Calculate Stripe fee based on country
        if country == "United Kingdom":
            stripe_fee = total_price * 0.025 + 0.50
        elif country in EU_COUNTRIES:
            stripe_fee = total_price * 0.015 + 0.50
        else:
            stripe_fee = total_price * 0.0325 + 0.50
        
        return round(stripe_fee, 2)
    
    def map_csv_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert Lodgify CSV row to internal normalized reservation schema.
        
        CSV columns:
        - Id, Source, SourceText, Name, Email, Phone, People, CountryName
        - DateArrival, DateDeparture, Nights, Currency, TotalAmount, IncludedVatTotal
        - DateCreated, Status
        
        Mapped output (Google Sheets schema):
        - reservation_id, origin, lodgify_id, airbnb_id, guest_name, guest_email
        - guest_phone, guests_count, check_in, check_out, nights, currency
        - total_price, country, reservation_date, status, vat_amount
        - anticipation_days, welcome_sent
        
        Args:
            row: Dictionary containing CSV row data
            
        Returns:
            Dictionary with normalized reservation data ready for Google Sheets,
            or None if Status is not "Booked"
        """
        # Extract CSV values (handle missing keys gracefully)
        csv_id = str(row.get("Id", "")).strip()
        source = str(row.get("Source", "")).strip()
        source_text = str(row.get("SourceText", "")).strip()
        name = str(row.get("Name", "")).strip()
        guest_email = str(row.get("Email", "")).strip()
        phone = str(row.get("Phone", "")).strip()
        people = row.get("People", "")
        date_arrival = row.get("DateArrival", "")
        date_departure = row.get("DateDeparture", "")
        nights = row.get("Nights", "")
        currency = str(row.get("Currency", "")).strip()
        total = row.get("TotalAmount", "")
        included_vat_total = row.get("IncludedVatTotal", "")
        reservation_date_raw = row.get("DateCreated", "")
        status = str(row.get("Status", "")).strip()
        
        # Filter: only process reservations with Status == "Booked"
        if status != "Booked":
            logger.debug(f"Skipping reservation {csv_id}: Status is '{status}', expected 'Booked'")
            return None
        
        # Normalize phone first (needed for country extraction)
        # Try "Phone" from CSV, or "guest_phone" if already normalized
        phone_raw = phone if phone else str(row.get("guest_phone", "")).strip()
        normalized_phone = self._normalize_phone(phone_raw)
        
        # Extract country from normalized phone
        country = self._country_from_phone(normalized_phone)
        
        # Normalize dates
        check_in = self._normalize_date(date_arrival)
        check_out = self._normalize_date(date_departure)
        reservation_date = self._normalize_date(reservation_date_raw)
        
        # Map lodgify_id
        if source in ["Website", "Direct", "Lodgify"]:
            lodgify_id = csv_id
        else:
            lodgify_id = ""
        
        # Map airbnb_id
        if source == "Airbnb":
            airbnb_id = source_text
        else:
            airbnb_id = ""
        
        # Map origin (normalize source to lowercase)
        origin = source.lower() if source else ""
        
        # Map guests_count
        try:
            guests_count = int(float(people)) if people and str(people).strip() else ""
        except (ValueError, TypeError):
            guests_count = ""
        
        # Map nights
        try:
            nights_value = int(float(nights)) if nights and str(nights).strip() else ""
        except (ValueError, TypeError):
            nights_value = ""
        
        # Map total_price
        try:
            total_price = float(total) if total and str(total).strip() else 0.0
        except (ValueError, TypeError):
            total_price = 0.0
        
        # Map vat_amount (from IncludedVatTotal if available, otherwise will be calculated)
        try:
            vat_amount = float(included_vat_total) if included_vat_total and str(included_vat_total).strip() else 0.0
        except (ValueError, TypeError):
            vat_amount = 0.0
        
        # Calculate anticipation_days
        anticipation_days = self._calculate_anticipation_days(check_in, reservation_date)
        
        # Build normalized reservation dictionary
        mapped = {
            "reservation_id": csv_id,  # Use CSV Id as reservation_id
            "origin": origin,
            "lodgify_id": lodgify_id,
            "airbnb_id": airbnb_id,
            "guest_name": name,
            "guest_email": guest_email,
            "guest_phone": normalized_phone,
            "guests_count": guests_count,
            "check_in": check_in,
            "check_out": check_out,
            "nights": nights_value,
            "currency": currency,
            "total_price": total_price,
            "country": country,
            "reservation_date": reservation_date,
            "status": status,
            "vat_amount": vat_amount,  # Will be recalculated by calculate_financials if needed
            "anticipation_days": anticipation_days,
            "welcome_sent": "",  # Always empty for new imports
        }
        
        # Helper method to safely convert to float
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
        
        # Compute financial fields (airbnb_fee comes from Google Sheets later)
        # Calculate lodgify_fee - check if source is "Website" (before normalization)
        if source == "Website":
            lodgify_fee = mapped["total_price"] * 0.01
        else:
            lodgify_fee = 0.0
        
        # Calculate stripe_fee (only for website bookings)
        stripe_fee = self._calculate_stripe_fee(source, total_price, phone)
        
        # Calculate dynamic_fee (applies from 2024-11-12 forward)
        if mapped["reservation_date"] >= "2024-11-12":
            dynamic_fee = mapped["total_price"] * 0.008
        else:
            dynamic_fee = 0.0
        
        # Initialize airbnb_fee as 0.0 (will be computed from Google Sheets config later)
        airbnb_fee = 0.0
        
        # Calculate total_fees - ensure all values are floats before summing
        airbnb_fee = _to_float(airbnb_fee)
        lodgify_fee = _to_float(lodgify_fee)
        stripe_fee = _to_float(stripe_fee)
        dynamic_fee = _to_float(dynamic_fee)
        
        total_fees = airbnb_fee + lodgify_fee + stripe_fee + dynamic_fee
        
        # Calculate payout_expected (excludes dynamic_fee)
        payout_expected = total_price - airbnb_fee - lodgify_fee - stripe_fee
        
        # Add computed financial fields to mapped dictionary
        mapped["airbnb_fee"] = airbnb_fee
        mapped["lodgify_fee"] = lodgify_fee
        mapped["stripe_fee"] = stripe_fee
        mapped["dynamic_fee"] = dynamic_fee
        mapped["total_fees"] = total_fees
        mapped["payout_expected"] = payout_expected
        
        logger.debug(f"Mapped reservation {csv_id}: {name} ({check_in} to {check_out})")
        
        return mapped

