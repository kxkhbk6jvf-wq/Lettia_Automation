"""
Invoice Service.
Handles generation of invoice-ready data from reservations and SEF data.
"""

import logging
import re
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
from difflib import SequenceMatcher

from googleapiclient.errors import HttpError

from services.google_sheets import GoogleSheetsService
from services.state_manager import StateManager, InvoiceStateManager

# Configure logging
logger = logging.getLogger(__name__)


def normalize_price(value):
    """
    Normalize price values to float, handling various formats.
    
    Handles:
    - None values → 0.0
    - Numeric values → float
    - String values with formatting:
      - Removes non-breaking spaces (\xa0) and regular spaces
      - Removes currency symbols (€, EUR)
      - Handles thousand separators (removes dots)
      - Converts comma decimal separator to dot
      - Examples: "2 000,00" → 2000.0, "873,50" → 873.5
    
    Args:
        value: Price value (can be None, int, float, or string)
        
    Returns:
        float: Normalized price value, or 0.0 if conversion fails
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value)
    s = s.replace('\xa0', '').replace(' ', '')
    s = s.replace('€', '').replace('EUR', '').strip()
    # First remove thousand separators, then convert comma to dot
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except:
        return 0.0


class InvoiceService:
    """
    Service for generating invoice-ready data from reservations and SEF forms.
    
    Generates invoice lines based on reservations:
    - Airbnb: 1 line
    - Website: 3 lines (pre-invoice, credit note, final invoice)
    
    Matches reservations with SEF forms to extract guest identity.
    Calculates tourist tax and builds invoice descriptions.
    """
    
    # Invoice Google Sheet ID
    INVOICE_SHEET_ID = "1uZ7XooiY_-VNInux45DgBXhDbGuGH7EObU-Yzx-YtOY"
    INVOICE_SHEET_NAME = "Invoices_Lettia"
    
    # Column order for Invoices_Lettia sheet (must match exactly)
    INVOICE_COLUMNS = [
        "reservation_id",
        "external_id",
        "guest_name",
        "passport_number",
        "country",
        "check_in",
        "check_out",
        "nights",
        "guests_count",  # NEW: Total guests in reservation
        "tt_guests_count",  # NEW: Number of guests paying tourist tax
        "accommodation_value",
        "tourist_tax",
        "invoice_description",
        "tourist_tax_description",
        "airbnb_fee",
        "lodgify_fee",
        "stripe_fee",
        "dynamic_fee",
        "total_fees",
        "invoice_total",
        "invoice_status",
        "invoice_number",
        "invoice_date",
    ]
    
    def __init__(self, sheets_service: GoogleSheetsService):
        """
        Initialize InvoiceService.
        
        Args:
            sheets_service: GoogleSheetsService instance
        """
        self.sheets = sheets_service
        self.invoice_state = InvoiceStateManager()
    
    def parse_sef_date(self, raw: Any) -> Optional[date]:
        """
        Robust parser for SEF date formats.
        
        Accepts multiple variations and always returns a datetime.date.
        
        Handles formats:
        - 25-9-14 (YY-M-D)
        - 2025/08/24 (YYYY/MM/DD)
        - 2025-08-24 (YYYY-MM-DD)
        - 2025-08-24 07:45:10 (YYYY-MM-DD HH:MM:SS)
        - 2025/08/24 07:45:10 (YYYY/MM/DD HH:MM:SS)
        - DD/MM/YYYY
        - And more variations
        
        Args:
            raw: Raw date value from SEF sheet (can be string, None, etc.)
            
        Returns:
            datetime.date object if parsing succeeds, None otherwise
        """
        if not raw or str(raw).strip() == "":
            return None
        
        text = str(raw).strip()
        
        # List of potential formats
        formats = [
            "%y-%m-%d",          # 25-9-14, 25-08-24
            "%y/%m/%d",          # 25/9/14, 25/08/24
            "%Y/%m/%d",          # 2025/08/24
            "%Y-%m-%d",          # 2025-08-24
            "%Y-%m-%d %H:%M:%S", # 2025-08-24 07:45:10
            "%Y/%m/%d %H:%M:%S", # 2025/08/24 07:45:10
            "%d/%m/%Y",          # DD/MM/YYYY
            "%d-%m-%Y",          # DD-MM-YYYY
            "%d/%m/%y",          # DD/MM/YY
            "%d-%m-%y",          # DD-MM-YY
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(text, fmt).date()
            except:
                pass
        
        # Fallback: attempt to extract just the YYYY-MM-DD or YYYY/MM/DD portion
        m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text)
        if m:
            y, mth, d = m.groups()
            try:
                return date(int(y), int(mth), int(d))
            except:
                pass
        
        # Try to extract YY-MM-DD or YY/MM/DD (2-digit year)
        m = re.search(r"(\d{2})[-/](\d{1,2})[-/](\d{1,2})", text)
        if m:
            y, mth, d = m.groups()
            try:
                # Assume 20XX for 2-digit years
                year = 2000 + int(y) if int(y) < 50 else 1900 + int(y)
                return date(year, int(mth), int(d))
            except:
                pass
        
        # If everything fails, log and return None
        logger.warning(f"[InvoiceService] Could not parse SEF date: {repr(raw)}")
        return None
    
    def _parse_reservation_date(self, raw: Any) -> Optional[date]:
        """
        Parse reservation date from Google Sheets.
        
        Reservation dates are typically in YYYY-MM-DD format,
        but we handle variations as well.
        
        Args:
            raw: Raw date value from reservation (usually YYYY-MM-DD string)
            
        Returns:
            datetime.date object if parsing succeeds, None otherwise
        """
        if not raw or str(raw).strip() == "":
            return None
        
        text = str(raw).strip()
        
        # Standard formats for reservation dates
        formats = [
            "%Y-%m-%d",          # 2025-08-24 (most common)
            "%Y/%m/%d",          # 2025/08/24
            "%d/%m/%Y",          # 24/08/2025
            "%d-%m-%Y",          # 24-08-2025
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(text, fmt).date()
            except:
                pass
        
        # Fallback: extract YYYY-MM-DD pattern
        m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text)
        if m:
            y, mth, d = m.groups()
            try:
                return date(int(y), int(mth), int(d))
            except:
                pass
        
        logger.warning(f"[InvoiceService] Could not parse reservation date: {repr(raw)}")
        return None
    
    def _fuzzy_match_score(self, name1: str, name2: str) -> float:
        """
        Calculate fuzzy matching score between two names.
        
        Args:
            name1: First name to compare
            name2: Second name to compare
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        if not name1 or not name2:
            return 0.0
        
        # Normalize names: lowercase, strip
        name1_norm = str(name1).lower().strip()
        name2_norm = str(name2).lower().strip()
        
        if not name1_norm or not name2_norm:
            return 0.0
        
        # Use SequenceMatcher for similarity
        return SequenceMatcher(None, name1_norm, name2_norm).ratio()
    
    def _debug_matching_diagnostics(self, reservation: Dict[str, Any], sef_forms: List[Dict[str, Any]]) -> None:
        """
        Print detailed diagnostic information for SEF matching.
        
        This method provides comprehensive debugging information about why
        a reservation may or may not match with SEF forms.
        
        Args:
            reservation: Reservation dictionary
            sef_forms: List of all SEF form dictionaries
        """
        reservation_id = str(reservation.get("reservation_id", "")).strip()
        origin = str(reservation.get("origin", "")).lower().strip()
        
        # Get external ID
        if origin == "airbnb":
            external_id = str(reservation.get("airbnb_id", reservation_id)).strip()
        else:
            external_id = str(reservation.get("lodgify_id", reservation_id)).strip()
        
        guest_name = str(reservation.get("guest_name", "")).strip()
        reservation_check_in = self._parse_reservation_date(reservation.get("check_in"))
        reservation_check_out = self._parse_reservation_date(reservation.get("check_out"))
        
        print("\n" + "=" * 70)
        print(f"DIAGNOSTIC FOR RESERVATION {reservation_id}")
        print("=" * 70)
        print()
        
        # Reservation info
        print(f"reservation_id: {reservation_id}")
        print(f"external_id: {external_id}")
        print(f"guest_name: {guest_name}")
        print(f"check_in (parsed): {reservation_check_in}")
        print(f"check_out (parsed): {reservation_check_out}")
        
        if not reservation_check_in or not reservation_check_out:
            print(f"\nFAIL REASON: Invalid or missing reservation dates")
            print("=" * 70)
            return
        
        # Find SEF candidates within ±1 day window (same arrival year ± 1 day)
        from datetime import timedelta
        candidates = []
        
        for sef in sef_forms:
            sef_check_in = self.parse_sef_date(sef.get("Check-in date"))
            sef_check_out = self.parse_sef_date(sef.get("Check-out date"))
            
            if sef_check_in and reservation_check_in:
                # Check if same year or within ±1 day window
                same_year = sef_check_in.year == reservation_check_in.year
                days_diff_in = (sef_check_in - reservation_check_in).days
                days_diff_out = (sef_check_out - reservation_check_out).days if (sef_check_out and reservation_check_out) else 999
                
                # Consider candidates within ±1 day window (same arrival year ± 1 day)
                if same_year and abs(days_diff_in) <= 1:
                    candidates.append({
                        "sef": sef,
                        "check_in": sef_check_in,
                        "check_out": sef_check_out,
                        "days_diff_in": days_diff_in,
                        "days_diff_out": days_diff_out,
                    })
        
        if not candidates:
            print(f"\nFAIL REASON: No SEF forms for these dates")
            print("=" * 70)
            return
        
        # Print each candidate
        for idx, cand in enumerate(candidates, 1):
            sef = cand["sef"]
            sef_name = str(sef.get("Full Name", "")).strip()
            sef_check_in = cand["check_in"]
            sef_check_out = cand["check_out"]
            days_diff_in = cand["days_diff_in"]
            days_diff_out = cand["days_diff_out"]
            
            # Calculate fuzzy score
            fuzzy_score = self._fuzzy_match_score(guest_name, sef_name)
            
            # Get additional info
            passport = str(sef.get("Identification Card Number", "")).strip()
            country = str(sef.get("Country of Residence", "")).strip()
            
            # Age classification (assume all SEF forms are adults ≥16 for now)
            age_classification = "adult"  # Default assumption
            
            print(f"\n-- SEF Candidate {idx} --")
            print(f"SEF full_name: {sef_name}")
            print(f"SEF check_in_date (parsed): {sef_check_in}")
            print(f"SEF check_out_date (parsed): {sef_check_out}")
            print(f"Days difference to reservation check-in: {days_diff_in:+d}")
            print(f"Days difference to reservation check-out: {days_diff_out:+d}")
            print(f"Fuzzy similarity score between names: {fuzzy_score:.2f}")
            print(f"Whether age < 16 (and therefore excluded): {age_classification != 'adult'}")
            print(f"Passport number: {passport if passport else 'N/A'}")
            print(f"Country of residence: {country if country else 'N/A'}")
        
        # Determine match status and print fail reason
        exact_date_matches = [c for c in candidates if c["days_diff_in"] == 0 and c["days_diff_out"] == 0]
        
        fail_reason = None
        
        if not exact_date_matches:
            if candidates:
                min_diff = min(abs(c["days_diff_in"]) for c in candidates)
                fail_reason = f"Check-in date mismatch ({min_diff} days apart)"
            else:
                fail_reason = "No SEF forms for these dates"
        else:
            # Check fuzzy scores for exact date matches
            best_candidate = None
            best_score = 0.0
            
            for cand in exact_date_matches:
                sef = cand["sef"]
                sef_name = str(sef.get("Full Name", "")).strip()
                score = self._fuzzy_match_score(guest_name, sef_name)
                
                if score > best_score:
                    best_score = score
                    best_candidate = cand
            
            if best_score > 0.65:
                print(f"\nMATCH SUCCESSFUL")
            elif best_score > 0.0:
                fail_reason = f"Fuzzy name score too low: {best_score:.2f}"
            else:
                fail_reason = "Multiple candidates but none passed constraints"
        
        if fail_reason:
            print(f"\nFAIL REASON: {fail_reason}")
        
        print("=" * 70)
    
    def _match_primary_guest(
        self, 
        reservation: Dict[str, Any], 
        sef_forms: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Match reservation to primary guest from SEF forms.
        
        Args:
            reservation: Reservation dictionary
            sef_forms: List of SEF form dictionaries
            
        Returns:
            Primary guest SEF form dictionary, or None if no match
        """
        reservation_check_in = self._parse_reservation_date(reservation.get("check_in"))
        reservation_check_out = self._parse_reservation_date(reservation.get("check_out"))
        reservation_guest_name = str(reservation.get("guest_name", "")).strip()
        
        if not reservation_check_in or not reservation_check_out:
            logger.warning(f"Reservation {reservation.get('reservation_id')} missing or invalid check_in/check_out dates")
            return None
        
        # Filter SEF forms matching exact dates
        matching_dates = []
        for sef in sef_forms:
            sef_check_in = self.parse_sef_date(sef.get("Check-in date"))
            sef_check_out = self.parse_sef_date(sef.get("Check-out date"))
            
            # Compare parsed date objects
            if sef_check_in and sef_check_out:
                if sef_check_in == reservation_check_in and sef_check_out == reservation_check_out:
                    matching_dates.append(sef)
                    logger.debug(f"Matched SEF form dates: check_in={sef_check_in}, check_out={sef_check_out} for reservation {reservation.get('reservation_id')}")
        
        if not matching_dates:
            logger.debug(f"No SEF forms found with matching dates for reservation {reservation.get('reservation_id')}")
            # Return first form if no date match (fallback)
            if sef_forms:
                return sef_forms[0]
            return None
        
        # Compare names and find best match
        best_match = None
        best_score = 0.0
        
        for sef in matching_dates:
            sef_name = str(sef.get("Full Name", "")).strip()
            score = self._fuzzy_match_score(reservation_guest_name, sef_name)
            
            if score > best_score:
                best_score = score
                best_match = sef
        
        # If best score > 0.65, use it; otherwise use first matching date
        if best_score > 0.65:
            matched_guest_name = str(best_match.get("Full Name", "")).strip() if best_match else "Unknown"
            logger.debug(f"Found primary guest match with score {best_score:.2f} for reservation {reservation.get('reservation_id')}: '{matched_guest_name}'")
            logger.debug(f"Parsed SEF dates for '{matched_guest_name}': check_in={reservation_check_in}, check_out={reservation_check_out}")
            return best_match
        elif matching_dates:
            matched_guest_name = str(matching_dates[0].get("Full Name", "")).strip() if matching_dates[0] else "Unknown"
            logger.debug(f"Using first matching date form (score {best_score:.2f} < 0.65) for reservation {reservation.get('reservation_id')}: '{matched_guest_name}'")
            logger.debug(f"Parsed SEF dates for '{matched_guest_name}': check_in={reservation_check_in}, check_out={reservation_check_out}")
            return matching_dates[0]
        elif sef_forms:
            logger.debug(f"Using first available SEF form (no date match) for reservation {reservation.get('reservation_id')}")
            return sef_forms[0]
        
        return None
    
    def _calculate_tourist_tax(
        self, 
        reservation: Dict[str, Any], 
        sef_forms: List[Dict[str, Any]]
    ) -> Tuple[float, int]:
        """
        Calculate tourist tax for a reservation.
        
        Tax = €2/night per adult
        Adults = number of SEF forms (assumed all ≥16)
        
        Business rule: Tourist tax applies only for check-in dates from 2025-01-01 onwards.
        If check_in < 2025-01-01, tourist_tax = 0 and tt_guests_count = 0.
        
        Args:
            reservation: Reservation dictionary
            sef_forms: List of SEF forms for this reservation
            
        Returns:
            Tuple of (tourist_tax_amount, tt_guests_count)
        """
        nights = float(reservation.get("nights", 0.0))
        if nights <= 0:
            return (0.0, 0)
        
        # Parse check-in date and enforce 2025-01-01 rule
        reservation_check_in = self._parse_reservation_date(reservation.get("check_in"))
        reservation_check_out = self._parse_reservation_date(reservation.get("check_out"))
        
        # Check if check-in is before 2025-01-01
        cutoff_date = date(2025, 1, 1)
        if not reservation_check_in or reservation_check_in < cutoff_date:
            logger.debug(f"Tourist tax not applicable: check-in date {reservation_check_in} is before 2025-01-01")
            return (0.0, 0)
        
        # Filter SEF forms by matching dates
        matching_forms = []
        for sef in sef_forms:
            sef_check_in = self.parse_sef_date(sef.get("Check-in date"))
            sef_check_out = self.parse_sef_date(sef.get("Check-out date"))
            
            # Compare parsed date objects
            if sef_check_in and sef_check_out and reservation_check_in and reservation_check_out:
                if sef_check_in == reservation_check_in and sef_check_out == reservation_check_out:
                    matching_forms.append(sef)
        
        # If no exact date match, use all forms
        if not matching_forms:
            matching_forms = sef_forms
        
        num_adults = len(matching_forms)
        if num_adults == 0:
            logger.warning(f"No SEF forms found for tourist tax calculation for reservation {reservation.get('reservation_id')}")
            return (0.0, 0)
        
        tourist_tax = num_adults * nights * 2.00
        return (round(tourist_tax, 2), num_adults)
    
    def _build_lodging_description(
        self, 
        reservation: Dict[str, Any], 
        invoice_type: str = ""
    ) -> str:
        """
        Build lodging invoice description.
        
        For Website bookings:
        - pre_invoice: "Website Booking {id} – {check_in} – {check_out} – Deposit"
        - credit_note: "Website Booking {id} – {check_in} – {check_out} – Credit note for deposit"
        - final_invoice: "Website Booking {id} – {check_in} – {check_out}"
        
        For Airbnb bookings (single line):
        - "Airbnb Booking {id} – {check_in} – {check_out}"
        
        Args:
            reservation: Reservation dictionary
            invoice_type: Type of invoice ("airbnb", "pre_invoice", "credit_note", "final_invoice")
            
        Returns:
            Description string
        """
        origin = str(reservation.get("origin", "")).lower().strip()
        check_in = str(reservation.get("check_in", "")).strip()
        check_out = str(reservation.get("check_out", "")).strip()
        
        if origin == "airbnb":
            airbnb_id = str(reservation.get("airbnb_id", "")).strip()
            return f"Airbnb Booking {airbnb_id} – {check_in} – {check_out}"
        else:
            # Website booking
            lodgify_id = str(reservation.get("lodgify_id", "")).strip()
            base_description = f"Website Booking {lodgify_id} – {check_in} – {check_out}"
            
            if invoice_type == "pre_invoice":
                return f"{base_description} – Deposit"
            elif invoice_type == "credit_note":
                return f"{base_description} – Credit note for deposit"
            elif invoice_type == "final_invoice":
                return base_description
            else:
                # Fallback for unknown types
                return base_description
    
    def _build_tourist_tax_description(
        self, 
        reservation: Dict[str, Any], 
        tourist_tax: float,
        tt_guests_count: int
    ) -> str:
        """
        Build tourist tax description with consistency check.
        
        New format: "Touristic Tax – {X} guests × {N} nights"
        Performs consistency check between tax amount and guest count.
        
        Args:
            reservation: Reservation dictionary
            tourist_tax: Tourist tax amount
            tt_guests_count: Number of guests paying tourist tax
            
        Returns:
            Description string (or "ERROR in tourist tax count – please review manually" if inconsistency)
        """
        # For zero tax, return empty string
        if tourist_tax <= 0:
            return ""
        
        nights = int(float(reservation.get("nights", 0)))
        if nights <= 0:
            return ""
        
        # Perform consistency check
        # Formula: amount = guests * nights * 2€
        # Therefore: inferred_guests = amount / 2 / nights
        inferred_guests = tourist_tax / 2.0 / nights
        
        # Check if inferred matches actual count (allow small floating point differences)
        if abs(inferred_guests - tt_guests_count) < 0.01:
            # Consistent: use the format
            return f"Touristic Tax – {tt_guests_count} guests × {nights} nights"
        else:
            # Inconsistent: return error message
            logger.warning(
                f"Tourist tax inconsistency for reservation {reservation.get('reservation_id')}: "
                f"tax={tourist_tax}, nights={nights}, inferred_guests={inferred_guests:.2f}, "
                f"tt_guests_count={tt_guests_count}"
            )
            return "ERROR in tourist tax count – please review manually"
    
    def _build_invoice_line(
        self,
        reservation: Dict[str, Any],
        primary_guest: Optional[Dict[str, Any]],
        invoice_type: str,
        lodging_amount: float,
        tourist_tax: float,
        tt_guests_count: int,
        guests_count: int
    ) -> Dict[str, Any]:
        """
        Build a single invoice line dictionary.
        
        Args:
            reservation: Reservation dictionary
            primary_guest: Primary guest SEF form (may be None)
            invoice_type: Type of invoice ("airbnb", "pre_invoice", "credit_note", "final_invoice")
            lodging_amount: Amount for lodging
            tourist_tax: Tourist tax amount
            tt_guests_count: Number of guests paying tourist tax
            guests_count: Total number of guests in reservation
            
        Returns:
            Invoice line dictionary ready for Google Sheets
        """
        # Extract primary guest data - use SEF when available, fallback to Lodgify
        guest_name = str(reservation.get("guest_name", "")).strip()  # Default to Lodgify guest_name
        passport_number = ""
        country = str(reservation.get("country", "")).strip()  # Default to reservation country
        
        if primary_guest:
            # Use SEF data when available
            sef_full_name = str(primary_guest.get("Full Name", "")).strip()
            sef_passport = str(primary_guest.get("Identification Card Number", "")).strip()
            sef_country = str(primary_guest.get("Country of Residence", "")).strip()
            
            if sef_full_name:
                guest_name = sef_full_name
            if sef_passport:
                passport_number = sef_passport
            if sef_country:
                country = sef_country
        
        # Determine invoice status (Forecast or Issued)
        # Status is determined when writing to sheet (check if invoice_number + invoice_date exist)
        
        # Build descriptions with invoice_type for Website booking variations
        description_lodging = self._build_lodging_description(reservation, invoice_type)
        description_tourist_tax = self._build_tourist_tax_description(reservation, tourist_tax, tt_guests_count)
        
        # Calculate total
        total_amount = lodging_amount + tourist_tax
        
        # Build invoice line
        invoice_line = {
            "reservation_id": str(reservation.get("reservation_id", "")).strip(),
            "invoice_type": invoice_type,
            "invoice_status": "Forecast",  # Will be updated when writing if invoice_number/date exist
            "guest_name": guest_name,  # Use SEF name when available
            "passport_number": passport_number,  # Use SEF passport when available
            "country": country,  # Use SEF country when available
            "guests_count": guests_count,  # Total guests in reservation
            "tt_guests_count": tt_guests_count,  # Number of guests paying tourist tax
            "primary_document_number": passport_number,  # Keep for backwards compatibility
            "primary_country": country,  # Keep for backwards compatibility
            "invoice_number": "",  # Preserve existing, only write if empty
            "invoice_date": "",  # Preserve existing, only write if empty
            "check_in": str(reservation.get("check_in", "")).strip(),
            "check_out": str(reservation.get("check_out", "")).strip(),
            "nights": reservation.get("nights", 0),
            "lodging_amount": lodging_amount,
            "tourist_tax": tourist_tax,
            "total_amount": total_amount,
            "description_lodging": description_lodging,
            "description_tourist_tax": description_tourist_tax,
            "airbnb_fee": normalize_price(reservation.get("airbnb_fee", 0.0)),
            "lodgify_fee": normalize_price(reservation.get("lodgify_fee", 0.0)),
            "stripe_fee": normalize_price(reservation.get("stripe_fee", 0.0)),
            "dynamic_fee": normalize_price(reservation.get("dynamic_fee", 0.0)),
        }
        
        return invoice_line
    
    def generate_invoice_lines(self, reservation: Dict[str, Any], sef_forms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate invoice lines for a reservation.
        
        Args:
            reservation: Reservation dictionary
            sef_forms: List of SEF form dictionaries for this reservation
            
        Returns:
            List of invoice line dictionaries
        """
        origin = str(reservation.get("origin", "")).lower().strip()
        total_price = normalize_price(reservation.get("total_price", 0.0))
        
        # Match primary guest
        primary_guest = self._match_primary_guest(reservation, sef_forms)
        
        # Calculate tourist tax and get tt_guests_count
        tourist_tax, tt_guests_count = self._calculate_tourist_tax(reservation, sef_forms)
        
        # Get total guests count from reservation
        guests_count = int(float(reservation.get("guests_count", 0)))
        
        invoice_lines = []
        
        if origin == "airbnb":
            # Airbnb: 1 line (full amount + tourist tax)
            invoice_lines.append(
                self._build_invoice_line(
                    reservation,
                    primary_guest,
                    "airbnb",
                    total_price,
                    tourist_tax,
                    tt_guests_count,
                    guests_count
                )
            )
        else:
            # Website: 3 lines
            # Row 1: Pre-invoice (30% of total)
            pre_invoice_amount = round(total_price * 0.30, 2)
            invoice_lines.append(
                self._build_invoice_line(
                    reservation,
                    primary_guest,
                    "pre_invoice",
                    pre_invoice_amount,
                    0.0,  # No tax on pre-invoice
                    0,  # No tt_guests_count on pre-invoice
                    guests_count  # Still show total guests
                )
            )
            
            # Row 2: Credit note (-30% of total)
            credit_amount = round(total_price * 0.30, 2)
            invoice_lines.append(
                self._build_invoice_line(
                    reservation,
                    primary_guest,
                    "credit_note",
                    -credit_amount,  # Negative for credit
                    0.0,  # No tax on credit
                    0,  # No tt_guests_count on credit
                    guests_count  # Still show total guests
                )
            )
            
            # Row 3: Final invoice (100% of total + tourist tax)
            invoice_lines.append(
                self._build_invoice_line(
                    reservation,
                    primary_guest,
                    "final_invoice",
                    total_price,
                    tourist_tax,
                    tt_guests_count,
                    guests_count
                )
            )
        
        return invoice_lines
    
    def _print_debug_preview(self, preview_data: List[Dict[str, Any]]) -> None:
        """
        Print formatted debug preview of all invoice lines that would be written.
        
        Args:
            preview_data: List of dictionaries containing reservation and invoice data
        """
        print("\n" + "=" * 74)
        print("DEBUG MODE: INVOICE PREVIEW")
        print("=" * 74)
        print()
        
        if not preview_data:
            print("No invoice lines would be generated.")
            print()
            return
        
        for preview in preview_data:
            reservation = preview["reservation"]
            reservation_id = preview["reservation_id"]
            external_id = preview["external_id"]
            primary_guest = preview["primary_guest"]
            invoice_lines = preview["invoice_lines"]
            origin = preview["origin"]
            expected_lines = preview["expected_lines"]
            
            # Extract guest info from invoice_line (already has SEF name if available)
            first_line = invoice_lines[0] if invoice_lines else {}
            
            # Get guest info from invoice_line (uses SEF when available)
            guest_name = str(first_line.get("guest_name", "")).strip()
            passport = str(first_line.get("passport_number", "")).strip()
            country = str(first_line.get("country", "")).strip()
            
            # Get matched SEF guest name for display
            matched_sef_name = ""
            if primary_guest:
                matched_sef_name = str(primary_guest.get("Full Name", "")).strip()
            
            # Get values from reservation and invoice lines
            nights = first_line.get("nights", 0)
            # For accommodation value, use total_price from reservation (not first line amount)
            total_price = normalize_price(reservation.get("total_price", 0.0))
            lodging_amount = total_price  # Total accommodation value
            tourist_tax = preview.get("tourist_tax", 0.0)
            tt_guests_count = preview.get("tt_guests_count", 0)
            guests_count = preview.get("guests_count", 0)
            
            # Calculate fees
            airbnb_fee = normalize_price(reservation.get("airbnb_fee", 0.0))
            lodgify_fee = normalize_price(reservation.get("lodgify_fee", 0.0))
            stripe_fee = normalize_price(reservation.get("stripe_fee", 0.0))
            dynamic_fee = normalize_price(reservation.get("dynamic_fee", 0.0))
            total_fees = normalize_price(reservation.get("total_fees", 0.0))
            
            # Get descriptions
            invoice_description = first_line.get("description_lodging", "")
            tourist_tax_description = first_line.get("description_tourist_tax", "")
            
            # Calculate total from all lines
            invoice_total = sum(normalize_price(line.get("total_amount", 0.0)) for line in invoice_lines)
            
            # Format values
            origin_display = origin.capitalize() if origin else "Unknown"
            
            print("-" * 74)
            print(f"Reservation: {reservation_id} ({origin_display})")
            print(f"External ID: {external_id}")
            print(f"Guest Name: {guest_name if guest_name else 'N/A'}")
            if primary_guest:
                print(f"Matched SEF Guest: {matched_sef_name}")
            else:
                print("Matched SEF Guest: Not found")
            print(f"Passport: {passport if passport else 'N/A'}")
            print(f"Country: {country if country else 'N/A'}")
            # Get guests_count and tt_guests_count from first invoice line
            first_line_guests = first_line.get("guests_count", 0)
            first_line_tt_guests = first_line.get("tt_guests_count", 0)
            check_in_date = str(reservation.get("check_in", "")).strip()
            
            print(f"guests_count: {first_line_guests}")
            print(f"tt_guests_count: {first_line_tt_guests}")
            print(f"check_in date: {check_in_date}")
            print(f"Nights: {nights}")
            print(f"Tourist Tax: {tourist_tax:.2f} €")
            print(f"Accommodation Value: {lodging_amount:,.2f} €")
            print(f"Airbnb Fee: {airbnb_fee:.2f}")
            print(f"Lodgify Fee: {lodgify_fee:.2f}")
            print(f"Stripe Fee: {stripe_fee:.2f}")
            print(f"Dynamic Fee: {dynamic_fee:.2f}")
            print(f"Total Fees: {total_fees:.2f}")
            print(f"Invoice Description: {invoice_description}")
            if tourist_tax_description:
                print(f"Tourist Tax Description: {tourist_tax_description}")
            print(f"Invoice Total: {invoice_total:,.2f} €")
            print(f"Invoice Status: Predicted")
            print(f"Lines generated: {expected_lines} ({origin} booking)")
            print("-" * 74)
            print()
        
        print("=" * 74)
        print(f"Total: {len(preview_data)} reservation(s) would generate invoice lines")
        print("=" * 74)
        print()
    
    def write_invoice_lines_batch(
        self,
        reservation_id: str,
        rows: List[List[Any]]
    ) -> bool:
        """
        Write multiple invoice rows in a single batch request to Google Sheets.
        
        This method performs ONE API call for all rows, eliminating rate limit issues.
        Includes automatic retry logic for 429 errors (quota exceeded).
        
        Args:
            reservation_id: Reservation ID for logging purposes
            rows: List of lists, where each inner list represents one row (23 columns)
            
        Returns:
            True if write succeeded, False otherwise
        """
        if not rows:
            logger.warning(f"No rows to write for reservation {reservation_id}")
            return False
        
        try:
            # Get the sheets service
            sheets_service = self.sheets.sheets_service
            
            # Prepare batch append request
            range_name = f"{self.INVOICE_SHEET_NAME}!A:A"  # Append to column A to auto-detect end
            body = {
                'values': rows
            }
            
            # Attempt batch write with retry logic
            max_retries = 1
            retry_count = 0
            
            while retry_count <= max_retries:
                try:
                    sheets_service.spreadsheets().values().append(
                        spreadsheetId=self.INVOICE_SHEET_ID,
                        range=range_name,
                        valueInputOption='USER_ENTERED',
                        insertDataOption='INSERT_ROWS',
                        body=body
                    ).execute()
                    
                    logger.debug(f"Successfully wrote {len(rows)} rows in batch for reservation {reservation_id}")
                    return True
                    
                except HttpError as e:
                    # Check if it's a 429 rate limit error
                    if e.resp.status == 429:
                        if retry_count < max_retries:
                            retry_count += 1
                            wait_time = 10  # Wait 10 seconds before retry
                            logger.warning(
                                f"Rate limit (429) exceeded for reservation {reservation_id}. "
                                f"Waiting {wait_time} seconds before retry {retry_count}/{max_retries}..."
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(
                                f"Rate limit (429) exceeded for reservation {reservation_id} "
                                f"even after {max_retries} retry(ies). Skipping..."
                            )
                            return False
                    else:
                        # Other HTTP errors - don't retry
                        logger.error(
                            f"HTTP error writing batch rows for reservation {reservation_id}: "
                            f"{e.resp.status} - {str(e)}"
                        )
                        return False
                except Exception as e:
                    # Non-HTTP errors - don't retry
                    logger.error(
                        f"Unexpected error writing batch rows for reservation {reservation_id}: "
                        f"{str(e)}"
                    )
                    return False
            
            return False
            
        except Exception as e:
            logger.error(
                f"Error in batch write for reservation {reservation_id}: {str(e)}",
                exc_info=True
            )
            return False
    
    def write_invoice_lines(
        self,
        reservation: Dict[str, Any],
        invoice_lines: List[Dict[str, Any]],
        debug: bool = False
    ) -> bool:
        """
        Write invoice lines to Google Sheets Invoices_Lettia tab.
        
        Maps invoice line data to the exact column structure required by the sheet
        and appends rows in batch mode.
        
        Args:
            reservation: Reservation dictionary
            invoice_lines: List of invoice line dictionaries to write
            debug: If True, do not write anything (just return True)
            
        Returns:
            True if write succeeded (or debug mode), False otherwise
        """
        if debug:
            logger.debug(f"[DEBUG MODE] Would write {len(invoice_lines)} invoice lines for reservation {reservation.get('reservation_id')}")
            return True
        
        if not invoice_lines:
            logger.warning(f"No invoice lines to write for reservation {reservation.get('reservation_id')}")
            return False
        
        try:
            # Get origin and external_id
            origin = str(reservation.get("origin", "")).lower().strip()
            reservation_id = str(reservation.get("reservation_id", "")).strip()
            
            if origin == "airbnb":
                external_id = str(reservation.get("airbnb_id", reservation_id)).strip()
            else:
                external_id = str(reservation.get("lodgify_id", reservation_id)).strip()
            
            # Prepare rows for batch write
            rows_to_write = []
            
            for invoice_line in invoice_lines:
                # Map invoice_line to column structure
                row_values = []
                
                for col in self.INVOICE_COLUMNS:
                    value = ""
                    
                    if col == "reservation_id":
                        value = reservation_id
                    elif col == "external_id":
                        value = external_id
                    elif col == "guest_name":
                        # Use guest_name from invoice_line (already has SEF name if available)
                        value = str(invoice_line.get("guest_name", "")).strip()
                    elif col == "passport_number":
                        value = str(invoice_line.get("passport_number", "")).strip()
                    elif col == "country":
                        value = str(invoice_line.get("country", "")).strip()
                    elif col == "check_in":
                        value = str(reservation.get("check_in", "")).strip()
                    elif col == "check_out":
                        value = str(reservation.get("check_out", "")).strip()
                    elif col == "nights":
                        nights = invoice_line.get("nights", 0)
                        value = int(float(nights)) if nights else 0
                    elif col == "guests_count":
                        # Total guests in reservation
                        value = invoice_line.get("guests_count", 0)
                        value = int(float(value)) if value else 0
                    elif col == "tt_guests_count":
                        # Number of guests paying tourist tax
                        value = invoice_line.get("tt_guests_count", 0)
                        value = int(float(value)) if value else 0
                    elif col == "accommodation_value":
                        value = normalize_price(invoice_line.get("lodging_amount", 0.0))
                    elif col == "tourist_tax":
                        value = normalize_price(invoice_line.get("tourist_tax", 0.0))
                    elif col == "invoice_description":
                        value = str(invoice_line.get("description_lodging", "")).strip()
                    elif col == "tourist_tax_description":
                        value = str(invoice_line.get("description_tourist_tax", "")).strip()
                    elif col == "airbnb_fee":
                        value = normalize_price(invoice_line.get("airbnb_fee", 0.0))
                    elif col == "lodgify_fee":
                        value = normalize_price(invoice_line.get("lodgify_fee", 0.0))
                    elif col == "stripe_fee":
                        value = normalize_price(invoice_line.get("stripe_fee", 0.0))
                    elif col == "dynamic_fee":
                        value = normalize_price(invoice_line.get("dynamic_fee", 0.0))
                    elif col == "total_fees":
                        # Calculate total fees from individual fees
                        airbnb_fee = normalize_price(invoice_line.get("airbnb_fee", 0.0))
                        lodgify_fee = normalize_price(invoice_line.get("lodgify_fee", 0.0))
                        stripe_fee = normalize_price(invoice_line.get("stripe_fee", 0.0))
                        dynamic_fee = normalize_price(invoice_line.get("dynamic_fee", 0.0))
                        value = airbnb_fee + lodgify_fee + stripe_fee + dynamic_fee
                    elif col == "invoice_total":
                        value = normalize_price(invoice_line.get("total_amount", 0.0))
                    elif col == "invoice_status":
                        # Status is determined by invoice_number + invoice_date
                        # Since we're writing new rows, always start with "Forecast"
                        value = "Forecast"
                    elif col == "invoice_number":
                        # Empty for new rows (user will fill manually)
                        value = ""
                    elif col == "invoice_date":
                        # Empty for new rows (user will fill manually)
                        value = ""
                    
                    # Convert None to empty string
                    if value is None:
                        value = ""
                    
                    row_values.append(value)
                
                rows_to_write.append(row_values)
            
            # Write all rows in a single batch request
            success = self.write_invoice_lines_batch(reservation_id, rows_to_write)
            
            if success:
                logger.info(f"Successfully wrote {len(rows_to_write)} invoice lines for reservation {reservation_id}")
                return True
            else:
                logger.error(f"Failed to write {len(rows_to_write)} invoice lines for reservation {reservation_id}")
                return False
            
        except Exception as e:
            logger.error(f"Error writing invoice lines for reservation {reservation.get('reservation_id')}: {str(e)}", exc_info=True)
            return False

    def generate_all_invoices(self, debug: bool = False) -> bool:
        """
        Generate all invoice lines from reservations and SEF data.
        
        This method:
        - Loads reservations from Google Sheets
        - Loads SEF forms from Google Sheets
        - Matches reservations with SEF forms
        - Generates invoice lines for each reservation
        - Writes to Invoices_Lettia sheet (only new reservations)
        
        Args:
            debug: If True, run in dry-run mode (no writes, preview only)
        
        Returns:
            True if generation succeeded, False otherwise
        """
        try:
            if not debug:
                logger.info("Starting invoice generation...")
            
            # Use invoice sheet ID
            invoice_sheet_id = self.INVOICE_SHEET_ID
            
            # Load reservations
            logger.info("Loading reservations from Google Sheets...")
            reservations = self.sheets.get_reservations_data()
            logger.info(f"Loaded {len(reservations)} reservations")
            
            # Sort reservations by reservation_date (chronological order)
            def get_reservation_date(res):
                """Helper to parse reservation_date for sorting."""
                date_str = str(res.get("reservation_date", "")).strip()
                if not date_str:
                    return None
                try:
                    return self._parse_reservation_date(date_str)
                except:
                    return None
            
            # Filter out reservations without dates and sort the rest
            reservations_with_dates = []
            reservations_without_dates = []
            
            for res in reservations:
                res_date = get_reservation_date(res)
                if res_date:
                    reservations_with_dates.append((res_date, res))
                else:
                    reservations_without_dates.append(res)
            
            # Sort by reservation_date (earliest first)
            reservations_with_dates.sort(key=lambda x: x[0])
            
            # Reconstruct reservations list: sorted by date first, then those without dates
            reservations = [res for _, res in reservations_with_dates] + reservations_without_dates
            logger.info(f"Sorted {len(reservations_with_dates)} reservations by reservation_date, {len(reservations_without_dates)} without dates appended at end")
            
            # Load SEF forms
            logger.info("Loading SEF forms from Google Sheets...")
            sef_forms = self.sheets.get_sef_data()
            logger.info(f"Loaded {len(sef_forms)} SEF forms")
            
            # Load existing invoices to check what's already generated
            logger.info("Loading existing invoices...")
            try:
                existing_invoices = self.sheets.get_invoices_data(invoice_sheet_id)
                # Count lines per reservation_id
                reservation_line_counts = {}
                for invoice in existing_invoices:
                    res_id = str(invoice.get("reservation_id", "")).strip()
                    if res_id:
                        reservation_line_counts[res_id] = reservation_line_counts.get(res_id, 0) + 1
                logger.info(f"Found {len(reservation_line_counts)} reservations with existing invoices")
            except Exception as e:
                logger.warning(f"Could not load existing invoices: {str(e)}. Proceeding...")
                existing_invoices = []
                reservation_line_counts = {}
            
            # Get headers for Invoices_Lettia sheet
            logger.info("Loading Invoices_Lettia headers...")
            try:
                invoice_headers = self.sheets.get_headers("Invoices_Lettia", sheet_id=invoice_sheet_id)
                logger.info(f"Loaded {len(invoice_headers)} headers from Invoices_Lettia")
            except Exception as e:
                logger.error(f"Could not load Invoices_Lettia headers: {str(e)}")
                return False
            
            # Group SEF forms by reservation (using check-in/check-out dates as proxy)
            # We'll match them on-the-fly per reservation
            
            # Process each reservation
            generated_count = 0
            skipped_count = 0
            error_count = 0
            preview_data = []  # Collect preview data for debug mode
            
            for reservation in reservations:
                reservation_id = str(reservation.get("reservation_id", "")).strip()
                if not reservation_id:
                    continue
                
                try:
                    # Check if already in state (skip state check in debug mode)
                    if not debug:
                        if self.invoice_state.invoice_already_imported(reservation_id):
                            logger.debug(f"Skipping reservation {reservation_id} - already in invoice state")
                            skipped_count += 1
                            continue
                    
                    # Check if already has invoice lines in sheet
                    # Determine expected lines count based on origin
                    origin = str(reservation.get("origin", "")).lower().strip()
                    expected_lines = 1 if origin == "airbnb" else 3
                    
                    # Check if we already have the expected number of lines
                    existing_lines_count = reservation_line_counts.get(reservation_id, 0)
                    if existing_lines_count >= expected_lines:
                        logger.debug(f"Skipping reservation {reservation_id} - already has {existing_lines_count} invoice lines (expected {expected_lines})")
                        # Mark in state even if we didn't generate (only if not debug)
                        if not debug:
                            self.invoice_state.mark_invoice_imported(reservation_id)
                        skipped_count += 1
                        continue
                    
                    # Find matching SEF forms for this reservation
                    reservation_check_in = self._parse_reservation_date(reservation.get("check_in"))
                    reservation_check_out = self._parse_reservation_date(reservation.get("check_out"))
                    
                    matching_sef_forms = []
                    for sef in sef_forms:
                        sef_check_in = self.parse_sef_date(sef.get("Check-in date"))
                        sef_check_out = self.parse_sef_date(sef.get("Check-out date"))
                        
                        # Compare parsed date objects
                        if sef_check_in and sef_check_out and reservation_check_in and reservation_check_out:
                            if sef_check_in == reservation_check_in and sef_check_out == reservation_check_out:
                                matching_sef_forms.append(sef)
                    
                    # Print diagnostic information in debug mode
                    if debug:
                        self._debug_matching_diagnostics(reservation, sef_forms)
                    
                    # Match primary guest for preview
                    # Use all sef_forms to find best match (matching_sef_forms may be empty)
                    primary_guest = self._match_primary_guest(reservation, sef_forms)
                    
                    # Generate invoice lines for this reservation
                    invoice_lines = self.generate_invoice_lines(reservation, matching_sef_forms)
                    
                    if not invoice_lines:
                        logger.warning(f"No invoice lines generated for reservation {reservation_id}")
                        error_count += 1
                        continue
                    
                    # Collect preview data in debug mode, or write to sheets in normal mode
                    if debug:
                        # Calculate tourist tax and get tt_guests_count for preview
                        tourist_tax, tt_guests_count = self._calculate_tourist_tax(reservation, matching_sef_forms)
                        # Get guests_count from reservation
                        guests_count = int(float(reservation.get("guests_count", 0)))
                        
                        # Get external ID
                        external_id = reservation_id
                        if origin == "airbnb":
                            external_id = str(reservation.get("airbnb_id", reservation_id)).strip()
                        else:
                            external_id = str(reservation.get("lodgify_id", reservation_id)).strip()
                        
                        preview_data.append({
                            "reservation": reservation,
                            "reservation_id": reservation_id,
                            "external_id": external_id,
                            "primary_guest": primary_guest,
                            "invoice_lines": invoice_lines,
                            "tourist_tax": tourist_tax,
                            "tt_guests_count": tt_guests_count,
                            "guests_count": guests_count,
                            "origin": origin,
                            "expected_lines": expected_lines
                        })
                    else:
                        # Write invoice lines to sheet (only if not debug)
                        success = self.write_invoice_lines(reservation, invoice_lines, debug=False)
                        if success:
                            # Mark as imported in state after successful write
                            self.invoice_state.mark_invoice_imported(reservation_id)
                            generated_count += 1
                            logger.info(f"Successfully processed reservation {reservation_id}")
                        else:
                            error_count += 1
                            logger.error(f"Failed to write invoice lines for reservation {reservation_id}")
                            continue
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error generating invoices for reservation {reservation_id}: {str(e)}", exc_info=True)
                    continue
            
            # In debug mode, print preview
            if debug:
                self._print_debug_preview(preview_data)
            
            # Log summary
            logger.info("=" * 70)
            if debug:
                logger.info("Invoice generation DEBUG/DRY-RUN completed (no writes performed)")
            else:
                logger.info("Invoice generation completed")
            logger.info("=" * 70)
            logger.info(f"Total reservations processed: {len(reservations)}")
            logger.info(f"  - Generated invoices: {generated_count}")
            logger.info(f"  - Skipped (already exist): {skipped_count}")
            logger.info(f"  - Errors: {error_count}")
            logger.info("=" * 70)
            
            return generated_count > 0 or len(reservations) == 0
            
        except Exception as e:
            logger.error(f"Failed to generate invoices: {str(e)}", exc_info=True)
            return False

