"""
Lodgify API Service.
Complete integration with Lodgify API v2 for reservations, guests, units, pricing, and availability.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlencode

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError as RequestsConnectionError

from config.settings import get_lodgify_api_key, get_lodgify_property_id

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class LodgifyAPIError(Exception):
    """Custom exception for Lodgify API errors."""
    pass


class LodgifyService:
    """
    Complete Lodgify API v2 service implementation.
    
    Supports all major API endpoints:
    - Reservations (list, get by ID, date filtering)
    - Guests (get by reservation)
    - Units (list, get by ID)
    - Rates/Pricing (get by unit and date range)
    - Availability (get by unit and date range)
    
    Features:
    - Retry logic with exponential backoff
    - Comprehensive error handling
    - Request/response logging
    - Data normalization helpers
    - Type hints and docstrings
    """
    
    def __init__(self, api_key: Optional[str] = None, property_id: Optional[str] = None):
        """
        Initialize LodgifyService with API credentials.
        
        Args:
            api_key: Optional API key (defaults to LODGIFY_API_KEY from config)
            property_id: Optional property ID (defaults to LODGIFY_PROPERTY_ID from config)
            
        Raises:
            ValueError: If API key or property ID is missing
        """
        self.api_key = api_key or get_lodgify_api_key()
        self.property_id = property_id or get_lodgify_property_id()
        self.base_url = "https://api.lodgify.com/v2"
        
        if not self.api_key:
            raise ValueError("LODGIFY_API_KEY is required but not provided")
        if not self.property_id:
            raise ValueError("LODGIFY_PROPERTY_ID is required but not provided")
        
        self.headers = {
            "X-ApiKey": self.api_key,  # Lodgify standard header
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Request settings
        self.timeout = 30  # seconds
        self.max_retries = 3
        self.retry_delays = [1, 2, 4]  # Exponential backoff delays in seconds
        
        logger.info(f"LodgifyService initialized for property ID: {self.property_id}")
    
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """
        Private method to handle HTTP requests with retry logic and error handling.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path (without base URL)
            params: Optional query parameters
            data: Optional request body data (for form data)
            json: Optional JSON body data (for JSON payloads)
            retry_count: Current retry attempt number
            
        Returns:
            Parsed JSON response as dictionary
            
        Raises:
            LodgifyAPIError: For API errors or unexpected status codes
            Timeout: For timeout errors after all retries
            RequestsConnectionError: For connection errors after all retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Build query string if params provided
        if params:
            url = f"{url}?{urlencode(params)}"
        
        logger.info(f"[Lodgify API] {method} {url}")
        if params:
            logger.debug(f"[Lodgify API] Query params: {params}")
        
        # Use json parameter if provided, otherwise fall back to data
        json_payload = json if json is not None else data
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=json_payload,
                timeout=self.timeout
            )
            
            logger.info(f"[Lodgify API] Response status: {response.status_code}")
            
            # Handle successful responses
            if response.status_code == 200:
                try:
                    result = response.json()
                    logger.debug(f"[Lodgify API] Response received: {len(str(result))} chars")
                    return result
                except ValueError as e:
                    logger.error(f"[Lodgify API] Failed to parse JSON response: {e}")
                    raise LodgifyAPIError(f"Invalid JSON response from Lodgify API: {e}")
            
            # Handle 204 No Content
            elif response.status_code == 204:
                return {}
            
            # Handle 4xx client errors (don't retry)
            # Note: 404 errors for reservations/search are handled in get_reservations()
            elif 400 <= response.status_code < 500:
                error_msg = f"Client error {response.status_code}"
                try:
                    error_detail = response.json()
                    error_msg = f"{error_msg}: {error_detail}"
                except ValueError:
                    error_msg = f"{error_msg}: {response.text[:200]}"
                
                logger.error(f"[Lodgify API] {error_msg}")
                raise LodgifyAPIError(error_msg)
            
            # Handle 5xx server errors (retry)
            elif 500 <= response.status_code < 600:
                if retry_count < self.max_retries:
                    delay = self.retry_delays[retry_count] if retry_count < len(self.retry_delays) else 8
                    logger.warning(
                        f"[Lodgify API] Server error {response.status_code}. "
                        f"Retrying in {delay}s (attempt {retry_count + 1}/{self.max_retries})"
                    )
                    time.sleep(delay)
                    return self._request(method, endpoint, params, data, json, retry_count + 1)
                else:
                    error_msg = f"Server error {response.status_code} after {self.max_retries} retries"
                    logger.error(f"[Lodgify API] {error_msg}")
                    raise LodgifyAPIError(error_msg)
            
            # Unexpected status code
            else:
                error_msg = f"Unexpected status code {response.status_code}"
                logger.error(f"[Lodgify API] {error_msg}")
                raise LodgifyAPIError(error_msg)
        
        except Timeout as e:
            if retry_count < self.max_retries:
                delay = self.retry_delays[retry_count] if retry_count < len(self.retry_delays) else 8
                logger.warning(
                    f"[Lodgify API] Timeout error. Retrying in {delay}s "
                    f"(attempt {retry_count + 1}/{self.max_retries})"
                )
                time.sleep(delay)
                return self._request(method, endpoint, params, data, json, retry_count + 1)
            else:
                logger.error(f"[Lodgify API] Timeout after {self.max_retries} retries")
                raise
        
        except RequestsConnectionError as e:
            if retry_count < self.max_retries:
                delay = self.retry_delays[retry_count] if retry_count < len(self.retry_delays) else 8
                logger.warning(
                    f"[Lodgify API] Connection error. Retrying in {delay}s "
                    f"(attempt {retry_count + 1}/{self.max_retries})"
                )
                time.sleep(delay)
                return self._request(method, endpoint, params, data, json, retry_count + 1)
            else:
                logger.error(f"[Lodgify API] Connection error after {self.max_retries} retries: {e}")
                raise
        
        except RequestException as e:
            logger.error(f"[Lodgify API] Request error: {e}")
            raise LodgifyAPIError(f"Request failed: {e}") from e
    
    def get_reservations(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get reservations with optional date filtering.
        
        Uses the Lodgify API endpoint: POST /v2/reservations/search
        
        Args:
            start_date: Optional start date filter (ISO 8601 format: YYYY-MM-DD)
            end_date: Optional end date filter (ISO 8601 format: YYYY-MM-DD)
            
        Returns:
            List of reservation dictionaries from result["data"]
            
        Example:
            >>> service = LodgifyService()
            >>> reservations = service.get_reservations(start_date="2025-12-01", end_date="2025-12-31")
        """
        # Build JSON payload (not query parameters)
        payload: Dict[str, Any] = {
            "propertyId": self.property_id
        }
        
        # Add date filters to payload if provided
        if start_date:
            payload["from"] = self._normalize_date_to_iso(start_date)
        if end_date:
            payload["to"] = self._normalize_date_to_iso(end_date)
        
        try:
            # Use POST request with JSON body
            result = self._request(
                method="POST",
                endpoint="reservations/search",
                json=payload
            )
            
            # Lodgify returns reservations inside result["data"]
            if isinstance(result, dict) and "data" in result:
                reservations = result.get("data", [])
                if isinstance(reservations, list):
                    logger.info(f"[Lodgify API] Retrieved {len(reservations)} reservations from search")
                    return reservations
                else:
                    logger.warning(f"[Lodgify API] Expected 'data' to be a list, got {type(reservations)}")
                    return []
            elif isinstance(result, list):
                # Fallback: if result is directly a list, return it
                logger.info(f"[Lodgify API] Retrieved {len(result)} reservations (direct list format)")
                return result
            else:
                logger.warning(f"[Lodgify API] Unexpected reservation search response format: {type(result)}")
                return []
        
        except LodgifyAPIError as e:
            # Check if it's a 404 error - handle gracefully for search endpoint
            error_str = str(e)
            if "404" in error_str or "not found" in error_str.lower():
                logger.info("[Lodgify API] Reservations search returned 404 (no reservations found or endpoint not available)")
                return []
            else:
                # Re-raise other API errors
                logger.error(f"[Lodgify API] Error searching reservations: {error_str}")
                raise
    
    def get_reservation_by_id(self, reservation_id: Union[str, int]) -> Dict[str, Any]:
        """
        Get a specific reservation by ID.
        
        Args:
            reservation_id: Reservation ID
            
        Returns:
            Reservation dictionary with full details
            
        Raises:
            LodgifyAPIError: If reservation not found or API error occurs
            
        Example:
            >>> service = LodgifyService()
            >>> reservation = service.get_reservation_by_id("12345")
        """
        result = self._request("GET", f"reservations/{reservation_id}")
        
        if isinstance(result, dict):
            return result
        elif isinstance(result, list) and len(result) > 0:
            return result[0]
        else:
            raise LodgifyAPIError(f"Invalid response format for reservation {reservation_id}")
    
    def get_units(self) -> List[Dict[str, Any]]:
        """
        Get all units (properties) for the configured property ID.
        
        Returns:
            List of unit dictionaries
            
        Example:
            >>> service = LodgifyService()
            >>> units = service.get_units()
        """
        params: Dict[str, Any] = {}
        if self.property_id:
            params["propertyId"] = self.property_id
        
        result = self._request("GET", "properties", params=params)
        
        # Handle different response formats
        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            if "data" in result:
                return result["data"]
            elif "items" in result:
                return result["items"]
            elif "properties" in result:
                return result["properties"]
            else:
                logger.warning(f"[Lodgify API] Unexpected units response format: {list(result.keys())}")
                return []
        else:
            logger.warning(f"[Lodgify API] Unexpected units response type: {type(result)}")
            return []
    
    def get_unit(self, unit_id: Union[str, int]) -> Dict[str, Any]:
        """
        Get a specific unit (property) by ID.
        
        Args:
            unit_id: Unit/Property ID
            
        Returns:
            Unit dictionary with full details
            
        Raises:
            LodgifyAPIError: If unit not found or API error occurs
            
        Example:
            >>> service = LodgifyService()
            >>> unit = service.get_unit("12345")
        """
        result = self._request("GET", f"properties/{unit_id}")
        
        if isinstance(result, dict):
            return result
        elif isinstance(result, list) and len(result) > 0:
            return result[0]
        else:
            raise LodgifyAPIError(f"Invalid response format for unit {unit_id}")
    
    def get_guests(self, reservation_id: Union[str, int]) -> List[Dict[str, Any]]:
        """
        Get guests associated with a specific reservation.
        
        Args:
            reservation_id: Reservation ID
            
        Returns:
            List of guest dictionaries
            
        Example:
            >>> service = LodgifyService()
            >>> guests = service.get_guests("12345")
        """
        # Try to get guests from reservation endpoint first
        try:
            reservation = self.get_reservation_by_id(reservation_id)
            
            # Extract guests from reservation data
            guests = []
            if isinstance(reservation, dict):
                # Common guest field names in Lodgify
                if "guests" in reservation:
                    guests = reservation["guests"] if isinstance(reservation["guests"], list) else [reservation["guests"]]
                elif "guest" in reservation:
                    guests = [reservation["guest"]] if isinstance(reservation["guest"], dict) else reservation["guest"]
                elif "contact" in reservation:
                    guests = [reservation["contact"]]
            
            if guests:
                return guests
        except Exception as e:
            logger.warning(f"[Lodgify API] Could not extract guests from reservation: {e}")
        
        # Fallback: try dedicated guests endpoint
        try:
            params = {"reservationId": str(reservation_id)}
            result = self._request("GET", "guests", params=params)
            
            if isinstance(result, list):
                return result
            elif isinstance(result, dict):
                if "data" in result:
                    return result["data"]
                elif "items" in result:
                    return result["items"]
                elif "guests" in result:
                    return result["guests"]
        except Exception as e:
            logger.warning(f"[Lodgify API] Could not fetch guests via guests endpoint: {e}")
        
        # Return empty list if all methods fail
        logger.warning(f"[Lodgify API] No guests found for reservation {reservation_id}")
        return []
    
    def get_rates(
        self,
        unit_id: Union[str, int],
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Get pricing/rates for a unit within a date range.
        
        Args:
            unit_id: Unit/Property ID
            start_date: Start date (ISO 8601 format: YYYY-MM-DD)
            end_date: End date (ISO 8601 format: YYYY-MM-DD)
            
        Returns:
            List of rate dictionaries
            
        Example:
            >>> service = LodgifyService()
            >>> rates = service.get_rates("12345", "2025-12-01", "2025-12-31")
        """
        params = {
            "startDate": self._normalize_date_to_iso(start_date),
            "endDate": self._normalize_date_to_iso(end_date)
        }
        
        result = self._request("GET", f"properties/{unit_id}/rates", params=params)
        
        # Handle different response formats
        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            if "data" in result:
                return result["data"]
            elif "items" in result:
                return result["items"]
            elif "rates" in result:
                return result["rates"]
            else:
                logger.warning(f"[Lodgify API] Unexpected rates response format: {list(result.keys())}")
                return []
        else:
            logger.warning(f"[Lodgify API] Unexpected rates response type: {type(result)}")
            return []
    
    def get_availability(
        self,
        unit_id: Union[str, int],
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Get availability for a unit within a date range.
        
        Args:
            unit_id: Unit/Property ID
            start_date: Start date (ISO 8601 format: YYYY-MM-DD)
            end_date: End date (ISO 8601 format: YYYY-MM-DD)
            
        Returns:
            List of availability dictionaries
            
        Example:
            >>> service = LodgifyService()
            >>> availability = service.get_availability("12345", "2025-12-01", "2025-12-31")
        """
        params = {
            "startDate": self._normalize_date_to_iso(start_date),
            "endDate": self._normalize_date_to_iso(end_date)
        }
        
        result = self._request("GET", f"properties/{unit_id}/availability", params=params)
        
        # Handle different response formats
        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            if "data" in result:
                return result["data"]
            elif "items" in result:
                return result["items"]
            elif "availability" in result:
                return result["availability"]
            else:
                logger.warning(f"[Lodgify API] Unexpected availability response format: {list(result.keys())}")
                return []
        else:
            logger.warning(f"[Lodgify API] Unexpected availability response type: {type(result)}")
            return []
    
    def sync_reservations_to_sheet(self, sheet_service: Any) -> bool:
        """
        Placeholder method for syncing reservations to Google Sheets.
        
        This method will be implemented in the future to:
        - Fetch reservations from Lodgify
        - Transform data format
        - Update Google Sheets with reservation data
        - Handle conflicts and duplicates
        
        Args:
            sheet_service: Google Sheets service instance (to be implemented)
            
        Returns:
            True if sync succeeded, False otherwise
            
        Note:
            This is a placeholder method. Implementation will be added later.
        """
        logger.info("[Lodgify Service] sync_reservations_to_sheet() called (placeholder)")
        logger.warning("[Lodgify Service] sync_reservations_to_sheet() not yet implemented")
        return False
    
    # ==================== Data Normalization Helpers ====================
    
    def _normalize_date_to_iso(self, date_value: Union[str, datetime]) -> str:
        """
        Normalize date to ISO 8601 format (YYYY-MM-DD).
        
        Args:
            date_value: Date as string in various formats or datetime object
            
        Returns:
            ISO 8601 formatted date string (YYYY-MM-DD)
            
        Raises:
            ValueError: If date cannot be parsed
        """
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%d")
        
        if isinstance(date_value, str):
            date_str = date_value.strip()
            
            # Already in ISO format
            if len(date_str) == 10 and date_str.count("-") == 2:
                try:
                    datetime.strptime(date_str, "%Y-%m-%d")
                    return date_str
                except ValueError:
                    pass
            
            # Try other common formats
            formats = [
                "%Y/%m/%d",
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%m/%d/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            raise ValueError(f"Unable to parse date: {date_value}")
        
        raise ValueError(f"Invalid date type: {type(date_value)}")
    
    def normalize_reservation(self, reservation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a reservation dictionary to a standard structure.
        
        Flattens nested JSON and extracts key fields for easier use
        in Google Sheets and dashboards.
        
        Args:
            reservation: Raw reservation dictionary from API
            
        Returns:
            Normalized reservation dictionary with flattened structure
            
        Example:
            >>> service = LodgifyService()
            >>> raw_reservation = {...}  # From API
            >>> normalized = service.normalize_reservation(raw_reservation)
        """
        normalized: Dict[str, Any] = {
            "id": reservation.get("id", ""),
            "property_id": reservation.get("propertyId", reservation.get("property_id", "")),
            "unit_id": reservation.get("unitId", reservation.get("unit_id", "")),
            "check_in": reservation.get("checkIn", reservation.get("check_in", "")),
            "check_out": reservation.get("checkOut", reservation.get("check_out", "")),
            "status": reservation.get("status", ""),
            "total_price": reservation.get("totalPrice", reservation.get("total_price", 0)),
            "currency": reservation.get("currency", ""),
            "channel": reservation.get("channel", reservation.get("bookingChannel", "")),
            "created_at": reservation.get("createdAt", reservation.get("created_at", "")),
            "updated_at": reservation.get("updatedAt", reservation.get("updated_at", "")),
        }
        
        # Extract guest information safely
        guest_info = self.extract_guest_info(reservation)
        normalized.update(guest_info)
        
        # Normalize dates
        if normalized["check_in"]:
            try:
                normalized["check_in"] = self._normalize_date_to_iso(normalized["check_in"])
            except ValueError:
                pass
        
        if normalized["check_out"]:
            try:
                normalized["check_out"] = self._normalize_date_to_iso(normalized["check_out"])
            except ValueError:
                pass
        
        return normalized
    
    def extract_guest_info(self, reservation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Safely extract guest information from reservation data.
        
        Handles various nested structures that Lodgify may return.
        
        Args:
            reservation: Reservation dictionary from API
            
        Returns:
            Dictionary with guest fields (name, email, phone, etc.)
        """
        guest_info: Dict[str, Any] = {
            "guest_name": "",
            "guest_email": "",
            "guest_phone": "",
            "guest_count": 0,
        }
        
        # Try multiple possible locations for guest data
        guest_data = None
        
        if "guests" in reservation:
            guests_list = reservation["guests"]
            if isinstance(guests_list, list) and len(guests_list) > 0:
                guest_data = guests_list[0]
            elif isinstance(guests_list, dict):
                guest_data = guests_list
        
        if not guest_data and "guest" in reservation:
            guest_data = reservation["guest"]
        
        if not guest_data and "contact" in reservation:
            guest_data = reservation["contact"]
        
        if guest_data and isinstance(guest_data, dict):
            guest_info["guest_name"] = (
                guest_data.get("name") or
                guest_data.get("fullName") or
                f"{guest_data.get('firstName', '')} {guest_data.get('lastName', '')}".strip()
            )
            guest_info["guest_email"] = guest_data.get("email", "")
            guest_info["guest_phone"] = guest_data.get("phone", "")
            guest_info["guest_count"] = (
                guest_data.get("guestCount") or
                guest_data.get("guest_count") or
                reservation.get("guestCount", 0)
            )
        
        return guest_info
    
    def flatten_lodgify_json(self, data: Union[Dict, List], prefix: str = "") -> Dict[str, Any]:
        """
        Flatten nested Lodgify JSON structure for easier use in Sheets/dashboards.
        
        Args:
            data: Nested dictionary or list to flatten
            prefix: Optional prefix for flattened keys
            
        Returns:
            Flattened dictionary with dot-notation keys
            
        Example:
            >>> service = LodgifyService()
            >>> nested = {"guest": {"name": "John", "contact": {"email": "john@example.com"}}}
            >>> flattened = service.flatten_lodgify_json(nested)
            >>> # Result: {"guest.name": "John", "guest.contact.email": "john@example.com"}
        """
        flattened: Dict[str, Any] = {}
        
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}.{key}" if prefix else key
                
                if isinstance(value, dict):
                    flattened.update(self.flatten_lodgify_json(value, new_key))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            flattened.update(self.flatten_lodgify_json(item, f"{new_key}[{i}]"))
                        else:
                            flattened[f"{new_key}[{i}]"] = item
                else:
                    flattened[new_key] = value
        
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    flattened.update(self.flatten_lodgify_json(item, f"{prefix}[{i}]"))
                else:
                    flattened[f"{prefix}[{i}]"] = item
        
        return flattened


if __name__ == "__main__":
    """
    Minimal manual test block.
    
    Usage:
        python -m services.lodgify_service
    """
    try:
        service = LodgifyService()
        print("=" * 70)
        print("Lodgify Service - Manual Test")
        print("=" * 70)
        print()
        
        print("Testing get_reservations()...")
        reservations = service.get_reservations()
        print(f"✓ Retrieved {len(reservations)} reservations")
        print()
        
        if reservations:
            print("Sample reservation structure:")
            import json
            print(json.dumps(reservations[0], indent=2)[:500] + "...")
        else:
            print("No reservations found")
        
        print()
        print("=" * 70)
        print("Test completed!")
        print("=" * 70)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

