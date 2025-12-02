"""
Reservation Merger Service.
Handles idempotent merging of reservation data.
"""

import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger(__name__)


class ReservationMerger:
    """
    Service for merging reservation data with idempotent behavior.
    
    Ensures that:
    - New data overrides empty/null fields in existing data
    - Existing data is preserved when new data is empty
    - Financial fields are not touched (will be recalculated)
    - Column order is preserved
    """
    
    # Financial fields that should NOT be merged (will be recalculated)
    FINANCIAL_FIELDS = {
        "airbnb_fee",
        "lodgify_fee",
        "stripe_fee",
        "dynamic_fee",
        "total_fees",
        "vat_amount",
        "net_revenue",
        "payout_expected",
        "price_per_night",
        "price_per_guest_per_night",
    }
    
    # Fields that should always be overwritten from new data (never preserved)
    ALWAYS_OVERWRITE_FIELDS = {
        "country",  # Always extracted from phone, should always be updated
        "payout_expected",  # Always recalculated, should always be updated
    }
    
    def __init__(self):
        """Initialize reservation merger."""
        logger.debug("ReservationMerger initialized")
    
    def _is_empty(self, value: Any) -> bool:
        """
        Check if a value is considered empty.
        
        Args:
            value: Value to check
            
        Returns:
            True if value is empty, None, or whitespace-only string
        """
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, (int, float)):
            return value == 0
        return False
    
    def merge(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge new reservation data into existing data with idempotent behavior.
        
        Rules:
        - new always overrides empty or null fields in existing
        - existing keeps any fields that new leaves empty
        - Financial fields are NOT merged (will be recalculated later)
        - Always preserve the Google Sheets column order
        
        Args:
            existing: Existing reservation data from Google Sheets
            new: New reservation data to merge in
            
        Returns:
            Merged reservation dictionary ready for upsert
            
        Example:
            >>> merger = ReservationMerger()
            >>> existing = {"guest_name": "John", "guest_email": ""}
            >>> new = {"guest_name": "", "guest_email": "john@example.com"}
            >>> merged = merger.merge(existing, new)
            >>> # Result: {"guest_name": "John", "guest_email": "john@example.com"}
        """
        merged = {}
        
        # Get all unique keys from both dictionaries
        all_keys = set(existing.keys()) | set(new.keys())
        
        for key in all_keys:
            # Handle financial fields - always use new values (will be recalculated anyway)
            if key in self.FINANCIAL_FIELDS:
                new_value = new.get(key, None)
                
                # Always use new value if present, otherwise use existing (will be recalculated later)
                if new_value is not None and not (isinstance(new_value, str) and new_value.strip() == ""):
                    merged[key] = new_value
                else:
                    # Use existing value temporarily (will be recalculated by calculate_financials)
                    merged[key] = existing.get(key, 0.0) if existing.get(key) is not None else 0.0
                continue
            
            # Handle fields that should always be overwritten from new data
            if key in self.ALWAYS_OVERWRITE_FIELDS:
                new_value = new.get(key, None)
                # Always use new value, even if empty (country should always be updated from phone)
                merged[key] = new_value if new_value is not None else ""
                logger.debug(f"Merged {key}: always overwriting with new value '{new_value}'")
                continue
            
            existing_value = existing.get(key, None)
            new_value = new.get(key, None)
            
            # Rule: new overrides empty/null in existing
            if self._is_empty(existing_value) and not self._is_empty(new_value):
                merged[key] = new_value
                logger.debug(f"Merged {key}: using new value '{new_value}' (existing was empty)")
            
            # Rule: existing keeps fields that new leaves empty
            elif not self._is_empty(existing_value) and self._is_empty(new_value):
                merged[key] = existing_value
                logger.debug(f"Merged {key}: keeping existing value '{existing_value}' (new was empty)")
            
            # Rule: if both have values, new takes precedence
            elif not self._is_empty(existing_value) and not self._is_empty(new_value):
                merged[key] = new_value
                logger.debug(f"Merged {key}: using new value '{new_value}' (overriding existing)")
            
            # Both empty: use new (which is empty)
            else:
                merged[key] = new_value if new_value is not None else existing_value
        
        logger.debug(f"Merged reservation {merged.get('reservation_id', 'unknown')}")
        
        return merged

