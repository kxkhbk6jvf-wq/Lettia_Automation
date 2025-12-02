"""
State Manager Service.
Handles persistent state tracking for reservations synchronization.
"""

import json
import logging
from pathlib import Path
from typing import Set, List

# Configure logging
logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages persistent state for tracking imported reservations and notes.
    
    Stores state in a JSON file to enable idempotent synchronization:
    - Tracks which reservation IDs have been imported
    - Tracks which reservation IDs have received financial notes
    """
    
    def __init__(self, path: str = "state/reservations_state.json"):
        """
        Initialize StateManager.
        
        Args:
            path: Path to the state JSON file (relative to project root)
        """
        # Get project root (parent of services directory)
        project_root = Path(__file__).parent.parent
        self.state_path = project_root / path
        
        # Ensure state directory exists
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load or initialize state
        if self.state_path.exists():
            try:
                with open(self.state_path, 'r', encoding='utf-8') as f:
                    self.state = json.load(f)
                logger.info(f"Loaded state from {self.state_path}")
            except Exception as e:
                logger.warning(f"Failed to load state file: {str(e)}. Initializing empty state.")
                self.state = self._empty_state()
        else:
            self.state = self._empty_state()
            logger.info(f"Initialized new state file at {self.state_path}")
    
    def _empty_state(self) -> dict:
        """Return an empty state dictionary."""
        return {
            "imported_ids": [],
            "notes_filled": []
        }
    
    def save(self) -> None:
        """Save current state to disk."""
        try:
            with open(self.state_path, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
            logger.debug(f"Saved state to {self.state_path}")
        except Exception as e:
            logger.error(f"Failed to save state file: {str(e)}", exc_info=True)
            raise
    
    def mark_imported(self, reservation_id: str) -> None:
        """
        Mark a reservation ID as imported.
        
        Args:
            reservation_id: Reservation ID to mark as imported
        """
        reservation_id = str(reservation_id).strip()
        if not reservation_id:
            return
        
        imported_ids = set(self.state.get("imported_ids", []))
        if reservation_id not in imported_ids:
            imported_ids.add(reservation_id)
            self.state["imported_ids"] = sorted(list(imported_ids))
            self.save()
            logger.debug(f"Marked reservation {reservation_id} as imported")
    
    def mark_notes_filled(self, reservation_id: str) -> None:
        """
        Mark a reservation ID as having notes filled.
        
        Args:
            reservation_id: Reservation ID to mark as having notes filled
        """
        reservation_id = str(reservation_id).strip()
        if not reservation_id:
            return
        
        notes_filled = set(self.state.get("notes_filled", []))
        if reservation_id not in notes_filled:
            notes_filled.add(reservation_id)
            self.state["notes_filled"] = sorted(list(notes_filled))
            self.save()
            logger.debug(f"Marked reservation {reservation_id} as notes filled")
    
    def already_imported(self, reservation_id: str) -> bool:
        """
        Check if a reservation ID has already been imported.
        
        Args:
            reservation_id: Reservation ID to check
            
        Returns:
            True if already imported, False otherwise
        """
        reservation_id = str(reservation_id).strip()
        if not reservation_id:
            return False
        
        imported_ids = set(self.state.get("imported_ids", []))
        return reservation_id in imported_ids
    
    def notes_already_filled(self, reservation_id: str) -> bool:
        """
        Check if a reservation ID already has notes filled.
        
        Args:
            reservation_id: Reservation ID to check
            
        Returns:
            True if notes already filled, False otherwise
        """
        reservation_id = str(reservation_id).strip()
        if not reservation_id:
            return False
        
        notes_filled = set(self.state.get("notes_filled", []))
        return reservation_id in notes_filled


class InvoiceStateManager:
    """
    State manager specifically for invoice tracking.
    
    Uses separate state file for invoice-specific tracking.
    Tracks which reservations have had invoices generated.
    """
    
    def __init__(self, path: str = "state/invoices_state.json"):
        """
        Initialize InvoiceStateManager.
        
        Args:
            path: Path to the invoice state JSON file (relative to project root)
        """
        # Get project root (parent of services directory)
        project_root = Path(__file__).parent.parent
        self.state_path = project_root / path
        
        # Ensure state directory exists
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load or initialize state
        if self.state_path.exists():
            try:
                with open(self.state_path, 'r', encoding='utf-8') as f:
                    self.state = json.load(f)
                logger.info(f"Loaded invoice state from {self.state_path}")
            except Exception as e:
                logger.warning(f"Failed to load invoice state file: {str(e)}. Initializing empty state.")
                self.state = self._empty_state()
        else:
            self.state = self._empty_state()
            logger.info(f"Initialized new invoice state file at {self.state_path}")
    
    def _empty_state(self) -> dict:
        """Return an empty invoice state dictionary."""
        return {
            "imported_invoice_ids": [],
            "notes_filled_invoice_ids": []
        }
    
    def save(self) -> None:
        """Save current state to disk."""
        try:
            with open(self.state_path, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
            logger.debug(f"Saved invoice state to {self.state_path}")
        except Exception as e:
            logger.error(f"Failed to save invoice state file: {str(e)}", exc_info=True)
            raise
    
    def mark_invoice_imported(self, reservation_id: str) -> None:
        """Mark a reservation ID as having invoices generated."""
        reservation_id = str(reservation_id).strip()
        if not reservation_id:
            return
        
        imported_ids = set(self.state.get("imported_invoice_ids", []))
        if reservation_id not in imported_ids:
            imported_ids.add(reservation_id)
            self.state["imported_invoice_ids"] = sorted(list(imported_ids))
            self.save()
            logger.debug(f"Marked reservation {reservation_id} as invoice imported")
    
    def invoice_already_imported(self, reservation_id: str) -> bool:
        """Check if invoices for a reservation have already been generated."""
        reservation_id = str(reservation_id).strip()
        if not reservation_id:
            return False
        
        imported_ids = set(self.state.get("imported_invoice_ids", []))
        return reservation_id in imported_ids

